"""
tweet_processor.py — Full tweet ingestion → enrichment → forwarding pipeline.

Works against the local Podman Kafka containers (KAFKA_ENV=local, default)
or Confluent Cloud (KAFKA_ENV=cloud).

Run modes (--mode flag):
  produce   → publish sample raw tweets to RAW_TWEET_TOPIC
  consume   → consume, enrich, and forward to ENRICHED_TWEET_TOPIC
  pipeline  → both concurrently (default)

Windows-compatible: uses signal.signal() + threading.Event instead of
loop.add_signal_handler() which is Unix-only.
"""

import argparse
import asyncio
import logging
import os
import re
import signal
import threading
import time
import uuid

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator

from kafka_config import producer_config, consumer_config

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── Signal handling (Windows + Unix) ─────────────────────────────────────────
_shutdown = threading.Event()


def _handle_signal(sig, frame) -> None:
    logger.warning("Signal %s received — shutting down …", sig)
    _shutdown.set()


signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

RAW_TOPIC      = os.getenv("RAW_TWEET_TOPIC",      "tweet-ingestion")
ENRICHED_TOPIC = os.getenv("ENRICHED_TWEET_TOPIC", "tweet-processed")


# ── Pydantic models ───────────────────────────────────────────────────────────

class RawTweet(BaseModel):
    tweet_id:      str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_handle:   str
    text:          str
    language:      str = "en"
    created_at:    int = Field(default_factory=lambda: int(time.time() * 1000))
    retweet_count: int = 0
    like_count:    int = 0

    @field_validator("user_handle")
    @classmethod
    def strip_at(cls, v: str) -> str:
        return v.lstrip("@")


class EnrichedTweet(BaseModel):
    tweet_id:        str
    user_handle:     str
    text:            str
    language:        str
    created_at:      int
    retweet_count:   int
    like_count:      int
    hashtags:        list[str]
    mentions:        list[str]
    url_count:       int
    sentiment_label: str    # "positive" | "neutral" | "negative"
    word_count:      int
    processed_at:    int = Field(default_factory=lambda: int(time.time() * 1000))


# ── Enrichment ────────────────────────────────────────────────────────────────

_POSITIVE   = {"great", "awesome", "love", "excellent", "fantastic", "happy", "good", "best", "amazing"}
_NEGATIVE   = {"bad", "terrible", "hate", "awful", "horrible", "sad", "worst", "poor", "broken"}
_URL_RE     = re.compile(r"https?://\S+")
_HASHTAG_RE = re.compile(r"#(\w+)")
_MENTION_RE = re.compile(r"@(\w+)")


def _sentiment(text: str) -> str:
    tokens = set(text.lower().split())
    pos = len(tokens & _POSITIVE)
    neg = len(tokens & _NEGATIVE)
    return "positive" if pos > neg else ("negative" if neg > pos else "neutral")


def enrich(raw: RawTweet) -> EnrichedTweet:
    return EnrichedTweet(
        **raw.model_dump(),
        hashtags=_HASHTAG_RE.findall(raw.text),
        mentions=_MENTION_RE.findall(raw.text),
        url_count=len(_URL_RE.findall(raw.text)),
        sentiment_label=_sentiment(raw.text),
        word_count=len(raw.text.split()),
    )


# ── Sample data ───────────────────────────────────────────────────────────────

SAMPLE_TWEETS: list[RawTweet] = [
    RawTweet(user_handle="@alice",    text="Just tried Kafka 8.2 + #Confluent — amazing KRaft performance! 🚀 https://confluent.io",  retweet_count=12, like_count=47),
    RawTweet(user_handle="@bob_dev",  text="Running @Podman containers is great for local Kafka dev. @alice agree?",                  retweet_count=3,  like_count=21),
    RawTweet(user_handle="@charlie",  text="Terrible flaky tests today. Nothing is working. #devlife",                                retweet_count=5,  like_count=8),
    RawTweet(user_handle="@diana_ml", text="Attending #KafkaSummit next week! @bob_dev @alice will you be there?",                    retweet_count=2,  like_count=15),
    RawTweet(user_handle="@eve",      text="The new #Pydantic v2 is the best — love the performance gains over v1!",                  retweet_count=8,  like_count=60),
    RawTweet(user_handle="@frank",    text="Worst deployment ever. Bad config, broken pipelines. Everything is sad.",                 retweet_count=0,  like_count=3),
    RawTweet(user_handle="@grace_ai", text="Fascinating paper on LLMs dropping today! Check it out https://arxiv.org #AI #ML",        retweet_count=34, like_count=120),
    RawTweet(user_handle="@henry",    text="#Podman 5.x + cp-kafka:8.2.0 — smooth setup with no ZooKeeper needed. Excellent work!",   retweet_count=7,  like_count=33),
]


# ── Delivery callback ─────────────────────────────────────────────────────────

def _delivery_report(err, msg) -> None:
    if err:
        logger.error("Delivery failed: %s", err)
    else:
        logger.debug("✓ Delivered  topic=%-22s  offset=%d", msg.topic(), msg.offset())


# ── Producer coroutine ────────────────────────────────────────────────────────

async def run_producer() -> None:
    producer = Producer(producer_config())
    logger.info("Tweet producer started — topic: %s", RAW_TOPIC)

    try:
        for tweet in SAMPLE_TWEETS:
            payload = tweet.model_dump_json().encode()
            producer.produce(
                topic=RAW_TOPIC,
                key=tweet.tweet_id.encode(),
                value=payload,
                headers={"content-type": "application/json", "source": "demo"},
                on_delivery=_delivery_report,
            )
            producer.poll(0)
            logger.info("Queued tweet  @%-12s  len=%d", tweet.user_handle, len(tweet.text))
            await asyncio.sleep(0.25)

        logger.info("Flushing tweet producer …")
        producer.flush()
        logger.info("All tweets published.")

    except KafkaException as exc:
        logger.exception("Producer error: %s", exc)
    except KeyboardInterrupt:
        producer.flush()


# ── Consumer / transformer coroutine ─────────────────────────────────────────

async def run_consumer() -> None:
    consumer         = Consumer(consumer_config("tweet-processor-group"))
    forward_producer = Producer(producer_config())

    consumer.subscribe([RAW_TOPIC])
    logger.info(
        "Tweet processor started — consuming: '%s'  forwarding: '%s'",
        RAW_TOPIC, ENRICHED_TOPIC,
    )

    stats = {"processed": 0, "errors": 0, "positive": 0, "negative": 0, "neutral": 0}

    try:
        while not _shutdown.is_set():
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                await asyncio.sleep(0)
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("EOF partition=%d offset=%d", msg.partition(), msg.offset())
                else:
                    raise KafkaException(msg.error())
                continue

            try:
                raw = RawTweet.model_validate_json(msg.value().decode())
            except Exception as exc:
                logger.error("Parse error offset=%d: %s", msg.offset(), exc)
                stats["errors"] += 1
                consumer.commit(asynchronous=False)
                continue

            enriched = enrich(raw)
            stats["processed"] += 1
            stats[enriched.sentiment_label] += 1

            logger.info(
                "Enriched  @%-12s  sentiment=%-8s  hashtags=%-20s  words=%d",
                enriched.user_handle,
                enriched.sentiment_label,
                str(enriched.hashtags) if enriched.hashtags else "—",
                enriched.word_count,
            )

            forward_producer.produce(
                topic=ENRICHED_TOPIC,
                key=enriched.tweet_id.encode(),
                value=enriched.model_dump_json().encode(),
                on_delivery=_delivery_report,
            )
            forward_producer.poll(0)
            consumer.commit(asynchronous=False)

    except KafkaException as exc:
        logger.exception("Consumer error: %s", exc)
    except KeyboardInterrupt:
        logger.warning("Interrupted.")
    finally:
        logger.info(
            "Stats — processed=%d  errors=%d  positive=%d  neutral=%d  negative=%d",
            stats["processed"], stats["errors"],
            stats["positive"], stats["neutral"], stats["negative"],
        )
        forward_producer.flush()
        consumer.close()
        logger.info("Tweet processor closed.")


# ── Pipeline ──────────────────────────────────────────────────────────────────

async def run_pipeline() -> None:
    await asyncio.gather(run_producer(), run_consumer())


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tweet Processing Pipeline")
    parser.add_argument(
        "--mode",
        choices=["produce", "consume", "pipeline"],
        default="pipeline",
        help="Run mode (default: pipeline)",
    )
    args = parser.parse_args()

    modes = {
        "produce":  run_producer,
        "consume":  run_consumer,
        "pipeline": run_pipeline,
    }

    asyncio.run(modes[args.mode]())
