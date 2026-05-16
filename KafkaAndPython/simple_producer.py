"""
simple_producer.py — Plain-text Kafka producer.

Works against both the local Podman containers (KAFKA_ENV=local, default)
and Confluent Cloud (KAFKA_ENV=cloud).  All connection details come from .env
via kafka_config.py.
"""

import asyncio
import logging
import os
import time

from confluent_kafka import Producer, KafkaException
from dotenv import load_dotenv

from kafka_config import producer_config

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ── Delivery callback ─────────────────────────────────────────────────────────

def _delivery_report(err, msg) -> None:
    if err:
        logger.error("Delivery failed  key=%s: %s", msg.key(), err)
    else:
        logger.info(
            "✓ Delivered  topic=%-22s  partition=%d  offset=%d  key=%s",
            msg.topic(), msg.partition(), msg.offset(),
            msg.key().decode() if msg.key() else None,
        )


# ── Producer ──────────────────────────────────────────────────────────────────

async def produce_messages(topic: str, messages: list[str]) -> None:
    producer = Producer(producer_config())
    logger.info("Producer ready — topic: %s  messages: %d", topic, len(messages))

    try:
        for idx, payload in enumerate(messages):
            key = f"key-{idx:04d}"
            producer.produce(
                topic=topic,
                key=key.encode(),
                value=payload.encode(),
                on_delivery=_delivery_report,
            )
            producer.poll(0)
            await asyncio.sleep(0.1)

        logger.info("Flushing …")
        producer.flush()
        logger.info("All messages delivered.")

    except KafkaException as exc:
        logger.exception("Kafka error: %s", exc)
        raise
    except KeyboardInterrupt:
        logger.warning("Interrupted — flushing before exit …")
        producer.flush()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    TOPIC = os.getenv("TOPIC_NAME", "telemetry-events")
    ts = int(time.time())
    sample_messages = [
        f"Hello Kafka 8.2 — message #{i:02d} (ts={ts})"
        for i in range(10)
    ]
    asyncio.run(produce_messages(TOPIC, sample_messages))
