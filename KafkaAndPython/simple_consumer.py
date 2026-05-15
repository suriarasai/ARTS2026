"""
simple_consumer.py — Plain-text Kafka consumer.

Works against both the local Podman containers (KAFKA_ENV=local, default)
and Confluent Cloud (KAFKA_ENV=cloud).
"""

import asyncio
import logging
import os
import signal

from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv

from kafka_config import consumer_config

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

_shutdown = asyncio.Event()


def _install_signal_handlers(loop: asyncio.AbstractEventLoop) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown.set)


# ── Message handler ───────────────────────────────────────────────────────────

async def _process_message(key: str | None, value: str) -> None:
    """Replace with real downstream logic (DB write, HTTP call, etc.)."""
    logger.info("  → key=%-12s  value=%s", key, value)
    await asyncio.sleep(0)


# ── Consumer loop ─────────────────────────────────────────────────────────────

async def consume_messages(
    topic: str,
    group_id: str = "simple-consumer-group",
) -> None:
    consumer = Consumer(consumer_config(group_id))
    consumer.subscribe([topic])
    logger.info("Subscribed — topic='%s'  group='%s'", topic, group_id)

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

            key = msg.key().decode() if msg.key() else None
            value = msg.value().decode()

            logger.info(
                "Received  topic=%-22s  partition=%d  offset=%d",
                msg.topic(), msg.partition(), msg.offset(),
            )
            await _process_message(key, value)
            consumer.commit(asynchronous=False)

    except KafkaException as exc:
        logger.exception("Kafka error: %s", exc)
    except KeyboardInterrupt:
        logger.warning("Interrupted.")
    finally:
        logger.info("Closing consumer …")
        consumer.close()
        logger.info("Done.")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    TOPIC = os.getenv("TOPIC_NAME", "telemetry-events")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _install_signal_handlers(loop)

    try:
        loop.run_until_complete(consume_messages(TOPIC))
    finally:
        loop.close()
