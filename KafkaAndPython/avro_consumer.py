"""
avro_consumer.py — Avro-deserialised Kafka consumer.

Fetches the schema automatically from the local Schema Registry container
(http://localhost:8081) when KAFKA_ENV=local, or Confluent Cloud SR when
KAFKA_ENV=cloud.

Windows-compatible: uses signal.signal() + threading.Event instead of
loop.add_signal_handler() which is Unix-only.
"""

import asyncio
import logging
import os
import signal
import threading

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from dotenv import load_dotenv

from kafka_config import consumer_config, schema_registry_config

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


# ── Business logic ────────────────────────────────────────────────────────────

async def _handle_telemetry(event: dict) -> None:
    logger.info(
        "TelemetryEvent  id=%-10s  device=%-12s  metric=%-12s  value=%7.2f  ts=%d",
        event["event_id"],
        event["device_id"],
        event["metric"],
        event["value"],
        event["timestamp"],
    )
    await asyncio.sleep(0)


# ── Consumer loop ─────────────────────────────────────────────────────────────

async def consume_avro_messages(
    topic: str,
    group_id: str = "avro-consumer-group",
) -> None:
    sr_client        = SchemaRegistryClient(schema_registry_config())
    avro_deserializer = AvroDeserializer(sr_client)   # schema resolved via magic bytes

    consumer = Consumer(consumer_config(group_id))
    consumer.subscribe([topic])
    logger.info(
        "Avro consumer subscribed — topic='%s'  group='%s'  SR=%s",
        topic, group_id, schema_registry_config()["url"],
    )

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

            event: dict = avro_deserializer(
                msg.value(),
                SerializationContext(msg.topic(), MessageField.VALUE),
            )
            await _handle_telemetry(event)
            consumer.commit(asynchronous=False)

    except KafkaException as exc:
        logger.exception("Kafka / Avro error: %s", exc)
    except KeyboardInterrupt:
        logger.warning("Interrupted.")
    finally:
        logger.info("Closing consumer …")
        consumer.close()
        logger.info("Done.")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    TOPIC = os.getenv("TOPIC_NAME", "telemetry-events")
    asyncio.run(consume_avro_messages(TOPIC))
