"""
avro_consumer.py — Avro-deserialised Kafka consumer.

Fetches the schema automatically from the local Schema Registry container
(http://localhost:8081) when KAFKA_ENV=local, or from Confluent Cloud SR
when KAFKA_ENV=cloud.
"""

import asyncio
import logging
import os
import signal

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

_shutdown = asyncio.Event()


def _install_signal_handlers(loop: asyncio.AbstractEventLoop) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown.set)


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
    sr_client = SchemaRegistryClient(schema_registry_config())
    # schema_str=None → schema is resolved via magic bytes from the registry
    avro_deserializer = AvroDeserializer(sr_client)

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

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _install_signal_handlers(loop)

    try:
        loop.run_until_complete(consume_avro_messages(TOPIC))
    finally:
        loop.close()
