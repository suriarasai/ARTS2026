"""
avro_producer.py — Avro-serialised Kafka producer.

Registers the TelemetryEvent schema with the local Schema Registry container
(http://localhost:8081) when KAFKA_ENV=local, or with Confluent Cloud SR when
KAFKA_ENV=cloud.
"""

import asyncio
import logging
import os
import time

from confluent_kafka import Producer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from dotenv import load_dotenv

from kafka_config import producer_config, schema_registry_config

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── Avro schema ────────────────────────────────────────────────────────────────

TELEMETRY_SCHEMA_STR = """
{
  "type": "record",
  "name": "TelemetryEvent",
  "namespace": "com.example.telemetry",
  "fields": [
    {"name": "event_id",   "type": "string"},
    {"name": "device_id",  "type": "string"},
    {"name": "metric",     "type": "string"},
    {"name": "value",      "type": "double"},
    {"name": "timestamp",  "type": "long", "logicalType": "timestamp-millis"}
  ]
}
"""


def _to_dict(event: dict, _ctx) -> dict:
    """Identity transform — the dict is already Avro-compatible."""
    return event


def _delivery_report(err, msg) -> None:
    if err:
        logger.error("Delivery failed: %s", err)
    else:
        logger.info(
            "✓ Avro delivered  topic=%-22s  partition=%d  offset=%d",
            msg.topic(), msg.partition(), msg.offset(),
        )


# ── Producer ──────────────────────────────────────────────────────────────────

async def produce_avro_messages(topic: str) -> None:
    sr_client = SchemaRegistryClient(schema_registry_config())
    avro_serializer = AvroSerializer(sr_client, TELEMETRY_SCHEMA_STR, _to_dict)

    producer = Producer(producer_config())
    logger.info("Avro producer ready — topic: %s  SR: %s", topic, schema_registry_config()["url"])

    sample_events = [
        {
            "event_id": f"evt-{i:04d}",
            "device_id": f"device-{i % 5:03d}",
            "metric": "cpu_usage",
            "value": round(20.0 + i * 1.5, 2),
            "timestamp": int(time.time() * 1000),
        }
        for i in range(8)
    ]

    try:
        for event in sample_events:
            serialised = avro_serializer(
                event,
                SerializationContext(topic, MessageField.VALUE),
            )
            producer.produce(
                topic=topic,
                key=event["device_id"].encode(),
                value=serialised,
                on_delivery=_delivery_report,
            )
            producer.poll(0)
            logger.info("Queued event_id=%s  device=%s", event["event_id"], event["device_id"])
            await asyncio.sleep(0.1)

        logger.info("Flushing …")
        producer.flush()

    except KafkaException as exc:
        logger.exception("Kafka / Avro error: %s", exc)
        raise
    except KeyboardInterrupt:
        logger.warning("Interrupted — flushing …")
        producer.flush()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    TOPIC = os.getenv("TOPIC_NAME", "telemetry-events")
    asyncio.run(produce_avro_messages(TOPIC))
