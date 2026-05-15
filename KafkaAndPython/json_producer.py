"""
json_producer.py — JSON-serialised Kafka producer with Pydantic validation.

Produces SensorReading messages to the local Podman Kafka container
(KAFKA_ENV=local) or Confluent Cloud (KAFKA_ENV=cloud).
"""

import asyncio
import logging
import os
import time
import uuid

from confluent_kafka import Producer, KafkaException
from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator

from kafka_config import producer_config

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ── Pydantic model ─────────────────────────────────────────────────────────────

class SensorReading(BaseModel):
    reading_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    sensor_id: str
    location: str
    temperature: float
    humidity: float
    timestamp: int = Field(default_factory=lambda: int(time.time() * 1000))

    @field_validator("temperature")
    @classmethod
    def temperature_range(cls, v: float) -> float:
        if not -50 <= v <= 100:
            raise ValueError(f"temperature {v} out of range [-50, 100]")
        return v

    @field_validator("humidity")
    @classmethod
    def humidity_range(cls, v: float) -> float:
        if not 0 <= v <= 100:
            raise ValueError(f"humidity {v} must be 0–100")
        return v


# ── Delivery callback ─────────────────────────────────────────────────────────

def _delivery_report(err, msg) -> None:
    if err:
        logger.error("Delivery failed  key=%s: %s", msg.key(), err)
    else:
        logger.info(
            "✓ JSON delivered  topic=%-22s  partition=%d  offset=%d",
            msg.topic(), msg.partition(), msg.offset(),
        )


# ── Producer ──────────────────────────────────────────────────────────────────

async def produce_json_messages(topic: str, readings: list[SensorReading]) -> None:
    producer = Producer(producer_config())
    logger.info("JSON producer ready — topic: %s  count: %d", topic, len(readings))

    try:
        for reading in readings:
            payload = reading.model_dump_json().encode()
            producer.produce(
                topic=topic,
                key=reading.sensor_id.encode(),
                value=payload,
                headers={"content-type": "application/json"},
                on_delivery=_delivery_report,
            )
            producer.poll(0)
            logger.info("Queued sensor_id=%s  temp=%.1f°C  hum=%.1f%%",
                        reading.sensor_id, reading.temperature, reading.humidity)
            await asyncio.sleep(0.05)

        logger.info("Flushing …")
        producer.flush()

    except KafkaException as exc:
        logger.exception("Kafka error: %s", exc)
        raise
    except KeyboardInterrupt:
        logger.warning("Interrupted — flushing …")
        producer.flush()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    TOPIC = os.getenv("SENSOR_TOPIC", "sensor-readings")

    sample_readings = [
        SensorReading(
            sensor_id=f"sensor-{i:03d}",
            location=f"zone-{chr(65 + i % 4)}",
            temperature=round(18.0 + i * 1.8, 1),
            humidity=round(40.0 + i * 1.2, 1),
        )
        for i in range(10)
    ]

    asyncio.run(produce_json_messages(TOPIC, sample_readings))
