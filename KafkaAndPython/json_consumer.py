"""
json_consumer.py — JSON-deserialised Kafka consumer with Pydantic validation.

Consumes SensorReading messages from the local Podman Kafka container
(KAFKA_ENV=local) or Confluent Cloud (KAFKA_ENV=cloud).
"""

import asyncio
import logging
import os
import signal

from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError, field_validator

from kafka_config import consumer_config

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

_shutdown = asyncio.Event()


# ── Pydantic model ─────────────────────────────────────────────────────────────

class SensorReading(BaseModel):
    reading_id: str
    sensor_id: str
    location: str
    temperature: float
    humidity: float
    timestamp: int

    @field_validator("temperature")
    @classmethod
    def temperature_range(cls, v: float) -> float:
        if not -50 <= v <= 100:
            raise ValueError(f"temperature {v} out of range")
        return v

    @field_validator("humidity")
    @classmethod
    def humidity_range(cls, v: float) -> float:
        if not 0 <= v <= 100:
            raise ValueError(f"humidity {v} out of range")
        return v


# ── Signal handling ───────────────────────────────────────────────────────────

def _install_signal_handlers(loop: asyncio.AbstractEventLoop) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown.set)


# ── Business logic ────────────────────────────────────────────────────────────

async def _process_reading(reading: SensorReading) -> None:
    logger.info(
        "SensorReading  id=%.8s…  sensor=%-10s  loc=%-8s  temp=%5.1f°C  hum=%5.1f%%",
        reading.reading_id,
        reading.sensor_id,
        reading.location,
        reading.temperature,
        reading.humidity,
    )
    if reading.temperature > 35.0:
        logger.warning("🌡  HIGH TEMP ALERT  sensor=%s  temp=%.1f°C",
                       reading.sensor_id, reading.temperature)
    await asyncio.sleep(0)


# ── Consumer loop ─────────────────────────────────────────────────────────────

async def consume_json_messages(
    topic: str,
    group_id: str = "json-consumer-group",
) -> None:
    consumer = Consumer(consumer_config(group_id))
    consumer.subscribe([topic])
    logger.info("JSON consumer subscribed — topic='%s'  group='%s'", topic, group_id)

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
                reading = SensorReading.model_validate_json(msg.value().decode())
            except ValidationError as exc:
                logger.error("Validation failed offset=%d: %s", msg.offset(), exc)
                consumer.commit(asynchronous=False)   # skip poison-pill
                continue

            await _process_reading(reading)
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
    TOPIC = os.getenv("SENSOR_TOPIC", "sensor-readings")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _install_signal_handlers(loop)

    try:
        loop.run_until_complete(consume_json_messages(TOPIC))
    finally:
        loop.close()
