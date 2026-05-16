"""
kafka_config.py — Shared configuration factory.

Reads KAFKA_ENV from .env (via python-dotenv):
  "local"  →  PLAINTEXT to the local Podman containers
  "cloud"  →  SASL_SSL  to Confluent Cloud

Import and call the helpers in every producer/consumer script.
"""

import logging
import os

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


# ── Environment selector ───────────────────────────────────────────────────────

def _env() -> str:
    value = os.getenv("KAFKA_ENV", "local").lower()
    if value not in ("local", "cloud"):
        raise ValueError(f"KAFKA_ENV must be 'local' or 'cloud', got: {value!r}")
    return value


# ── Bootstrap / Schema Registry helpers ───────────────────────────────────────

def bootstrap_servers() -> str:
    if _env() == "local":
        return os.getenv("LOCAL_BOOTSTRAP_SERVERS", "localhost:9092")
    return os.environ["CLOUD_BOOTSTRAP_SERVERS"]


def schema_registry_url() -> str:
    if _env() == "local":
        return os.getenv("LOCAL_SCHEMA_REGISTRY_URL", "http://localhost:8081")
    return os.environ["CLOUD_SCHEMA_REGISTRY_URL"]


# ── Producer config ───────────────────────────────────────────────────────────

def producer_config(extra: dict | None = None) -> dict:
    base: dict = {
        "bootstrap.servers": bootstrap_servers(),
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 300,
        "enable.idempotence": True,
        "compression.type": "lz4",
        # ── Windows fix: force IPv4 so localhost resolves to 127.0.0.1 not ::1
        "broker.address.family": "v4",
    }
    if _env() == "cloud":
        base.update({
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": os.environ["CLOUD_SASL_USERNAME"],
            "sasl.password": os.environ["CLOUD_SASL_PASSWORD"],
        })
    if extra:
        base.update(extra)
    logger.debug("Producer config (env=%s): %s", _env(),
                 {k: v for k, v in base.items() if "password" not in k})
    return base


# ── Consumer config ───────────────────────────────────────────────────────────

def consumer_config(group_id: str, extra: dict | None = None) -> dict:
    base: dict = {
        "bootstrap.servers": bootstrap_servers(),
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "session.timeout.ms": 30_000,
        "max.poll.interval.ms": 300_000,
        # ── Windows fix: force IPv4 so localhost resolves to 127.0.0.1 not ::1
        "broker.address.family": "v4",
    }
    if _env() == "cloud":
        base.update({
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": os.environ["CLOUD_SASL_USERNAME"],
            "sasl.password": os.environ["CLOUD_SASL_PASSWORD"],
        })
    if extra:
        base.update(extra)
    logger.debug("Consumer config (env=%s, group=%s)", _env(), group_id)
    return base


# ── Schema Registry client config ─────────────────────────────────────────────

def schema_registry_config() -> dict:
    cfg: dict = {"url": schema_registry_url()}
    if _env() == "cloud":
        cfg["basic.auth.user.info"] = (
            f"{os.environ['CLOUD_SCHEMA_REGISTRY_API_KEY']}:"
            f"{os.environ['CLOUD_SCHEMA_REGISTRY_API_SECRET']}"
        )
    return cfg
