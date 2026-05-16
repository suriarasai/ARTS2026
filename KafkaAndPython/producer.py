"""
producer.py — Publishes sample app events to the 'appevents' Kafka topic.

Usage:
    python producer.py                  # sends 10 events then exits
    python producer.py --count 50       # sends 50 events
    python producer.py --continuous     # streams events until Ctrl+C
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ── Configuration ────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "appevents"
NUM_PARTITIONS = 3
REPLICATION_FACTOR = 1

# Sample data for realistic-looking events
EVENT_TYPES = ["page_view", "button_click", "form_submit", "login", "logout", "purchase"]
PAGES = ["/home", "/products", "/cart", "/checkout", "/profile", "/search"]
USER_IDS = [f"user_{i:04d}" for i in range(1, 21)]


# ── Helpers ───────────────────────────────────────────────────────────────────
def ensure_topic_exists(bootstrap_servers: str, topic: str) -> None:
    """Create the topic if it doesn't already exist."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = admin.list_topics(timeout=10).topics
    if topic not in existing:
        new_topic = NewTopic(topic, num_partitions=NUM_PARTITIONS, replication_factor=REPLICATION_FACTOR)
        futures = admin.create_topics([new_topic])
        for t, future in futures.items():
            try:
                future.result()
                print(f"[admin] Topic '{t}' created ({NUM_PARTITIONS} partitions).")
            except Exception as exc:
                # Topic may already exist in a race — that's fine.
                print(f"[admin] Could not create topic '{t}': {exc}")
    else:
        print(f"[admin] Topic '{topic}' already exists.")


def build_event() -> dict:
    """Return a randomly generated application event payload."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(EVENT_TYPES),
        "user_id": random.choice(USER_IDS),
        "page": random.choice(PAGES),
        "session_duration_s": round(random.uniform(1.0, 300.0), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def delivery_report(err, msg) -> None:
    """Called once per message when delivery succeeds or fails."""
    if err:
        print(f"[error] Delivery failed for message: {err}")
    else:
        value = json.loads(msg.value().decode())
        print(
            f"[sent]  event_id={value['event_id'][:8]}…  "
            f"type={value['event_type']:<12}  "
            f"partition={msg.partition()}  offset={msg.offset()}"
        )


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka app-event producer")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--count", type=int, default=10, help="Number of events to send (default: 10)")
    group.add_argument("--continuous", action="store_true", help="Keep sending events until Ctrl+C")
    parser.add_argument("--delay", type=float, default=0.5, help="Seconds between messages (default: 0.5)")
    args = parser.parse_args()

    ensure_topic_exists(BOOTSTRAP_SERVERS, TOPIC)

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "acks": "all",                  # strongest durability guarantee
            "retries": 3,
            "linger.ms": 10,               # small batching window
            "compression.type": "snappy",
        }
    )

    print(f"\nProducing to topic '{TOPIC}' on {BOOTSTRAP_SERVERS}")
    print("Press Ctrl+C to stop.\n")

    sent = 0
    try:
        while args.continuous or sent < args.count:
            event = build_event()
            producer.produce(
                topic=TOPIC,
                key=event["user_id"],            # partition by user for ordering
                value=json.dumps(event).encode(),
                on_delivery=delivery_report,
            )
            producer.poll(0)                     # serve delivery callbacks
            sent += 1
            time.sleep(args.delay)
    except KeyboardInterrupt:
        print("\n[info] Interrupted by user.")
    finally:
        remaining = producer.flush(timeout=30)
        if remaining:
            print(f"[warn] {remaining} message(s) were not delivered.")
        print(f"\n[done] Sent {sent} event(s).")


if __name__ == "__main__":
    main()
