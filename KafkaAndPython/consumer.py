"""
consumer.py — Reads app events from the 'appevents' Kafka topic.

Usage:
    python consumer.py                      # reads from latest offset
    python consumer.py --from-beginning     # replays all historical events
    python consumer.py --group my-group     # use a custom consumer group
"""

import argparse
import json
import signal
import sys
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException

# ── Configuration ────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "appevents"
DEFAULT_GROUP_ID = "appevents-consumer-group"

# ── Signal handling (cross-platform: works on Windows AND Unix) ───────────────
_running = True


def _handle_signal(sig, frame):
    global _running
    print("\n[info] Shutdown signal received — draining and closing…")
    _running = False


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ── Formatting ────────────────────────────────────────────────────────────────
def format_event(msg) -> str:
    """Pretty-print a consumed Kafka message."""
    try:
        payload: dict = json.loads(msg.value().decode())
    except (json.JSONDecodeError, UnicodeDecodeError):
        payload = {"raw": msg.value()}

    event_id   = payload.get("event_id", "?")[:8]
    event_type = payload.get("event_type", "unknown")
    user_id    = payload.get("user_id", "?")
    page       = payload.get("page", "?")
    ts         = payload.get("timestamp", "")

    received_at = datetime.now(timezone.utc).strftime("%H:%M:%S")

    return (
        f"[{received_at}]  "
        f"partition={msg.partition()} offset={msg.offset():>6}  │  "
        f"event_id={event_id}…  type={event_type:<12}  "
        f"user={user_id}  page={page}  ts={ts}"
    )


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka app-event consumer")
    parser.add_argument(
        "--from-beginning", action="store_true",
        help="Read all messages from offset 0"
    )
    parser.add_argument(
        "--group", default=DEFAULT_GROUP_ID,
        help=f"Consumer group ID (default: {DEFAULT_GROUP_ID})"
    )
    parser.add_argument(
        "--timeout", type=float, default=1.0,
        help="Poll timeout in seconds (default: 1.0)"
    )
    args = parser.parse_args()

    auto_offset_reset = "earliest" if args.from_beginning else "latest"

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": args.group,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
            "session.timeout.ms": 30000,
            "max.poll.interval.ms": 300000,
        }
    )

    consumer.subscribe(
        [TOPIC],
        on_assign=lambda c, parts: print(
            f"[assigned] Partitions: {[p.partition for p in parts]}"
        ),
        on_revoke=lambda c, parts: print(
            f"[revoked]  Partitions: {[p.partition for p in parts]}"
        ),
    )

    print(f"\nConsuming from topic '{TOPIC}' on {BOOTSTRAP_SERVERS}")
    print(f"  Group ID       : {args.group}")
    print(f"  Offset reset   : {auto_offset_reset}")
    print("Press Ctrl+C to stop.\n")

    received = 0
    try:
        while _running:
            msg = consumer.poll(timeout=args.timeout)

            if msg is None:
                continue  # no message within timeout window

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(
                        f"[info] End of partition {msg.partition()} "
                        f"at offset {msg.offset()}"
                    )
                else:
                    raise KafkaException(msg.error())
            else:
                print(format_event(msg))
                received += 1

    except KafkaException as exc:
        print(f"[fatal] Kafka error: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        consumer.close()
        print(f"\n[done] Received {received} event(s).")


if __name__ == "__main__":
    main()
