# Kafka + Python — Local Development Stack

A complete local Apache Kafka development environment running on **Podman Desktop**
(Windows), with eight Python scripts covering plain-text, JSON, Avro, and
streaming-pipeline patterns.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Podman Containers                            │
│                                                                     │
│   ┌──────────────────────────────┐   ┌──────────────────────────┐   │
│   │   Kafka Broker (KRaft)       │   │   Schema Registry        │   │
│   │   confluentinc/cp-kafka:8.2  │   │   cp-schema-registry:8.2 │   │
│   │   localhost:9092             │   │   localhost:8081         │   │
│   └──────────────────────────────┘   └──────────────────────────┘   │
│                                                                     │
│   ┌──────────────────────────────┐                                  │
│   │   Kafka UI                   │                                  │
│   │   provectuslabs/kafka-ui     │                                  │
│   │   localhost:8080             │                                  │
│   └──────────────────────────────┘                                  │
└─────────────────────────────────────────────────────────────────────┘

 Python scripts (run on host)
 ├── kafka_config.py          shared config factory
 ├── simple_producer.py       plain-text producer
 ├── simple_consumer.py       plain-text consumer
 ├── json_producer.py         JSON + Pydantic producer
 ├── json_consumer.py         JSON + Pydantic consumer
 ├── avro_producer.py         Avro + Schema Registry producer
 ├── avro_consumer.py         Avro + Schema Registry consumer
 └── tweet_processor.py       end-to-end streaming pipeline
```

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Project Layout](#2-project-layout)
3. [Environment Configuration (.env)](#3-environment-configuration-env)
4. [Infrastructure — Kafka Stack on Podman](#4-infrastructure--kafka-stack-on-podman)
5. [Python Setup](#5-python-setup)
6. [Code Walkthrough](#6-code-walkthrough)
   - [kafka_config.py](#61-kafka_configpy--shared-configuration-factory)
   - [simple_producer.py](#62-simple_producerpy--plain-text-producer)
   - [simple_consumer.py](#63-simple_consumerpy--plain-text-consumer)
   - [json_producer.py](#64-json_producerpy--json-producer-with-pydantic)
   - [json_consumer.py](#65-json_consumerpy--json-consumer-with-pydantic)
   - [avro_producer.py](#66-avro_producerpy--avro-producer-with-schema-registry)
   - [avro_consumer.py](#67-avro_consumerpy--avro-consumer-with-schema-registry)
   - [tweet_processor.py](#68-tweet_processorpy--end-to-end-streaming-pipeline)
7. [Step-by-Step Execution Guide](#7-step-by-step-execution-guide)
8. [Viewing Messages in Kafka UI](#8-viewing-messages-in-kafka-ui)
9. [Topic Reference](#9-topic-reference)
10. [Troubleshooting](#10-troubleshooting)
11. [Architecture Notes](#11-architecture-notes)

---

## 1. Prerequisites

| Tool | Minimum Version | Purpose | Install |
|------|----------------|---------|---------|
| Podman Desktop | 1.9 | Container runtime for Windows | https://podman-desktop.io |
| `compose` | 2.x | Compose provider used by Podman | bundled with Docker Desktop or `winget install Docker.DockerCompose` |
| Python | 3.12 – 3.14 | Script runtime | https://python.org/downloads |
| pip | 24+ | Package installer | bundled with Python |

> **Note:** Podman on Windows delegates to `docker-compose.exe` as its compose
> provider. This is expected and shown in the startup output — it works correctly.

---

## 2. Project Layout

```
KafkaAndPython/
├── compose.yml             Podman/Docker Compose stack definition
├── .env                    Local environment variables (never commit secrets)
├── kafka_config.py         Shared Kafka + Schema Registry config factory
├── simple_producer.py      Plain-text producer → telemetry-events
├── simple_consumer.py      Plain-text consumer ← telemetry-events
├── json_producer.py        JSON (Pydantic) producer → sensor-readings
├── json_consumer.py        JSON (Pydantic) consumer ← sensor-readings
├── avro_producer.py        Avro producer → telemetry-events (Confluent wire format)
├── avro_consumer.py        Avro consumer ← telemetry-events
├── tweet_processor.py      Pipeline: tweet-ingestion → enrich → tweet-processed
└── requirements.txt        Python dependencies
```

---

## 3. Environment Configuration (.env)

All scripts read their connection details from a `.env` file in the project root.
Copy the template below and save it as `.env`:

```dotenv
# ── Environment selector ──────────────────────────────────────────────────────
# "local" → Podman containers on localhost
# "cloud" → Confluent Cloud (fill in CLOUD_* vars below)
KAFKA_ENV=local

# ── Local Podman settings ─────────────────────────────────────────────────────
LOCAL_BOOTSTRAP_SERVERS=localhost:9092
LOCAL_SCHEMA_REGISTRY_URL=http://localhost:8081

# ── Topic names ───────────────────────────────────────────────────────────────
TOPIC_NAME=telemetry-events
SENSOR_TOPIC=sensor-readings
RAW_TWEET_TOPIC=tweet-ingestion
ENRICHED_TWEET_TOPIC=tweet-processed

# ── Confluent Cloud (only needed when KAFKA_ENV=cloud) ────────────────────────
#CLOUD_BOOTSTRAP_SERVERS=pkc-xxxxx.region.provider.confluent.cloud:9092
#CLOUD_SASL_USERNAME=<API_KEY>
#CLOUD_SASL_PASSWORD=<API_SECRET>
#CLOUD_SCHEMA_REGISTRY_URL=https://psrc-xxxxx.region.provider.confluent.cloud
#CLOUD_SCHEMA_REGISTRY_API_KEY=<SR_KEY>
#CLOUD_SCHEMA_REGISTRY_API_SECRET=<SR_SECRET>
```

> **Important:** Add `.env` to your `.gitignore`. It will contain secrets when
> switching to Confluent Cloud.

---

## 4. Infrastructure — Kafka Stack on Podman

### What the compose stack runs

| Container | Image | Port | Purpose |
|-----------|-------|------|---------|
| `broker` | `confluentinc/cp-kafka:8.2.0` | 9092 | Kafka broker + KRaft controller (no ZooKeeper) |
| `schema-registry` | `confluentinc/cp-schema-registry:8.2.0` | 8081 | Avro schema storage and validation |
| `kafka-ui` | `provectuslabs/kafka-ui:latest` | 8080 | Browser-based Kafka management UI |

### KRaft mode explained

Confluent Platform 8.x uses **KRaft** (Kafka Raft) instead of ZooKeeper.
The broker container runs in *combined mode* — it acts as both the Kafka broker
and the KRaft controller in a single process. This removes the need for a
separate ZooKeeper container, making the stack simpler and faster to start.

### Starting the stack

**Option A — Podman Desktop UI**

1. Open Podman Desktop and confirm a Podman machine is running (green dot, bottom-left).
2. Navigate to **Compose → Open Compose file** and select `compose.yml`.
3. Click **Start**. Wait until all three containers show a green **Running** badge.

**Option B — Terminal**

```powershell
# Start all three containers in the background
podman compose up -d

# Stream logs to watch startup progress (Ctrl+C to stop tailing)
podman compose logs -f

# Confirm all containers are healthy
podman compose ps
```

Expected healthy output:

```
NAME               STATUS
broker             running (healthy)
schema-registry    running (healthy)
kafka-ui           running (healthy)
```

> First startup downloads ~1 GB of images. Subsequent starts take 15–30 seconds.

### Stopping the stack

```powershell
# Stop containers, keep data volumes (topics and messages are preserved)
podman compose stop

# Stop AND destroy everything including volumes (clean slate)
podman compose down -v
```

### Fixing DNS on first run (Windows only)

If containers fail to pull images with a name resolution error:

```powershell
# SSH into the Podman VM and set a reliable DNS server
podman machine ssh
echo "nameserver 8.8.8.8" | sudo tee /etc/resolv.conf
exit

# Then retry
podman compose up -d
```

---

## 5. Python Setup

### Create a virtual environment

```powershell
# Python 3.12 recommended (pre-built wheels, no compiler needed)
py -3.12 -m venv .venv

# Activate
.venv\Scripts\activate
```

### Install dependencies

```powershell
pip install -r requirements.txt
```

`requirements.txt` installs:

| Package | Version | Purpose |
|---------|---------|---------|
| `confluent-kafka[schemaregistry,avro]` | 2.14.0 | Kafka client + Avro serialisation (pre-built wheel, no C compiler) |
| `pydantic` | 2.11.4 | JSON schema validation and serialisation |
| `python-dotenv` | 1.1.0 | `.env` file loading |

### Verify installation

```powershell
python -c "import confluent_kafka; print(confluent_kafka.__version__)"
# Expected: 2.14.0
```

---

## 6. Code Walkthrough

### 6.1 `kafka_config.py` — Shared Configuration Factory

This module is imported by every other script. It reads `KAFKA_ENV` from `.env`
and returns the correct connection dictionaries for local or cloud environments.

**Key functions:**

```python
producer_config()            # → dict for confluent_kafka.Producer(...)
consumer_config(group_id)    # → dict for confluent_kafka.Consumer(...)
schema_registry_config()     # → dict for SchemaRegistryClient(...)
```

**Windows-specific fix included:**

```python
"broker.address.family": "v4"
```

On Windows, `localhost` resolves to IPv6 `::1` before IPv4 `127.0.0.1`.
The Podman VM's port-forward only binds to IPv4, causing connection failures.
This single setting forces librdkafka to use IPv4 exclusively, affecting every
script automatically since they all import from `kafka_config`.

**Local producer settings:**

```python
{
    "bootstrap.servers": "localhost:9092",
    "acks": "all",               # wait for all in-sync replicas
    "enable.idempotence": True,  # exactly-once delivery
    "compression.type": "lz4",  # fast compression
    "broker.address.family": "v4",
}
```

**Local consumer settings:**

```python
{
    "bootstrap.servers": "localhost:9092",
    "group.id": group_id,
    "auto.offset.reset": "earliest",   # start from beginning on first run
    "enable.auto.commit": False,       # manual commit for at-least-once safety
    "broker.address.family": "v4",
}
```

---

### 6.2 `simple_producer.py` — Plain-text Producer

Produces 10 plain UTF-8 string messages to the `telemetry-events` topic.

**Topic:** `telemetry-events` (set via `TOPIC_NAME` in `.env`)

**Key design decisions:**

- Uses `asyncio` so it can be integrated into async applications.
- `producer.poll(0)` is called after each `produce()` to fire delivery callbacks
  without blocking — this keeps the queue drained and callbacks timely.
- `producer.flush()` at the end waits until all in-flight messages are acknowledged
  by the broker before the script exits.

**Message format:**

```
Hello Kafka 8.2 — message #00 (ts=1778913415)
Hello Kafka 8.2 — message #01 (ts=1778913415)
...
```

**Run:**

```powershell
python simple_producer.py
```

---

### 6.3 `simple_consumer.py` — Plain-text Consumer

Reads plain-text messages from `telemetry-events` and logs them.

**Topic:** `telemetry-events`
**Consumer group:** `simple-consumer-group`

**Key design decisions:**

- **Cross-platform signal handling:** Uses `signal.signal()` +
  `threading.Event` instead of `loop.add_signal_handler()`.
  `loop.add_signal_handler()` is a Unix-only asyncio method that raises
  `NotImplementedError` on Windows. `threading.Event.set()` is safe to call
  from a signal handler on all platforms.

```python
_shutdown = threading.Event()

def _handle_signal(sig, frame) -> None:
    _shutdown.set()

signal.signal(signal.SIGINT,  _handle_signal)   # Ctrl+C
signal.signal(signal.SIGTERM, _handle_signal)   # kill / task manager
```

- **Manual commit:** `consumer.commit(asynchronous=False)` commits the offset
  only after the message has been successfully processed. This gives
  at-least-once delivery guarantees — if the script crashes mid-message, the
  offset is not advanced and the message is redelivered on restart.

- **`auto.offset.reset=earliest`** (set in `kafka_config.py`): on first run
  with a new group ID, the consumer reads from the beginning of the topic.

**Run:**

```powershell
python simple_consumer.py
```

---

### 6.4 `json_producer.py` — JSON Producer with Pydantic

Produces 10 sensor readings as JSON to the `sensor-readings` topic.
Each message is validated by a Pydantic model before being serialised.

**Topic:** `sensor-readings` (set via `SENSOR_TOPIC` in `.env`)

**Pydantic model:**

```python
class SensorReading(BaseModel):
    reading_id:  str    # UUID, auto-generated
    sensor_id:   str    # e.g. "sensor-003"
    location:    str    # e.g. "zone-A"
    temperature: float  # validated: must be -50 to 100°C
    humidity:    float  # validated: must be 0–100%
    timestamp:   int    # Unix milliseconds, auto-generated
```

Pydantic's `field_validator` raises a `ValueError` at construction time if
temperature or humidity is out of range — the invalid message is never queued.

**Message key:** `sensor_id` — messages from the same sensor always land on the
same partition, preserving per-sensor ordering.

**Run:**

```powershell
python json_producer.py
```

---

### 6.5 `json_consumer.py` — JSON Consumer with Pydantic

Reads JSON sensor readings, validates them with Pydantic, and logs alerts for
high-temperature readings.

**Topic:** `sensor-readings`
**Consumer group:** `json-consumer-group`

**Poison-pill handling:**

```python
try:
    reading = SensorReading.model_validate_json(msg.value().decode())
except ValidationError as exc:
    logger.error("Validation failed offset=%d: %s", msg.offset(), exc)
    consumer.commit(asynchronous=False)   # skip this message and move on
    continue
```

If a malformed or out-of-range message arrives it is logged and skipped rather
than crashing the consumer. The offset is committed so the bad message is not
redelivered.

**High-temperature alert:**

```python
if reading.temperature > 35.0:
    logger.warning("🌡  HIGH TEMP ALERT  sensor=%s  temp=%.1f°C", ...)
```

**Run:**

```powershell
python json_consumer.py
```

---

### 6.6 `avro_producer.py` — Avro Producer with Schema Registry

Produces 8 telemetry events serialised in Avro binary format.
The schema is automatically registered with Schema Registry on first run.

**Topic:** `telemetry-events`
**Schema Registry:** `http://localhost:8081`

**Avro schema (TelemetryEvent):**

```json
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
```

**Confluent wire format:** Every message is prefixed with:

```
0x00           ← magic byte (1 byte)
<schema-id>    ← schema ID from Schema Registry (4 bytes, big-endian)
<avro-payload> ← Avro binary-encoded record
```

This is why the Avro consumer cannot read plain-text messages — it looks for
the `0x00` magic byte and fails with `Invalid magic byte` if it is absent.
Always use a dedicated topic for Avro messages, or delete and recreate the topic
before switching formats.

> **Important:** Run `avro_producer.py` before `avro_consumer.py`.
> The topic must contain only Avro-encoded messages.

**Run:**

```powershell
python avro_producer.py
```

---

### 6.7 `avro_consumer.py` — Avro Consumer with Schema Registry

Reads Avro-encoded telemetry events. The schema is resolved automatically
via the schema ID embedded in each message's magic bytes — no schema string
needs to be hardcoded in the consumer.

**Topic:** `telemetry-events`
**Consumer group:** `avro-consumer-group`

```python
avro_deserializer = AvroDeserializer(sr_client)  # schema_str=None → auto-resolve

event: dict = avro_deserializer(
    msg.value(),
    SerializationContext(msg.topic(), MessageField.VALUE),
)
```

**Run:**

```powershell
python avro_consumer.py
```

---

### 6.8 `tweet_processor.py` — End-to-End Streaming Pipeline

The most complete example. It implements a full streaming pipeline:

```
tweet-ingestion  →  [enrich]  →  tweet-processed
  (raw tweets)      (inline)      (enriched JSON)
```

**Topics:**

- Input: `tweet-ingestion` (set via `RAW_TWEET_TOPIC`)
- Output: `tweet-processed` (set via `ENRICHED_TWEET_TOPIC`)

**Pydantic models:**

```python
class RawTweet(BaseModel):
    tweet_id, user_handle, text, language,
    created_at, retweet_count, like_count

class EnrichedTweet(BaseModel):
    # all RawTweet fields, plus:
    hashtags,        # list[str] — extracted #tags
    mentions,        # list[str] — extracted @handles
    url_count,       # int       — number of URLs
    sentiment_label, # str       — "positive" | "neutral" | "negative"
    word_count,      # int
    processed_at     # int       — Unix milliseconds
```

**Sentiment analysis (keyword-based):**

```python
_POSITIVE = {"great", "awesome", "love", "excellent", "fantastic", ...}
_NEGATIVE = {"bad", "terrible", "hate", "awful", "horrible", ...}
# Counts token overlap; majority wins; ties → "neutral"
```

**Three run modes:**

| Mode | Command | What it does |
|------|---------|-------------|
| `pipeline` (default) | `python tweet_processor.py` | Produces and consumes concurrently via `asyncio.gather` |
| `produce` | `python tweet_processor.py --mode produce` | Publishes 8 sample tweets and exits |
| `consume` | `python tweet_processor.py --mode consume` | Reads, enriches, and forwards indefinitely |

**Pipeline flow in `pipeline` mode:**

```python
await asyncio.gather(run_producer(), run_consumer())
```

Both coroutines run on the same event loop. The producer publishes a tweet
every 250 ms; the consumer polls every 1 s, enriches each raw tweet, and
forwards the result to `tweet-processed`. The `_shutdown` event causes both
to exit cleanly on Ctrl+C.

---

## 7. Step-by-Step Execution Guide

Follow this order for a clean first-time run through all examples.

### Step 0 — Start the Kafka stack

```powershell
podman compose up -d
podman compose ps   # wait until all three containers show "healthy"
```

Open **http://localhost:8080** to confirm Kafka UI is reachable.

---

### Step 1 — Plain-text: simple producer & consumer

Open two PowerShell terminals, both with the venv activated.

**Terminal 1 — start the consumer first:**

```powershell
python simple_consumer.py
```

Wait for the `[assigned] Partitions: [0, 1, 2]` line — the consumer is now ready.

**Terminal 2 — produce messages:**

```powershell
python simple_producer.py
```

**Terminal 1** output (expected):

```
2026-05-16 09:01:23,451 [INFO] __main__ — Received  topic=telemetry-events  partition=1  offset=0
2026-05-16 09:01:23,451 [INFO] __main__ —   → key=key-0000      value=Hello Kafka 8.2 — message #00 (ts=1778913415)
...
```

Press **Ctrl+C** in Terminal 1 to stop the consumer.

---

### Step 2 — JSON: sensor producer & consumer

**Terminal 1 — start the consumer:**

```powershell
python json_consumer.py
```

**Terminal 2 — produce readings:**

```powershell
python json_producer.py
```

**Terminal 1** output (expected):

```
2026-05-16 09:02:11,102 [INFO] __main__ — SensorReading  id=3f8a1c2d…  sensor=sensor-000  loc=zone-A   temp= 18.0°C  hum= 40.0%
2026-05-16 09:02:11,204 [INFO] __main__ — SensorReading  id=9b4e7f01…  sensor=sensor-001  loc=zone-B   temp= 19.8°C  hum= 41.2%
2026-05-16 09:02:17,801 [WARNING] __main__ — 🌡  HIGH TEMP ALERT  sensor=sensor-009  temp=36.2°C
```

---

### Step 3 — Avro: telemetry producer & consumer

> **Important:** The `telemetry-events` topic may contain plain-text messages
> from Step 1. Delete it before using Avro or messages will fail to deserialise.

**Delete the topic (choose one method):**

```powershell
# Option A — Kafka UI: http://localhost:8080 → Topics → telemetry-events → Delete Topic

# Option B — CLI
podman exec broker kafka-topics --bootstrap-server localhost:9092 `
  --delete --topic telemetry-events
```

**Terminal 2 — produce Avro messages first:**

```powershell
python avro_producer.py
```

Wait for `All messages delivered.` before continuing.

**Terminal 1 — consume:**

```powershell
python avro_consumer.py
```

**Terminal 1** output (expected):

```
2026-05-16 09:03:44,210 [INFO] __main__ — TelemetryEvent  id=evt-0000    device=device-000  metric=cpu_usage   value=  20.00  ts=1778913415000
2026-05-16 09:03:44,211 [INFO] __main__ — TelemetryEvent  id=evt-0001    device=device-001  metric=cpu_usage   value=  21.50  ts=1778913415000
...
```

You can also browse the registered schema at:
**http://localhost:8080 → Schema Registry → com.example.telemetry.TelemetryEvent**

---

### Step 4 — Tweet pipeline (full end-to-end)

The tweet processor is self-contained — it creates its own topics automatically.

**Single terminal — pipeline mode (produce + consume concurrently):**

```powershell
python tweet_processor.py
```

Expected output:

```
2026-05-16 09:05:01,100 [INFO] tweet_processor — Tweet producer started — topic: tweet-ingestion
2026-05-16 09:05:01,101 [INFO] tweet_processor — Tweet processor started — consuming: 'tweet-ingestion'  forwarding: 'tweet-processed'
2026-05-16 09:05:01,350 [INFO] tweet_processor — Queued tweet  @alice        len=89
2026-05-16 09:05:01,612 [INFO] tweet_processor — Enriched  @alice        sentiment=positive  hashtags=['Confluent']   words=12
2026-05-16 09:05:01,600 [INFO] tweet_processor — Queued tweet  @bob_dev      len=66
...
2026-05-16 09:05:03,800 [INFO] tweet_processor — Stats — processed=8  errors=0  positive=4  neutral=2  negative=2
```

Press **Ctrl+C** to stop.

**Run modes separately (two terminals):**

```powershell
# Terminal 1 — consume mode (start first)
python tweet_processor.py --mode consume

# Terminal 2 — produce mode
python tweet_processor.py --mode produce
```

---

## 8. Viewing Messages in Kafka UI

Open **http://localhost:8080** in your browser.

### Browse topics

1. Click **local-kraft** in the left sidebar.
2. Click **Topics** — all topics created by the scripts appear here.
3. Click any topic name → **Messages** tab to see messages with key, value,
   partition, offset, and timestamp.
   - For JSON: values are pretty-printed automatically.
   - For Avro: values are decoded using the Schema Registry.

### Monitor consumer groups

1. Click **Consumers** in the left sidebar.
2. Select a group (e.g. `simple-consumer-group`) to see:
   - Per-partition lag (how far behind the consumer is).
   - Current offset vs log-end offset.

### Browse schemas (Avro)

1. Click **Schema Registry** in the left sidebar.
2. Click `com.example.telemetry.TelemetryEvent` to view:
   - The full Avro schema JSON.
   - Schema version history.

### Produce test messages manually

1. Click any topic → **Produce Message**.
2. Enter a key and value, then click **Produce** — useful for testing consumers
   without running a producer script.

---

## 9. Topic Reference

| Topic | Script(s) | Format | Consumer Group |
|-------|-----------|--------|---------------|
| `telemetry-events` | `simple_producer` / `simple_consumer` | Plain UTF-8 | `simple-consumer-group` |
| `telemetry-events` | `avro_producer` / `avro_consumer` | Avro (Confluent wire format) | `avro-consumer-group` |
| `sensor-readings` | `json_producer` / `json_consumer` | JSON | `json-consumer-group` |
| `tweet-ingestion` | `tweet_processor` | JSON (RawTweet) | `tweet-processor-group` |
| `tweet-processed` | `tweet_processor` | JSON (EnrichedTweet) | — (output only) |

> **Do not mix formats on the same topic.** Plain-text and Avro messages on
> `telemetry-events` will cause `Invalid magic byte` errors. Delete the topic
> and recreate it when switching formats (or set different `TOPIC_NAME` values
> in `.env`).

---

## 10. Troubleshooting

### Containers won't start — name resolution error

```
lookup registry-1.docker.io: Temporary failure in name resolution
```

DNS is broken inside the Podman VM. Fix:

```powershell
podman machine ssh
echo "nameserver 8.8.8.8" | sudo tee /etc/resolv.conf
exit
podman compose up -d
```

---

### Broker container is unhealthy

The healthcheck may be firing before Kafka is fully ready on a slow machine.
Check the actual broker logs:

```powershell
podman logs broker
```

If you see `Kafka Server started` near the bottom, the broker is fine — the
healthcheck just timed out. Bring the stack down and up again:

```powershell
podman compose down
podman compose up -d
```

---

### `Connect to ipv6#[::1]:9092 failed`

Windows resolves `localhost` to IPv6 `::1` before IPv4. The fix is already
applied in `kafka_config.py`:

```python
"broker.address.family": "v4"
```

Confirm you are using the latest `kafka_config.py` and the setting is present
in both `producer_config()` and `consumer_config()`.

---

### `Invalid magic byte` in avro_consumer

The topic contains plain-text messages that were not produced by `avro_producer`.
Delete the topic and re-produce:

```powershell
podman exec broker kafka-topics --bootstrap-server localhost:9092 `
  --delete --topic telemetry-events

python avro_producer.py   # produce Avro messages first
python avro_consumer.py   # then consume
```

---

### Consumer receives 0 messages

The consumer started after the producer with `auto.offset.reset=latest`.
Replay existing messages by resetting the consumer group offset:

```powershell
podman exec broker kafka-consumer-groups `
  --bootstrap-server localhost:9092 `
  --group simple-consumer-group `
  --reset-offsets --to-earliest `
  --topic telemetry-events `
  --execute
```

Or simply start the consumer before the producer next time.

---

### `pip install` fails — filename too long (Windows MAX_PATH)

Enable long paths (run PowerShell as Administrator, then restart terminal):

```powershell
New-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem" `
  -Name "LongPathsEnabled" -Value 1 -PropertyType DWORD -Force
```

---

### `confluent-kafka` fails to build — Python 3.14 / no pre-built wheel

Use version 2.14.0 which ships pre-built `cp314` wheels for Windows:

```powershell
pip install "confluent-kafka[schemaregistry,avro]==2.14.0"
```

---

## 11. Architecture Notes

### Why KRaft instead of ZooKeeper?

ZooKeeper was Kafka's external dependency for cluster metadata and leader
election for over a decade. Confluent Platform 8.x replaces it with **KRaft**
(Kafka Raft) — a consensus protocol built directly into Kafka. Benefits:

- One fewer container to run.
- Faster controller failover.
- Simpler configuration — no separate ZooKeeper connection string.
- All future Kafka releases are KRaft-only.

### Why manual offset commits?

All consumers in this project use `enable.auto.commit=False` and commit
manually after processing each message:

```python
consumer.commit(asynchronous=False)
```

This gives **at-least-once** delivery semantics: if the script crashes between
receiving and committing, the broker resends the message on restart.
Auto-commit gives **at-most-once** — a crash before the auto-commit interval
means the message is silently lost.

### Why `broker.address.family=v4`?

On Windows, Python's socket library and librdkafka both use `getaddrinfo()` to
resolve `localhost`. Windows returns IPv6 `::1` first in the preference list,
even when the service only listens on IPv4 `127.0.0.1`. The Podman VM
port-forward binds only to IPv4, so the IPv6 connection attempt always times
out. Setting `broker.address.family=v4` skips IPv6 resolution entirely.

### Why `threading.Event` instead of `asyncio.Event` for shutdown?

`asyncio.Event` combined with `loop.add_signal_handler()` is the idiomatic
asyncio shutdown pattern — but `loop.add_signal_handler()` calls
`signal.set_wakeup_fd()`, which is **Unix-only** and raises `NotImplementedError`
on Windows. `threading.Event` is safe to `.set()` from a signal handler on all
platforms, and `_shutdown.is_set()` works identically inside an async poll loop.

### Switching to Confluent Cloud

1. Fill in the `CLOUD_*` variables in `.env`.
2. Set `KAFKA_ENV=cloud`.
3. Run any script — no code changes required.

`kafka_config.py` automatically applies `SASL_SSL` + `PLAIN` authentication for
cloud and plain `PLAINTEXT` for local. Topic names and producer/consumer logic
are identical in both environments.
