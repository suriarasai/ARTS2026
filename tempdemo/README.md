# Live Temperature Demo — Kafka + Spring Boot + Chart.js

A two-panel Spring Boot web application that demonstrates real-time event streaming with Apache Kafka and WebSockets.

---

## What This App Does

| Panel | Role | Stack |
|---|---|---|
| **Producer Panel** (left) | User enters a temperature → REST call → Kafka topic | Spring MVC REST, `KafkaTemplate` |
| **Consumer Panel** (right) | Kafka message → WebSocket → live Chart.js line chart | Spring Kafka `@KafkaListener`, STOMP/SockJS, Chart.js 4.x |

The full data flow is:

```
[Browser input]
  → POST /api/temperature
    → KafkaProducerService.send()
      → Kafka topic "livetemperature"
        → KafkaConsumerService.consume()
          → SimpMessagingTemplate → /topic/temperature (WebSocket)
            → Chart.js line chart updates in real time
```

---

## Upgrade Summary (v2.6 → v3.4)

| Component | Old | New |
|---|---|---|
| Spring Boot | 2.6.5 | **3.4.5** |
| Java | 11 | **21 (LTS)** |
| Spring Kafka | 2.8.x | **3.3.x** (managed) |
| Kafka client | 3.1.x | **3.9.x** (managed) |
| Bootstrap | 4.6.0 | **5.3.3** |
| jQuery | 3.6.0 | **3.7.1** |
| Chart.js | 2.7.2 | **4.4.3** |
| SockJS WebJar | 1.1.2 | **1.5.1** |
| STOMP WebJar | 2.3.3 | **2.3.4** |
| ZooKeeper | required | **removed — KRaft only** |

### New files added

| File | Purpose |
|---|---|
| `KafkaProducerConfig.java` | Explicit `KafkaTemplate` bean wiring |
| `KafkaProducerService.java` | `kafkaTemplate.send()` wrapper |
| `ProducerController.java` | `POST /api/temperature` REST endpoint |

### Chart.js 2.x → 4.x API changes (home.html)

```javascript
// ── OLD (Chart.js 2.7) ──────────────────────────
options: {
  title: { display: true, text: '...' },       // top-level
  tooltips: { mode: 'index' },                 // plural
  scales: {
    xAxes: [{ type: 'time',                    // array
      scaleLabel: { labelString: 'Time' } }],  // scaleLabel
    yAxes: [{ scaleLabel: { labelString: '°C' } }]
  }
}

// ── NEW (Chart.js 4.4) ──────────────────────────
options: {
  plugins: {
    title:   { display: true, text: '...' },   // under plugins
    tooltip: { mode: 'index' },                // singular
  },
  scales: {
    x: { type: 'time',                         // object, not array
      title: { display: true, text: 'Time' } },// title (not scaleLabel)
    y: { title: { display: true, text: '°C' } }
  }
}
```

### KIP-848 — Next-generation consumer group protocol

Added to `KafkaConsumerConfig.java`:

```java
// Enables incremental rebalance — no full stop-the-world reassignment
props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");  // KIP-848
```

Requires `kafka-clients` ≥ 3.7 and a Kafka 4.x broker in KRaft mode.

### KIP-1118 — Producer deadlock safety (Kafka 4.1+)

In `KafkaProducerService.java`, `flush()` is **never** called inside a `send()` callback. This is now enforced by the broker — calling flush inside a callback throws immediately.

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Java | 21+ | `java -version` to verify |
| Maven | 3.8+ | or use `./mvnw` |
| Apache Kafka | 4.x | KRaft mode (no ZooKeeper) |

---

## Running Kafka (KRaft mode — no ZooKeeper)

Kafka 4.0+ runs entirely without ZooKeeper. Start the broker with:

```bash
# 1. Generate a cluster UUID (first time only)
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

# 2. Format the storage directory
bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c config/kraft/server.properties

# 3. Start the broker
bin/kafka-server-start.sh config/kraft/server.properties
```

On Windows use `.bat` equivalents.

> **Important:** ZooKeeper commands (`--zookeeper 127.0.0.1:2181`) no longer exist.  
> All CLI tools now use `--bootstrap-server localhost:9092`.

---

## Create the Kafka Topic

```bash
# Linux / macOS
kafka-topics.sh --create \
  --topic livetemperature \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1

# Windows
kafka-topics.bat --create ^
  --topic livetemperature ^
  --bootstrap-server localhost:9092 ^
  --replication-factor 1 ^
  --partitions 1
```

Verify:

```bash
kafka-topics.sh --list --bootstrap-server localhost:9092
kafka-topics.sh --describe --topic livetemperature --bootstrap-server localhost:9092
```

---

## Running the Application

```bash
./mvnw spring-boot:run
```

Open **http://localhost:5656/home** in your browser.

---

## Using the App

### Producer Panel (left)

1. Type a numeric temperature (e.g. `36.5`) in the input field.
2. Click **Send to Kafka** (or press Enter).
3. The value is published to the `livetemperature` Kafka topic.
4. A timestamped log of recent sends appears below the button.

### Consumer Panel (right)

- The chart subscribes to `/topic/temperature` over STOMP/WebSocket.
- Every message arriving from Kafka appears as a new point on the live line chart.
- The current temperature badge updates in real time.
- Click **Clear chart** to reset.

### CLI Producer (alternative)

You can also send values directly from the command line:

```bash
kafka-console-producer.sh \
  --broker-list localhost:9092 \
  --topic livetemperature
```

Type any numeric value and press Enter — it will appear on the chart within milliseconds.

---

## Project Structure

```
tempdemo-upgraded/
├── pom.xml
├── README.md
└── src/
    └── main/
        ├── java/sg/edu/iss/tempdemo/
        │   ├── TempdemoApplication.java
        │   ├── configurations/
        │   │   ├── KafkaConsumerConfig.java   ← KIP-848 consumer protocol
        │   │   ├── KafkaProducerConfig.java   ← NEW: KafkaTemplate wiring
        │   │   └── WebSocketConfig.java       ← STOMP /live-temperature endpoint
        │   ├── controllers/
        │   │   ├── HomeController.java        ← GET /home → home.html
        │   │   └── ProducerController.java    ← NEW: POST /api/temperature
        │   └── services/
        │       ├── KafkaConsumerService.java  ← @KafkaListener → WebSocket push
        │       └── KafkaProducerService.java  ← NEW: KafkaTemplate.send()
        └── resources/
            ├── application.properties
            └── templates/
                └── home.html                  ← Bootstrap 5 two-panel UI + Chart.js 4.x
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `GROUP_PROTOCOL_CONFIG` error on start | kafka-clients < 3.7 | Upgrade Kafka broker to 4.x; let Spring Boot manage the client version |
| Chart not updating | WebSocket not connected | Check browser console for STOMP errors; ensure app is on port 5656 |
| `NoSuchMethodError` on `jakarta.*` | Old code using `javax.*` | Spring Boot 3.x uses Jakarta EE 10 — `javax` is `jakarta` |
| Topic not found | Topic not created | Run `kafka-topics.sh --create …` as above |
| `flush() deadlock` error | flush() in callback | Move `kafkaTemplate.flush()` outside the `send()` callback (KIP-1118) |

---

## Further Reading

- [Spring Boot 3.x Migration Guide](https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-3.0-Migration-Guide)
- [Spring Kafka 3.x Reference](https://docs.spring.io/spring-kafka/docs/current/reference/html/)
- [Kafka KIP-848 (Next-gen consumer group)](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848)
- [Chart.js 4.x Migration Guide](https://www.chartjs.org/docs/latest/migration/v4-migration.html)
- [AKHQ — Kafka web UI](https://akhq.io)
