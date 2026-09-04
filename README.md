<p align="center">
  <h1 align="center">📦 End-to-End Streaming Data Pipeline</h1>
  <p align="center">
    <strong>API → Kafka → Spark → MongoDB</strong>, orchestrated by a single Airflow DAG
  </p>
</p>

<p align="center">
  Continuously fetch synthetic user records from <code>randomuser.me</code>, publish them to Kafka as
  schema-governed Avro, enrich them in Spark, and persist the results to a MongoDB replica set —
  all kicked off with one click in Airflow.
</p>

---

## 📋 Table of Contents

- [✨ Features](#-features)
- [🏗️ Architecture](#-architecture)
- [🖼️ Screenshots](#️-screenshots)
- [🛠️ Tech Stack](#️-tech-stack)
- [🚀 Quick Start](#-quick-start)
- [▶️ Running the Pipeline](#️-running-the-pipeline)
- [⚙️ How It Works](#️-how-it-works)
- [🔧 Configuration](#-configuration)
- [🧹 Resetting the Data](#-resetting-the-data)
- [🔍 Troubleshooting](#-troubleshooting)

---

## ✨ Features

- **⚡ Single-trigger pipeline** — one Airflow DAG (`user_pipeline`) starts the producer and the Spark consumer in parallel.
- **🛡️ Fault-tolerant by design** — 2 Kafka brokers (KRaft, replicated topics), 2 Spark workers, a 3-node MongoDB replica set with majority write concern, and 2 Airflow Celery workers.
- **📜 Schema-governed Avro** — producer and consumer share one Avro contract registered in the Schema Registry; both sides must agree or the pipeline fails loudly.
- **⏱️ Real-time processing** — Spark consumes with 5-second micro-batches; no batch jobs involved.
- **✨ Spark enrichment** — each record is decorated in-flight: `email`/`username` lowercased, `age` derived from `dob`, and a `processed_at` processing timestamp added.
- **🔁 Resumable** — Spark checkpoints track Kafka offsets, so the consumer picks up where it left off across restarts.

---

## 🏗️ Architecture

<!-- TODO: add architecture diagram → docs/images/architecture.png -->

```
randomuser.me API
      │  fetch user (HTTP)
      ▼
┌─────────────┐   users_created topic (Avro)   ┌──────────────────────┐   spark-submit   ┌──────────────┐
│   Airflow   │ ─────────────────────────────► │ Kafka ×2 + controller │ ───────────────► │   Spark      │
│  (producer) │   schema-registry <topic>-value │   KRaft cluster       │                  │ master+2 W   │
└─────────────┘                                 └──────────────────────┘                  └──────┬───────┘
      │  register / encode                              ▲                                        │ foreachBatch
      ▼                                                 │ fetch schema / decode                      ▼
┌──────────────────┐                                   │                              ┌────────────────────┐
│ schema-registry   │ ◄────────────────────────────────┘                ┌───┐          │ MongoDB 3-node rs0   │
└──────────────────┘                                                   │Spark│◄────────│  mongodb1/2/3 (users) │
                                                                       └────┘ write     └────────────────────┘
```

**The flow at a glance:**

1. A **Python operator** in Airflow fetches a random user from `randomuser.me`, maps it onto the Avro schema, and publishes it to the Kafka topic `users_created` (encoded Avro, wired with the Confluent magic-byte header).
2. A **Spark streaming job** (submitted by a second task in the same DAG) reads the topic, strips the wire header, decodes the Avro payload, enriches each record, and writes every 5-second micro-batch to MongoDB.
3. **MongoDB** stores the enriched documents in the `users` collection, replicated across a 3-node replica set for HA.

> **High availability:** Kafka runs 2 broker+controller nodes plus a dedicated controller-only 3rd node, so a single broker loss keeps both data and the controller quorum. Internal topics and `users_created` replicate across both brokers (`replication.factor=2`).

---

## 🖼️ Screenshots

<!-- TODO: add Airflow DAG view screenshot → docs/images/screenshots/airflow.png -->
<!-- TODO: add Kafka UI messages screenshot → docs/images/screenshots/kafka-ui.png -->
<!-- TODO: add Mongo Express users collection screenshot → docs/images/screenshots/mongo-express.png -->

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| 🎛️ Orchestration | Apache Airflow 3.3.1 — CeleryExecutor, 2 workers |
| 📨 Message broker | Apache Kafka 8.3.1 (Confluent) — KRaft mode, 2 brokers + 1 controller |
| 🧾 Schema contract | Confluent Schema Registry 8.3.1 — Avro |
| ⚡ Stream processing | Apache Spark 4.2.0 — standalone master + 2 workers, MongoDB Spark Connector |
| 🍃 Target store | MongoDB 7.0 — 3-node replica set `rs0`, auth enabled |
| 🐍 Producer | Python — `confluent-kafka`, RandomUser API |
| 🐘 Airflow metadata DB | PostgreSQL 16 |
| ⚡ Celery broker | Redis 7.2 |
| 🖥️ UIs | Kafka UI, Mongo Express |

### 📌 Services & Ports

| URL | Service |
|-----|---------|
| http://localhost:8080 | Airflow UI |
| http://localhost:8081 | Schema Registry (REST) |
| http://localhost:8082 | Kafka UI |
| http://localhost:8083 | Mongo Express |
| http://localhost:8090 | Spark Master UI |
| http://localhost:8091 | Spark Worker 1 UI |
| http://localhost:9094 | Kafka external listener (kafka1) |
| http://localhost:27017 | MongoDB (mongodb1) |

---

## 🚀 Quick Start

### ✅ Prerequisites

- Docker with the Docker Compose plugin
- Python 3.11+ *(only needed once, to generate secrets)*
- ~8 GB free RAM for Docker *(the stack is memory-heavy)*

### 1️⃣ Configure the environment

```bash
cp .env.example .env
```

Generate the required secrets and paste them into `.env`:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"   # FERNET_KEY
python -c "import secrets; print(secrets.token_hex(32))"                                     # JWT / API secrets
```

> ⚠️ `.env` is **gitignored** — never commit it.

| Variable | Description |
|----------|-------------|
| `FERNET_KEY` | Airflow encryption key (base64 Fernet token) |
| `AIRFLOW__API_AUTH__JWT_SECRET` | Airflow API JWT secret |
| `AIRFLOW__API__SECRET_KEY` | Airflow API session/CSRF secret |
| `_AIRFLOW_WWW_USER_USERNAME` | Airflow UI username (default `airflow`) |
| `_AIRFLOW_WWW_USER_PASSWORD` | Airflow UI password |
| `MONGO_ROOT_USERNAME` / `MONGO_ROOT_PASSWORD` | MongoDB admin credentials (init + Spark writer) |
| `MONGO_EXPRESS_USERNAME` / `MONGO_EXPRESS_PASSWORD` | Mongo Express web-UI login |

### 2️⃣ Start the stack

The `airflow-worker` image expects a local PySpark wheel (gitignored, it's ~430 MB). Fetch it once before the first build:

```bash
./airflow/worker/download-pyspark-wheel.sh
```

```bash
docker compose up -d --build
```

Wait until everything is healthy:

```bash
docker compose ps
```

### 3️⃣ Open the UIs

| URL | Credentials |
|-----|-------------|
| http://localhost:8080 | Airflow — from `_AIRFLOW_WWW_USER_*` |
| http://localhost:8082 | Kafka UI — no login |
| http://localhost:8083 | Mongo Express — from `MONGO_EXPRESS_*` |

---

## ▶️ Running the Pipeline

### 🎬 Start

1. Open **Airflow UI** → DAGs → **`user_pipeline`** → **Run**.

The run starts two tasks in parallel:

| Task | What it does |
|------|--------------|
| **`stream_data_from_api`** | Fetches one random user/second from the RandomUser API → publishes Avro to Kafka `users_created`. Runs for `PIPELINE_DURATION` seconds (default `60`), then finishes. |
| **`spark_consumer`** | Submits `spark/jobs/spark_stream.py` to the Spark cluster → consumes, enriches, and writes in 5-second micro-batches until stopped. |

### 🛑 Stop the consumer

Open the DAG → **Graph view** → select `spark_consumer` → **Mark Failed**. The producer stops by itself after `PIPELINE_DURATION`.

### ✅ Verify the data

```bash
docker compose exec mongodb1 mongosh \
  "mongodb://$MONGO_ROOT_USERNAME:$MONGO_ROOT_PASSWORD@mongodb1:27017,mongodb2:27017,mongodb3:27017/users_db?replicaSet=rs0&authSource=admin" \
  --eval "db.users.find().limit(5)"
```

---

## ⚙️ How It Works

### 📤 Producer — `dags/kafka_stream.py`

1. Reads the Avro contract from `dags/schemas/user.avsc` *(single source of truth)*.
2. Fetches a random user from `randomuser.me`, flattens the API payload, converts dates to epoch milliseconds.
3. Serializes with the Schema Registry (`users_created-value` subject), embedding the `[magic byte][4-byte schema id]` wire header.
4. Publishes with `acks=all` + an idempotent producer for exactly-once messaging guarantees.

### 📥 Consumer — `spark/jobs/spark_stream.py`

1. Reads `users_created` via the Kafka source connector (`startingOffsets=earliest`, `failOnDataLoss=false`).
2. Strips the 5-byte Confluent header, then decodes the Avro payload with the shared schema.
3. **Enriches** each record:
   - `email` / `username` → lowercased
   - `age` → derived from `dob`
   - `processed_at` → processing timestamp
4. Writes each micro-batch to MongoDB (`users_db.users`) with a **majority** write concern.
5. Checkpoints offsets to `CHECKPOINT_LOCATION`, keeping the stream resumable.

---

## 🔧 Configuration

Topology and behavior are configurable via `.env` (injected by `compose.yml`):

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `broker1:9092,broker2:9092` | Kafka bootstrap servers (comma-separated) |
| `KAFKA_TOPIC` | `users_created` | Kafka topic produced/consumed by the pipeline |
| `SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` | Schema Registry base URL |
| `PIPELINE_DURATION` | `60` | How long the producer runs (seconds) |
| `AIRFLOW_UID` | `1000` | Host user UID for correct file ownership |
| `_PIP_ADDITIONAL_REQUIREMENTS` | *(empty)* | Extra pip packages installed into Airflow at startup |

> Airflow settings are passed as `AIRFLOW__<SECTION>__<KEY>` environment variables (taking precedence over `config/airflow.cfg`) — see `compose.yml` for the full list.

---

## 🧹 Resetting the Data

Start fresh (empty Kafka topic + empty MongoDB):

```bash
# 1. Stop the running pipeline (Airflow UI → mark spark_consumer as failed)

# 2. Empty MongoDB
docker compose exec mongodb1 mongosh \
  "mongodb://$MONGO_ROOT_USERNAME:$MONGO_ROOT_PASSWORD@mongodb1:27017,mongodb2:27017,mongodb3:27017/users_db?replicaSet=rs0&authSource=admin" \
  --eval "db.users.drop()"

# 3. Delete + recreate the Kafka topic (deletion is async — wait for it to disappear)
docker compose exec kafka1 kafka-topics --bootstrap-server broker1:9092,broker2:9092 \
  --delete --topic users_created
docker compose exec kafka1 kafka-topics --bootstrap-server broker1:9092,broker2:9092 \
  --create --topic users_created --partitions 1 --replication-factor 2 --config min.insync.replicas=2

# 4. Clear Spark checkpoints so offsets restart from the beginning
rm -rf logs-worker1/spark-checkpoints/users logs-worker2/spark-checkpoints/users
```

**Nuclear option** — wipe everything (Airflow metadata, Kafka data, all volumes), **without** removing images or the build cache:

```bash
docker compose down -v
```

> ✅ No re-install of PySpark or rebuild of the Dockerfiles is needed to come back up.

---

## 🔍 Troubleshooting

| Symptom | Fix |
|---------|-----|
| **Consumer never picks up data / resumes from an old offset** | Clear Spark checkpoints (`logs-worker1/spark-checkpoints/users`, `logs-worker2/spark-checkpoints/users`) so the stream restarts from `earliest`. |
| **Streaming query fails after a code change ("schema mismatch")** | The checkpoint stores the query schema — clear checkpoints before re-running with changed job logic. |
| **Kafka topic delete "didn't work"** | Deletion is asynchronous in KRaft — wait a few seconds, confirm with `kafka-topics --list`, then recreate. |
| **MongoDB writes fail** | The Spark writer needs the replica set **primary**. The `MONGO_URI` seed list auto-discovers it — check health with `docker compose exec mongodb1 mongosh --eval "rs.status()"`. |
| **Schema Registry subjects accumulate** | If the Avro schema changes incompatibly, delete the obsolete `users_created-value` subject (the image ships no `curl` — use `requests` from an Airflow container). |

---

## 📁 Project Structure

```
.
├── airflow/
│   ├── Dockerfile                    # shared Airflow image (dependencies)
│   └── worker/
│       ├── Dockerfile                # worker-only image: adds PySpark + JDK
│       ├── requirements-worker.txt   # worker-only Python deps (pyspark)
│       └── wheels/                   # cached PySpark wheel (offline build)
├── compose.yml                       # full stack definition (21 services)
├── config/
│   └── airflow.cfg                   # Airflow config defaults
├── dags/
│   ├── kafka_stream.py               # user_pipeline DAG (producer + consumer)
│   └── schemas/
│       └── user.avsc                 # Avro data contract (single source of truth)
├── docker/
│   └── mongodb-init.sh               # idempotent rs0 replica set bootstrap
├── kafka/
│   └── controller.properties         # config for the controller-only KRaft node
├── mongo/
│   └── Dockerfile                    # mongod with baked-in auth keyfile
├── spark/
│   ├── Dockerfile                    # spark image + job code
│   └── jobs/
│       └── spark_stream.py           # streaming job (consume → transform → write)
└── .env.example                      # environment template (copy to .env)
```

---

Made with ❤️ for the data engineering community.