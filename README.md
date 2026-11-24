<div align="center">

# 🌊 Realtime ETL

**Streaming pipeline — Kafka → PySpark → Airflow — designed for high-volume event ingestion.**

![Status](https://img.shields.io/badge/status-active-22C55E?style=for-the-badge)
![Tested](https://img.shields.io/badge/core_logic-unit_tested-6366F1?style=for-the-badge)
![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Spark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

</div>

---

## 📖 Overview

**Realtime ETL** ingests high-volume event streams, cleans and enriches them in-flight with **Spark
Structured Streaming**, and lands them in an analytics-ready warehouse — while **Airflow** orchestrates
the batch layer, backfills and data-quality checks around it.

It's a **Lambda-style** setup: a **speed layer** for fresh data (seconds) and a **batch layer** for
correctness and reprocessing. Designed to be observable, replayable and idempotent — the boring
properties that matter when you're ingesting **high-volume event streams**.

---

## ✨ Features

- ⚡ **Real-time ingestion** from Kafka topics
- 🧹 **In-flight transforms** — parse, validate, dedupe, enrich, aggregate
- 🧊 **Exactly-once-ish** semantics with checkpointing & idempotent writes
- 🕰️ **Batch orchestration** with Airflow (backfills, reprocessing, SLAs)
- ✅ **Data-quality gates** — schema checks, null/anomaly guards
- 📈 **Analytics-ready sinks** — partitioned tables + dashboards
- 🔁 **Replayable** — reprocess any window from raw topic offsets

---

## 🏗️ Architecture

```
   SOURCES              SPEED LAYER (streaming)            SERVING
   ───────              ──────────────────────            ───────
 ┌──────────┐   ┌──────────────┐   ┌────────────────┐   ┌──────────────┐
 │  Apps /   │   │              │   │  PySpark        │   │  Warehouse    │
 │  services │──▶│    Kafka     │──▶│  Structured     │──▶│  (partitioned │
 │  IoT /    │   │  topics      │   │  Streaming      │   │  fact tables) │
 │  webhooks │   │              │   │  parse·clean·   │   └──────┬───────┘
 └──────────┘   └──────┬───────┘   │  enrich·agg     │          ▼
                       │           └───────┬────────┘   ┌──────────────┐
                       │  raw topic         │  checkpoint │  Dashboards  │
                       │  (replay)          ▼             │  / Power BI  │
                       │           ┌────────────────┐    └──────────────┘
                       │           │  Dead-letter    │
                       │           │  queue (DLQ)    │
                       │           └────────────────┘
                       │
   BATCH LAYER         ▼
 ┌───────────────────────────────────────────────────────────────┐
 │                         Apache Airflow                          │
 │  backfills · reprocessing · data-quality DAGs · SLA & alerts    │
 └───────────────────────────────────────────────────────────────┘
```

**Stack**

| Layer | Tech |
|---|---|
| Transport | Apache Kafka |
| Stream processing | PySpark (Structured Streaming) |
| Orchestration | Apache Airflow |
| Storage | Partitioned warehouse tables (Parquet / SQL) |
| Quality | Schema + rule-based checks |
| Deploy | Docker Compose |

---

## 📂 Project layout

```
realtime-etl/
├── etl/
│   ├── transforms.py     # ★ pure-Python logic: parse·validate·enrich·dedupe·aggregate
│   ├── streaming.py      # PySpark Structured Streaming job (Kafka → clean → agg → sinks)
│   ├── simulate.py       # local runner — same logic, no Spark/Kafka
│   ├── producer.py       # sample Kafka producer (falls back to stdout)
│   └── config.py
├── schemas/
│   └── event.py          # event contract (shared by Python + Spark)
├── dags/                 # Airflow DAGs
│   ├── backfill.py       # daily batch recompute
│   ├── quality_checks.py # freshness + null gates
│   └── reprocess.py      # on-demand offset replay
├── data/
│   └── sample_events.jsonl
├── tests/                # unit tests on the transform logic
├── docker-compose.yml    # kafka + airflow (local)
└── requirements.txt
```

---

## 🚀 Getting started

### Try it in 10 seconds — no Kafka, no Spark

The transformation logic lives in pure Python (`etl/transforms.py`), so you can run the
whole pipeline locally over a sample file:

```bash
git clone https://github.com/minelatech/realtime-etl.git
cd realtime-etl
python -m etl.simulate data/sample_events.jsonl
```

You'll see clean events, the dead-letter queue (malformed / invalid records) and the
windowed aggregates — the same logic the Spark job runs in production.

Run the unit tests:

```bash
python -m pytest -q
```

### Full stack (Kafka + Airflow)

```bash
pip install -r requirements.txt
docker compose up -d           # kafka + airflow  → Airflow UI at http://localhost:8080
```

### Run the streaming job

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  etl/streaming.py
```

### Produce test events

```bash
# with a broker running → sends to Kafka; without one → prints JSONL to stdout
python -m etl.producer --rate 500 --count 10000
```

### Environment variables

```env
KAFKA_BOOTSTRAP=localhost:9092
KAFKA_TOPIC=events
CHECKPOINT_DIR=./chk
WAREHOUSE_URI=./warehouse
```

---

## 📊 Operations

- **Checkpointing** keeps offsets so restarts resume where they stopped
- **DLQ** captures malformed events for later inspection instead of crashing the job
- **Airflow SLAs** alert when a batch DAG runs late
- **Replay** any period by resetting the consumer group to the desired offsets

---

## 🗺️ Roadmap

- [ ] Schema registry (Avro/Protobuf) for stronger contracts
- [ ] Autoscaling Spark executors on lag
- [ ] Great Expectations for richer data-quality suites
- [ ] Real-time metrics exporter (Prometheus/Grafana)

---

<div align="center">

Built by **[MINELATECH](https://minelatech.fr)** — *your data becomes decisions.*

</div>
