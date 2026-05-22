# Real-Time Order Intelligence Platform (ROIP)

> An enterprise-grade, event-driven data platform processing 5M+ daily
> events through Apache Kafka, Spark Structured Streaming, and a
> Medallion Lakehouse — built to demonstrate production data engineering
> at scale.

[![CI](https://github.com/YOUR_USERNAME/roip/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/roip/actions)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://python.org)
[![Apache Spark 3.5](https://img.shields.io/badge/spark-3.5-orange.svg)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/delta--lake-3.1-blue.svg)](https://delta.io)
[![License MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

---

## What this platform does

ROIP solves the operational data problem: raw order, payment, inventory,
and delivery events arrive from multiple source systems in real-time,
often late, out of order, or duplicated. ROIP ingests, validates,
transforms, and serves this data — making it available for business
decisions within 30 seconds of an event occurring.

Think of it as the internal data platform that powers the ops dashboard
at a mid-sized e-commerce or logistics company.

---

## Architecture

```
Event Sources → Kafka (6 topics, partitioned by order_id)
             → Spark Structured Streaming (Bronze, 30s micro-batch)
             → Spark Batch (Silver SCD2, Gold aggregates, daily)
             → Delta Lakehouse (Bronze / Silver / Gold)
             → Snowflake (BI serving layer)
             → Grafana (pipeline observability)
```

**Key engineering decisions** (see [docs/adr/](docs/adr/) for full reasoning):
- Kafka partitioned by `order_id` for per-order event ordering
- Delta Lake over Iceberg for PySpark ecosystem maturity
- `foreachBatch` over continuous streaming for exactly-once Delta writes
- KEDA autoscaling on Kafka consumer lag, not CPU/memory
- SCD Type 2 for customer dimension to preserve historical accuracy

---

## Tech stack

| Layer | Technology |
|---|---|
| Event streaming | Apache Kafka 3.7 + Schema Registry |
| Stream processing | Spark Structured Streaming 3.5 |
| Batch processing | PySpark 3.5 + Delta Lake 3.1 |
| Orchestration | Apache Airflow 3.x |
| Storage format | Delta Lake / Parquet (Snappy) |
| Serving | Snowflake |
| Data quality | Great Expectations 0.18 |
| Observability | Prometheus + Grafana |
| CI/CD | GitHub Actions |
| Containerisation | Docker + Kubernetes (Helm) |
| Autoscaling | KEDA (Kafka lag-based) |

---

## Quick start (local)

**Prerequisites:** Docker Desktop, WSL2 (Windows), Python 3.11, Java 17

```bash
git clone https://github.com/YOUR_USERNAME/roip.git
cd roip
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
make up          # starts Kafka, Schema Registry, Prometheus, Grafana
```

**Run the full pipeline:**

```bash
# Terminal 1: start event simulation
python ingestion/producers/order_producer.py

# Terminal 2: start streaming ingestion
python streaming/jobs/bronze_stream.py

# Terminal 3: run daily batch (after Bronze has data)
python batch/jobs/bronze_to_silver.py --date $(date +%Y-%m-%d)
python batch/jobs/silver_to_gold.py   --date $(date +%Y-%m-%d)

# Terminal 4: verify outputs
python streaming/utils/bronze_inspector.py
python batch/jobs/gold_inspector.py
```

**Open dashboards:**
- Grafana: http://localhost:3000 (admin / roip_admin)
- Prometheus: http://localhost:9090

---

## Scalability

| Scale | Architecture |
|---|---|
| 1M events/day | Single Docker Compose, local Spark |
| 10M events/day | 3 Kafka brokers, 4 Spark workers, managed cloud |
| 100M events/day | MSK + EMR Serverless + S3 + Snowflake multi-cluster |
| 1B+ events/day | Kafka federation + Spark on K8s spot + Delta OPTIMIZE |

---

## Running tests

```bash
pytest tests/unit/        -v   # fast, no Spark (~10s)
pytest tests/integration/ -v   # Spark required (~90s)
pytest tests/             -v --cov=. --cov-report=term-missing
```

---

## Kubernetes deployment

```bash
minikube start --cpus=4 --memory=8192
helm install roip infra/k8s/helm/roip/ --namespace roip --create-namespace
kubectl get all -n roip
```

---

## Project structure

```
roip/
├── ingestion/        Kafka producers, Avro schemas, event simulator
├── streaming/        Spark Structured Streaming jobs + utils
├── batch/            Bronze→Silver→Gold batch jobs + reconciliation
├── quality/          Great Expectations suites + Prometheus metrics
├── orchestration/    Airflow DAGs
├── serving/          Snowflake DDL, views, loader
├── monitoring/       Prometheus config + Grafana dashboards
├── infra/            Docker, Kubernetes Helm charts, Terraform
├── tests/            Unit + integration tests
└── docs/             Architecture Decision Records
```

---

## Engineering challenges solved

- **Late-arriving events** — Spark watermarking (10-min tolerance)
- **Duplicate events** — Stateful dedup by `event_id` in foreachBatch
- **Schema evolution** — Avro Schema Registry + Delta `mergeSchema`
- **Idempotent batch jobs** — Delta MERGE on primary keys
- **Data quality gating** — Great Expectations blocking pipeline on critical failures
- **Autoscaling** — KEDA scales Spark pods on Kafka consumer lag
- **Historical accuracy** — SCD Type 2 for customer dimension
- **Reconciliation** — Nightly Bronze vs Silver count comparison with <1% tolerance

---

## License

MIT — built as a portfolio project demonstrating enterprise data engineering patterns.