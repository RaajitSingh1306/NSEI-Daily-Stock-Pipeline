# NSEI Daily Stock Pipeline — Financial Data Lakehouse & Feature Store

[![Data Pipeline](https://img.shields.io/badge/Orchestrator-Apache%20Airflow-017CEE)](#architecture)
[![Distributed ETL](https://img.shields.io/badge/ETL-PySpark%20%7C%20Parquet-E25A1C)](#spark-transformation-layer)
[![Data Warehouse](https://img.shields.io/badge/Warehouse-DuckDB-FFF000)](#duckdb-warehouse-layer)
[![Transformation](https://img.shields.io/badge/Transformations-dbt%20Core-FF694B)](#dbt-transformation--testing)
[![Object Store](https://img.shields.io/badge/Object%20Storage-LocalStack%20S3-0052CC)](#s3-data-lake-layers)
[![Docker](https://img.shields.io/badge/Containers-Docker%20Compose-2496ED)](#docker-stack)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A production-grade, end-to-end quantitative financial data engineering pipeline that ingests daily market data for **50+ National Stock Exchange of India (NSE) symbols**, stores raw events in an S3 data lake, executes distributed PySpark transformations, ingests cleaned datasets into a DuckDB analytical warehouse, and models dimensional feature marts using **dbt Core**.

The pipeline doubles as an institutional **ML Feature Store**, directly materializing rolling volatility, drawdown, and liquidity features utilized by downstream machine learning models (such as the [Volatility Intelligence Platform](https://github.com/RaajitSingh1306/volatility-intelligence-platform)).

---

## Table of Contents

1. [What This Project Does](#what-this-project-does)
2. [Why It Was Built](#why-it-was-built)
3. [End-to-End Lakehouse Architecture](#end-to-end-lakehouse-architecture)
4. [Project Directory Layout](#project-directory-layout)
5. [Key Design Decisions & Production Guarantees](#key-design-decisions--production-guarantees)
6. [Where & How to Start (Local Setup)](#where--how-to-start-local-setup)
   - [Prerequisites](#prerequisites)
   - [Step 1: Directory Initialization](#step-1-directory-initialization)
   - [Step 2: Start the Docker Stack](#step-2-start-the-docker-stack)
   - [Step 3: Access Airflow UI](#step-3-access-airflow-ui)
   - [Step 4: Trigger Pipeline (Manual or CLI)](#step-4-trigger-pipeline-manual-or-cli)
   - [Step 5: Query Analytical Marts in DuckDB](#step-5-query-analytical-marts-in-duckdb)
   - [Step 6: Inspect LocalStack S3 Objects](#step-6-inspect-localstack-s3-objects)
7. [Troubleshooting & Gotchas](#troubleshooting--gotchas)
8. [Stopping the Infrastructure](#stopping-the-infrastructure)
9. [Connected Portfolio Projects](#connected-portfolio-projects)

---

## What This Project Does

The automated daily pipeline handles the complete lifecycle of financial time-series data:

1. **Market Holiday Gating**: Checks if the target date is an active NSE trading session; cleanly exits if the exchange is closed.
2. **Raw Ingestion (Airflow)**: Pulls daily OHLCV JSON payloads for 50+ tickers and stores raw compressed Parquet files into S3 partitioned by `date=YYYY-MM-DD/symbol=XYZ/`.
3. **Data Quality Gate**: Validates raw schemas, identifies malformed records, nulls, or zero-price anomalies, and routes corrupted rows to an S3 quarantine prefix.
4. **Distributed Transformations (PySpark)**: Computes VWAP (Volume Weighted Average Price), daily return, intraday price range %, gap %, green candle flags, and writes clean partitioned Parquet to the processed layer.
5. **Analytical Loading (DuckDB)**: Copies processed Parquet batches into DuckDB via vectorized `httpfs` S3 integration.
6. **Data Modeling & Feature Store (dbt)**: Builds staging views (`stg_nsei_daily`) and dimensional marts:
   - `mart_nsei_rolling_metrics`: 5d, 10d, and 20d rolling returns, rolling volatility, Sharpe ratio, and drawdown from rolling highs.
   - `mart_sector_performance`: Cross-sectional sector return aggregation, market breadth %, and daily sector momentum ranking.
7. **Automated Testing & Alerting**: Executes dbt schema tests (uniqueness, not-null, referential integrity) and triggers pipeline summary notifications.

---

## Why It Was Built

* **Decoupling ETL from Modeling**: In machine learning, model code should not perform raw network ingestion or data cleaning. This pipeline establishes an enterprise-grade lakehouse that feeds clean, precomputed features to ML models.
* **Guaranteed Idempotency**: Financial data pipelines must handle backfills and network retries gracefully. Every stage is strictly idempotent (`DELETE + INSERT` partitions by `trade_date`), ensuring zero duplicates upon re-runs.
* **Local Cloud Parity**: Utilizes LocalStack to emulate AWS S3 locally, allowing full testing of distributed Spark and cloud storage patterns without incurring AWS infrastructure costs.

---

## End-to-End Lakehouse Architecture

```text
[NSE / Yahoo Finance API]
       │
       ▼ (Airflow DAG: check_market_day → ingest_nsei_data)
[LocalStack S3 Raw Layer]
 s3://nsei-datalake/raw/nsei/daily/date=YYYY-MM-DD/symbol=XYZ/
       │
       ▼ (Airflow: validate_raw_data)
[Data Quality Gate] ──────── bad rows ────────► S3 Quarantine Layer
       │
       ▼ (Airflow: spark_transform)
[PySpark Processing: transform_nsei.py]
 • VWAP, daily returns, dollar volume
 • Intraday range %, gap %, green candle flag
       │
       ▼ (Written to Processed S3)
[LocalStack S3 Processed Layer]
 s3://nsei-datalake/processed/nsei/daily/date=YYYY-MM-DD/symbol=XYZ/
       │
       ▼ (Airflow: load_to_duckdb)
[DuckDB Analytical Warehouse] (/opt/warehouse/nsei.duckdb)
       │
       ▼ (Airflow: dbt_run → dbt_test)
[dbt Core Semantic Layer]
 ├── staging.stg_nsei_daily
 ├── marts.mart_nsei_rolling_metrics   ──► ML Feature Store (Fed to Volatility Models)
 └── marts.mart_sector_performance     ──► Cross-Sectional Breadth & Sector Momentum
```

---

## Project Directory Layout

```text
NSEI Daily Stock Pipeline/
├── dags/
│   └── nsei_pipeline_dag.py        # Master Airflow orchestration DAG
├── spark_jobs/
│   ├── transform_nsei.py           # PySpark distributed ETL job
│   └── nsei_utils.py               # Shared trading calendar & schema validator
├── dbt_project/
│   ├── dbt_project.yml             # dbt project configuration
│   ├── profiles.yml                # DuckDB connection profile
│   ├── seeds/
│   │   └── seed_symbol_metadata.csv # Symbol sector & industry mappings
│   └── models/
│       ├── staging/
│       │   ├── stg_nsei_daily.sql  # Staging view on raw DuckDB table
│       │   └── schema.yml          # Staging tests & documentation
│       └── marts/
│           ├── mart_nsei_rolling_metrics.sql # Rolling features & Sharpe
│           └── mart_sector_performance.sql   # Sector breadth & rank
├── docker/
│   ├── docker-compose.yml          # Multi-container local orchestration
│   └── localstack_init.sh          # S3 bucket creation script
├── notebooks/
│   └── demo.py                     # DuckDB querying demo script
├── How_to_run.md                   # Detailed deployment runbook
├── requirements.txt                # Local development dependencies
└── ReadMe.md                       # Project documentation
```

---

## Key Design Decisions & Production Guarantees

| Component | Choice | Architectural Rationale |
|---|---|---|
| **Storage Format** | Parquet + Snappy | Columnar, splittable, 10x compression over CSV, native predicate pushdown. |
| **Partitioning Strategy** | `date=YYYY-MM-DD/symbol=XYZ/` | Matches common query access patterns; facilitates partition pruning. |
| **Idempotency** | Atomic partition replacement | Any date or range can be backfilled safely without data duplication. |
| **Data Quality Gate** | Pre-Spark Quarantine | Fail-fast validation prevents downstream table corruption while preserving bad rows for audit. |
| **Warehouse Materialization** | Views (Staging), Tables (Marts)| Staging views maintain lineage; physical marts ensure sub-second analytical queries. |
| **Scale Capacity** | 50+ symbols daily | ~18,000 rows/year per symbol; Spark handles parallel distributed batch scaling. |

---

## Where & How to Start (Local Setup)

### Prerequisites

* **Docker Desktop** installed and running (allocate at least 4 GB RAM in Docker Settings).
* **Git** command-line tools.

### Step 1: Directory Initialization

From the project root, create the persistent volume folders required by Docker:

```bash
cd "NSEI Daily Stock Pipeline"
mkdir -p logs data
```

*(These ensure Airflow log files and the DuckDB warehouse database are persisted cleanly on the host).*

### Step 2: Start the Docker Stack

Navigate to the `docker/` folder and boot the containers:

```bash
cd docker
docker compose up -d
```

This launches 5 coordinated containers:
* `postgres`: Airflow metadata database.
* `airflow-init`: One-time DB migration and admin user creation.
* `airflow-webserver`: Web UI on port `8080`.
* `airflow-scheduler`: Task orchestrator and runner.
* `localstack`: AWS S3 mock service on port `4566`.

**Wait ~60 seconds** for initialization to complete. Verify:

```bash
docker compose logs airflow-init | tail -5
# Expect: "Admin user admin created"
```

### Step 3: Access Airflow UI

Open your browser:
* **URL**: `http://localhost:8080`
* **Username**: `admin`
* **Password**: `admin`

You will see the `nsei_daily_pipeline` DAG listed.

### Step 4: Trigger Pipeline (Manual or CLI)

**Option A — Via Airflow UI:**
1. Click `nsei_daily_pipeline`.
2. Click **Trigger DAG w/ config**.
3. Input configuration JSON:
   ```json
   {"ds": "2025-04-24"}
   ```
4. Click **Trigger**.

**Option B — Via Docker CLI:**
```bash
docker exec -it docker-airflow-scheduler-1 \
  airflow dags trigger nsei_daily_pipeline \
  --conf '{"ds": "2025-04-24"}'
```

The DAG will run tasks sequentially:
`check_market_day` $\rightarrow$ `ingest_nsei_data` $\rightarrow$ `validate_raw_data` $\rightarrow$ `spark_transform` $\rightarrow$ `load_to_duckdb` $\rightarrow$ `dbt_run` $\rightarrow$ `dbt_test` $\rightarrow$ `send_pipeline_summary`.

### Step 5: Query Analytical Marts in DuckDB

Query DuckDB directly inside the running scheduler container:

```bash
docker exec -it docker-airflow-scheduler-1 python3 -c "
import duckdb
con = duckdb.connect('/opt/warehouse/nsei.duckdb')

print('--- ML Feature Store Mart Sample ---')
print(con.execute('''
    SELECT symbol, trade_date, vol_regime, sharpe_20d, drawdown_from_20d_high
    FROM marts.mart_nsei_rolling_metrics
    ORDER BY trade_date DESC, symbol
    LIMIT 5
''').df())

print('\n--- Sector Performance Mart Sample ---')
print(con.execute('''
    SELECT trade_date, sector, sector_return, breadth_pct, daily_rank
    FROM marts.mart_sector_performance
    ORDER BY trade_date DESC, daily_rank
    LIMIT 5
''').df())
"
```

### Step 6: Inspect LocalStack S3 Objects

Verify that raw and processed Parquet files landed in S3:

```bash
docker exec -it docker-localstack-1 \
  awslocal s3 ls s3://nsei-datalake/raw/nsei/daily/ --recursive
```

---

## Troubleshooting & Gotchas

* **`load_to_duckdb` fails with S3 read error**:
  DuckDB's `httpfs` extension needs the LocalStack endpoint override. Ensure `SET s3_endpoint='localstack:4566'; SET s3_use_ssl=false;` is configured in `nsei_pipeline_dag.py`.
* **`mart_sector_performance` fails with seed table missing**:
  Execute dbt seed inside the container:
  ```bash
  docker exec -it docker-airflow-scheduler-1 bash -c \
    "cd /opt/nsei_pipeline/dbt_project && dbt seed --profiles-dir /opt/dbt"
  ```
* **Backfilling a date range**:
  ```bash
  docker exec -it docker-airflow-scheduler-1 \
    airflow dags backfill nsei_daily_pipeline \
    --start-date 2025-01-01 --end-date 2025-04-01
  ```

---

## Stopping the Infrastructure

To stop all containers while preserving database state and S3 data:

```bash
cd docker
docker compose down
```

To wipe everything clean for a fresh restart:

```bash
docker compose down -v
```

---

## Connected Portfolio Projects

* **[Volatility Intelligence Platform](https://github.com/RaajitSingh1306/volatility-intelligence-platform)**: Consumes the rolling volatility and drawdown metrics generated in `marts.mart_nsei_rolling_metrics` as upstream features.
* **[Nifty Sector Rotation](https://github.com/RaajitSingh1306/Nifty-Sector-Rotation)**: Directly utilizes the cross-sectional breadth and sector return aggregations computed in `marts.mart_sector_performance`.

---

## License & Disclaimer

MIT License. Designed for quantitative data engineering and feature store demonstration.
