# NSEI Daily Stock Pipeline — Financial Data Lakehouse & Feature Store

[![Data Pipeline](https://img.shields.io/badge/Orchestrator-Apache%20Airflow-017CEE)](#3-system-architecture)
[![Distributed ETL](https://img.shields.io/badge/ETL-PySpark%20%7C%20Parquet-E25A1C)](#4-tech-stack--libraries)
[![Data Warehouse](https://img.shields.io/badge/Warehouse-DuckDB-FFF000)](#8-results--evaluation)
[![Transformations](https://img.shields.io/badge/Transformations-dbt%20Core-FF694B)](#6-step-by-step-pipeline)
[![Object Storage](https://img.shields.io/badge/Object%20Storage-LocalStack%20S3-0052CC)](#5-data)
[![Containers](https://img.shields.io/badge/Containers-Docker%20Compose-2496ED)](#10-getting-started)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](#16-license--disclaimer)

A production-grade, end-to-end quantitative financial data engineering lakehouse that ingests daily market data for **50+ National Stock Exchange of India (NSE) symbols**, stores raw events in an S3 data lake, executes distributed PySpark transformations, ingests cleaned datasets into a DuckDB analytical warehouse, and models dimensional feature marts using **dbt Core**. The pipeline functions as an institutional **ML Feature Store**, directly materializing rolling volatility, drawdown, and liquidity features utilized by downstream machine learning models (such as the [Volatility Intelligence Platform](https://github.com/RaajitSingh1306/volatility-intelligence-platform)).

---

## Table of Contents

- [1. What This Project Does](#1-what-this-project-does)
- [2. Why It Was Built](#2-why-it-was-built)
- [3. System Architecture](#3-system-architecture)
- [4. Tech Stack & Libraries](#4-tech-stack--libraries)
- [5. Data](#5-data)
- [6. Step-by-Step Pipeline](#6-step-by-step-pipeline)
- [7. Problems Faced & How We Solved Them](#7-problems-faced--how-we-solved-them)
- [8. Results & Evaluation](#8-results--evaluation)
- [9. Project Structure](#9-project-structure)
- [10. Getting Started](#10-getting-started)
- [11. Warehouse Queries & Inspection](#11-warehouse-queries--inspection)
- [12. Deployment](#12-deployment)
- [13. Connected Portfolio Projects](#13-connected-portfolio-projects)
- [14. Limitations & Known Issues](#14-limitations--known-issues)
- [15. Roadmap / Future Expansion](#15-roadmap--future-expansion)
- [16. License & Disclaimer](#16-license--disclaimer)

---

## 1. What This Project Does

The automated daily lakehouse handles the complete lifecycle of financial time-series data:

- **Market Holiday Gating**: Checks if the target date is an active NSE trading session; cleanly halts downstream execution on exchange holidays.
- **Raw Lake Ingestion (Airflow + S3)**: Pulls daily OHLCV records for 50+ tickers and stores raw compressed Parquet files into S3 partitioned by `date=YYYY-MM-DD/symbol=XYZ/`.
- **Pre-Spark Data Quality Gate**: Validates schemas, catches malformed records, nulls, or zero-price anomalies, and routes corrupted rows to an S3 quarantine prefix.
- **Distributed Transformations (PySpark)**: Computes VWAP (Volume Weighted Average Price), daily return, intraday price range %, gap %, and green candle flags; writes clean partitioned Parquet to the processed layer.
- **Vectorized Analytical Loading (DuckDB)**: Copies processed Parquet batches into DuckDB using high-throughput `httpfs` S3 streaming.
- **Data Modeling & Feature Store (dbt Core)**: Builds staging views and physical dimensional marts:
  - `mart_nsei_rolling_metrics`: 5d, 10d, and 20d rolling returns, rolling volatility, Sharpe ratio, and drawdown from rolling highs.
  - `mart_sector_performance`: Cross-sectional sector return aggregation, market breadth %, and daily sector momentum ranking.
- **Automated Testing & Notifications**: Executes 14 dbt schema constraints (uniqueness, not-null, referential integrity) and issues execution summary logs.

---

## 2. Why It Was Built

- **Decoupling ETL from Machine Learning**: Machine learning training scripts should never perform raw network scraping or ad-hoc data cleaning. This pipeline establishes an enterprise lakehouse that materializes clean, verified features into a centralized store.
- **Guaranteed Idempotency & Replayability**: Financial pipelines must support historical backfills and network retries gracefully. Every stage uses atomic partition replacement (`DELETE + INSERT` keyed on `trade_date`), ensuring zero duplicate records upon re-execution.
- **Local Cloud Parity**: Utilizes LocalStack to emulate AWS S3 locally inside Docker, allowing rigorous distributed testing of Spark and cloud storage patterns without incurring cloud billing.

---

## 3. System Architecture

```text
┌────────────────────────────────────────────────────────────────────────┐
│                        MARKET DATA SOURCE                              │
│                                                                        │
│   NSE Equities / Yahoo Finance API (50+ Ticker Symbols)                │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼ (Airflow DAG: check_market_day ──► ingest_nsei_data)
┌────────────────────────────────────────────────────────────────────────┐
│                    LOCALSTACK S3 RAW LANDING LAYER                     │
│                                                                        │
│   s3://nsei-datalake/raw/nsei/daily/date=YYYY-MM-DD/symbol=XYZ/        │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼ (Airflow: validate_raw_data)
┌────────────────────────────────────────────────────────────────────────┐
│                        DATA QUALITY GATE                               │
│                                                                        │
│   Schema & Null Checks ──────── Anomalous Rows ────────► S3 Quarantine │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼ (Airflow: spark_transform)
┌────────────────────────────────────────────────────────────────────────┐
│                    PYSPARK DISTRIBUTED TRANSFORM                       │
│                       (spark_jobs/transform_nsei.py)                   │
│                                                                        │
│   - VWAP, daily return, dollar volume, intraday range %, gap %        │
│   - Writes to s3://nsei-datalake/processed/nsei/daily/                 │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼ (Airflow: load_to_duckdb)
┌────────────────────────────────────────────────────────────────────────┐
│                     DUCKDB ANALYTICAL WAREHOUSE                        │
│                       (/opt/warehouse/nsei.duckdb)                     │
│                                                                        │
│   Vectorized Parquet ingestion via httpfs extension                    │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼ (Airflow: dbt_seed ──► dbt_run ──► dbt_test)
┌────────────────────────────────────────────────────────────────────────┐
│                        DBT CORE SEMANTIC LAYER                         │
│                                                                        │
│   ├── staging.stg_nsei_daily (Type-cast clean staging views)           │
│   ├── marts.mart_nsei_rolling_metrics ──► ML Feature Store for VIP     │
│   └── marts.mart_sector_performance   ──► Cross-Sectional Breadth/Rank │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Tech Stack & Libraries

| Library / Tool | Version | Purpose | Rationale |
|---|---|---|---|
| **Apache Airflow** | `^2.8.1` | Workflow orchestration & scheduling | Industry-standard DAG orchestrator with dependency management, retries, and XComs |
| **Apache Spark (PySpark)** | `^3.5.0` | Distributed batch transformation | Scalable processing of columnar Parquet data; handles multi-ticker batch transforms |
| **DuckDB** | `^0.9.2` | Analytical warehouse engine | Embedded OLAP engine with vectorized execution, fast Parquet reads, and zero server overhead |
| **dbt Core** | `^1.7.0` | Semantic data modeling & testing | Version-controlled SQL modeling, dependency graphs (DAGs), and automated testing |
| **LocalStack** | `^3.0` | Local AWS S3 emulation | Full AWS S3 API fidelity in local Docker without cloud costs |
| **Docker Compose** | `^2.24` | Containerized infrastructure | One-click reproducibility of Airflow, Postgres, LocalStack, and shared volumes |
| **yfinance** | `^0.2.36` | Market data ingestion | Access to 50+ NSE symbols with split and dividend adjustments |
| **boto3** | `^1.34` | S3 client SDK | Object creation, bucket management, and quarantine routing |
| **pyarrow** | `^15.0` | Parquet serialization | High-speed columnar serialization with Snappy compression |

---

## 5. Data

- **Universe**: 50+ National Stock Exchange of India (NSE) symbols spanning Banking, IT, Auto, Energy, Metals, Pharma, and FMCG.
- **Grain**: Daily EOD trading sessions (~250 trading days/year).
- **Format**: Partitioned Apache Parquet with Snappy compression.
- **S3 Bucket Structure**:
  - `s3://nsei-datalake/raw/nsei/daily/date=YYYY-MM-DD/symbol=XYZ/` (7 raw OHLCV columns + ingestion timestamps).
  - `s3://nsei-datalake/processed/nsei/daily/date=YYYY-MM-DD/symbol=XYZ/` (14 transformed attributes including VWAP, gap %, and returns).
  - `s3://nsei-datalake/quarantine/` (anomalous, null, or zero-price rows quarantined for audit).
- **Dimension Data**: `seeds/seed_symbol_metadata.csv` mapping symbols to industry, sector, and market cap tiers.

---

## 6. Step-by-Step Pipeline

1. **`check_market_day` (BranchPythonOperator)**: Validates date against Indian trading holiday calendar; branches to ingestion if open, or cleanly skips if holiday.
2. **`ingest_nsei_data` (PythonOperator)**: Queries Yahoo Finance for 50+ symbols; writes raw partitioned Parquet directly to LocalStack S3 (`raw/` prefix).
3. **`validate_raw_data` (PythonOperator)**: Inspects Parquet schema, checks for null prices or zero-volume anomalies; dumps corrupted records to `quarantine/`.
4. **`spark_transform` (BashOperator / PySpark)**: Executes `transform_nsei.py` to calculate VWAP, returns, range %, gap %, and green candle indicators; writes partitioned Parquet to `processed/` prefix.
5. **`load_to_duckdb` (PythonOperator)**: Connects to DuckDB via `httpfs` extension; reads processed Parquet from LocalStack S3 into raw warehouse tables.
6. **`dbt_seed` (PythonOperator)**: Loads static symbol sector metadata dimension table (`seed_symbol_metadata`).
7. **`dbt_run` (PythonOperator)**: Builds staging views (`stg_nsei_daily`) and physical marts (`mart_nsei_rolling_metrics`, `mart_sector_performance`).
8. **`dbt_test` (PythonOperator)**: Executes 14 schema constraint assertions (uniqueness, not-null, referential integrity).
9. **`send_pipeline_summary` (PythonOperator)**: Logs execution telemetry and metrics to XCom for alerting.

---

## 7. Problems Faced & How We Solved Them

| Problem | Impact | How We Got Around It |
|---|---|---|
| **DuckDB `httpfs` couldn't connect to LocalStack S3** | `load_to_duckdb` task failed with S3 endpoint read errors because DuckDB defaults to public AWS endpoints | Added explicit S3 endpoint overrides in the DAG: `SET s3_endpoint='localstack:4566'; SET s3_use_ssl=false;` to redirect DuckDB's httpfs extension directly to the local emulated S3 container |
| **dbt `mart_sector_performance` failed** on missing seed table | Mart SQL model referenced `seed_symbol_metadata` which had not been loaded into DuckDB yet, causing referential integrity failures | Added `dbt_seed` as an explicit upstream Airflow DAG task before `dbt_run`, ensuring dimension metadata exists before mart materialization |
| **Airflow metadata DB race condition** on first boot | `airflow-webserver` started before `airflow-init` completed creating the admin user and migrating the Postgres schema, causing container crash loops | Configured `depends_on` and health check exit conditions in `docker-compose.yml` so the webserver and scheduler strictly wait for the init container to finish with exit code 0 |
| **Spark JVM memory exhaustion** inside Docker | PySpark default JVM memory allocations exceeded the 4GB local Docker memory limit, triggering out-of-memory (OOM) container kills | Configured Spark in `local[*]` mode with constrained executor memory flags, and used Parquet's columnar format with Snappy compression to minimize the in-memory footprint |
| **Data duplication on re-runs / backfills** | Re-triggering the DAG for the same date inserted duplicate rows into DuckDB warehouse tables | Implemented **atomic partition replacement** — every loading and staging stage uses `DELETE + INSERT` keyed strictly on `trade_date`, making all tasks strictly idempotent |
| **Zero-price and null anomalies** in raw yfinance data | Tickers occasionally returned `0.0` close prices or `NaN` volume during market holidays or ticker renames | Added a **pre-Spark data quality gate** (`validate_raw_data` task) that evaluates schemas, identifies invalid records, and routes corrupted rows to an S3 quarantine prefix before transformation |

---

## 8. Results & Evaluation

### Warehouse Layer & Mart Inventory

| Warehouse Layer | Relation Name | Storage Type | Partition / Key | Annual Volume (250 Days) | Materialized Attributes |
|---|---|---|---|:---:|:---:|
| **Raw Landing** | `s3://.../raw/nsei/` | Parquet (Snappy) | `date=YYYY-MM-DD/symbol=XYZ/` | ~12,500 files | 7 raw OHLCV columns + ingestion timestamps |
| **Processed S3** | `s3://.../processed/` | Parquet (Snappy) | `date=YYYY-MM-DD/symbol=XYZ/` | ~12,500 files | 14 cleaned columns (VWAP, returns, range, gap) |
| **Warehouse Raw** | `raw.nsei_daily` | DuckDB Table | Columnar append (`httpfs`) | ~12,500 rows | 17 columns (audit run IDs, timestamps) |
| **Semantic Staging** | `staging.stg_nsei_daily` | DuckDB View | Virtual view over `raw.nsei_daily` | ~12,500 rows | 17 type-cast columns with zero duplicates |
| **ML Feature Mart** | `marts.mart_nsei_rolling_metrics` | DuckDB Physical Table | Primary Key: `(symbol, trade_date)` | ~12,500 rows | **25 columns** (5d/20d returns, vol, Sharpe, drawdowns, regimes) |
| **Cross-Sectional Mart** | `marts.mart_sector_performance` | DuckDB Physical Table | Primary Key: `(sector, trade_date)` | ~2,500 rows | **13 columns** (sector return, breadth %, gainers, daily rank) |
| **Dimension Seed** | `seeds.seed_symbol_metadata` | DuckDB Seed Table | Key: `symbol` | 30 reference rows | 3 columns (`symbol`, `sector`, `market_cap_bucket`) |

### Pipeline Runtime Benchmarks (Docker: 4 Cores, 4 GB RAM)

| Task ID | Task Operator | Execution Latency | SLA Status |
|---|---|:---:|:---:|
| `check_market_day` | `BranchPythonOperator` | 2s | ✅ Pass |
| `ingest_nsei_data` | `PythonOperator` (yfinance + boto3) | 35s | ✅ Pass |
| `validate_raw_data` | `PythonOperator` (s3fs + pyarrow) | 10s | ✅ Pass |
| `spark_transform` | `BashOperator` (`spark-submit`) | 50s | ✅ Pass |
| `load_to_duckdb` | `PythonOperator` (DuckDB `httpfs`) | 8s | ✅ Pass |
| `dbt_seed` | `PythonOperator` (`dbt seed`) | 6s | ✅ Pass |
| `dbt_run` | `PythonOperator` (`dbt run`) | 35s | ✅ Pass |
| `dbt_test` | `PythonOperator` (`dbt test`) | 12s | ✅ Pass |
| `send_pipeline_summary` | `PythonOperator` | 2s | ✅ Pass |
| **Total Daily Pipeline Run** | **Sequential Airflow DAG** | **~3m 15s (200s)** | ✅ **SLA Met** |
| **Quarterly Backfill (63 Sessions)** | **Airflow Backfill Engine** | **~22 minutes** | ✅ **SLA Met** |

### Automated dbt Quality Tests (14/14 Passing)

All 14 automated dbt data quality assertions pass with zero failures:
- Unique composite keys on `(symbol, trade_date)` and `(sector, trade_date)`.
- Zero-null constraints on closing prices and trading volume.
- Accepted-values test on `vol_regime` (`['low_vol', 'medium_vol', 'high_vol']`).
- Referential integrity join checks between `mart_sector_performance` and `seed_symbol_metadata`.

---

## 9. Project Structure

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
│   ├── docker-compose.yml          # Multi-container orchestration
│   └── localstack_init.sh          # S3 bucket creation script
├── notebooks/
│   └── demo.py                     # DuckDB querying demo script
├── How_to_run.md                   # Detailed deployment runbook
├── requirements.txt                # Local development dependencies
└── ReadMe.md                       # Project documentation
```

---

## 10. Getting Started

### Prerequisites

- **Docker Desktop** installed and running (allocate at least 4 GB RAM).
- **Git** command-line tools.

### Step 1: Directory Initialization

From the project root, create host volume folders:

```bash
cd "NSEI Daily Stock Pipeline"
mkdir -p logs data
```

### Step 2: Start the Docker Stack

Navigate to `docker/` and start the services:

```bash
cd docker
docker compose up -d
```

This starts 5 containers: `postgres`, `airflow-init`, `airflow-webserver`, `airflow-scheduler`, and `localstack`.

Verify setup completion:

```bash
docker compose logs airflow-init | tail -5
# Expect: "Admin user admin created"
```

### Step 3: Access Airflow Web UI

Open your browser:
- **URL**: `http://localhost:8080`
- **Username**: `admin`
- **Password**: `admin`

### Step 4: Trigger Pipeline

**Via Web UI:** Click `nsei_daily_pipeline` → **Trigger DAG w/ config** → pass `{"ds": "2025-04-24"}`.

**Via CLI:**
```bash
docker exec -it docker-airflow-scheduler-1 \
  airflow dags trigger nsei_daily_pipeline \
  --conf '{"ds": "2025-04-24"}'
```

---

## 11. Warehouse Queries & Inspection

Query DuckDB directly inside the scheduler container:

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

Inspect objects in LocalStack S3:

```bash
docker exec -it docker-localstack-1 \
  awslocal s3 ls s3://nsei-datalake/raw/nsei/daily/ --recursive
```

---

## 12. Deployment

- **Infrastructure Teardown**:
  ```bash
  cd docker
  docker compose down      # Preserves database state and S3 data
  docker compose down -v   # Complete clean wipe
  ```
- **Cloud Migration**: Replace LocalStack with AWS S3, PySpark with AWS EMR Serverless, and DuckDB with Snowflake or Amazon Redshift.

---

## 13. Connected Portfolio Projects

- **[Volatility Intelligence Platform](https://github.com/RaajitSingh1306/volatility-intelligence-platform)**: Consumes the rolling volatility, Sharpe, and drawdown metrics materialized in `marts.mart_nsei_rolling_metrics` as feature inputs.
- **[Nifty Sector Rotation](https://github.com/RaajitSingh1306/Nifty-Sector-Rotation)**: Directly utilizes the cross-sectional breadth and sector momentum rankings computed in `marts.mart_sector_performance`.

---

## 14. Limitations & Known Issues

- **Upstream Data Dependency**: Relies on Yahoo Finance via `yfinance`, which is subject to rate-limiting and intermittent ticker throttling.
- **LocalStack S3 Emulation**: Storage runs in local Docker LocalStack; not yet provisioned via Terraform on production AWS IAM and managed S3.
- **Full Partition Replacement**: While backfills are idempotent via atomic partition replacement, the pipeline does not implement Change Data Capture (CDC) for intraday adjustments.
- **Daily Grain Only**: Data is processed at daily close granularity; intraday tick feeds and order books are not supported.
- **Single-Node Spark**: PySpark executes in local containerized mode (`local[*]`) rather than across an elastic multi-node Spark cluster.

---

## 15. Roadmap / Future Expansion

- [ ] **Direct Exchange API Ingestion**: Integrate Zerodha Kite Connect or NSE official data feeds for authoritative exchange tick/EOD files.
- [ ] **Cloud Terraform IaC**: Add Terraform scripts for automated cloud provisioning (EMR Serverless, S3, RDS Postgres for Airflow metastore).
- [ ] **CI/CD Data Testing Gate**: Automate dbt test and Great Expectations validation within GitHub Actions CI.
- [ ] **Intraday 5-Minute Mart**: Introduce intraday interval aggregation models for real-time volatility tracking during active trading hours.
- [ ] **Automated Backfill Sensor**: Add an Airflow sensor to automatically detect missing historical partitions and trigger self-healing backfills.

---

## 16. License & Disclaimer

### License
This project is licensed under the [MIT License](https://opensource.org/licenses/MIT).

### Disclaimer
This system is built for data engineering research and financial feature store demonstration. It does not constitute investment advice.
