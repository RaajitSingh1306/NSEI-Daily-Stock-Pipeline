# NSEI Daily Stock Pipeline

End-to-end data engineering pipeline: **NSE India API → S3 → PySpark → DuckDB → dbt**

## Architecture

```
[NSE/yfinance API]
       │
       ▼ (Airflow: ingest_nsei_data)
[S3 raw layer]
 s3://nsei-datalake/raw/nsei/daily/date=YYYY-MM-DD/symbol=XYZ/
       │
       ▼ (Airflow: validate_raw_data)
[Data quality gate]  ── bad rows ──► S3 quarantine
       │
       ▼ (Airflow: spark_transform)
[PySpark: transform_nsei.py]
 • daily_return, VWAP, dollar_volume
 • intraday_range_pct, gap_pct, is_green
 • Quarantine anomalies
       │
       ▼ written to S3 processed layer
[S3 processed layer]
 s3://nsei-datalake/processed/nsei/daily/date=YYYY-MM-DD/symbol=XYZ/
       │
       ▼ (Airflow: load_to_duckdb)
[DuckDB warehouse]  raw.nsei_daily
       │
       ▼ (Airflow: dbt_run → dbt_test)
[dbt models]
 staging.stg_nsei_daily
 marts.mart_nsei_rolling_metrics   ← 5/10/20d rolling metrics + Sharpe + drawdown
 marts.mart_sector_performance     ← sector-level aggregation + breadth
```

## Project Structure

```
nsei_pipeline/
├── dags/
│   └── nsei_pipeline_dag.py       # Airflow DAG
├── spark_jobs/
│   ├── transform_nsei.py          # PySpark transform
│   └── nsei_utils.py              # Shared utilities (trading day checker)
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       │   ├── stg_nsei_daily.sql
│       │   └── schema.yml
│       └── marts/
│           ├── mart_nsei_rolling_metrics.sql
│           └── mart_sector_performance.sql
└── docker/
    ├── docker-compose.yml         # Local dev stack
    └── localstack_init.sh
```

## Quick Start (local)

```bash
# 1. Start stack
cd docker && docker compose up -d

# 2. Create S3 buckets (auto via localstack_init.sh)

# 3. Trigger pipeline manually for a date
docker exec -it <airflow-scheduler> airflow dags trigger \
  nsei_daily_pipeline --conf '{"ds": "2024-01-15"}'

# 4. Open Airflow UI
open http://localhost:8080   # admin / admin

# 5. Query DuckDB directly
python3 -c "
import duckdb
con = duckdb.connect('/opt/warehouse/nsei.duckdb')
print(con.execute('SELECT * FROM marts.mart_nsei_rolling_metrics LIMIT 5').df())
"
```

## Key Design Decisions

| Decision | Choice | Reason |
|---|---|---|
| File format | Parquet + Snappy | Columnar, splittable, efficient for analytics |
| Partitioning | date= / symbol= | Predicate pushdown on most common query patterns |
| Idempotency | DELETE + INSERT on trade_date | Safe reruns without duplicates |
| DQ gate | Pre-Spark validation + quarantine | Fail fast, preserve bad rows for debugging |
| dbt materialisation | staging=view, marts=table | Views for lineage, tables for query performance |

## Main Talking Points

- **Scale**: handles 50+ symbols daily, ~18,000 rows/year per symbol
- **Reliability**: 2x retry with exponential backoff, market-day gate, quarantine layer
- **Idempotency**: full pipeline re-runnable for any date without side effects
- **Observability**: XCom-based metrics, dbt test suite, DQ issue tracking
- **Feature store**: `mart_nsei_rolling_metrics` doubles as ML feature table for volatility regime classifier (P1 project)
