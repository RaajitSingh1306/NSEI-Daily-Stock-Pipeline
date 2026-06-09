# How to run NSEI Pipeline locally

## Prerequisites

- Docker Desktop installed and running (4 GB RAM allocation minimum)
- Git
- That's it — everything else runs inside Docker

---

## Step 1 — Create the missing local folders

```bash
mkdir -p logs data
```

These are mounted as volumes in docker-compose. Without them Docker will
create them as root-owned and Airflow will fail to write logs.

---

## Step 3 — Start the stack

```bash
cd docker
docker compose up -d
```

This starts 5 containers:
| Container | Purpose | Port |
|---|---|---|
| `postgres` | Airflow metadata DB | internal |
| `airflow-init` | One-time DB migration + admin user | — |
| `airflow-webserver` | Airflow UI | 8080 |
| `airflow-scheduler` | Runs DAGs on schedule | — |
| `localstack` | S3 emulation (no AWS account needed) | 4566 |

**Wait ~60 seconds** for `airflow-init` to finish installing packages and
running `airflow db upgrade`. Check it's done:

```bash
docker compose logs airflow-init | tail -5
# Should end with: "Admin user admin created"
```

---

## Step 4 — Open Airflow UI

```
http://localhost:8080
Username: admin
Password: admin
```

You should see the DAG `nsei_daily_pipeline` listed but **paused**.

---

## Step 5 — Trigger a manual run for a past date

Option A — via UI:
1. Click `nsei_daily_pipeline`
2. Click the ▶ (Trigger DAG) button → "Trigger DAG w/ config"
3. Enter: `{"ds": "2025-04-24"}`
4. Click Trigger

Option B — via CLI:
```bash
# Get the scheduler container name
docker compose ps

# Trigger
docker exec -it docker-airflow-scheduler-1 \
  airflow dags trigger nsei_daily_pipeline \
  --conf '{"ds": "2025-04-24"}'
```

---

## Step 6 — Watch it run

In the Airflow UI → click the DAG → Graph view.

Expected task sequence:
```
check_market_day → ingest_nsei_data → validate_raw_data
  → spark_transform → load_to_duckdb → dbt_run → dbt_test → send_pipeline_summary
```

Note: `spark_transform` runs `spark-submit` in local mode inside the
Airflow container (set via `AIRFLOW_VAR_SPARK_MASTER=local[*]`).

---

## Step 7 — Query the results

```bash
# Open a Python shell inside the Airflow container
docker exec -it docker-airflow-scheduler-1 python3
```

```python
import duckdb
con = duckdb.connect("/opt/warehouse/nsei.duckdb")

# Raw layer
con.execute("SELECT * FROM raw.nsei_daily LIMIT 5").df()

# Staging
con.execute("SELECT * FROM staging.stg_nsei_daily LIMIT 5").df()

# Rolling metrics mart
con.execute("""
    SELECT symbol, trade_date, vol_regime, sharpe_20d, drawdown_from_20d_high
    FROM marts.mart_nsei_rolling_metrics
    ORDER BY trade_date DESC, symbol
    LIMIT 10
""").df()

# Sector performance
con.execute("""
    SELECT trade_date, sector, sector_return, breadth_pct, daily_rank
    FROM marts.mart_sector_performance
    ORDER BY trade_date DESC, daily_rank
""").df()
```

---

## Step 8 — Verify S3 raw files

```bash
# List what landed in LocalStack S3
docker exec -it docker-localstack-1 \
  awslocal s3 ls s3://nsei-datalake/raw/nsei/daily/ --recursive
```

---

## Troubleshooting

**`airflow-init` keeps restarting**
→ Run `docker compose logs airflow-init` — usually a pip install timeout.
→ Fix: add `--timeout 120` to the pip install line in docker-compose.yml.

**`spark_transform` fails with "spark-submit not found"**
→ PySpark isn't installed yet. It's installed by `airflow-init` at startup.
→ Check: `docker exec airflow-scheduler-1 which spark-submit`
→ If missing: `docker exec airflow-scheduler-1 pip install pyspark`

**`load_to_duckdb` fails with S3 read error**
→ The `spark_transform` step wrote to LocalStack, but DuckDB's `httpfs`
   extension needs the LocalStack endpoint override.
→ Fix: in `nsei_pipeline_dag.py`, confirm `load_to_duckdb` sets:
   `con.execute("SET s3_endpoint='localstack:4566'; SET s3_use_ssl=false;")`

**`dbt_run` fails with "profile not found"**
→ Check profiles.yml is mounted: `docker exec airflow-scheduler-1 cat /opt/dbt/profiles.yml`
→ The docker-compose volume maps `./dbt_project/profiles.yml → /opt/dbt/profiles.yml`

**`mart_sector_performance` fails with "seed_symbol_metadata not found"**
→ Run the dbt seed first:
```bash
docker exec -it docker-airflow-scheduler-1 bash -c \
  "cd /opt/nsei_pipeline/dbt_project && dbt seed --profiles-dir /opt/dbt"
```

---

## Backfill a date range

```bash
docker exec -it docker-airflow-scheduler-1 \
  airflow dags backfill nsei_daily_pipeline \
  --start-date 2025-01-01 \
  --end-date 2025-04-01
```

The pipeline is idempotent — safe to re-run any date range.

---

## Stop everything

```bash
cd docker
docker compose down          # stops containers, keeps volumes (data preserved)
docker compose down -v       # stops + deletes all volumes (fresh start)
```