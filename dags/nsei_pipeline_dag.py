"""
NSEI Daily Stock Pipeline
Ingests NSEI OHLCV data → S3 (raw) → PySpark transform → S3 (processed) → DuckDB → dbt
Schedule: 18:30 IST Mon–Fri (13:00 UTC)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

import logging

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "raajit",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

S3_BUCKET          = Variable.get("nsei_s3_bucket",  default_var="nsei-datalake")
S3_RAW_PREFIX      = "raw/nsei/daily"
S3_PROCESSED_PREFIX = "processed/nsei/daily"
DUCKDB_PATH        = Variable.get("duckdb_path",     default_var="/opt/warehouse/nsei.duckdb")
SYMBOLS_STR        = Variable.get("nsei_symbols",    default_var="RELIANCE,TCS,INFY,HDFCBANK,ICICIBANK,SBIN,BAJFINANCE,LT,HINDUNILVR,KOTAKBANK")
SPARK_MASTER       = Variable.get("spark_master",    default_var="local[*]")

DBT_PROJECT_DIR    = "/opt/nsei_pipeline/dbt_project"
DBT_PROFILES_DIR   = "/opt/dbt"


# ── Callables ─────────────────────────────────────────────────────────────────

# def check_market_day(**context) -> str:
#     import sys
#     sys.path.insert(0, "/opt/nsei_pipeline")
#     from spark_jobs.nsei_utils import is_trading_day

#     ds = context["ds"]
#     if is_trading_day(ds):
#         return "ingest_nsei_data"
#     logger.info("%s is not a trading day — skipping", ds)
#     return "skip_non_trading_day"

def check_market_day(**context) -> str:
    return "ingest_nsei_data"

def ingest_nsei_data(**context):
    import io
    from datetime import datetime, timedelta

    import boto3
    import pyarrow as pa
    import pyarrow.parquet as pq
    import yfinance as yf

    ds      = context["ds"]
    symbols = [s.strip() for s in SYMBOLS_STR.split(",")]
    end_dt  = (datetime.strptime(ds, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    s3 = boto3.client("s3")
    ingested, failed = [], []

    for symbol in symbols:
        ticker = f"{symbol}.NS"
        try:
            df = yf.download(
                ticker,
                start=ds,
                end=end_dt,
                interval="1d",
                progress=False,
                auto_adjust=True,
            )
            if df.empty:
                logger.warning("No data for %s on %s", symbol, ds)
                failed.append(symbol)
                continue

            df = df.reset_index()
            df.columns = [str(c[0]).lower().replace(" ", "_") if isinstance(c, tuple) else str(c).lower().replace(" ", "_") for c in df.columns]
            df["symbol"]      = symbol
            df["ingested_at"] = datetime.utcnow().isoformat()
            df["source"]      = "yfinance"

            table = pa.Table.from_pandas(df)
            buf   = io.BytesIO()
            pq.write_table(table, buf)
            buf.seek(0)

            key = f"{S3_RAW_PREFIX}/date={ds}/symbol={symbol}/data.parquet"
            s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.read())
            ingested.append(symbol)
            logger.info("Ingested %s → s3://%s/%s", symbol, S3_BUCKET, key)

        except Exception as exc:
            logger.error("Failed %s: %s", symbol, exc)
            failed.append(symbol)

    context["ti"].xcom_push(key="ingested_symbols", value=ingested)
    context["ti"].xcom_push(key="failed_symbols",   value=failed)

    if not ingested:
        raise ValueError(f"All symbols failed ingestion for {ds}")

    logger.info("Ingestion complete: %d ok, %d failed", len(ingested), len(failed))


def validate_raw_data(**context):
    import s3fs
    import pyarrow.parquet as pq

    ds       = context["ds"]
    ingested = context["ti"].xcom_pull(key="ingested_symbols", task_ids="ingest_nsei_data") or []

    fs     = s3fs.S3FileSystem(
        key="test",
        secret="test",
        endpoint_url="http://localstack:4566",
        client_kwargs={"region_name": "ap-south-1"},
    )
    issues = []

    for symbol in ingested:
        path = f"{S3_BUCKET}/{S3_RAW_PREFIX}/date={ds}/symbol={symbol}/data.parquet"
        try:
            df = pq.read_table(f"s3://{path}", filesystem=fs).to_pandas()

            if len(df) == 0:
                issues.append(f"{symbol}: empty file")
                continue

            nulls = df[["open", "high", "low", "close", "volume"]].isnull().sum().sum()
            if nulls > 0:
                issues.append(f"{symbol}: {nulls} null OHLCV values")

            if (df["high"] < df["low"]).any():
                issues.append(f"{symbol}: high < low anomaly")

            if (df["close"] <= 0).any():
                issues.append(f"{symbol}: non-positive close price")

        except Exception as exc:
            issues.append(f"{symbol}: validation error — {exc}")

    if issues:
        logger.warning("DQ issues: %s", issues)

    context["ti"].xcom_push(key="dq_issues", value=issues)
    logger.info("Validation complete — %d issue(s)", len(issues))


def load_to_duckdb(**context):
    import duckdb

    ds  = context["ds"]
    con = duckdb.connect(DUCKDB_PATH)

    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL parquet; LOAD parquet;")

    con.execute(f"""
        SET s3_region='ap-south-1';
        SET s3_endpoint='localstack:4566';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
        SET s3_access_key_id='test';
        SET s3_secret_access_key='test';
    """)

    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw.nsei_daily (
            symbol               VARCHAR,
            trade_date           DATE,
            open                 DOUBLE,
            high                 DOUBLE,
            low                  DOUBLE,
            close                DOUBLE,
            volume               BIGINT,
            prev_close           DOUBLE,
            daily_return         DOUBLE,
            vwap                 DOUBLE,
            dollar_volume        DOUBLE,
            intraday_range_pct   DOUBLE,
            gap_pct              DOUBLE,
            is_green             BOOLEAN,
            ingested_at          TIMESTAMP,
            processed_at         TIMESTAMP,
            pipeline_run_id      VARCHAR
        );
    """)

    processed_path = f"s3://{S3_BUCKET}/{S3_PROCESSED_PREFIX}/date={ds}/**/*.parquet"

    con.execute(f"DELETE FROM raw.nsei_daily WHERE trade_date = '{ds}'")
    con.execute(f"""
        INSERT INTO raw.nsei_daily
        SELECT
            symbol,
            trade_date::DATE,
            open::DOUBLE,
            high::DOUBLE,
            low::DOUBLE,
            close::DOUBLE,
            volume::BIGINT,
            prev_close::DOUBLE,
            daily_return::DOUBLE,
            vwap::DOUBLE,
            dollar_volume::DOUBLE,
            intraday_range_pct::DOUBLE,
            gap_pct::DOUBLE,
            is_green::BOOLEAN,
            ingested_at::TIMESTAMP,
            processed_at::TIMESTAMP,
            '{context["run_id"]}' AS pipeline_run_id
        FROM read_parquet('{processed_path}', hive_partitioning=true)
    """)

    row_count = con.execute(
        f"SELECT COUNT(*) FROM raw.nsei_daily WHERE trade_date = '{ds}'"
    ).fetchone()[0]

    con.close()
    logger.info("Loaded %d rows to DuckDB for %s", row_count, ds)

    if row_count == 0:
        raise ValueError(f"DuckDB load produced 0 rows for {ds}")


def run_dbt_seed(**context):
    import subprocess
    result = subprocess.run(
        ["dbt", "seed", "--profiles-dir", DBT_PROFILES_DIR, "--project-dir", DBT_PROJECT_DIR],
        capture_output=True, text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"dbt seed failed:\n{result.stderr}")


def run_dbt_models(**context):
    import subprocess
    ds = context["ds"]
    result = subprocess.run(
        [
            "dbt", "run",
            "--profiles-dir", DBT_PROFILES_DIR,
            "--project-dir",  DBT_PROJECT_DIR,
            "--vars", f"{{run_date: '{ds}'}}",
        ],
        capture_output=True, text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"dbt run failed:\n{result.stderr}")


def run_dbt_tests(**context):
    import subprocess
    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", DBT_PROFILES_DIR, "--project-dir", DBT_PROJECT_DIR],
        capture_output=True, text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"dbt test failed:\n{result.stderr}")


def send_pipeline_summary(**context):
    ti       = context["ti"]
    ingested = ti.xcom_pull(key="ingested_symbols", task_ids="ingest_nsei_data") or []
    failed   = ti.xcom_pull(key="failed_symbols",   task_ids="ingest_nsei_data") or []
    dq       = ti.xcom_pull(key="dq_issues",        task_ids="validate_raw_data") or []

    summary = (
        f"NSEI Pipeline — {context['ds']}\n"
        f"✓ Ingested : {len(ingested)} symbols\n"
        f"✗ Failed   : {len(failed)} {failed or ''}\n"
        f"⚠ DQ issues: {len(dq)}"
    )
    logger.info("\n%s", summary)


# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="nsei_daily_pipeline",
    default_args=DEFAULT_ARGS,
    description="NSEI OHLCV: yfinance → S3 → PySpark → DuckDB → dbt",
    schedule_interval="30 13 * * 1-5",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["nsei", "finance", "de-pipeline"],
) as dag:

    check_market = BranchPythonOperator(
        task_id="check_market_day",
        python_callable=check_market_day,
    )

    skip = EmptyOperator(task_id="skip_non_trading_day")

    ingest = PythonOperator(
        task_id="ingest_nsei_data",
        python_callable=ingest_nsei_data,
    )

    validate = PythonOperator(
        task_id="validate_raw_data",
        python_callable=validate_raw_data,
    )

    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command=(
            "spark-submit "
            "--master {{ var.value.spark_master }} "
            "--conf spark.sql.adaptive.enabled=true "
            "--conf spark.sql.adaptive.coalescePartitions.enabled=true "
            "--conf spark.hadoop.fs.s3a.access.key=test "
            "--conf spark.hadoop.fs.s3a.secret.key=test "
            "--conf spark.hadoop.fs.s3a.endpoint=http://localstack:4566 "
            "--conf spark.hadoop.fs.s3a.path.style.access=true "
            "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
            "--conf spark.jars.packages=org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "--py-files /opt/nsei_pipeline/spark_jobs/nsei_utils.py "
            "/opt/nsei_pipeline/spark_jobs/transform_nsei.py "
            "--date {{ ds }} "
            f"--input s3a://{S3_BUCKET}/{S3_RAW_PREFIX} "
            f"--output s3a://{S3_BUCKET}/{S3_PROCESSED_PREFIX}"
        ),
        retries=1,
    )

    load_duckdb = PythonOperator(
        task_id="load_to_duckdb",
        python_callable=load_to_duckdb,
    )

    dbt_seed = PythonOperator(
        task_id="dbt_seed",
        python_callable=run_dbt_seed,
    )

    dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=run_dbt_models,
    )

    dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=run_dbt_tests,
    )

    notify = PythonOperator(
        task_id="send_pipeline_summary",
        python_callable=send_pipeline_summary,
        trigger_rule="all_done",
    )

    check_market >> [ingest, skip]
    ingest >> validate >> spark_transform >> load_duckdb >> dbt_seed >> dbt_run >> dbt_test >> notify