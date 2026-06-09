"""
PySpark Transform: NSEI Raw → Processed
Reads raw Parquet from S3 (date partition + 1 prior day for LAG),
engineers features, writes processed Parquet back to S3.

Features:
  prev_close          LAG(close, 1) — requires 2-day window read
  daily_return        (close - prev_close) / prev_close
  vwap                (high + low + close) / 3
  dollar_volume       close * volume
  intraday_range_pct  (high - low) / open * 100
  gap_pct             (open - prev_close) / prev_close * 100
  is_green            close >= open

Run:
    spark-submit transform_nsei.py \
        --date  2025-04-24 \
        --input  s3a://nsei-datalake/raw/nsei/daily \
        --output s3a://nsei-datalake/processed/nsei/daily
"""

import argparse
import logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, BooleanType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_spark(s3_endpoint: str = "http://localstack:4566") -> SparkSession:
    return (
        SparkSession.builder
        .appName("nsei_daily_transform")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def _prior_date(ds: str) -> str:
    return (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")


def transform(spark: SparkSession, input_path: str, output_path: str, run_date: str) -> int:
    prior_date = _prior_date(run_date)

    # ── Read today + 1 prior day so LAG has context ───────────────────────────
    frames = []
    for dt in (prior_date, run_date):
        partition_path = f"{input_path}/date={dt}"
        try:
            frames.append(
                spark.read.parquet(partition_path)
                     .withColumn("_partition_date", F.lit(dt))
            )
        except Exception:
            logger.warning("No partition found at %s — skipping", partition_path)

    if not frames:
        raise RuntimeError(f"No input data found for {run_date} or {prior_date}")

    from functools import reduce
    from pyspark.sql import DataFrame
    raw = reduce(DataFrame.union, frames)

    # ── Normalise column names ────────────────────────────────────────────────
    raw = raw.toDF(*[c.lower().replace(" ", "_") for c in raw.columns])

    # Rename 'date' → 'trade_date' if present (yfinance output)
    if "date" in raw.columns:
        raw = raw.withColumnRenamed("date", "trade_date")

    # ── Cast ──────────────────────────────────────────────────────────────────
    raw = (
        raw
        .withColumn("trade_date", F.to_date("trade_date"))
        .withColumn("open",       F.col("open").cast(DoubleType()))
        .withColumn("high",       F.col("high").cast(DoubleType()))
        .withColumn("low",        F.col("low").cast(DoubleType()))
        .withColumn("close",      F.col("close").cast(DoubleType()))
        .withColumn("volume",     F.col("volume").cast(LongType()))
    )

    # ── Quarantine bad rows ───────────────────────────────────────────────────
    bad_filter = (
        F.col("close").isNull() |
        F.col("volume").isNull() |
        (F.col("high") < F.col("low")) |
        (F.col("close") <= 0)
    )
    bad = raw.filter(bad_filter)
    bad_count = bad.count()
    if bad_count > 0:
        logger.warning("Quarantining %d bad rows", bad_count)
        (
            bad.filter(F.col("trade_date") == F.lit(run_date))
               .write.mode("append")
               .parquet(f"{output_path}/_quarantine/date={run_date}")
        )

    clean = raw.filter(~bad_filter)

    # ── Window for LAG ────────────────────────────────────────────────────────
    w = Window.partitionBy("symbol").orderBy("trade_date")

    # ── Feature engineering ───────────────────────────────────────────────────
    enriched = (
        clean
        .withColumn("prev_close",
            F.lag("close", 1).over(w))
        .withColumn("daily_return",
            F.when(
                F.col("prev_close").isNotNull() & (F.col("prev_close") != 0),
                (F.col("close") - F.col("prev_close")) / F.col("prev_close")
            ).otherwise(F.lit(None).cast(DoubleType())))
        .withColumn("vwap",
            (F.col("high") + F.col("low") + F.col("close")) / F.lit(3.0))
        .withColumn("dollar_volume",
            F.col("close") * F.col("volume").cast(DoubleType()))
        .withColumn("intraday_range_pct",
            F.when(F.col("open") != 0,
                (F.col("high") - F.col("low")) / F.col("open") * F.lit(100.0)
            ).otherwise(F.lit(None).cast(DoubleType())))
        .withColumn("gap_pct",
            F.when(
                F.col("prev_close").isNotNull() & (F.col("prev_close") != 0),
                (F.col("open") - F.col("prev_close")) / F.col("prev_close") * F.lit(100.0)
            ).otherwise(F.lit(None).cast(DoubleType())))
        .withColumn("is_green",
            (F.col("close") >= F.col("open")).cast(BooleanType()))
        .withColumn("processed_at", F.current_timestamp())
    )

    # ── Filter to target date only ────────────────────────────────────────────
    output = enriched.filter(F.col("trade_date") == F.lit(run_date))

    output = output.select(
        "symbol",
        "trade_date",
        "open", "high", "low", "close",
        "volume",
        "prev_close",
        "daily_return",
        "vwap",
        "dollar_volume",
        "intraday_range_pct",
        "gap_pct",
        "is_green",
        "ingested_at",
        "processed_at",
    )

    # ── Write processed Parquet ───────────────────────────────────────────────
    (
        output
        .repartition("symbol")
        .write
        .partitionBy("symbol")
        .mode("overwrite")
        .parquet(f"{output_path}/date={run_date}")
    )

    count = output.count()
    logger.info("Wrote %d rows → %s/date=%s", count, output_path, run_date)
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",     required=True, help="YYYY-MM-DD")
    parser.add_argument("--input",    required=True, help="S3 raw base path")
    parser.add_argument("--output",   required=True, help="S3 processed base path")
    parser.add_argument("--s3-endpoint", default="http://localstack:4566")
    args = parser.parse_args()

    spark = get_spark(s3_endpoint=args.s3_endpoint)
    try:
        n = transform(spark, args.input, args.output, args.date)
        if n == 0:
            raise SystemExit(1)
    finally:
        spark.stop()