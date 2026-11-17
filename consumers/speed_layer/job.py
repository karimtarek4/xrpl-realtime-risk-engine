"""Speed layer — PySpark Structured Streaming job.

Reads validated payment envelopes from xrpl.payments, computes 10-minute
velocity features per account (payment_count, total_amount_xrp), and writes
the results to a local JSON file sink.

Concepts (watermark, event_time vs received_at, windowing) → Notion: XRPL Study Log

Data flow:
    xrpl.payments (Kafka)
        → parse: account, event_time (Timestamp), amount_xrp
        → withWatermark("event_time", "2 min")
        → groupBy(10-min window, account)
        → agg(count, sum)
        → append to /tmp/speed_layer/features/

Submit:
    docker exec xrpl-spark-master /opt/spark/bin/spark-submit \\
      --master spark://spark-master:7077 \\
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
      /opt/spark/jobs/job.py

Production swap: replace KAFKA_BOOTSTRAP with MSK endpoint, OUTPUT_PATH /
CHECKPOINT_PATH with s3:// paths. No logic changes needed.
"""
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, get_json_object, sum as spark_sum, window
from pyspark.sql.types import DoubleType, TimestampType


KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "xrpl.payments"
SPARK_MASTER = "spark://spark-master:7077"
OUTPUT_PATH = "/tmp/speed_layer/features"
CHECKPOINT_PATH = "/tmp/speed_layer/checkpoints/features"
WINDOW_DURATION = "10 minutes"
WATERMARK_DELAY = "2 minutes"

# Dev only — stop after N micro-batches so the job exits without being killed.
MAX_BATCHES = 20


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("xrpl-speed-layer")
        .master(SPARK_MASTER)
        .getOrCreate()
    )


def parse_payments(raw):
    """Pull account, event_time, amount_xrp out of the Kafka JSON envelope.

    - Kafka value arrives as bytes → cast to string first.
    - event_time cast to TimestampType: required by withWatermark and window().
    - amount_xrp is null for token (non-XRP) payments; kept so they still
      contribute to payment_count, just not to total_amount_xrp.
    """
    return (
        raw
        .selectExpr("CAST(value AS STRING) AS json")
        .select(
            get_json_object(col("json"), "$.account").alias("account"),
            get_json_object(col("json"), "$.event_time")
                .cast(TimestampType())
                .alias("event_time"),
            get_json_object(col("json"), "$.amount_xrp")
                .cast(DoubleType())
                .alias("amount_xrp"),
        )
    )


def compute_velocity_features(payments):
    """Aggregate payments into 10-minute per-account velocity features.

    - withWatermark before groupBy: Spark requires this order.
    - window() slices event_time into 10-min buckets; each event falls into
      exactly one bucket based on when it happened on the ledger.
    - spark_sum alias avoids shadowing Python's built-in sum.

    Output row: { window: {start, end}, account, payment_count, total_amount_xrp }
    """
    return (
        payments
        .withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(
            window(col("event_time"), WINDOW_DURATION),
            col("account"),
        )
        .agg(
            count("*").alias("payment_count"),
            spark_sum("amount_xrp").alias("total_amount_xrp"),
        )
    )


def run():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        # "earliest" replays all available messages on startup — swap to
        # "latest" in production so we only process new events.
        .option("startingOffsets", "earliest")
        .load()
    )

    payments = parse_payments(raw)
    features = compute_velocity_features(payments)

    # outputMode("append"): with watermark + aggregation, Spark holds a window
    # result until it's final (watermark passed the window end), then emits it
    # once. "complete" or "update" would emit partial counts mid-window.
    query = (
        features.writeStream
        .format("json")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start()
    )

    # recentProgress is a ring buffer (~100 entries); deduplicate by batchId.
    # Without this while loop, the main thread would exit immediately after .start(),
    seen_batches: set[int] = set()
    while query.isActive:
        for p in query.recentProgress:
            seen_batches.add(p["batchId"])
        if len(seen_batches) >= MAX_BATCHES:
            query.stop()
            break
        time.sleep(1)


if __name__ == "__main__":
    run()
