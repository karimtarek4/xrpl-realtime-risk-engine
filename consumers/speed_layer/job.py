"""Speed layer — PySpark Structured Streaming job.

Reads validated payment envelopes from xrpl.payments, computes 10-minute
velocity features per account (payment_count, total_amount_xrp), and writes
results to two sinks in parallel:
  - DynamoDB (features table, eu-north-1) — serving store for the decision engine

Concepts (watermark, event_time vs received_at, windowing) → Notion: XRPL Study Log

Data flow:
    xrpl.payments (Kafka)
        → parse: account, event_time (Timestamp), amount_xrp
        → withWatermark("event_time", "2 min")
        → groupBy(10-min window, account)
        → agg(count, sum)
        → DynamoDB features table  (partition: account_id, sort: "rt", TTL: now+2h)

Submit:
    bash consumers/speed_layer/submit.sh   (or: make speed-layer)

Production swap: replace KAFKA_BOOTSTRAP with MSK endpoint, OUTPUT_PATH /
CHECKPOINT_PATH with s3:// paths, startingOffsets with "latest". No logic changes.
"""
import os
import time
from datetime import datetime, timezone, timedelta

import boto3
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, get_json_object, sum as spark_sum, window
from pyspark.sql.types import DoubleType, TimestampType

load_dotenv()

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "xrpl.payments"
SPARK_MASTER = "spark://spark-master:7077"
CHECKPOINT_PATH_DYNAMO = "/tmp/speed_layer/checkpoints/features_dynamo"
WINDOW_DURATION = "10 minutes"
WATERMARK_DELAY = "2 minutes"
FEATURE_SET = "rt"
TTL_HOURS = 2

# Dev only — stop after N micro-batches so the job exits without being killed.
MAX_BATCHES = 20


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("xrpl-speed-layer")
        .master(SPARK_MASTER)
        .getOrCreate()
    )


def get_dynamo_table():
    """Return the DynamoDB features table resource.

    Credentials and region come from .env (never hardcoded).
    In production, remove endpoint_url and credentials — boto3 picks them
    up from the IAM role attached to the EMR cluster automatically.
    """
    dynamodb = boto3.resource(
        "dynamodb",
        region_name=os.getenv("AWS_REGION", "eu-north-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    return dynamodb.Table("features")


def write_to_dynamo(batch_df, batch_id):
    """foreachBatch sink — called by Spark once per micro-batch with final rows.

    Each row is an upsert (put_item overwrites if the key already exists).
    This is intentional — we always want the latest window result per account,
    not a history of all windows (that lives in the file sink / S3).

    expires_at is a Unix timestamp (int). DynamoDB's TTL compares it against
    the current time and deletes the item automatically after TTL_HOURS.
    """
    table = get_dynamo_table()
    expires_at = int((datetime.now(timezone.utc) + timedelta(hours=TTL_HOURS)).timestamp())

    # batch_df is a regular (non-streaming) DataFrame inside foreachBatch.
    # collect() pulls all rows to the driver — fine for feature rows which are
    # small aggregates, not raw events.
    rows = batch_df.collect()

    # Multiple windows for the same account can finalize in the same micro-batch.
    # Keep only the latest window_end per account so an older window never
    # overwrites a newer one that was written first in the same batch.
    latest: dict = {}
    for row in rows:
        acct = row["account"]
        if acct not in latest or row["window"]["end"] > latest[acct]["window"]["end"]:
            latest[acct] = row
    rows = list(latest.values())

    with table.batch_writer() as batch:
        for row in rows:
            item = {
                "account_id": row["account"],
                "feature_set": FEATURE_SET,
                "payment_count": row["payment_count"],
                "window_start": row["window"]["start"].isoformat(),
                "window_end": row["window"]["end"].isoformat(),
                "expires_at": expires_at,
            }
            # total_amount_xrp is null for token-only accounts — omit rather
            # than storing None, which DynamoDB rejects.
            if row["total_amount_xrp"] is not None:
                item["total_amount_xrp"] = str(row["total_amount_xrp"])
            batch.put_item(Item=item)

    print(f"[batch {batch_id}] wrote {len(rows)} rows to DynamoDB")


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
        # Don't crash if Kafka was restarted and old offsets are gone.
        .option("failOnDataLoss", "false")
        .load()
    )

    payments = parse_payments(raw)
    features = compute_velocity_features(payments)

    dynamo_query = (
        features.writeStream
        .foreachBatch(write_to_dynamo)
        .option("checkpointLocation", CHECKPOINT_PATH_DYNAMO)
        .outputMode("append")
        .start()
    )

    seen_batches: set[int] = set()
    while dynamo_query.isActive:
        for p in dynamo_query.recentProgress:
            seen_batches.add(p["batchId"])
        if len(seen_batches) >= MAX_BATCHES:
            dynamo_query.stop()
            break
        time.sleep(1)


if __name__ == "__main__":
    run()
