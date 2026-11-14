"""Speed layer — PySpark Structured Streaming job.

Reads validated payment events from the xrpl.payments Kafka topic,
parses the three fields needed for feature computation (account,
event_time, amount_xrp), and writes each row to a local JSON file sink.

This is step 1 (parse + dump). Windowed aggregations (10-min velocity,
watermarking) will be added on top once the end-to-end flow is verified.

Data flow:
    xrpl.payments (Kafka)
        → parse JSON fields
        → append to /tmp/speed_layer/payments/

Submit:
    docker exec xrpl-spark-master /opt/spark/bin/spark-submit \\
      --master spark://spark-master:7077 \\
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
      /opt/spark/jobs/job.py

EMR migration: replace KAFKA_BOOTSTRAP with the MSK broker endpoint and
OUTPUT_PATH / CHECKPOINT_PATH with s3:// paths. No other changes needed.
"""
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object
from pyspark.sql.types import DoubleType


KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "xrpl.payments"
SPARK_MASTER = "spark://spark-master:7077"
OUTPUT_PATH = "/tmp/speed_layer/payments"
CHECKPOINT_PATH = "/tmp/speed_layer/checkpoints/payments"
MAX_BATCHES = 20


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("xrpl-speed-layer")
        .master(SPARK_MASTER)
        .getOrCreate()
    )


def run():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Kafka delivers messages as binary; cast value to string to get the JSON envelope.
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Extract the three fields needed for velocity features.
    # amount_xrp is null for token payments — those are kept for counting but
    # excluded from XRP sum aggregations downstream.
    # just adding transformation
    payments = (
        raw
        .selectExpr("CAST(value AS STRING) AS json")
        .select(
            get_json_object(col("json"), "$.account").alias("account"),
            get_json_object(col("json"), "$.event_time").alias("event_time"),
            get_json_object(col("json"), "$.amount_xrp").cast(DoubleType()).alias("amount_xrp"),
        )
    )

    #  start() is what kicks off the stream
    query = (
        payments.writeStream
        .format("json")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start()
    )

    # Dev-mode cap: stop after N completed micro-batches so the job exits
    # on its own instead of running until killed. recentProgress is capped
    # at ~100 entries so we track completed batchIds explicitly.
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
