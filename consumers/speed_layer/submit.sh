#!/usr/bin/env bash
# Submit the speed layer PySpark job to the Spark cluster.
#
# Dev:  runs against the local Docker Spark master.
# EMR:  set SPARK_MASTER to the EMR master URL, OUTPUT_PATH and
#       CHECKPOINT_PATH to s3:// paths via environment variables.

set -euo pipefail

SPARK_MASTER="${SPARK_MASTER:-spark://spark-master:7077}"
CONTAINER="${SPARK_CONTAINER:-xrpl-spark-master}"
JOB_PATH="/opt/spark/jobs/job.py"
IVY_DIR="/tmp/.ivy2"

docker exec "$CONTAINER" mkdir -p "$IVY_DIR/cache"

# Install Python dependencies inside the container if not already present.
# boto3 and python-dotenv are not in the base Spark image.
docker exec -e HOME=/tmp "$CONTAINER" pip install --quiet --user boto3 python-dotenv

docker exec \
  -e HOME=/tmp \
  -e PYTHONPATH=/tmp/.local/lib/python3.8/site-packages \
  -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
  -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
  -e AWS_REGION="${AWS_REGION}" \
  "$CONTAINER" \
  /opt/spark/bin/spark-submit \
  --master "$SPARK_MASTER" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.jars.ivy="$IVY_DIR" \
  "$JOB_PATH"
