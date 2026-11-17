# Speed Layer

PySpark Structured Streaming job that computes real-time velocity features per XRPL account.

## What it does

Reads validated payment events from `xrpl.payments`, aggregates them into 10-minute windows per account, and writes the results to a JSON file sink (DynamoDB in production).

**Output per row:**
```json
{
  "window": {"start": "2026-11-14T14:50:00.000Z", "end": "2026-11-14T15:00:00.000Z"},
  "account": "rUjfTQpvBr6wsGGxMw6sRmRQGG76nvp8Ln",
  "payment_count": 22,
  "total_amount_xrp": 5519.53
}
```

- `payment_count` — how many payments the account sent in the window
- `total_amount_xrp` — total XRP moved; null for token-only payments

## Data flow

```
xrpl.payments (Kafka)
    → parse: account, event_time (Timestamp), amount_xrp
    → withWatermark("event_time", "2 min")
    → groupBy(10-min window, account)
    → agg(count, sum)
    → /tmp/speed_layer/features/
```

## Run

Start the stack first if not already up:
```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.spark.yml up -d
```

Submit the job:
```bash
docker exec xrpl-spark-master mkdir -p /tmp/.ivy2/cache && docker exec xrpl-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.jars.ivy=/tmp/.ivy2 /opt/spark/jobs/job.py
```

Output lands in `data/speed_layer/features/` on the host (mounted from `/tmp/speed_layer/features/` inside the container).

## Production swap

| What | Dev | Production |
|---|---|---|
| Sink | JSON file (`/tmp/speed_layer/features/`) | DynamoDB `account_features`, sort key `rt`, TTL 2h |
| Kafka bootstrap | `kafka:29092` | MSK broker endpoint |
| Output/checkpoint paths | `/tmp/...` | `s3://...` |
| `startingOffsets` | `earliest` | `latest` |
| `MAX_BATCHES` cap | 20 | remove — use `.awaitTermination()` |

## Concepts

Watermark, event_time vs received_at, windowing → **Notion: XRPL Study Log**
