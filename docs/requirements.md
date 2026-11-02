# Requirements: Real-Time Risk & Decisioning Platform

> Status: draft (design v5). For how the system is built, see [ARCHITECTURE.md](ARCHITECTURE.md).

## 1. Scope

Score **live XRPL payments** in near real time and return an **explainable risk decision** (`allow` / `review` / `block`) for each one, based on rolling per-account behaviour.

## 2. Priority Levels

| Priority | Meaning | Rule of thumb |
|---|---|---|
| **P0** | Must have | Needed for one working end-to-end flow: payment in → decision out |
| **P1** | Good to have | Makes it production-like: replay, batch layer, BI, API, SLOs |
| **P2** | Later | Hardening and extras: backfill, chaos/load tests, DLQ repair, ML model |

## 3. Functional Requirements (data-flow order)

### 3.1 Ingestion: Producer

| Requirement | Priority |
|---|---|
| Connect to XRPL via WebSocket; subscribe to the `transactions` and `ledger` streams. | P0 |
| Reconnect with backoff and re-subscribe after a disconnect. | P0 |
| Wrap each event in an envelope: `event_id` (tx hash), `event_time`, `received_at`, `ledger_index`, `ingest_mode`, raw payload. | P0 |
| Publish to `xrpl.raw`, key `event_id`; idempotent producer (`enable.idempotence=true`, `acks=all`). | P0 |
| Second endpoint (`s1.ripple.com` + `xrplcluster.com`) with failover. | P2 |
| Track `last_seen_ledger`; log and alert on ledger gaps. | P2 |

### 3.2 Schema Registry

| Requirement | Priority |
|---|---|
| Schema check on every topic write (producer, validator, Spark, decision service). | P1 |
| `xrpl.raw`: envelope schema only (payload kept as raw JSON). Strict schemas from `xrpl.payments` onward. | P1 |
| Backward-compatible mode; schema changes checked in CI. | P2 |

### 3.3 Kafka

| Requirement | Priority |
|---|---|
| Topics: `xrpl.raw`, `xrpl.payments`, `xrpl.dlq`, `xrpl.decisions`. | P0 |
| Partition keys: `xrpl.raw` = `event_id`; `xrpl.payments` = `account_id`; `xrpl.dlq` = `event_id`; `xrpl.decisions` = `account_id`. | P0 |
| Hot partitions: salt keys of heavy accounts (`account_id#0..n`) on account-keyed topics, merge downstream. | P1 |

### 3.4 Validator

| Requirement | Priority |
|---|---|
| Consume `xrpl.raw`; valid → `xrpl.payments`; invalid → `xrpl.dlq` with `reason_code`. | P0 |
| Validity: `Payment`, `tesSUCCESS`, `Account` and `Destination` present. | P0 |
| Normalise amount (XRP drops vs token object) using `delivered_amount`. | P0 |
| Deduplicate by tx hash (`dropDuplicatesWithinWatermark`); hash marked seen only when accepted. | P0 |

### 3.5 Raw Archive

| Requirement | Priority |
|---|---|
| Archiver copies `xrpl.raw` → S3 `/raw/`, partitioned by date; runs continuously, writes in batches. | P1 |
| `/raw/` keeps everything as received (including invalid and late events). | P1 |

### 3.6 Speed Layer (real-time features)

| Requirement | Priority |
|---|---|
| Consume `xrpl.payments`; event time = `close_time_iso`. | P0 |
| Watermark = 2 min; later events skipped for real-time windows and counted (`late_events_total`). | P0 |
| Sliding 10 min: payment count, total amount (velocity). | P0 |
| Sliding 1 h: new destinations (fan-out), failed-tx count. | P1 |
| Session window (30 min inactivity): activity-burst length and amount. | P2 |
| Write real-time features to DynamoDB (`account_id` + `rt`), TTL 2 h. | P0 |
| Checkpointing so restarts do not lose or double-count state. | P0 |

### 3.7 Batch Layer (daily features)

| Requirement | Priority |
|---|---|
| Read the day's S3 `/raw/`, including late events skipped by the speed layer. | P1 |
| Features: 30-day avg/std amount, tx per day, usual counterparties, usual active hours, account age, failed-tx share. | P1 |
| Write batch features to DynamoDB (`account_id` + `batch`), TTL 48 h. | P1 |
| Write S3 `/silver/` (clean) and `/gold/` (feature tables, decisions older than 30 days). | P1 |

### 3.8 Serving Store

| Requirement | Priority |
|---|---|
| Table `account_features`: partition key `account_id`, sort key `feature_set` (`rt` \| `batch`); no overwrites between layers. | P0 |
| TTL attribute `expires_at`; readers ignore items past `expires_at`. | P0 |
| Decision service reads both items in one query and merges them. | P1 |
| Local development with DynamoDB Local; same code against AWS. | P0 |

### 3.9 Decision Service

| Requirement | Priority |
|---|---|
| Triggered per event from `xrpl.payments`; reads features from DynamoDB. | P0 |
| Rules as versioned config, real-time only: velocity (10 min), partial payment (delivered < requested). | P0 |
| Rules needing 1 h windows or batch features: fan-out, failure streak, spend spike (> k × 30-day avg), new account with large amount. | P1 |
| Output to `xrpl.decisions`: `event_id`, `account_id`, `decision`, `score`, `reasons[]`, `rule_version`, `received_at`, `decided_at`. | P0 |
| Model score (rule-based script now; trained model decided later). | P2 |

### 3.10 Decision Storage, API and BI

| Requirement | Priority |
|---|---|
| Sink consumer: `xrpl.decisions` → PostgreSQL, idempotent (`ON CONFLICT (event_id)`), batched inserts. | P0 |
| PostgreSQL partitioned by day, 30 days; older decisions in S3 `/gold/`. | P1 |
| Metabase dashboards: flag rate per rule, top flagged accounts, trends. | P1 |
| FastAPI (read-only): `GET /decisions/{event_id}`, `GET /accounts/{id}/decisions`. | P1 |

### 3.11 Problem Events (DLQ)

| Requirement | Priority |
|---|---|
| One DLQ topic; each event carries `reason_code`, `stage`, `failed_at`, original event (`reason_code` also as a header). DLQ counts by reason in Grafana. | P0 |
| DLQ repair (hourly, Airflow): fixable → republish to `xrpl.raw`; unfixable → S3 `/dlq/`. | P2 |

### 3.12 Replay and Backfill

| Requirement | Priority |
|---|---|
| Replay: re-read S3 `/raw/` into Kafka for a chosen range (rule changes, re-validation). | P1 |
| Backfiller via JSON-RPC (`ledger_index` + `ledger`): gap and historical ranges → `xrpl.raw`; self-throttled on `"warning": "load"`. | P2 |

### 3.13 Observability and Testing

| Requirement | Priority |
|---|---|
| Prometheus scrapes all services: throughput, lag per partition, DLQ by reason, late events, reconnects. | P0 |
| Grafana dashboards on Prometheus. | P0 |
| End-to-end latency SLI (`decided_at − received_at`) with SLO panel and alerts (Slack/email). | P1 |
| Replayer / chaos tool: replay at 10–100x with duplicates, delays, reordering, hot keys, malformed events, bursts. | P2 |
| Load-testing report and operational runbooks. | P2 |

## 4. Non-Functional Requirements

| Attribute | Requirement | Priority |
|---|---|---|
| Durability | No acknowledged event lost: `acks=all`, Spark checkpoints, idempotent sinks; S3 raw archive (P1). | P0 |
| Idempotency | Every sink (PostgreSQL, DynamoDB, S3) tolerates repeated messages. | P0 |
| Burst handling | Kafka buffers bursts; consumers catch up without data loss. | P0 |
| Explainability | Every decision stores reasons and rule version. | P0 |
| State bounds | DynamoDB TTLs: rt 2 h, batch 48 h. PostgreSQL 30 days. | P0 |
| Availability | Local Docker, then a single AWS region for sessions. No multi-region; single points of failure documented as accepted risks. | P0 |
| Latency (SLI/SLO) | SLI = `decided_at − received_at`. SLO = 99% of decisions within 5 s. | P1 |
| Retention | Kafka: `xrpl.raw` 7 d, `xrpl.payments` 3 d, `xrpl.dlq` 14 d, `xrpl.decisions` 7 d. S3 `/raw/` 90 d. PostgreSQL 30 d. | P1 |
| Late-data tolerance | Real time accepts ≤ 2 min late; later events corrected by the daily batch. | P1 |
| Replayability | Any range reprocessable from S3 `/raw/` with new validator or rule versions. | P1 |
| Scalability | Grow from ~1M to 5M tx/day by adding partitions/consumers and Spark executors. | P1 |
| Throughput (load test) | Sustain 5M tx/day (≈58 events/s) and bursts of 5,000 events/s for 10 min; p99 < 5 s; lag recovers within 5 min. | P2 |
| Observability | Every service exposes metrics; every alert has a runbook. | P2 |

## 5. Open Questions

- Is recording the live XRPL feed and replaying it at 10–100x acceptable for volume and failure testing?
- Lambda vs Kappa: keep the batch layer? Proposal: keep it (30-day features; corrects late events).
- BI: is PostgreSQL (30 days) + Metabase enough, or is a real-time OLAP store (Druid / ClickHouse) needed?
- Is 5M tx/day sustained plus 5,000 events/s bursts the right load-test bar?
- Rule-based scoring vs a trained model (deferred).
