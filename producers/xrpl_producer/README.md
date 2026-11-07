# xrpl_producer

Connects to the XRPL WebSocket, wraps each transaction in an envelope, and publishes it to Kafka (`xrpl.raw`).

```
XRPL WebSocket
      │
      ▼
┌─────────────┐
│  stream.py  │  Layer 1 — open connection, subscribe, yield raw messages
└─────────────┘
      │  raw message
      ▼
┌──────────────┐
│ envelope.py  │  Layer 2 — wrap: event_id, event_time, ledger_index, payload …
└──────────────┘
      │  envelope
      ▼
┌────────────────┐
│ supervisor.py  │  Layer 3 — reconnect with backoff on disconnect
└────────────────┘
      │
      ▼
┌──────────────────────────────────────────┐
│               sinks.py                   │  Layer 5 — publish to sink
│  FileSystemSink        KafkaProducerSink │
│  (dev / experiments)   (production)      │
└──────────────────────────────────────────┘
      │                        │
      ▼                        ▼
  output.jsonl            xrpl.raw (Kafka)
```

## Layers

| Layer | File | Responsibility |
|---|---|---|
| 1 | `stream.py` | Open WebSocket, subscribe to `transactions` + `ledger` streams, yield raw messages |
| 2 | `envelope.py` | Wrap each message: `event_id`, `event_time`, `received_at`, `ledger_index`, `transaction_type`, `ingest_mode`, `payload` |
| 3 | `supervisor.py` | Reconnect with exponential backoff when the stream drops |
| 5 | `sinks.py` | Publish envelopes to a `MessageSink` (`FileSystemSink` for dev, `KafkaProducerSink` for production) |

> Layer 4 (watchdog) is deferred.

## Sinks

```python
# Dev — write to a file
async with FileSystemSink("output.jsonl") as sink:
    async for envelope in sink_messages(sink):
        ...

# Production — publish to Kafka
async with KafkaProducerSink("localhost:9092") as sink:
    async for envelope in sink_messages(sink):
        ...
```

## Envelope schema

```json
{
  "event_id":        "ABC123...",
  "event_time":      "2024-01-01T00:00:00Z",
  "received_at":     "2024-01-01T00:00:00.123Z",
  "ledger_index":    12345678,
  "transaction_type": "Payment",
  "ingest_mode":     "live",
  "payload":         { ...raw XRPL message... }
}
```

## Experiments

Runnable scripts in `experiments/xrpl_producer_sink/`:

| Script | What it does |
|---|---|
| `file_demo.py` | Feed `demo_input.jsonl` through the full pipeline to a file (no network) |
| `xrpl_to_file.py` | Connect to live XRPL, write envelopes to a file |
| `xrpl_to_kafka.py` | Connect to live XRPL, publish envelopes to `xrpl.raw` |

```bash
# Run from the experiments directory
python xrpl_to_kafka.py          # infinite stream
python xrpl_to_kafka.py 50       # stop after 50 messages
```
