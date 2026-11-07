"""Connect to real XRPL and publish envelopes to Kafka xrpl.raw topic.

Usage:
    python kafka_sink.py                        # run indefinitely (Ctrl+C to stop)
    python kafka_sink.py 50                     # stop after 50 messages
    python kafka_sink.py 50 localhost:9092      # custom broker address
"""
import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from xrpl_producer.sinks import KafkaProducerSink, sink_messages


async def run(limit: int = 0, bootstrap_servers: str = "localhost:9092"):
    indefinite = limit == 0
    if indefinite:
        print(f"Connecting to XRPL → Kafka ({bootstrap_servers}) ... (Ctrl+C to stop)")
    else:
        print(f"Connecting to XRPL → Kafka ({bootstrap_servers}) ... (stopping after {limit} messages)")
    print()

    async with KafkaProducerSink(bootstrap_servers) as sink:
        count = 0
        async for envelope in sink_messages(sink):
            count += 1
            tx_json = envelope["payload"].get("tx_json", {})
            tx_type = tx_json.get("TransactionType", "?")
            result  = envelope["payload"].get("meta", {}).get("TransactionResult", "?")
            print(f"[{count:>5}] {envelope['event_id'][:16]}...  type={tx_type:<15} result={result}  → xrpl.raw")
            if not indefinite and count >= limit:
                break

    print(f"\nDone. {count} messages sent to xrpl.raw.")


if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    broker = sys.argv[2] if len(sys.argv) > 2 else "localhost:9092"
    try:
        asyncio.run(run(limit=limit, bootstrap_servers=broker))
    except KeyboardInterrupt:
        print("\nStopped.")
