"""Option 3: Connect to real XRPL and run the full pipeline live."""
import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from xrpl_producer.sinks import FileSystemSink, sink_messages


HERE = os.path.dirname(os.path.abspath(__file__))


async def run(output_file: str = os.path.join(HERE, "live_output.jsonl"), limit: int = 20):
    print(f"Connecting to XRPL... (will capture {limit} enveloped messages)")
    print(f"Writing to: {output_file}\n")

    async with FileSystemSink(output_file) as sink:
        count = 0
        async for envelope in sink_messages(sink):
            count += 1
            tx_json = envelope["payload"].get("tx_json", {})
            tx_type = tx_json.get("TransactionType", "?")
            result  = envelope["payload"].get("meta", {}).get("TransactionResult", "?")
            print(f"[{count:>3}] {envelope['event_id'][:16]}...  type={tx_type:<15} result={result}")
            if count >= limit:
                break

    print(f"\nDone. {count} messages written to {output_file}")


if __name__ == "__main__":
    asyncio.run(run())
