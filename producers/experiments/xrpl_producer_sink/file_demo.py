"""Demo: Feed messages from a JSON file to a sink."""
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

from xrpl_producer.sinks import FileSystemSink, sink_messages
from xrpl_producer.stream import Received


async def demo_with_file(input_file: str, output_file: str = "sink_output.jsonl"):
    """Read JSON messages from file, send through sink, write enveloped output.

    Args:
        input_file: path to JSON lines file (one message per line)
        output_file: where to write enveloped messages
    """
    print(f"📖 Reading from: {input_file}")
    print(f"✍️  Writing to: {output_file}")
    print()

    # Load messages from file
    messages = []
    with open(input_file) as f:
        for line in f:
            if line.strip():
                messages.append(json.loads(line))

    print(f"📦 Loaded {len(messages)} messages\n")

    # Helper to envelope messages without network
    from xrpl_producer.envelope import build_envelope

    async with FileSystemSink(output_file) as sink:
        for msg in messages:
            try:
                envelope = build_envelope(msg, datetime.now(timezone.utc), "live")
                await sink.send(envelope)
                event_id = envelope["event_id"]
                ledger = envelope["ledger_index"]
                print(f"✓ {event_id:64} (ledger {ledger})")
            except Exception as e:
                print(f"✗ Failed to envelope: {e}")

    # Show output summary
    with open(output_file) as f:
        output_lines = f.readlines()

    print(f"\n✅ Wrote {len(output_lines)} enveloped messages")
    print(f"📄 First envelope:")
    if output_lines:
        first = json.loads(output_lines[0])
        print(f"   event_id: {first['event_id']}")
        print(f"   event_time: {first['event_time']}")
        print(f"   received_at: {first['received_at']}")
        print(f"   ledger_index: {first['ledger_index']}")
        print(f"   ingest_mode: {first['ingest_mode']}")
        print(f"   payload keys: {list(first['payload'].keys())}")


if __name__ == "__main__":
    test_input = "demo_input.jsonl"

    # Run demo
    asyncio.run(demo_with_file(test_input, "demo_output.jsonl"))

    # Show input vs output
    print("\n" + "=" * 80)
    print("INPUT (raw messages):")
    print("=" * 80)
    with open(test_input) as f:
        for i, line in enumerate(f, 1):
            msg = json.loads(line)
            print(f"{i}. hash={msg.get('hash')}, type={msg.get('type')}")

    print("\n" + "=" * 80)
    print("OUTPUT (enveloped):")
    print("=" * 80)
    with open("demo_output.jsonl") as f:
        for i, line in enumerate(f, 1):
            envelope = json.loads(line)
            print(f"{i}. event_id={envelope['event_id']}, ledger={envelope['ledger_index']}, mode={envelope['ingest_mode']}")
