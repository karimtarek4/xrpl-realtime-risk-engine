"""Tests for Layer 5: Sinks (message publishing)."""
import json
import tempfile
from datetime import datetime, timezone
from typing import Any, List
from unittest.mock import AsyncMock

import pytest

from unittest.mock import patch, MagicMock

from producers.xrpl_producer.sinks import FileSystemSink, KafkaProducerSink, MessageSink, sink_messages
from producers.xrpl_producer.stream import Received, StreamClosed


class CapturesSink(MessageSink):
    """Test sink that captures messages in memory."""

    def __init__(self):
        self.messages: List[dict] = []

    async def send(self, message: dict) -> None:
        self.messages.append(message)


@pytest.mark.asyncio
async def test_filesystem_sink_writes_json_lines():
    """FileSystemSink should write envelopes as valid JSON lines."""
    envelope = {
        "event_id": "tx1",
        "event_time": "2024-01-01T00:00:00Z",
        "received_at": "2024-01-01T00:00:00.001Z",
        "ledger_index": 1,
        "ingest_mode": "live",
        "payload": {"type": "transaction"},
    }

    with tempfile.NamedTemporaryFile(mode="r", delete=False, suffix=".jsonl") as f:
        temp_path = f.name

    async with FileSystemSink(temp_path) as sink:
        await sink.send(envelope)
        await sink.send(envelope)

    # Read and verify
    with open(temp_path) as f:
        lines = f.readlines()

    assert len(lines) == 2
    for line in lines:
        parsed = json.loads(line)
        assert parsed["event_id"] == "tx1"
        assert parsed["payload"]["type"] == "transaction"


@pytest.mark.asyncio
async def test_filesystem_sink_tracks_message_count():
    """FileSystemSink should track how many messages were sent."""
    async with FileSystemSink() as sink:
        await sink.send({"event_id": "tx1"})
        await sink.send({"event_id": "tx2"})
        assert sink.message_count == 2


@pytest.mark.asyncio
async def test_sink_messages_envelopes_and_sends(monkeypatch):
    """sink_messages should envelope messages and send to sink."""
    messages = [
        {"type": "transaction", "hash": "tx1", "close_time_iso": "2024-01-01T00:00:00Z", "ledger_index": 1},
        {"type": "transaction", "hash": "tx2", "close_time_iso": "2024-01-01T00:00:01Z", "ledger_index": 2},
    ]

    async def mock_supervise_stream(url, connect):
        for msg in messages:
            yield Received(received_at=datetime.now(timezone.utc), message=msg)

    monkeypatch.setattr("producers.xrpl_producer.sinks.supervise_stream", mock_supervise_stream)

    sink = CapturesSink()
    envelopes = []

    async for envelope in sink_messages(sink, url="ws://test"):
        envelopes.append(envelope)
        if len(envelopes) >= 2:
            break

    assert len(envelopes) == 2
    assert len(sink.messages) == 2

    # Verify first envelope
    assert envelopes[0]["event_id"] == "tx1"
    assert envelopes[0]["ingest_mode"] == "live"
    assert "payload" in envelopes[0]
    assert envelopes[0]["payload"]["hash"] == "tx1"


@pytest.mark.asyncio
async def test_sink_messages_uses_ingest_mode(monkeypatch):
    """sink_messages should set ingest_mode in envelope."""
    msg = {"type": "transaction", "hash": "tx1", "close_time_iso": "2024-01-01T00:00:00Z", "ledger_index": 1}

    async def mock_supervise_stream(url, connect):
        yield Received(received_at=datetime.now(timezone.utc), message=msg)

    monkeypatch.setattr("producers.xrpl_producer.sinks.supervise_stream", mock_supervise_stream)

    sink = CapturesSink()

    async for envelope in sink_messages(sink, url="ws://test", ingest_mode="backfill"):
        assert envelope["ingest_mode"] == "backfill"
        break


@pytest.mark.asyncio
async def test_sink_messages_skips_unenvelopable_messages(monkeypatch, caplog):
    """sink_messages should skip messages that can't be enveloped and log warning."""
    messages = [
        {"type": "transaction", "hash": "tx1", "close_time_iso": "2024-01-01T00:00:00Z", "ledger_index": 1},
        {"type": "transaction", "hash": "tx2"},  # Missing close_time_iso and ledger_index
        {"type": "transaction", "hash": "tx3", "close_time_iso": "2024-01-01T00:00:02Z", "ledger_index": 3},
    ]

    async def mock_supervise_stream(url, connect):
        for msg in messages:
            yield Received(received_at=datetime.now(timezone.utc), message=msg)

    monkeypatch.setattr("producers.xrpl_producer.sinks.supervise_stream", mock_supervise_stream)

    sink = CapturesSink()
    envelopes = []

    async for envelope in sink_messages(sink, url="ws://test"):
        envelopes.append(envelope)
        if len(envelopes) >= 2:
            break

    # Only 2 valid envelopes (skipped tx2)
    assert len(envelopes) == 2
    assert len(sink.messages) == 2
    assert envelopes[0]["event_id"] == "tx1"
    assert envelopes[1]["event_id"] == "tx3"

    # Should have logged warning
    assert "skipping unenvelopable message" in caplog.text


@pytest.mark.asyncio
async def test_sink_messages_preserves_payload_unchanged(monkeypatch):
    """sink_messages should keep original payload unchanged."""
    msg = {
        "type": "transaction",
        "hash": "tx1",
        "close_time_iso": "2024-01-01T00:00:00Z",
        "ledger_index": 1,
        "Account": "rN7n7otQDd6FczFgLdhmKKdhXqY5QBPg8D",
        "Destination": "rLHzPsX6oXkzU9qL8f7a6Xcxc8S7Lhhhtn",
        "Amount": {"currency": "USD", "issuer": "rvYAfWj5ghj4JqThQ7dQnRycSksV5j8Sx", "value": "100"},
    }

    async def mock_supervise_stream(url, connect):
        yield Received(received_at=datetime.now(timezone.utc), message=msg)

    monkeypatch.setattr("producers.xrpl_producer.sinks.supervise_stream", mock_supervise_stream)

    sink = CapturesSink()

    async for envelope in sink_messages(sink, url="ws://test"):
        # Payload should be identical to input
        assert envelope["payload"] == msg
        break


@pytest.mark.asyncio
async def test_sink_messages_adds_envelope_fields(monkeypatch):
    """sink_messages should add envelope fields (event_id, event_time, etc.)."""
    msg = {"type": "transaction", "hash": "tx1", "close_time_iso": "2024-01-01T00:00:00Z", "ledger_index": 1}

    async def mock_supervise_stream(url, connect):
        yield Received(received_at=datetime.now(timezone.utc), message=msg)

    monkeypatch.setattr("producers.xrpl_producer.sinks.supervise_stream", mock_supervise_stream)

    sink = CapturesSink()

    async for envelope in sink_messages(sink, url="ws://test"):
        # Check all envelope fields present
        assert "event_id" in envelope
        assert "event_time" in envelope
        assert "received_at" in envelope
        assert "ledger_index" in envelope
        assert "ingest_mode" in envelope
        assert "payload" in envelope

        # Check field values
        assert envelope["event_id"] == "tx1"
        assert envelope["event_time"] == "2024-01-01T00:00:00Z"
        assert envelope["ledger_index"] == 1
        assert envelope["ingest_mode"] == "live"
        break


# ---------------------------------------------------------------------------
# KafkaProducerSink tests
# ---------------------------------------------------------------------------

def _make_mock_producer():
    """Return an AIOKafkaProducer mock with async start/stop/send."""
    producer = MagicMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    producer.send = AsyncMock()
    return producer


@pytest.mark.asyncio
async def test_kafka_sink_starts_and_stops_producer():
    """KafkaProducerSink should start producer on enter and stop on exit."""
    mock_producer = _make_mock_producer()
    # patch intercepts that line and says: "whenever this code tries to create an AIOKafkaProducer, give it my fake one instead."
    with patch("producers.xrpl_producer.sinks.AIOKafkaProducer", return_value=mock_producer):
        async with KafkaProducerSink("localhost:9092"):
            mock_producer.start.assert_awaited_once()

        mock_producer.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_kafka_sink_sends_to_correct_topic():
    """KafkaProducerSink should send to xrpl.raw with the envelope as value."""
    mock_producer = _make_mock_producer()
    envelope = {"event_id": "abc123", "payload": {}, "ingest_mode": "live"}

    with patch("producers.xrpl_producer.sinks.AIOKafkaProducer", return_value=mock_producer):
        async with KafkaProducerSink("localhost:9092", topic="xrpl.raw") as sink:
            await sink.send(envelope)

    _, kwargs = mock_producer.send.call_args
    assert kwargs["value"] == envelope
    # topic is the first positional arg
    args, _ = mock_producer.send.call_args
    assert args[0] == "xrpl.raw"


@pytest.mark.asyncio
async def test_kafka_sink_uses_event_id_as_key():
    """KafkaProducerSink should use event_id as the partition key."""
    mock_producer = _make_mock_producer()
    envelope = {"event_id": "abc123", "payload": {}, "ingest_mode": "live"}

    with patch("producers.xrpl_producer.sinks.AIOKafkaProducer", return_value=mock_producer):
        async with KafkaProducerSink() as sink:
            await sink.send(envelope)

    _, kwargs = mock_producer.send.call_args
    assert kwargs["key"] == "abc123"
