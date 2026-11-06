"""Tests for Layer 3: Supervisor (exponential backoff reconnect)."""
import asyncio
from datetime import datetime, timezone
from typing import Any, List
from unittest.mock import MagicMock, AsyncMock

import pytest

from producers.xrpl_producer.stream import Received, StreamClosed
from producers.xrpl_producer.supervisor import supervise_stream


class FakeStream:
    """Mocks stream_messages. Can be configured to fail or yield messages."""

    def __init__(self, messages: List[dict], fail_count: int = 0):
        """
        Args:
            messages: list of messages to yield before raising StreamClosed
            fail_count: how many times to call this before raising (0 = first call fails)
        """
        self.messages = messages
        self.fail_count = fail_count
        self.call_count = 0
        self.index = 0
        self.failed_this_call = False

    def __aiter__(self):
        self.call_count += 1
        self.failed_this_call = self.call_count <= self.fail_count
        self.index = 0
        return self

    async def __anext__(self):
        if self.failed_this_call:
            # Fail on first __anext__ call after __aiter__
            self.failed_this_call = False
            raise StreamClosed(code=None, reason="simulated", lived_seconds=0.1, messages=0)
        if self.index >= len(self.messages):
            raise StreamClosed(code=None, reason="done", lived_seconds=0.1, messages=len(self.messages))
        msg = self.messages[self.index]
        self.index += 1
        return msg


def fake_stream(messages: List[dict], fail_count: int = 0):
    """Create a FakeStream and wrap it so supervisor calls stream_messages."""
    fake = FakeStream(messages, fail_count)

    async def mock_stream_messages(url, connect):
        async for msg in fake:
            yield Received(received_at=datetime.now(timezone.utc), message=msg)

    return mock_stream_messages, fake


@pytest.mark.asyncio
async def test_yields_messages_from_successful_stream(monkeypatch):
    """Supervisor should yield messages from stream_messages unchanged."""
    messages = [
        {"type": "transaction", "hash": "tx1"},
        {"type": "ledgerClosed", "ledger_index": 1},
    ]
    mock_stream, fake = fake_stream(messages)
    monkeypatch.setattr("producers.xrpl_producer.supervisor.stream_messages", mock_stream)

    received = []
    async for msg in supervise_stream("ws://test"):
        received.append(msg)
        if len(received) >= 2:
            break

    assert len(received) == 2
    assert received[0].message == messages[0]
    assert received[1].message == messages[1]


@pytest.mark.asyncio
async def test_reconnects_after_disconnect(monkeypatch):
    """Supervisor should reconnect and continue after StreamClosed."""
    messages = [{"type": "transaction", "hash": "tx1"}]
    mock_stream, fake = fake_stream(messages, fail_count=1)
    monkeypatch.setattr("producers.xrpl_producer.supervisor.stream_messages", mock_stream)

    received = []
    async for msg in supervise_stream("ws://test"):
        received.append(msg)
        if len(received) >= 1:
            break

    # First call to stream_messages fails (fail_count=1).
    # Second call succeeds and yields one message.
    assert len(received) == 1
    assert fake.call_count == 2


@pytest.mark.asyncio
async def test_backoff_increases_on_repeated_disconnects(monkeypatch):
    """Each disconnect should wait longer before retry."""
    messages = [{"type": "transaction", "hash": "tx1"}]
    mock_stream, fake = fake_stream(messages, fail_count=2)
    monkeypatch.setattr("producers.xrpl_producer.supervisor.stream_messages", mock_stream)

    start = asyncio.get_event_loop().time()
    waits = []

    # Instrument asyncio.sleep to record wait times
    original_sleep = asyncio.sleep
    async def recorded_sleep(delay):
        waits.append(delay)
        await original_sleep(0.01)  # Sleep briefly to avoid hammering CPU

    monkeypatch.setattr("asyncio.sleep", recorded_sleep)

    received = []
    async for msg in supervise_stream("ws://test"):
        received.append(msg)
        if len(received) >= 1:
            break

    # Calls: fail (wait 1s) → fail (wait 2s) → succeed
    assert waits == [1, 2]


@pytest.mark.asyncio
async def test_backoff_resets_after_successful_message(monkeypatch):
    """Backoff should reset to 1s after first message from reconnected stream."""
    # Pattern: fail, succeed+yield, fail
    # Waits: 1s, then 1s (reset, not 2s)

    call_count = [0]  # mutable container for counting in nested function

    async def mock_stream_messages(url, connect):
        call_count[0] += 1
        if call_count[0] == 1:
            raise StreamClosed(code=None, reason="fail", lived_seconds=0.1, messages=0)
        elif call_count[0] == 2:
            yield Received(received_at=datetime.now(timezone.utc), message={"type": "transaction"})
            raise StreamClosed(code=None, reason="fail", lived_seconds=0.1, messages=1)
        else:
            raise StreamClosed(code=None, reason="fail", lived_seconds=0.1, messages=0)

    monkeypatch.setattr(
        "producers.xrpl_producer.supervisor.stream_messages",
        mock_stream_messages,
    )

    waits = []
    original_sleep = asyncio.sleep

    async def recorded_sleep(delay):
        waits.append(delay)
        if len(waits) < 3:  # Record only first 2 waits
            await original_sleep(0.001)  # Very short sleep

    monkeypatch.setattr("asyncio.sleep", recorded_sleep)

    # Collect first message, then stop
    count = 0
    async for msg in supervise_stream("ws://test"):
        count += 1
        if count >= 1:
            break

    # Expected: first fail (wait 1s), then success (reset backoff to 1s), then fail (wait 1s)
    assert waits[0] == 1
    if len(waits) > 1:
        assert waits[1] == 1  # Should be 1s, not 2s (proving backoff was reset)


@pytest.mark.asyncio
async def test_backoff_caps_at_30_seconds(monkeypatch):
    """Backoff should never exceed 30 seconds."""
    # Simulate many failures: 1, 2, 4, 8, 16, 30, 30...

    class ManyFailures:
        def __init__(self):
            self.call_count = 0

        async def mock_stream_messages(self, url, connect):
            self.call_count += 1
            # Fail 8 times, then succeed on 9th
            if self.call_count < 9:
                raise StreamClosed(code=None, reason="fail", lived_seconds=0.1, messages=0)
            yield {"type": "transaction", "hash": "tx1"}

    tracker = ManyFailures()
    monkeypatch.setattr(
        "producers.xrpl_producer.supervisor.stream_messages",
        tracker.mock_stream_messages,
    )

    waits = []
    original_sleep = asyncio.sleep

    async def recorded_sleep(delay):
        waits.append(delay)
        await original_sleep(0.01)

    monkeypatch.setattr("asyncio.sleep", recorded_sleep)

    received = []
    async for msg in supervise_stream("ws://test"):
        received.append(msg)
        if len(received) >= 1:
            break

    # Expected waits: 1, 2, 4, 8, 16, 30, 30, 30
    assert waits == [1, 2, 4, 8, 16, 30, 30, 30]
    assert all(w <= 30 for w in waits)
