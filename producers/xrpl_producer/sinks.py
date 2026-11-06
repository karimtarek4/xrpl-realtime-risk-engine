"""Layer 5: Sinks. Publish enveloped messages to durable storage.

`sink_messages()` wraps Layer 3 (Supervisor). It envelopes each message and
sends it to a configured sink (MessageSink implementation).

Sinks are pluggable: FileSystemSink for testing/dev, KafkaProducerSink for
production (added when Kafka branch merges).
"""
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Dict, Optional

import websockets

from .envelope import build_envelope, EnvelopeError
from .stream import Received, StreamClosed
from .supervisor import supervise_stream

log = logging.getLogger(__name__)


class MessageSink(ABC):
    """Abstract base for message sinks."""

    @abstractmethod
    async def send(self, message: Dict[str, Any]) -> None:
        """Send an enveloped message to storage.

        Args:
            message: envelope dict with event_id, event_time, received_at, ledger_index, ingest_mode, payload

        Raises:
            SinkError: if send fails (caller decides retry strategy)
        """


class FileSystemSink(MessageSink):
    """Write enveloped messages as JSON lines to a file."""

    def __init__(self, file_path: Optional[str] = None):
        """
        Args:
            file_path: path to write JSON lines to. If None, write to stdout.
        """
        self.file_path = file_path
        self.file = None
        self.message_count = 0

    async def __aenter__(self):
        """Open file on entry."""
        if self.file_path:
            self.file = open(self.file_path, "a")
        return self

    async def __aexit__(self, *args):
        """Close file on exit."""
        if self.file:
            self.file.close()

    async def send(self, message: Dict[str, Any]) -> None:
        """Write message as JSON line."""
        line = json.dumps(message)
        if self.file:
            self.file.write(line + "\n")
        else:
            print(line)
        self.message_count += 1


class KafkaProducerSink(MessageSink):
    """Kafka producer (deferred: implemented when Kafka branch ready)."""

    async def send(self, message: Dict[str, Any]) -> None:
        """Send to xrpl.raw topic with idempotent producer settings.

        TODO: Implement when kafka branch merges.
        Expected: await producer.send("xrpl.raw", message, key=message["event_id"])
        """
        raise NotImplementedError("Kafka branch not merged yet. Use FileSystemSink for now.")


async def sink_messages(
    sink: MessageSink,
    url: str = "wss://s1.ripple.com",
    connect: Callable[..., Any] = websockets.connect,
    ingest_mode: str = "live",
) -> AsyncIterator[Dict[str, Any]]:
    """Receive messages from XRPL, envelope them, send to sink.

    Wraps Layer 3 (Supervisor). Takes raw messages, builds envelopes, sends
    to sink. Yields envelopes so caller can track what was sent.

    Args:
        sink: MessageSink implementation (FileSystemSink, KafkaProducerSink, etc.)
        url: XRPL WebSocket endpoint
        connect: injectable connection function (for testing)
        ingest_mode: "live" or "backfill"

    Yields:
        Envelope dict (event_id, event_time, received_at, ledger_index, ingest_mode, payload)

    Raises:
        EnvelopeError: if message cannot be enveloped (caller decides to skip or DLQ)
    """
    async for received in supervise_stream(url, connect):
        try:
            envelope = build_envelope(received.message, received.received_at, ingest_mode)
            await sink.send(envelope)
            yield envelope
        except EnvelopeError as e:
            log.warning("skipping unenvelopable message: %s", e)
            # TODO: send to DLQ instead of skipping (Batch 3)
