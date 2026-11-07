"""Layer 2: one WebSocket session with XRPL.

`stream_messages()` opens ONE connection, subscribes, and yields every `transaction`
and `ledgerClosed` message together with the moment it arrived. When the connection
ends, for any reason, it raises `StreamClosed` with the close code and reason.

It does not reconnect. That is the supervisor's job (Layer 3): one connection per
call keeps this layer small and easy to test.
"""
import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Callable, Dict, NamedTuple, Optional

import websockets
from websockets.exceptions import ConnectionClosed, InvalidHandshake

log = logging.getLogger(__name__)

DEFAULT_URL = "wss://s1.ripple.com"

# Library default is 1 MiB. Some XRPL transactions (large NFT pages) are bigger, and an
# oversized message would kill the connection (close code 1009). 8 MiB leaves headroom.
MAX_MESSAGE_BYTES = 8 * 1024 * 1024

YIELDED_TYPES = frozenset({"transaction", "ledgerClosed"})


class Received(NamedTuple):
    """A message and the moment we received it (timezone-aware UTC)."""

    received_at: datetime
    message: Dict[str, Any]


class StreamClosed(Exception):
    """The connection ended. Carries what the supervisor needs to log and decide."""

    def __init__(self, code: Optional[int], reason: str, lived_seconds: float, messages: int) -> None:
        super().__init__(
            f"stream closed: code={code} reason={reason!r} lived={lived_seconds:.0f}s messages={messages}"
        )
        self.code = code
        self.reason = reason
        self.lived_seconds = lived_seconds
        self.messages = messages


class SubscribeError(Exception):
    """The server answered our subscribe request with an error."""


def subscribe_request() -> Dict[str, Any]:
    """The request we send after connecting. Pins API v2 (tx_json, top-level hash, DeliverMax)."""
    return {"command": "subscribe", "streams": ["transactions", "ledger"], "api_version": 2}


async def stream_messages(
    url: str = DEFAULT_URL,
    connect: Callable[..., Any] = websockets.connect,
) -> AsyncIterator[Received]:
    """Connect, subscribe, and yield messages until the connection ends.

    `connect` is injectable so tests can use a fake connection instead of the network.

    Yields `Received` for `transaction` and `ledgerClosed` messages only. The subscribe
    `response` is checked and consumed; unknown types and non-JSON frames are skipped.

    Raises:
        StreamClosed:   the connection ended. `code` is the close code the server
                        sent, or None when there was none (no close frame, or the
                        connection never opened). We never invent a code.
        SubscribeError: the server rejected the subscribe request.
    """
    started = time.monotonic()
    yielded = 0
    try:
        async with connect(url, max_size=MAX_MESSAGE_BYTES) as ws:
            log.info("connected to %s", url)
            await ws.send(json.dumps(subscribe_request()))

            # wait for the next message from the connection, handle it, then wait for the next one,
            # loop runs until the connection dies,
            async for raw in ws:
                received_at = datetime.now(timezone.utc)  # stamp first, before any parsing

                try:
                    message = json.loads(raw)
                # Non jSON frames are skipped.
                except ValueError:
                    log.warning("skipping non-JSON frame (%d bytes)", len(raw))
                    continue
                if not isinstance(message, dict):
                    log.warning("skipping non-object JSON frame")
                    continue

                message_type = message.get("type")
                if message_type == "response":
                    if message.get("status") != "success":
                        raise SubscribeError(f"subscribe rejected: {message}")
                    log.info("subscription confirmed (ledger %s)", message.get("result", {}).get("ledger_index"))
                elif message_type in YIELDED_TYPES:
                    yielded += 1
                    yield Received(received_at, message)
                else:
                    log.debug("ignoring message type %r", message_type)

            # Loop ended without an exception: the server closed cleanly.
            # No code is not the same as 1006: we don't know why it ended, so say so.
            code = getattr(ws, "close_code", None)
            reason = getattr(ws, "close_reason", None) or ""
    except ConnectionClosed as exc:
        # Only a code the server actually sent is reported. No close frame means no code.
        code = exc.rcvd.code if exc.rcvd else None
        reason = exc.rcvd.reason if exc.rcvd else "no close frame"
    except (OSError, asyncio.TimeoutError, InvalidHandshake) as exc:
        # Never got a session open, so there was no close frame and no close code to report.
        code, reason = None, f"{type(exc).__name__}: {exc}"

    closed = StreamClosed(code, reason, time.monotonic() - started, yielded)
    log.warning("%s", closed)
    raise closed
