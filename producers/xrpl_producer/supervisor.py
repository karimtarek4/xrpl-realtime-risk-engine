"""Layer 3: Supervisor. Reconnect with exponential backoff after stream disconnects.

`supervise_stream()` wraps `stream_messages()` (Layer 2). On disconnect, it waits with
exponential backoff (1s, 2s, 4s, ... capped at 30s) and retries. After the first
successful message from a new connection, backoff resets to 1s. Never raises; keeps
retrying forever.
"""
import asyncio
import logging
from typing import Any, AsyncIterator, Callable

import websockets

from .stream import Received, StreamClosed, stream_messages

log = logging.getLogger(__name__)


async def supervise_stream(
    url: str = "wss://s1.ripple.com",
    connect: Callable[..., Any] = websockets.connect,
) -> AsyncIterator[Received]:
    """Reconnect with exponential backoff on stream disconnect.

    Wraps stream_messages(). On StreamClosed: waits with backoff, then retries.
    Backoff resets to 1s after first successful message from new connection.
    Yields Received; never raises.

    Args:
        url: XRPL WebSocket endpoint
        connect: injectable connection function (for testing)
    """
    backoff_secs = 1
    max_backoff = 30

    while True:
        try:
            first_message = True
            async for received in stream_messages(url, connect):
                if first_message:
                    backoff_secs = 1  # Reset after first message from new connection
                    first_message = False
                yield received
        except StreamClosed as e:
            log.warning(
                "reconnecting in %ds (lived %ds, saw %d messages)",
                backoff_secs,
                e.lived_seconds,
                e.messages,
            )
            await asyncio.sleep(backoff_secs)
            backoff_secs = min(backoff_secs * 2, max_backoff)
