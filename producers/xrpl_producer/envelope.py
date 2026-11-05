"""Layer 1: wrap one XRPL transaction message in our envelope.

Pure function: no network, no Kafka, no clock. The caller passes in `received_at`
(the moment the message arrived), so the result is fully predictable and testable.

The envelope does NOT judge the transaction. Failed payments, offers and batch
legs are all wrapped and kept: `xrpl.raw` stores everything as received, and the
validator downstream decides what is valid.
"""
from datetime import datetime, timezone
from typing import Any, Dict

INGEST_MODES = frozenset({"live", "backfill", "replay"})


class EnvelopeError(ValueError):
    """The message cannot be wrapped (wrong type, or a required field is missing)."""


def _format_utc(moment: datetime) -> str:
    """Datetime -> ISO 8601 UTC with milliseconds, e.g. 2026-11-05T07:48:31.123Z."""
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_envelope(
    message: Dict[str, Any],
    received_at: datetime,
    ingest_mode: str = "live",
) -> Dict[str, Any]:
    """Wrap a v2 `transaction` message.

    Fields:
        event_id     transaction `hash` (same event -> same id, so duplicates are detectable)
        event_time   `close_time_iso` (when XRPL closed the ledger, second precision)
        received_at  when we received it (our clock, millisecond precision)
        ledger_index ledger sequence number
        ingest_mode  "live" | "backfill" | "replay"
        payload      the original message, untouched

    Raises EnvelopeError if the message is not a transaction or lacks a required field.
    """
    if message.get("type") != "transaction":
        raise EnvelopeError(f"not a transaction message (type={message.get('type')!r})")
    if received_at.tzinfo is None:
        raise EnvelopeError("received_at must be timezone-aware (use UTC)")
    if ingest_mode not in INGEST_MODES:
        raise EnvelopeError(f"unknown ingest_mode {ingest_mode!r}; expected one of {sorted(INGEST_MODES)}")

    missing = [f for f in ("hash", "close_time_iso", "ledger_index") if message.get(f) is None]
    if missing:
        raise EnvelopeError(f"missing required field(s): {', '.join(missing)}")

    return {
        "event_id": message["hash"],
        "event_time": message["close_time_iso"],
        "received_at": _format_utc(received_at),
        "ledger_index": message["ledger_index"],
        "ingest_mode": ingest_mode,
        "payload": message,
    }
