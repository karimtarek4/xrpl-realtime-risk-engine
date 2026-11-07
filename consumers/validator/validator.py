"""Validation and normalisation logic for XRPL payment envelopes.

Validates envelopes from xrpl.raw and normalises the payment amount.
Pure functions — no Kafka, no I/O, easy to unit test.
"""
from dataclasses import dataclass
from typing import Any, Dict, Optional


REASON_NOT_PAYMENT = "not_payment"
REASON_FAILED_TX = "failed_tx"
REASON_MISSING_FIELD = "missing_field"
REASON_BAD_AMOUNT = "bad_amount"


@dataclass
class ValidPayment:
    envelope: Dict[str, Any]
    account: str
    destination: str
    amount_xrp: Optional[float]   # set if XRP payment (drops converted to XRP)
    amount_token: Optional[Dict]  # set if token payment


@dataclass
class InvalidPayment:
    envelope: Dict[str, Any]
    reason_code: str


def validate(envelope: Dict[str, Any]) -> ValidPayment | InvalidPayment:
    """Validate and normalise an envelope from xrpl.raw.

    Returns ValidPayment if the envelope passes all checks,
    InvalidPayment with a reason_code otherwise.
    """
    payload = envelope.get("payload", {})
    tx = payload.get("tx_json", payload)
    meta = payload.get("meta", {})

    if tx.get("TransactionType") != "Payment":
        return InvalidPayment(envelope, REASON_NOT_PAYMENT)

    if meta.get("TransactionResult") != "tesSUCCESS":
        return InvalidPayment(envelope, REASON_FAILED_TX)

    account = tx.get("Account")
    destination = tx.get("Destination")
    if not account or not destination:
        return InvalidPayment(envelope, REASON_MISSING_FIELD)

    amount = _normalise_amount(payload)
    if amount is None:
        return InvalidPayment(envelope, REASON_BAD_AMOUNT)

    amount_xrp, amount_token = amount
    return ValidPayment(envelope, account, destination, amount_xrp, amount_token)


def _normalise_amount(payload: Dict[str, Any]):
    """Extract and normalise the delivered amount.

    XRPL amounts are either:
    - XRP: a string of drops (1 XRP = 1,000,000 drops)
    - Token: {"currency": ..., "issuer": ..., "value": ...}

    Uses delivered_amount from meta when present (reflects actual delivery
    for partial payments). Falls back to tx_json Amount.

    Returns (xrp_float, None) for XRP, (None, token_dict) for tokens,
    or None if the amount cannot be parsed.
    """
    tx = payload.get("tx_json", payload)
    meta = payload.get("meta", {})

    raw = meta.get("delivered_amount") or tx.get("Amount")
    if raw is None:
        return None

    if isinstance(raw, str):
        try:
            return (int(raw) / 1_000_000, None)
        except ValueError:
            return None

    if isinstance(raw, dict):
        if {"currency", "issuer", "value"} <= raw.keys():
            return (None, raw)

    return None
