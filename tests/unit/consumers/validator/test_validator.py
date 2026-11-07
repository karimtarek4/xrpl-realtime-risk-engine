"""Tests for consumers/validator/validator.py"""
import json
from pathlib import Path

import pytest

from consumers.validator.validator import (
    REASON_BAD_AMOUNT,
    REASON_FAILED_TX,
    REASON_MISSING_FIELD,
    REASON_NOT_PAYMENT,
    InvalidPayment,
    ValidPayment,
    validate,
)

FIXTURES = Path(__file__).parents[3] / "fixtures"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def _envelope(payload: dict) -> dict:
    return {
        "event_id": payload.get("hash", "test_hash"),
        "event_time": payload.get("close_time_iso", "2024-01-01T00:00:00Z"),
        "received_at": "2024-01-01T00:00:00.001Z",
        "ledger_index": payload.get("ledger_index", 1),
        "transaction_type": payload.get("tx_json", {}).get("TransactionType"),
        "ingest_mode": "live",
        "payload": payload,
    }


# ---------------------------------------------------------------------------
# Valid payments
# ---------------------------------------------------------------------------

def test_valid_xrp_payment():
    envelope = _envelope(_load("payment_xrp_success.json"))
    result = validate(envelope)

    assert isinstance(result, ValidPayment)
    assert result.account == "rh37VYCg1TT8WiMs9xErd5Dg3H7C1L8jrC"
    assert result.destination == "rDkY4dCocVftin734fnmR44CQXcyf9jofp"
    assert result.amount_xrp == pytest.approx(1583.05)
    assert result.amount_token is None


def test_valid_token_payment():
    envelope = _envelope(_load("payment_token_success.json"))
    result = validate(envelope)

    assert isinstance(result, ValidPayment)
    assert result.amount_xrp is None
    assert isinstance(result.amount_token, dict)
    assert result.amount_token["currency"] == "524C555344000000000000000000000000000000"
    assert result.amount_token["value"] == "0.006"


def test_valid_self_payment():
    """Self-payments (Account == Destination) are valid — routing is not validator's concern."""
    envelope = _envelope(_load("payment_self.json"))
    result = validate(envelope)

    assert isinstance(result, ValidPayment)


def test_valid_payment_envelope_preserved():
    """The original envelope must be unchanged in the result."""
    envelope = _envelope(_load("payment_xrp_success.json"))
    result = validate(envelope)

    assert result.envelope is envelope


# ---------------------------------------------------------------------------
# Invalid: not a Payment
# ---------------------------------------------------------------------------

def test_rejects_offer_create():
    envelope = _envelope(_load("offer_create.json"))
    result = validate(envelope)

    assert isinstance(result, InvalidPayment)
    assert result.reason_code == REASON_NOT_PAYMENT


def test_rejects_ledger_closed():
    envelope = _envelope(_load("ledger_closed.json"))
    result = validate(envelope)

    assert isinstance(result, InvalidPayment)
    assert result.reason_code == REASON_NOT_PAYMENT


# ---------------------------------------------------------------------------
# Invalid: failed transaction
# ---------------------------------------------------------------------------

def test_rejects_failed_payment():
    envelope = _envelope(_load("payment_failed.json"))
    result = validate(envelope)

    assert isinstance(result, InvalidPayment)
    assert result.reason_code == REASON_FAILED_TX


# ---------------------------------------------------------------------------
# Invalid: missing fields
# ---------------------------------------------------------------------------

def test_rejects_missing_account():
    payload = _load("payment_xrp_success.json")
    del payload["tx_json"]["Account"]
    result = validate(_envelope(payload))

    assert isinstance(result, InvalidPayment)
    assert result.reason_code == REASON_MISSING_FIELD


def test_rejects_missing_destination():
    payload = _load("payment_xrp_success.json")
    del payload["tx_json"]["Destination"]
    result = validate(_envelope(payload))

    assert isinstance(result, InvalidPayment)
    assert result.reason_code == REASON_MISSING_FIELD


# ---------------------------------------------------------------------------
# Invalid: bad amount
# ---------------------------------------------------------------------------

def test_rejects_missing_amount():
    payload = _load("payment_xrp_success.json")
    del payload["meta"]["delivered_amount"]
    del payload["tx_json"]["DeliverMax"]
    # Also remove Amount if present
    payload["tx_json"].pop("Amount", None)
    result = validate(_envelope(payload))

    assert isinstance(result, InvalidPayment)
    assert result.reason_code == REASON_BAD_AMOUNT


def test_rejects_malformed_amount():
    payload = _load("payment_xrp_success.json")
    payload["meta"]["delivered_amount"] = "not_a_number"
    result = validate(_envelope(payload))

    assert isinstance(result, InvalidPayment)
    assert result.reason_code == REASON_BAD_AMOUNT


def test_rejects_incomplete_token_amount():
    payload = _load("payment_xrp_success.json")
    # dict but missing required keys
    payload["meta"]["delivered_amount"] = {"currency": "USD"}
    result = validate(_envelope(payload))

    assert isinstance(result, InvalidPayment)
    assert result.reason_code == REASON_BAD_AMOUNT


# ---------------------------------------------------------------------------
# Amount normalisation
# ---------------------------------------------------------------------------

def test_uses_delivered_amount_over_tx_amount():
    """delivered_amount takes precedence over tx_json Amount."""
    payload = _load("payment_xrp_success.json")
    payload["tx_json"]["Amount"] = "999999999"
    payload["meta"]["delivered_amount"] = "1000000"  # 1 XRP
    result = validate(_envelope(payload))

    assert isinstance(result, ValidPayment)
    assert result.amount_xrp == pytest.approx(1.0)


def test_falls_back_to_tx_amount_when_no_delivered():
    payload = _load("payment_xrp_success.json")
    del payload["meta"]["delivered_amount"]
    payload["tx_json"]["Amount"] = "2000000"  # 2 XRP
    result = validate(_envelope(payload))

    assert isinstance(result, ValidPayment)
    assert result.amount_xrp == pytest.approx(2.0)
