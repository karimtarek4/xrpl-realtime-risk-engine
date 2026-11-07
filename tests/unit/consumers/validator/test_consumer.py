"""Tests for consumers/validator/consumer.py"""
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from consumers.validator.consumer import _build_dlq, _build_payment, run
from consumers.validator.validator import InvalidPayment, ValidPayment


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _envelope(event_id: str = "tx1") -> dict:
    return {
        "event_id": event_id,
        "event_time": "2024-01-01T00:00:00Z",
        "received_at": "2024-01-01T00:00:00.001Z",
        "ledger_index": 1,
        "transaction_type": "Payment",
        "ingest_mode": "live",
        "payload": {},
    }


def _valid(event_id: str = "tx1", account: str = "rA", destination: str = "rB") -> ValidPayment:
    return ValidPayment(
        envelope=_envelope(event_id),
        account=account,
        destination=destination,
        amount_xrp=1.0,
        amount_token=None,
    )


def _invalid(event_id: str = "tx1", reason: str = "not_payment") -> InvalidPayment:
    return InvalidPayment(envelope=_envelope(event_id), reason_code=reason)


# ---------------------------------------------------------------------------
# _build_payment
# ---------------------------------------------------------------------------

def test_build_payment_flattens_fields():
    result = _build_payment(_valid())

    assert result["account"] == "rA"
    assert result["destination"] == "rB"
    assert result["amount_xrp"] == 1.0
    assert result["amount_token"] is None


def test_build_payment_preserves_envelope_fields():
    result = _build_payment(_valid())

    assert result["event_id"] == "tx1"
    assert result["event_time"] == "2024-01-01T00:00:00Z"
    assert result["ingest_mode"] == "live"


# ---------------------------------------------------------------------------
# _build_dlq
# ---------------------------------------------------------------------------

def test_build_dlq_has_required_fields():
    result = _build_dlq(_invalid(reason="not_payment"))

    assert result["reason_code"] == "not_payment"
    assert result["stage"] == "validator"
    assert "failed_at" in result
    assert result["original"] == _envelope()


def test_build_dlq_preserves_original_envelope():
    envelope = _envelope("abc")
    invalid = InvalidPayment(envelope=envelope, reason_code="failed_tx")
    result = _build_dlq(invalid)

    assert result["original"] is envelope


# ---------------------------------------------------------------------------
# run() — routing
# ---------------------------------------------------------------------------

def _make_kafka_mocks():
    consumer = MagicMock()
    consumer.start = AsyncMock()
    consumer.stop = AsyncMock()

    producer = MagicMock()
    producer.start = AsyncMock()
    producer.stop = AsyncMock()
    producer.send = AsyncMock()

    return consumer, producer


def _kafka_message(envelope: dict) -> MagicMock:
    msg = MagicMock()
    msg.value = envelope
    return msg


async def _aiter(items):
    for item in items:
        yield item


@pytest.mark.asyncio
async def test_run_routes_valid_payment_to_payments_topic():
    envelope = _envelope("tx1")
    envelope["payload"] = {
        "tx_json": {"TransactionType": "Payment", "Account": "rA", "Destination": "rB", "Amount": "1000000"},
        "meta": {"TransactionResult": "tesSUCCESS", "delivered_amount": "1000000"},
    }
    consumer, producer = _make_kafka_mocks()
    consumer.__aiter__ = lambda self: _aiter([_kafka_message(envelope)])

    with patch("consumers.validator.consumer.AIOKafkaConsumer", return_value=consumer), \
         patch("consumers.validator.consumer.AIOKafkaProducer", return_value=producer):
        await run()

    producer.send.assert_awaited_once()
    args, kwargs = producer.send.call_args
    assert args[0] == "xrpl.payments"
    assert kwargs["key"] == "rA"


@pytest.mark.asyncio
async def test_run_routes_invalid_payment_to_dlq():
    envelope = _envelope("tx2")
    envelope["payload"] = {
        "tx_json": {"TransactionType": "OfferCreate", "Account": "rA"},
        "meta": {"TransactionResult": "tesSUCCESS"},
    }
    consumer, producer = _make_kafka_mocks()
    consumer.__aiter__ = lambda self: _aiter([_kafka_message(envelope)])

    with patch("consumers.validator.consumer.AIOKafkaConsumer", return_value=consumer), \
         patch("consumers.validator.consumer.AIOKafkaProducer", return_value=producer):
        await run()

    producer.send.assert_awaited_once()
    args, kwargs = producer.send.call_args
    assert args[0] == "xrpl.dlq"
    assert kwargs["key"] == "tx2"
    assert kwargs["headers"] == [("reason_code", b"not_payment")]


@pytest.mark.asyncio
async def test_run_deduplicates_same_event_id():
    envelope = _envelope("tx1")
    envelope["payload"] = {
        "tx_json": {"TransactionType": "Payment", "Account": "rA", "Destination": "rB", "Amount": "1000000"},
        "meta": {"TransactionResult": "tesSUCCESS", "delivered_amount": "1000000"},
    }
    consumer, producer = _make_kafka_mocks()
    # same envelope twice
    consumer.__aiter__ = lambda self: _aiter([
        _kafka_message(envelope),
        _kafka_message(envelope),
    ])

    with patch("consumers.validator.consumer.AIOKafkaConsumer", return_value=consumer), \
         patch("consumers.validator.consumer.AIOKafkaProducer", return_value=producer):
        await run()

    # only one send despite two messages
    assert producer.send.await_count == 1


@pytest.mark.asyncio
async def test_run_skips_envelope_with_no_event_id():
    envelope = {"payload": {}, "ingest_mode": "live"}  # no event_id
    consumer, producer = _make_kafka_mocks()
    consumer.__aiter__ = lambda self: _aiter([_kafka_message(envelope)])

    with patch("consumers.validator.consumer.AIOKafkaConsumer", return_value=consumer), \
         patch("consumers.validator.consumer.AIOKafkaProducer", return_value=producer):
        await run()

    producer.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_stops_consumer_and_producer_on_exit():
    consumer, producer = _make_kafka_mocks()
    consumer.__aiter__ = lambda self: _aiter([])

    with patch("consumers.validator.consumer.AIOKafkaConsumer", return_value=consumer), \
         patch("consumers.validator.consumer.AIOKafkaProducer", return_value=producer):
        await run()

    consumer.stop.assert_awaited_once()
    producer.stop.assert_awaited_once()
