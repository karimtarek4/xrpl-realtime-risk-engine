import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from producers.xrpl_producer.envelope import EnvelopeError, build_envelope

FIXTURES = Path(__file__).resolve().parents[3] / "fixtures"
NOW = datetime(2026, 11, 5, 7, 48, 32, 123000, tzinfo=timezone.utc)

# Every saved fixture except the heartbeat. A new transaction fixture is picked up automatically.
TRANSACTION_FIXTURES = sorted(
    p.stem for p in FIXTURES.glob("*.json") if p.stem != "ledger_closed"
)


def load(name):
    return json.loads((FIXTURES / f"{name}.json").read_text())


def test_fields_are_mapped_from_the_message():
    msg = load("payment_xrp_success")
    env = build_envelope(msg, NOW)

    assert env["event_id"] == msg["hash"]
    assert env["event_time"] == msg["close_time_iso"]
    assert env["ledger_index"] == msg["ledger_index"]
    assert env["ingest_mode"] == "live"
    assert env["received_at"] == "2026-11-05T07:48:32.123Z"
    assert env["payload"] == msg


def test_envelope_has_exactly_the_agreed_fields():
    env = build_envelope(load("payment_xrp_success"), NOW)
    assert set(env) == {"event_id", "event_time", "received_at", "ledger_index", "transaction_type", "ingest_mode", "payload"}


@pytest.mark.parametrize("name", TRANSACTION_FIXTURES)
def test_every_transaction_fixture_maps_to_its_own_source_fields(name):
    # xrpl.raw keeps everything as received: failed payments, offers, batch legs.
    msg = load(name)
    env = build_envelope(msg, NOW)

    assert env["event_id"] == msg["hash"]
    assert env["event_time"] == msg["close_time_iso"]
    assert env["ledger_index"] == msg["ledger_index"]
    assert env["payload"] == msg


def test_ledger_closed_is_rejected_by_name():
    with pytest.raises(EnvelopeError, match="not a transaction"):
        build_envelope(load("ledger_closed"), NOW)


def test_envelope_is_json_serialisable():
    env = build_envelope(load("payment_token_success"), NOW)
    assert json.loads(json.dumps(env)) == env


def test_input_message_is_not_modified():
    msg = load("payment_xrp_success")
    before = copy.deepcopy(msg)
    build_envelope(msg, NOW)
    assert msg == before


def test_received_at_is_converted_to_utc():
    cairo = timezone(timedelta(hours=2))
    env = build_envelope(load("payment_xrp_success"), datetime(2026, 11, 5, 9, 48, 32, tzinfo=cairo))
    assert env["received_at"] == "2026-11-05T07:48:32.000Z"

def test_unknown_ingest_mode_is_rejected():
    with pytest.raises(EnvelopeError, match="ingest_mode"):
        build_envelope(load("payment_xrp_success"), NOW, ingest_mode="banana")

def test_naive_received_at_is_rejected():
    with pytest.raises(EnvelopeError, match="timezone-aware"):
        build_envelope(load("payment_xrp_success"), datetime(2026, 11, 5, 7, 48, 32))


@pytest.mark.parametrize("field", ["hash", "close_time_iso", "ledger_index"])
def test_missing_required_field_is_rejected(field):
    msg = load("payment_xrp_success")
    del msg[field]
    with pytest.raises(EnvelopeError, match=field):
        build_envelope(msg, NOW)
