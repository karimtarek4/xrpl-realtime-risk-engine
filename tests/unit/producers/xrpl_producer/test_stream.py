import asyncio
import json
from pathlib import Path

import pytest
from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close

from producers.xrpl_producer.stream import (
    MAX_MESSAGE_BYTES,
    StreamClosed,
    SubscribeError,
    stream_messages,
    subscribe_request,
)

FIXTURES = Path(__file__).resolve().parents[3] / "fixtures"

SUBSCRIBE_OK = {"type": "response", "status": "success", "result": {"ledger_index": 1}}


def fixture(name):
    return json.loads((FIXTURES / f"{name}.json").read_text())


class FakeConnection:
    """Stands in for a WebSocket: plays back `incoming`, then ends or raises `error`."""

    def __init__(self, incoming, error=None, close_code=None, close_reason=None):
        # what api sends
        self.incoming = [i if isinstance(i, (str, bytes)) else json.dumps(i) for i in incoming]
        self.error = error
        self.close_code = close_code
        self.close_reason = close_reason
        # what code sends
        self.sent = []

    async def send(self, data):
        # send subscribe request.
        self.sent.append(json.loads(data))

    def __aiter__(self):
        return self._play()

    async def _play(self):
        for item in self.incoming:
            yield item
        if self.error:
            raise self.error

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


def fake_connect(connection=None, fail_with=None):
    # returns function
    def connect(url, **kwargs):
        # function opens connection
        connect.calls.append((url, kwargs))
        if fail_with:
            raise fail_with
        return connection
    
    #  test look at connect.calls afterwards to see what was requested.
    connect.calls = []
    return connect


def run(connect):
    """Drain one session. Returns (received, StreamClosed-or-None)."""

    async def go():
        got = []
        try:
            async for item in stream_messages("wss://test", connect=connect):
                got.append(item)
        except StreamClosed as closed:
            return got, closed
        return got, None

    return asyncio.run(go())


def test_subscribe_request_asks_for_both_streams_on_api_v2():
    assert subscribe_request() == {
        "command": "subscribe",
        "streams": ["transactions", "ledger"],
        "api_version": 2,
    }


def test_sends_the_subscribe_request_on_connect():
    conn = FakeConnection([SUBSCRIBE_OK])
    run(fake_connect(conn))
    assert conn.sent == [subscribe_request()]


def test_connects_with_a_raised_message_size_limit():
    connect = fake_connect(FakeConnection([]))
    run(connect)
    assert connect.calls == [("wss://test", {"max_size": MAX_MESSAGE_BYTES})]


@pytest.mark.parametrize(
    "name", sorted(p.stem for p in FIXTURES.glob("*.json") if p.stem != "ledger_closed")
)
def test_every_transaction_fixture_passes_through_unchanged(name):
    msg = fixture(name)
    # We're playing the XRPL side, first subscribe ok, 
    got, _ = run(fake_connect(FakeConnection([SUBSCRIBE_OK, msg])))
    assert [r.message for r in got] == [msg]


def test_skips_the_response_and_unknown_message_types():
    tx = fixture("payment_xrp_success")
    noise = {"type": "serverStatus", "load_factor": 1}
    got, _ = run(fake_connect(FakeConnection([SUBSCRIBE_OK, noise, tx])))
    assert [r.message for r in got] == [tx]


def test_skips_frames_that_are_not_json_and_keeps_going():
    tx = fixture("payment_xrp_success")
    got, _ = run(fake_connect(FakeConnection([SUBSCRIBE_OK, "not json {", "[1, 2]", tx])))
    assert [r.message for r in got] == [tx]


def test_rejected_subscribe_raises():
    bad = {"type": "response", "status": "error", "error": "unknownStream"}
    with pytest.raises(SubscribeError, match="unknownStream"):
        run(fake_connect(FakeConnection([bad])))


def test_drop_without_close_frame_reports_no_code_after_delivering_what_arrived():
    tx = fixture("payment_xrp_success")
    conn = FakeConnection([SUBSCRIBE_OK, tx, tx], error=ConnectionClosedError(None, None))
    got, closed = run(fake_connect(conn))

    assert len(got) == 2
    assert closed.code is None
    assert closed.reason == "no close frame"
    assert closed.messages == 2


def test_close_code_and_reason_from_the_server_are_kept():
    conn = FakeConnection([SUBSCRIBE_OK], error=ConnectionClosedError(Close(1011, "server error"), None))
    _, closed = run(fake_connect(conn))
    assert (closed.code, closed.reason) == (1011, "server error")
