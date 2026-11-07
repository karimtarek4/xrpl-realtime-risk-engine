"""Validator consumer: reads xrpl.raw, validates, routes to xrpl.payments or xrpl.dlq."""
import asyncio
import json
import logging
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from .validator import validate, ValidPayment, InvalidPayment

log = logging.getLogger(__name__)

TOPIC_RAW = "xrpl.raw"
TOPIC_PAYMENTS = "xrpl.payments"
TOPIC_DLQ = "xrpl.dlq"


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _build_payment(valid: ValidPayment) -> dict:
    return {
        **valid.envelope,
        "account": valid.account,
        "destination": valid.destination,
        "amount_xrp": valid.amount_xrp,
        "amount_token": valid.amount_token,
    }


def _build_dlq(invalid: InvalidPayment) -> dict:
    return {
        "reason_code": invalid.reason_code,
        "stage": "validator",
        "failed_at": _now_utc(),
        "original": invalid.envelope,
    }


async def run(bootstrap_servers: str = "localhost:9092", group_id: str = "validator"):
    consumer = AIOKafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode()),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        enable_idempotence=True,
        acks="all",
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: k.encode() if k else None,
    )

    await consumer.start()
    await producer.start()
    log.info("Validator consumer started. Reading from %s", TOPIC_RAW)

    seen: set[str] = set()

    try:
        async for msg in consumer:
            envelope = msg.value
            event_id = envelope.get("event_id")

            if not event_id:
                log.warning("Received envelope with no event_id, skipping")
                continue

            if event_id in seen:
                log.debug("Duplicate event_id %s, skipping", event_id)
                continue

            result = validate(envelope)

            if isinstance(result, ValidPayment):
                payment = _build_payment(result)
                await producer.send(TOPIC_PAYMENTS, value=payment, key=result.account)
                seen.add(event_id)
                log.debug("Routed %s → %s", event_id, TOPIC_PAYMENTS)
            else:
                dlq_msg = _build_dlq(result)
                headers = [("reason_code", result.reason_code.encode())]
                await producer.send(TOPIC_DLQ, value=dlq_msg, key=event_id, headers=headers)
                seen.add(event_id)
                log.debug("Routed %s → %s (%s)", event_id, TOPIC_DLQ, result.reason_code)

    finally:
        await consumer.stop()
        await producer.stop()
        log.info("Validator consumer stopped.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(run())
