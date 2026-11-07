# Validator Consumer

Reads raw XRPL envelopes from `xrpl.raw`, validates them, and routes to `xrpl.payments` or `xrpl.dlq`.

```
xrpl.raw  ──►  validator  ──►  xrpl.payments   (valid Payment + tesSUCCESS)
                           └──►  xrpl.dlq        (everything else)
```

## Components

| File | Responsibility |
|---|---|
| `validator.py` | Pure validation and amount normalisation — no I/O |
| `consumer.py` | Kafka consumer loop, deduplication, routing |

## Validation rules

A message passes as a `ValidPayment` only if all of these hold:

1. `tx_json.TransactionType == "Payment"`
2. `meta.TransactionResult == "tesSUCCESS"`
3. `Account` and `Destination` fields are present
4. Amount can be normalised (XRP drops string or token dict)

Anything that fails a check is an `InvalidPayment` with one of these reason codes:

| Reason code | Meaning |
|---|---|
| `not_payment` | TransactionType is not Payment |
| `failed_tx` | TransactionResult is not tesSUCCESS |
| `missing_field` | Account or Destination absent |
| `bad_amount` | Amount field missing or unparseable |

## Amount normalisation

XRPL represents amounts in two forms:

- **XRP** — a string of drops (`"1000000"` = 1 XRP). Converted to a float in XRP.
- **Token** — a dict `{"currency": ..., "issuer": ..., "value": ...}`. Passed through as-is.

`delivered_amount` from `meta` is preferred over `tx_json.Amount` because it reflects the actual delivered amount for partial payments.

## Output schemas

### `xrpl.payments`

The original envelope fields plus the extracted payment fields at the top level:

```json
{
  "event_id": "...",
  "event_time": "...",
  "received_at": "...",
  "ledger_index": 123,
  "transaction_type": "Payment",
  "ingest_mode": "live",
  "payload": { ... },
  "account": "rSender...",
  "destination": "rReceiver...",
  "amount_xrp": 10.5,
  "amount_token": null
}
```

Kafka key: `account` (sender address).

### `xrpl.dlq`

```json
{
  "reason_code": "not_payment",
  "stage": "validator",
  "failed_at": "2024-01-01T00:00:00.000Z",
  "original": { ... }
}
```

Kafka key: `event_id`. Header: `reason_code` (bytes).

## Running

```bash
python -m consumers.validator.consumer
```

Connects to `localhost:9092` by default. Reads from `xrpl.raw` with consumer group `validator` and `auto_offset_reset="earliest"`.

## Statistics (live traffic)

From observing ~372k messages on `xrpl.raw`, the network breakdown is roughly:

- ~9% valid payments → `xrpl.payments`
- ~91% non-payment or failed → `xrpl.dlq`

This reflects the XRPL network's mix of transaction types (OfferCreate, TrustSet, EscrowCreate, etc.) alongside payments.

## Tests

```bash
pytest tests/unit/consumers/validator/
```

Covers pure validator logic (`test_validator.py`) and consumer routing/dedup (`test_consumer.py`) with mocked Kafka.
