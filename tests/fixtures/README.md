# Test fixtures

Written for: the team building the XRPL producer and validator, including people new to XRPL.

These are real XRPL messages, captured from the public stream at `wss://s1.ripple.com` with `api_version: 2`. Tests read them as input, so the same file always gives the same result. Each file is one message. `meta.AffectedNodes` is trimmed to two entries to keep files small. Nothing else was edited.

## Shape of a v2 message

Every transaction message has the same outer layout:

| Field | Meaning |
|---|---|
| `type` | `"transaction"` for transactions, `"ledgerClosed"` for heartbeats |
| `hash` | Unique transaction ID. This becomes our `event_id`. |
| `tx_json` | The transaction itself: `TransactionType`, `Account`, `Destination`, amounts and so on |
| `meta` | The outcome: `TransactionResult`, `delivered_amount` (for payments), `ParentBatchID` (for batch legs) |
| `engine_result` | Same outcome as `meta.TransactionResult`, at the top level |
| `validated` | `true` means the ledger is final |
| `close_time_iso` | When the ledger closed, in UTC. This becomes our `event_time`. |
| `ledger_index` | Ledger sequence number |

Two amount conventions to know:
- **Drops:** XRP is written as a string of drops. 1 XRP = 1,000,000 drops, so `"1583050000"` is about 1,583 XRP.
- **Token:** a token amount is an object with `currency` (a 40-character hex code), `issuer` (the account that issued it) and `value` (a decimal string).

## The fixtures

### payment_xrp_success.json
- **Case:** a normal, successful XRP payment.
- **Key facts:** `TransactionType` is `Payment`, `engine_result` is `tesSUCCESS`, `DeliverMax` is a drops string (`"1583050000"`), and `meta.delivered_amount` matches it. `Account` and `Destination` are different accounts.
- **Expected validator result:** accepted, sent to `xrpl.payments`. Amount is normalised from drops.
- **Why it matters:** this is the baseline "good payment" that the rest of the pipeline is built around.

### payment_token_success.json
- **Case:** a successful payment in a token (here RLUSD, a stablecoin).
- **Key facts:** `DeliverMax` is an object, not a string. `SendMax` is also an object, so the sender paid in a token too. `meta.delivered_amount` is the same object.
- **Expected validator result:** accepted, sent to `xrpl.payments`. Both string and object amounts must end up in one common format.
- **Why it matters:** it proves the amount normalisation handles tokens, not just XRP.

### payment_failed.json
- **Case:** a payment that was validated but did not happen.
- **Key facts:** `engine_result` and `meta.TransactionResult` are `tecPATH_DRY`, meaning no usable path existed for the payment. There is no `delivered_amount` because nothing moved. `validated` is still `true`.
- **Expected validator result:** rejected, sent to `xrpl.dlq` with `reason_code = tecPATH_DRY`.
- **Why it matters:** `validated: true` only means the ledger is final. It does not mean the payment succeeded. The validator must check the result code.

### payment_self.json
- **Case:** a successful payment where the sender and the receiver are the same account (`Account == Destination`).
- **Key facts:** `SendMax` is XRP drops (`"100"`) and `DeliverMax` is a token object (`BEAR`). This is a currency swap written as a payment to yourself. No money changes owner.
- **Expected validator result:** depends on the rule chosen for self-payments (see the open decision below). It should not be counted as a normal payment for velocity features.
- **Why it matters:** much of the stream is bot trading like this, and counting it as payments would make ordinary accounts look busy.

### payment_batch_inner.json
- **Case:** one leg of a `Batch` transaction. A batch bundles several transactions into one all-or-nothing submission.
- **Key facts:** `meta.ParentBatchID` is set, which marks it as a leg of a batch. `Fee` is `"0"` because the parent batch pays the fee. `SigningPubKey` is empty because the parent signs for it. It is also a self-payment.
- **Expected validator result:** depends on the batch rule (open decision below). It must be recognised as a batch leg, not as an independent payment.
- **Why it matters:** the parent `Batch` is a separate message. Without `ParentBatchID`, the validator cannot tell legs from standalone payments.

### offer_create.json
- **Case:** a successful `OfferCreate`, which places an order on XRPL's built-in exchange. It is not a payment.
- **Key facts:** `TransactionType` is `OfferCreate`. There is no `Destination`, no `DeliverMax`, and no `delivered_amount`. The order is described by `TakerGets` (what the account offers) and `TakerPays` (what it wants in return). `Fee` is `"15"`.
- **Expected validator result:** rejected as "not a payment", sent to `xrpl.dlq`.
- **Why it matters:** this is a large share of the stream. Dropping it is correct, but the reason must be recorded so DLQ counts by reason stay meaningful.

### ledger_closed.json
- **Case:** a `ledgerClosed` heartbeat, not a transaction.
- **Key facts:** `type` is `ledgerClosed`. It has `ledger_index`, `ledger_time`, `txn_count` (151 in this ledger), and `validated_ledgers`. It has no `hash`, no `tx_json`, and no `meta`.
- **Expected producer result:** not published to `xrpl.raw`. It only resets the "connection alive" timer and records the latest ledger index.
- **Why it matters:** ledgers close about every 4 seconds, so silence for much longer means the connection is dead. This is the signal the reconnect watchdog uses.

## Coverage

| Question | Fixture |
|---|---|
| Valid XRP payment | `payment_xrp_success.json` |
| Valid token payment | `payment_token_success.json` |
| Payment with a failure result | `payment_failed.json` |
| Payment to self (swap) | `payment_self.json` |
| Leg inside a batch | `payment_batch_inner.json` |
| Not a payment | `offer_create.json` |
| Not a transaction | `ledger_closed.json` |

## Open decision

`payment_self.json` and `payment_batch_inner.json` need a rule that is not yet written down:
- **Option A:** treat self-payments and batch legs as invalid and send them to the DLQ.
- **Option B:** keep them in `xrpl.payments` with a flag, so the features and rules decide what to do.

The fixtures support either option. Update the expected results above once the choice is made.

## Re-capturing

Live data changes, so these files are a snapshot. To make new fixtures, subscribe with `api_version: 2`, save the message whose case you need, and trim `meta.AffectedNodes`. Note the capture date here if you add one.
