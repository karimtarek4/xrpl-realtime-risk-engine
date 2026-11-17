# Risk Signals — What the Decision Engine Looks For

Living document. Updated as we observe patterns in real data and add new features.
Signals are grouped by type. Each entry notes the data source and rough severity.

---

## 1. Velocity (Speed Layer — 10-min window)

| Signal | Condition | Severity | Notes |
|---|---|---|---|
| High payment frequency | `payment_count > N` in a 10-min window | High | Saw 1,846 payments in one window from a single account — not human behaviour |
| Sustained high frequency | Same account high `payment_count` across multiple consecutive windows | High | Suggests a bot or automated sender, not a one-off burst |
| Large single transfer | `total_amount_xrp` is very high with `payment_count` of 1–2 | Medium | Could be legitimate (exchange settlement, whale) — needs 30-day baseline to contextualise |
| Spend spike | `total_amount_xrp` >> account's 30-day average (batch feature) | High | Round number amounts (e.g. exactly 2,000,000 XRP) are an additional flag |

---

## 2. Token Payments (no amount_xrp)

| Signal | Condition | Severity | Notes |
|---|---|---|---|
| High token payment frequency | `payment_count` high but `total_amount_xrp` is null | Medium | 24% of payments are token-only; can't compare amounts to XRP thresholds — treat count separately |

---

## 3. Behavioural Patterns (batch layer — 30-day window)

| Signal | Condition | Severity | Notes |
|---|---|---|---|
| New account, large transfer | Account age low + high `total_amount_xrp` | High | No history to establish a baseline — large amounts from new accounts are suspicious |
| Unusual active hours | Active at hours outside account's normal pattern | Medium | Requires 30-day baseline of usual active hours |
| New destinations (fan-out) | High number of unique destinations in 1-h window (P1 feature) | Medium | Account suddenly sending to many new counterparties it has never transacted with |

---

## 4. Observations from Real Data (2026-11-14)

Patterns spotted when inspecting speed layer output — will inform rule thresholds.

- `rpr6g53dmrKw78gk6xBX` — 1,846 payments in one 10-min window. Likely a bot or exchange hot wallet.
- `r9DW7jqMXqa32TaTujSx` — 1,377 and 913 payments across two separate windows. Sustained automated activity.
- `r9nsQj5fGr1TcDpCGqnt` — 42,995,364 XRP in 2 payments. Largest volume seen. Needs 30-day baseline to judge.
- `rPyCQm8E5j78PDbrfKF2` — exactly 2,000,000 XRP in 1 payment. Round number flag.
- 1,059 accounts active across multiple windows; 3 accounts spanned 20 windows (3+ hours of continuous activity).

---

## 5. Open Questions

- What threshold for `payment_count` separates legitimate high-frequency senders (exchanges) from suspicious ones?
- How do we handle known exchange hot wallets — whitelist, or separate scoring track?
- Token payments: can we infer value from a price oracle, or do we score them on count alone?
