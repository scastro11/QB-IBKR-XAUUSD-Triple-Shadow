# QB IBKR XAUUSD Triple Shadow

Public research repository for the Quant Boricua XAUUSD Triple Shadow forward-observation project.

> **Status:** observer / disarmed / no broker orders. This repository documents research; it is not investment advice and does not authorize live trading.

## Purpose

The project compares three session definitions over the same completed IBKR XAUUSD midpoint candles:

- **A:** legacy Helsinki session, 11:00–15:59.
- **B:** narrower Helsinki candidate, 13:00–14:59.
- **C:** confirmed Exness-clock interpretation, 11:00–15:59 UTC (07:00–11:59 Puerto Rico during the recorded period).

All recorded positions are virtual. The `broker_order` field is `NO` throughout the supplied dataset.

## Repository map

- `src/`: observer runner and optional telemetry adapter.
- `data/`: recorded signals, virtual-trade events, and observer state.
- `analysis/`: reproducible performance and drawdown calculation.
- `reports/`: reviewed text exports of the development and executive audit reports. Editable Word and binary PDF originals are retained outside the public repository.
- `SECURITY.md`: safe handling of credentials and private infrastructure.

## Reproduce the audit

```bash
python analysis/audit_triple_shadow.py
```

The script pairs each `VIRTUAL_ENTRY` with its corresponding `VIRTUAL_EXIT`, reports integrity checks, and calculates net result, win/loss counts, losing streak, and realized peak-to-trough drawdown for each strategy.

## Run the observer

1. Install Python 3.9+ and the official IBKR Python API.
2. Copy `.env.example` values into your local environment and adjust them for TWS or IB Gateway.
3. Keep `RUNNER_C_TELEMETRY_ENABLED=0` unless you have an authorized ingestion endpoint and token.
4. Start TWS/IB Gateway and run:

```bash
python src/runner_ibkr_triple_shadow_v1_2_4.py
```

The runner is intentionally observer-only. Review the source before executing it.

## Audit snapshot through 2026-11-10

The supplied event file contains 122 events forming 61 closed virtual trades, with no orphan entries, duplicate exits, or open positions. Strategy C recorded 33 trades: 9 winners, 24 losers, net **-$23.63** for one virtual XAU ounce before commissions, and a maximum realized drawdown of **$98.90** from a hypothetical $1,000 starting capital (**9.89%**, rounded). See the executive report and reproducible script for assumptions and limitations.

## Important limitations

- Results are forward-observation records, not broker fills.
- Commissions, financing, latency, and production execution are not fully modeled.
- The three strategies are clock/session variants, not independent market samples.
- No result in this repository establishes profitability or readiness for live capital.
- The unresolved signal-count and time-label differences discussed in the reports require independent replication.

## Collaboration

Issues and pull requests should include the data period, timezone, instrument definition, code version, and a reproducible command. Preserve original data; derived files belong in a separate path.

Quant Boricua: **No apostamos, calculamos.**
