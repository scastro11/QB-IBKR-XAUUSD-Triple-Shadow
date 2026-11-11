#!/usr/bin/env python3
"""Reproduce integrity, performance, and realized drawdown from event data."""

from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path

DATA = Path(__file__).resolve().parents[1] / "data" / "runner_ibkr_triple_shadow_events_v1_2.csv"
STARTING_CAPITAL = 1000.0


def load_events(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def main() -> None:
    events = load_events(DATA)
    open_trades: dict[str, list[dict[str, str]]] = defaultdict(list)
    results_r: dict[str, list[float]] = defaultdict(list)
    results_usd: dict[str, list[float]] = defaultdict(list)
    orphan_exits = 0

    for row in events:
        strategy = row["strategy"]
        if row["event"] == "VIRTUAL_ENTRY":
            open_trades[strategy].append(row)
        elif row["event"] == "VIRTUAL_EXIT":
            if not open_trades[strategy]:
                orphan_exits += 1
                continue
            entry_row = open_trades[strategy].pop(0)
            entry = float(entry_row["entry"])
            exit_price = float(row["exit"])
            pnl = exit_price - entry
            if entry_row["direction"] == "SHORT":
                pnl = -pnl
            results_r[strategy].append(float(row["result_r"]))
            results_usd[strategy].append(pnl)  # one virtual XAU ounce

    print(f"events={len(events)}")
    print(f"closed_trades={sum(map(len, results_r.values()))}")
    print(f"orphan_exits={orphan_exits}")
    print(f"open_positions={sum(map(len, open_trades.values()))}")

    for strategy in sorted(results_r):
        values_r = results_r[strategy]
        values_usd = results_usd[strategy]
        equity = STARTING_CAPITAL
        peak = equity
        max_drawdown = 0.0
        loss_streak = max_loss_streak = 0
        for value in values_usd:
            equity += value
            peak = max(peak, equity)
            max_drawdown = max(max_drawdown, peak - equity)
            loss_streak = loss_streak + 1 if value < 0 else 0
            max_loss_streak = max(max_loss_streak, loss_streak)

        net_r = sum(values_r)
        net_usd = sum(values_usd)
        print(
            f"{strategy}: trades={len(values_usd)} wins={sum(v > 0 for v in values_usd)} "
            f"losses={sum(v < 0 for v in values_usd)} net_r={net_r:.6f} "
            f"net_usd={net_usd:.2f} max_dd_usd={max_drawdown:.2f} "
            f"max_dd_pct={max_drawdown / STARTING_CAPITAL * 100:.2f}% "
            f"max_loss_streak={max_loss_streak}"
        )


if __name__ == "__main__":
    main()
