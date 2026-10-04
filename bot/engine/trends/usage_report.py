"""
Print real Claude usage/cost for the trend-awareness layer's two LLM calls
(nickname resolution, reasoning) — ground-truth token counts from
response.usage, not an estimate.

Run manually:  python -m engine.trends.usage_report
"""

from __future__ import annotations

import argparse

from engine import db as engine_db


def _print_window(days: int) -> None:
    rows = engine_db.trend_llm_usage_summary(days=days)
    label = "today" if days == 1 else f"last {days} days"
    if not rows:
        print(f"  {label}: no calls recorded")
        return
    total_cost = 0.0
    for r in rows:
        print(f"  {label} — {r['call_type']:<10} "
              f"calls={r['calls']:<5} "
              f"in={r['input_tokens']:<8} out={r['output_tokens']:<7} "
              f"≈${r['estimated_cost_usd']}")
        total_cost += r["estimated_cost_usd"]
    print(f"  {label} — TOTAL ≈ ${round(total_cost, 4)}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, action="append",
                        help="window(s) to report, in days. Repeatable. Default: 1 7 30")
    args = parser.parse_args()
    windows = args.days or [1, 7, 30]

    print("Trend-awareness LLM usage (real token counts, estimated cost at current Haiku pricing)")
    for days in windows:
        _print_window(days)
        print()


if __name__ == "__main__":
    main()
