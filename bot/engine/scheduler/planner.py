"""
Slot planner — assign ranked insights to the day's posting slots.

Diversity constraints:
  - at most one insight per industry until every industry is represented
  - at most one insight per rule per day (varied content types)
"""

from __future__ import annotations

from engine.models import RankedInsight
from engine.shared.logging import get_logger

log = get_logger("planner")


def plan_slots(ranked: list[RankedInsight], n_slots: int) -> list[RankedInsight]:
    """Pick n_slots insights from a best-first ranked list."""
    chosen: list[RankedInsight] = []
    used_industries: set[str] = set()
    used_rules: set[str] = set()

    # Pass 1: strict diversity — new industry AND new rule
    for r in ranked:
        if len(chosen) >= n_slots:
            break
        industry = str(r.insight.facts.get("industry") or "")
        if r.insight.rule in used_rules:
            continue
        if industry and industry in used_industries:
            continue
        chosen.append(r)
        used_rules.add(r.insight.rule)
        if industry:
            used_industries.add(industry)

    # Pass 2: relax industry, keep rule diversity
    for r in ranked:
        if len(chosen) >= n_slots:
            break
        if r in chosen or r.insight.rule in used_rules:
            continue
        chosen.append(r)
        used_rules.add(r.insight.rule)

    # Pass 3: fill with best remaining regardless
    for r in ranked:
        if len(chosen) >= n_slots:
            break
        if r not in chosen:
            chosen.append(r)

    log.info("planned %d/%d slots (rules: %s)", len(chosen), n_slots,
             [r.insight.rule for r in chosen])
    return chosen
