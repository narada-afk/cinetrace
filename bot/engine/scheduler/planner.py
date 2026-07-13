"""
Slot planner — assign ranked insights to the day's posting slots.

Diversity is a SCHEDULING concern only: ranking already ordered insights by
quality; the planner just prevents any one actor, rule, or category from
dominating the feed. Constraints applied (best-first, greedy):

  within a day    — one insight per rule, one per actor, spread across categories
  across the week — per-actor weekly cap, per-rule weekly cap (from history)
  ordering        — no two consecutive slots share a rule or category

Weekly history is passed in (actor ids used and rule counts in the last 7 days)
so the planner stays a pure function — testable without a database.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from engine.models import RankedInsight
from engine.shared.categories import category_of
from engine.shared.logging import get_logger

log = get_logger("planner")


@dataclass
class PlanHistory:
    """Recent posting history, for cross-day caps."""
    actor_ids_last_week: set[int] = field(default_factory=set)
    rule_counts_last_week: dict[str, int] = field(default_factory=dict)


@dataclass
class DiversityLimits:
    max_per_actor_per_day: int = 1
    max_per_actor_per_week: int = 2
    max_per_rule_per_week: int = 3
    max_per_category_per_day: int = 2


def plan_slots(ranked: list[RankedInsight], n_slots: int,
               history: PlanHistory | None = None,
               limits: DiversityLimits | None = None) -> list[RankedInsight]:
    """Pick n_slots insights from a best-first ranked list under diversity caps."""
    history = history or PlanHistory()
    limits = limits or DiversityLimits()

    chosen: list[RankedInsight] = []
    used_rules: set[str] = set()
    used_industries: set[str] = set()
    day_actor: dict[int, int] = {}
    day_category: dict[str, int] = {}

    def eligible(r: RankedInsight, *, require_new_industry: bool) -> bool:
        rule = r.insight.rule
        cat = category_of(rule)
        ids = r.insight.actor_ids()
        industry = str(r.insight.facts.get("industry") or "")

        if rule in used_rules:
            return False
        if history.rule_counts_last_week.get(rule, 0) >= limits.max_per_rule_per_week:
            return False
        if day_category.get(cat, 0) >= limits.max_per_category_per_day:
            return False
        for aid in ids:
            if day_actor.get(aid, 0) >= limits.max_per_actor_per_day:
                return False
            # weekly cap: history count + this day's usage
            wk = (aid in history.actor_ids_last_week) + day_actor.get(aid, 0)
            if wk >= limits.max_per_actor_per_week:
                return False
        if require_new_industry and industry and industry in used_industries:
            return False
        return True

    def take(r: RankedInsight) -> None:
        chosen.append(r)
        used_rules.add(r.insight.rule)
        day_category[category_of(r.insight.rule)] = day_category.get(category_of(r.insight.rule), 0) + 1
        industry = str(r.insight.facts.get("industry") or "")
        if industry:
            used_industries.add(industry)
        for aid in r.insight.actor_ids():
            day_actor[aid] = day_actor.get(aid, 0) + 1

    # Pass 1: strict — new rule, new industry, respecting all caps
    for r in ranked:
        if len(chosen) >= n_slots:
            break
        if eligible(r, require_new_industry=True):
            take(r)

    # Pass 2: relax the industry requirement (keep every other cap)
    for r in ranked:
        if len(chosen) >= n_slots:
            break
        if r not in chosen and eligible(r, require_new_industry=False):
            take(r)

    # No rule-agnostic fallback fill: under-filling a slot is preferable to
    # posting two of the same rule/actor in a day. With the full candidate
    # pool (14 rules × dozens each) this never triggers; it only bounds the
    # degenerate case where the pool itself lacks variety.
    ordered = _avoid_adjacent(chosen)
    log.info("planned %d/%d slots (rules: %s)", len(ordered), n_slots,
             [r.insight.rule for r in ordered])
    return ordered


def _avoid_adjacent(items: list[RankedInsight]) -> list[RankedInsight]:
    """Reorder greedily so no two neighbours share a rule or category."""
    remaining = list(items)
    out: list[RankedInsight] = []
    while remaining:
        prev = out[-1] if out else None
        pick = None
        for r in remaining:
            if prev is None or (
                r.insight.rule != prev.insight.rule
                and category_of(r.insight.rule) != category_of(prev.insight.rule)
            ):
                pick = r
                break
        if pick is None:          # unavoidable clash — take the best remaining
            pick = remaining[0]
        out.append(pick)
        remaining.remove(pick)
    return out
