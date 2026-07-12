"""
Ranking feature functions — each maps (Insight, RankContext) → [0, 1].

Pure functions: all external state (fingerprint history, fame stats) is
pre-fetched into RankContext so features are unit-testable without a DB.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from engine.models import Insight


@dataclass
class RankContext:
    # fingerprint → times this fact was discovered in the past year
    fingerprint_history: dict[str, int] = field(default_factory=dict)
    # actor_id → {"film_count": int, "costar_count": int, "is_primary": bool}
    fame_stats: dict[int, dict] = field(default_factory=dict)
    # rule name → static visual hint (from DiscoveryRule.visual_potential)
    rule_visual: dict[str, float] = field(default_factory=dict)
    now: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


def novelty(insight: Insight, fingerprint: str, ctx: RankContext) -> float:
    """1.0 for never-seen facts, decaying with each rediscovery."""
    seen = ctx.fingerprint_history.get(fingerprint, 0)
    return 1.0 / (1.0 + seen)


def surprise(insight: Insight, ctx: RankContext) -> float:
    """Magnitude-based: how extreme is the primary metric?

    Tiered like the backend's _wow_score, normalised to [0,1].
    Rules can override by putting a 'percentile' in facts (0-100).
    """
    pct = insight.facts.get("percentile")
    if isinstance(pct, (int, float)):
        return min(1.0, max(0.0, pct / 100))

    v = abs(float(insight.primary_metric.value))
    if v >= 500: return 1.0
    if v >= 200: return 0.85
    if v >= 100: return 0.7
    if v >= 50:  return 0.55
    if v >= 30:  return 0.4
    if v >= 15:  return 0.3
    if v >= 6:   return 0.2
    return 0.1


def popularity(insight: Insight, ctx: RankContext) -> float:
    """Fame of the involved actors — port of backend _fame_score, /50 normalised.

    Directors/industries contribute nothing (no fame stats); insights with
    no actor entities get a neutral 0.4 so director rules aren't buried.
    """
    ids = insight.actor_ids()
    stats = [ctx.fame_stats[i] for i in ids if i in ctx.fame_stats]
    if not stats:
        return 0.4

    total = 0.0
    for s in stats:
        fc = s.get("film_count", 0)
        cc = s.get("costar_count", 0)
        if fc >= 200: total += 25
        elif fc >= 100: total += 18
        elif fc >= 50: total += 10
        elif fc >= 20: total += 4
        if cc >= 200: total += 15
        elif cc >= 100: total += 10
        elif cc >= 50: total += 5
        if s.get("is_primary"): total += 8
        if s.get("is_primary") and fc >= 150: total += 10
    return min(1.0, (total / len(stats)) / 50.0)


def visual_potential(insight: Insight, ctx: RankContext) -> float:
    """Static per-rule hint + bonus when entities have stat-card slugs."""
    base = ctx.rule_visual.get(insight.rule, 0.5)
    has_slug = any(e.slug for e in insight.entities)
    return min(1.0, base + (0.1 if has_slug else 0.0))


def recency(insight: Insight, ctx: RankContext) -> float:
    """How recent is the underlying period? Timeless facts get a neutral 0.5."""
    periods = [m.period for m in insight.metrics if m.period]
    if not periods:
        return 0.5
    latest = max(p[1] for p in periods)
    age = max(0, ctx.now.year - latest)
    if age <= 1:  return 1.0
    if age <= 5:  return 0.8
    if age <= 10: return 0.6
    if age <= 20: return 0.4
    return 0.25


def completeness(insight: Insight, ctx: RankContext) -> float:
    return max(0.0, min(1.0, insight.completeness))
