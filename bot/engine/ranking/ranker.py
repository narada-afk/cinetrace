"""
Ranker — weighted composite of feature scores; weights come from EngineConfig.
"""

from __future__ import annotations

from engine.config import EngineConfig
from engine.models import Insight, RankedInsight, Score
from engine.ranking import features
from engine.ranking.features import RankContext
from engine.shared.fingerprint import canonical_fingerprint
from engine.shared.logging import get_logger

log = get_logger("ranker")

_FEATURES = {
    "novelty":          lambda i, fp, ctx: features.novelty(i, fp, ctx),
    "surprise":         lambda i, fp, ctx: features.surprise(i, ctx),
    "popularity":       lambda i, fp, ctx: features.popularity(i, ctx),
    "visual_potential": lambda i, fp, ctx: features.visual_potential(i, ctx),
    "recency":          lambda i, fp, ctx: features.recency(i, ctx),
    "completeness":     lambda i, fp, ctx: features.completeness(i, ctx),
}


def score_insight(insight: Insight, ctx: RankContext,
                  config: EngineConfig) -> RankedInsight:
    fp = canonical_fingerprint(insight)
    components = {
        name: round(fn(insight, fp, ctx), 4)
        for name, fn in _FEATURES.items()
    }
    total = sum(components[k] * w for k, w in config.weights.items())
    return RankedInsight(
        insight=insight,
        fingerprint=fp,
        score=Score(
            total=round(total, 4),
            components=components,
            weights_version=config.weights_version,
        ),
    )


def rank(insights: list[Insight], ctx: RankContext,
         config: EngineConfig) -> list[RankedInsight]:
    """Score, hard-filter, and sort (best first)."""
    ranked: list[RankedInsight] = []
    for ins in insights:
        r = score_insight(ins, ctx, config)
        if r.score.components["completeness"] < config.min_completeness:
            continue
        if ins.actor_ids() and r.score.components["popularity"] < config.min_fame:
            continue
        ranked.append(r)

    ranked.sort(key=lambda r: r.score.total, reverse=True)
    log.info("ranked %d → %d after hard filters", len(insights), len(ranked))
    return ranked
