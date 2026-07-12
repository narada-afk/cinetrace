"""
Telegram review-card payload builder for engine content items.

Keeps prose formatting out of the pipeline — this is the only place that
renders an insight's provenance for the human reviewer.
"""

from __future__ import annotations

from engine.models import RankedInsight


def review_header(ranked: RankedInsight, slot_label: str) -> str:
    ins = ranked.insight
    metric = ins.primary_metric
    comps = ranked.score.components
    return (
        f"🧠 {ins.rule}  ·  score {ranked.score.total:.2f}\n"
        f"📊 {metric.key} = {metric.value:g} {metric.unit or ''}\n"
        f"🎯 novelty {comps.get('novelty', 0):.2f} · surprise {comps.get('surprise', 0):.2f}"
        f" · fame {comps.get('popularity', 0):.2f}\n"
        f"🕐 {slot_label}"
    )
