"""
Deduplication — three layers:

1. Cooldown:  fingerprints posted within the cooldown window are dropped.
   The fingerprint is phrasing-independent (sorted entities, bucketed value),
   so "Mohanlal & Priyadarshan: 44 films" and "Priyadarshan directed Mohanlal
   44 times" share one cooldown.
2. Batch:     within a run, keep only the best-scored insight per fingerprint.
3. Diversity: max N insights per actor per batch (default 1) so one megastar
   doesn't fill every slot.

semantic_check() is a documented no-op hook for future embedding-based
similarity — the interface is here so adding it later touches one function.
"""

from __future__ import annotations

from engine.config import EngineConfig
from engine.models import RankedInsight
from engine.shared.logging import get_logger

log = get_logger("dedup")


def dedup(ranked: list[RankedInsight],
          on_cooldown: set[str],
          recently_used_actor_ids: set[int],
          config: EngineConfig) -> list[RankedInsight]:
    """Input must be sorted best-first (rank() guarantees this)."""
    seen_fp: set[str] = set()
    actor_counts: dict[int, int] = {}
    out: list[RankedInsight] = []

    for r in ranked:
        if r.fingerprint in on_cooldown:
            continue
        if r.fingerprint in seen_fp:
            continue
        ids = r.insight.actor_ids()
        if any(i in recently_used_actor_ids for i in ids):
            continue
        if any(actor_counts.get(i, 0) >= config.max_per_actor_per_day for i in ids):
            continue
        if not semantic_check(r):
            continue

        seen_fp.add(r.fingerprint)
        for i in ids:
            actor_counts[i] = actor_counts.get(i, 0) + 1
        out.append(r)

    log.info("dedup: %d → %d (cooldown %d fingerprints)",
             len(ranked), len(out), len(on_cooldown))
    return out


def semantic_check(ranked: RankedInsight) -> bool:
    """Hook for embedding-based similarity against recently posted content.

    Currently a no-op (returns True). To implement: embed the insight's
    entity+metric summary, compare against embeddings of the last N posted
    content_items, reject above a cosine threshold.
    """
    return True
