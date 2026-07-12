"""
Canonical fingerprints for semantic deduplication.

Two insights that describe the same underlying fact must produce the same
fingerprint regardless of phrasing or entity order:

  "Mohanlal and Priyadarshan collaborated 44 times"
  "Priyadarshan directed Mohanlal 44 times"

both reduce to sha1("director_loyalty|actor:123,director:priyadarshan|collab_count|B40")
because entities are sorted and the value is bucketed to a coarse band —
44 vs 45 films land in the same bucket, so a +1 change doesn't defeat dedup.
"""

from __future__ import annotations

import hashlib

from engine.models import Insight


def _bucket(value: float | int) -> str:
    """Coarse value band: one significant digit (44 → B40, 45 → B40, 128 → B100)."""
    v = abs(float(value))
    if v == 0:
        return "B0"
    if v < 10:
        return f"B{int(v)}"
    magnitude = 10 ** (len(str(int(v))) - 1)
    return f"B{int(round(v / magnitude) * magnitude)}"


def _entity_key(e) -> str:
    # Prefer stable DB id; fall back to normalized name (directors have no id)
    ident = str(e.id) if e.id else e.name.strip().lower()
    return f"{e.kind}:{ident}"


def canonical_fingerprint(insight: Insight) -> str:
    entities = ",".join(sorted(_entity_key(e) for e in insight.entities))
    metric   = insight.primary_metric
    raw = f"{insight.rule}|{entities}|{metric.key}|{_bucket(metric.value)}"
    return hashlib.sha1(raw.encode()).hexdigest()
