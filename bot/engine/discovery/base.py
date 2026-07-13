"""
Discovery rule interface + registry.

A rule is a class with:
  name: str                — unique registered name (== Insight.rule)
  visual_potential: float  — static 0-1 hint for the ranker
  sql() -> str             — the raw SQL (parametrised, read-only)
  params() -> dict         — bind params for sql()
  rows_to_insights(rows)   — pure function: DB rows → list[Insight]
                             (unit-testable without a database)

discover(conn) runs sql() and feeds rows to rows_to_insights().

Adding a rule = create one module in engine/discovery/ with @register.
Auto-import in __init__.py picks it up — nothing else changes.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Sequence

import psycopg2.extras

from engine.models import Insight
from engine.shared.logging import get_logger

log = get_logger("discovery")

_REGISTRY: dict[str, "DiscoveryRule"] = {}


def register(rule_cls: type["DiscoveryRule"]) -> type["DiscoveryRule"]:
    """Class decorator: instantiate and register a discovery rule."""
    instance = rule_cls()
    if instance.name in _REGISTRY:
        raise ValueError(f"duplicate discovery rule name: {instance.name}")
    _REGISTRY[instance.name] = instance
    return rule_cls


def all_rules() -> list["DiscoveryRule"]:
    return list(_REGISTRY.values())


def get_rule(name: str) -> "DiscoveryRule":
    return _REGISTRY[name]


class DiscoveryRule(ABC):
    name: str
    visual_potential: float = 0.5   # static hint; entities with slugs add bonus
    # Human-readable hint shown in rule-health output when the rule emits nothing
    empty_remediation: str = "check source-table coverage for this rule's WHERE clause"

    def __init__(self) -> None:
        # Populated by discover(); read by the pipeline for rule-health reporting
        self.last_health: dict[str, Any] = {}

    @abstractmethod
    def sql(self) -> str: ...

    def params(self) -> dict[str, Any]:
        return {"limit": 100}

    @abstractmethod
    def rows_to_insights(self, rows: Sequence[dict]) -> list[Insight]:
        """Pure function — no DB, no prose. Unit-test this."""

    def discover(self, conn) -> list[Insight]:
        import time
        t0 = time.time()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(self.sql(), self.params())
                rows = cur.fetchall()
        except Exception as e:
            self.last_health = {
                "rule": self.name, "status": "broken",
                "reason": str(e)[:300],
                "rows_scanned": 0, "rows_emitted": 0,
                "seconds": round(time.time() - t0, 2),
            }
            raise
        insights = self.rows_to_insights(rows)
        seconds = round(time.time() - t0, 2)
        if not insights:
            status, reason = "warning", f"0 insights emitted — {self.empty_remediation}"
        elif seconds > 5:
            status, reason = "warning", f"slow: {seconds}s"
        else:
            status, reason = "healthy", None
        self.last_health = {
            "rule": self.name, "status": status, "reason": reason,
            "rows_scanned": len(rows), "rows_emitted": len(insights),
            "seconds": seconds,
        }
        log.info("%s: %d rows → %d insights (%.2fs)", self.name, len(rows),
                 len(insights), seconds)
        return insights
