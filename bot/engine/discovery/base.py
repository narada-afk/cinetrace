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

    @abstractmethod
    def sql(self) -> str: ...

    def params(self) -> dict[str, Any]:
        return {"limit": 100}

    @abstractmethod
    def rows_to_insights(self, rows: Sequence[dict]) -> list[Insight]:
        """Pure function — no DB, no prose. Unit-test this."""

    def discover(self, conn) -> list[Insight]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(self.sql(), self.params())
            rows = cur.fetchall()
        insights = self.rows_to_insights(rows)
        log.info("%s: %d rows → %d insights", self.name, len(rows), len(insights))
        return insights
