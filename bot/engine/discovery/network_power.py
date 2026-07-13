"""Actors connected to the most unique co-stars.

Ported from backend/app/insight_engine.py::_network_power.
"""

from __future__ import annotations

from typing import Sequence

from engine.discovery.base import DiscoveryRule, register
from engine.models import Entity, Insight, Metric
from engine.shared.slugs import actor_slug


@register
class NetworkPower(DiscoveryRule):
    name = "network_power"
    visual_potential = 0.5

    def sql(self) -> str:
        return """
        SELECT a.id, a.name, a.industry,
               COUNT(DISTINCT ac.actor2_id) AS costar_count,
               ast.film_count
        FROM   actors a
        JOIN   actor_stats          ast ON ast.actor_id = a.id
        JOIN   actor_collaborations ac  ON ac.actor1_id = a.id
        WHERE  a.is_primary_actor = TRUE
        GROUP  BY a.id, a.name, a.industry, ast.film_count
        HAVING COUNT(DISTINCT ac.actor2_id) >= 100
        ORDER  BY costar_count DESC
        LIMIT  %(limit)s
        """

    def rows_to_insights(self, rows: Sequence[dict]) -> list[Insight]:
        return [
            Insight(
                rule=self.name,
                entities=[Entity(kind="actor", id=r["id"], name=r["name"],
                                 slug=actor_slug(r["name"]))],
                metrics=[
                    Metric(key="unique_costars", value=r["costar_count"], unit="co-stars"),
                    Metric(key="film_count", value=r["film_count"], unit="films"),
                ],
                facts={"industry": r["industry"]},
                confidence=1.0,   # verified counts from precomputed tables
            )
            for r in rows
        ]
