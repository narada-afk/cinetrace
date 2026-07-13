"""Actors who almost never repeat a director — highest unique-director ratio."""

from __future__ import annotations

from typing import Sequence

from engine.discovery.base import DiscoveryRule, register
from engine.models import Entity, Insight, Metric
from engine.shared.slugs import actor_slug


@register
class CollaborationDiversity(DiscoveryRule):
    name = "collaboration_diversity"
    visual_potential = 0.5

    def sql(self) -> str:
        return """
        SELECT a.id, a.name, a.industry,
               COUNT(DISTINCT ads.director)   AS unique_directors,
               ast.film_count,
               ROUND(COUNT(DISTINCT ads.director) * 100.0
                     / NULLIF(ast.film_count, 0)) AS pct
        FROM   actor_director_stats ads
        JOIN   actor_stats ast ON ast.actor_id = ads.actor_id
        JOIN   actors a        ON a.id = ads.actor_id
        WHERE  a.is_primary_actor = TRUE
          AND  ast.film_count >= 30
        GROUP  BY a.id, a.name, a.industry, ast.film_count
        HAVING COUNT(DISTINCT ads.director) >= 25
        ORDER  BY unique_directors DESC
        LIMIT  %(limit)s
        """

    def rows_to_insights(self, rows: Sequence[dict]) -> list[Insight]:
        return [
            Insight(
                rule=self.name,
                entities=[Entity(kind="actor", id=r["id"], name=r["name"],
                                 slug=actor_slug(r["name"]))],
                metrics=[
                    Metric(key="unique_directors", value=r["unique_directors"], unit="directors"),
                    Metric(key="film_count", value=r["film_count"], unit="films"),
                    Metric(key="directors_per_100_films", value=int(r["pct"] or 0), unit="%"),
                ],
                facts={"industry": r["industry"]},
                confidence=0.9,   # ratio derived from two tables (minor assumptions)
            )
            for r in rows
        ]
