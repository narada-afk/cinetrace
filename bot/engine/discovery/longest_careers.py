"""Actors with the longest active careers (first film → last film span)."""

from __future__ import annotations

from typing import Sequence

from engine.discovery.base import DiscoveryRule, register
from engine.models import Entity, Insight, Metric
from engine.shared.slugs import actor_slug


@register
class LongestCareers(DiscoveryRule):
    name = "longest_careers"
    visual_potential = 0.7   # career chart

    def sql(self) -> str:
        return """
        SELECT a.id, a.name, a.industry,
               ast.first_film_year, ast.last_film_year, ast.film_count,
               ast.last_film_year - ast.first_film_year AS span_years
        FROM   actor_stats ast
        JOIN   actors a ON a.id = ast.actor_id
        WHERE  a.is_primary_actor = TRUE
          AND  ast.first_film_year IS NOT NULL
          AND  ast.last_film_year  IS NOT NULL
          AND  ast.last_film_year - ast.first_film_year >= 30
        ORDER  BY span_years DESC, ast.film_count DESC
        LIMIT  %(limit)s
        """

    def rows_to_insights(self, rows: Sequence[dict]) -> list[Insight]:
        return [
            Insight(
                rule=self.name,
                entities=[Entity(kind="actor", id=r["id"], name=r["name"],
                                 slug=actor_slug(r["name"]))],
                metrics=[
                    Metric(key="career_span_years", value=r["span_years"], unit="years",
                           period=(r["first_film_year"], r["last_film_year"])),
                    Metric(key="film_count", value=r["film_count"], unit="films"),
                ],
                facts={"first_film_year": r["first_film_year"],
                       "last_film_year": r["last_film_year"],
                       "industry": r["industry"]},
                confidence=1.0,   # spans from actor_stats (clean year data)
            )
            for r in rows
        ]
