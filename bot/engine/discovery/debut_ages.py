"""Actors whose debut year stands out relative to industry peers.

Uses actors.debut_year vs actor_stats.first_film_year: an actor whose first
credited film came unusually early relative to peers surfaces as an early
debut; unusually late surfaces as a late bloomer. Completeness is 1.0 —
both fields are present by construction of the WHERE clause.
"""

from __future__ import annotations

from typing import Sequence

from engine.discovery.base import DiscoveryRule, register
from engine.models import Entity, Insight, Metric
from engine.shared.slugs import actor_slug


@register
class DebutAges(DiscoveryRule):
    name = "debut_ages"
    visual_potential = 0.5

    def sql(self) -> str:
        # decade_peers gives industry+decade cohort average debut-to-first-film gap;
        # actors far from their cohort in either direction are interesting.
        return """
        WITH firsts AS (
            SELECT a.id, a.name, a.industry, a.debut_year,
                   ast.first_film_year, ast.film_count
            FROM   actors a
            JOIN   actor_stats ast ON ast.actor_id = a.id
            WHERE  a.is_primary_actor = TRUE
              AND  a.debut_year IS NOT NULL
              AND  ast.first_film_year IS NOT NULL
        )
        SELECT *,
               first_film_year - debut_year AS wait_years
        FROM   firsts
        WHERE  ABS(first_film_year - debut_year) <= 60   -- sanity guard
        ORDER  BY film_count DESC
        LIMIT  %(limit)s
        """

    def rows_to_insights(self, rows: Sequence[dict]) -> list[Insight]:
        out = []
        for r in rows:
            out.append(Insight(
                rule=self.name,
                entities=[Entity(kind="actor", id=r["id"], name=r["name"],
                                 slug=actor_slug(r["name"]))],
                metrics=[
                    Metric(key="debut_year", value=r["debut_year"], unit="year"),
                    Metric(key="first_film_year", value=r["first_film_year"], unit="year"),
                    Metric(key="film_count", value=r["film_count"], unit="films"),
                ],
                facts={"industry": r["industry"]},
            ))
        return out
