"""Actors who acted in the most distinct languages (movies.language field).

Language data is enrichment-dependent (sparse) — completeness reflects
how many of the actor's films carry a language value.
"""

from __future__ import annotations

from typing import Sequence

from engine.discovery.base import DiscoveryRule, register
from engine.models import Entity, Insight, Metric
from engine.shared.slugs import actor_slug
from engine.shared.sql import ALL_CREDITS_CTE


@register
class MostMultilingual(DiscoveryRule):
    name = "most_multilingual"
    visual_potential = 0.5

    def sql(self) -> str:
        return f"""
        WITH {ALL_CREDITS_CTE}
        SELECT a.id, a.name, a.industry,
               COUNT(DISTINCT LOWER(m.language)) FILTER (
                   WHERE m.language IS NOT NULL AND m.language <> ''
               )                                              AS lang_count,
               ARRAY_AGG(DISTINCT m.language) FILTER (
                   WHERE m.language IS NOT NULL AND m.language <> ''
               )                                              AS languages,
               COUNT(*) FILTER (
                   WHERE m.language IS NOT NULL AND m.language <> ''
               )                                              AS films_with_lang,
               COUNT(*)                                       AS total_films
        FROM   all_credits ac
        JOIN   actors a ON a.id = ac.actor_id
        JOIN   movies m ON m.id = ac.movie_id
        WHERE  a.is_primary_actor = TRUE
        GROUP  BY a.id, a.name, a.industry
        HAVING COUNT(DISTINCT LOWER(m.language)) FILTER (
                   WHERE m.language IS NOT NULL AND m.language <> ''
               ) >= 4
        ORDER  BY lang_count DESC
        LIMIT  %(limit)s
        """

    def rows_to_insights(self, rows: Sequence[dict]) -> list[Insight]:
        out = []
        for r in rows:
            coverage = r["films_with_lang"] / max(1, r["total_films"])
            out.append(Insight(
                rule=self.name,
                entities=[Entity(kind="actor", id=r["id"], name=r["name"],
                                 slug=actor_slug(r["name"]))],
                metrics=[
                    Metric(key="language_count", value=r["lang_count"], unit="languages"),
                ],
                facts={"languages": sorted(r["languages"] or []),
                       "industry": r["industry"]},
                completeness=round(min(1.0, 0.4 + coverage * 0.6), 2),
                # language field is enrichment-dependent; counts are lower bounds
                confidence=round(min(1.0, 0.3 + coverage * 0.7), 2),
            ))
        return out
