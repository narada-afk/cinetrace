"""Career comebacks: actors with a 5+ year gap between films who then returned."""

from __future__ import annotations

from datetime import datetime
from typing import Sequence

from engine.discovery.base import DiscoveryRule, register
from engine.models import Entity, Insight, Metric
from engine.shared.slugs import actor_slug
from engine.shared.sql import PRIMARY_CREDITS_CTE, sane_year

CURRENT_YEAR = datetime.now().year


@register
class LongestFilmGaps(DiscoveryRule):
    name = "longest_film_gaps"
    visual_potential = 0.7   # career chart shows the gap clearly

    def sql(self) -> str:
        return f"""
        WITH {PRIMARY_CREDITS_CTE},
        years AS (
            SELECT DISTINCT ac.actor_id, m.release_year
            FROM   all_credits ac
            JOIN   movies m ON m.id = ac.movie_id
            WHERE  {sane_year("m.release_year")}
        ),
        gaps AS (
            SELECT actor_id, release_year AS gap_end,
                   LAG(release_year) OVER (
                       PARTITION BY actor_id ORDER BY release_year
                   ) AS gap_start
            FROM   years
        ),
        biggest AS (
            SELECT DISTINCT ON (actor_id)
                   actor_id, gap_start, gap_end, gap_end - gap_start AS gap_years
            FROM   gaps
            WHERE  gap_start IS NOT NULL
            ORDER  BY actor_id, gap_end - gap_start DESC
        )
        SELECT a.id, a.name, a.industry, ast.film_count,
               b.gap_start, b.gap_end, b.gap_years
        FROM   biggest b
        JOIN   actors a        ON a.id = b.actor_id
        JOIN   actor_stats ast ON ast.actor_id = b.actor_id
        WHERE  b.gap_years >= 5
          AND  a.is_primary_actor = TRUE
          AND  ast.film_count >= 20
        ORDER  BY b.gap_years DESC
        LIMIT  %(limit)s
        """

    def rows_to_insights(self, rows: Sequence[dict]) -> list[Insight]:
        return [
            Insight(
                rule=self.name,
                entities=[Entity(kind="actor", id=r["id"], name=r["name"],
                                 slug=actor_slug(r["name"]))],
                metrics=[
                    Metric(key="gap_years", value=r["gap_years"], unit="years",
                           period=(r["gap_start"], r["gap_end"])),
                    Metric(key="film_count", value=r["film_count"], unit="films"),
                ],
                facts={"last_film_before_gap": r["gap_start"],
                       "comeback_year": r["gap_end"],
                       "industry": r["industry"]},
                # A "gap" is inferred from absence of credits — missing
                # filmography years look identical to a real hiatus.
                confidence=0.6,
            )
            for r in rows
        ]
