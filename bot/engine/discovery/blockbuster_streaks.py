"""Longest consecutive-year streaks of ₹100Cr+ films per actor.

Box office is sparse: completeness reflects the actor's box office data
coverage so the ranker can penalise thin evidence.
"""

from __future__ import annotations

from typing import Sequence

from engine.discovery.base import DiscoveryRule, register
from engine.models import Entity, Insight, Metric
from engine.shared.slugs import actor_slug
from engine.shared.sql import NOT_BROKEN, PRIMARY_CREDITS_CTE


@register
class BlockbusterStreaks(DiscoveryRule):
    name = "blockbuster_streaks"
    visual_potential = 0.7

    def sql(self) -> str:
        return f"""
        WITH {PRIMARY_CREDITS_CTE},
        hit_years AS (
            SELECT DISTINCT ac.actor_id, m.release_year
            FROM   all_credits ac
            JOIN   movies m ON m.id = ac.movie_id
            WHERE  m.box_office >= 100
              AND  m.release_year IS NOT NULL
              AND  {NOT_BROKEN}
        ),
        streaks AS (
            SELECT actor_id, release_year,
                   release_year - ROW_NUMBER() OVER (
                       PARTITION BY actor_id ORDER BY release_year
                   ) AS grp
            FROM   hit_years
        ),
        best AS (
            SELECT actor_id,
                   COUNT(*)           AS streak_len,
                   MIN(release_year)  AS streak_start,
                   MAX(release_year)  AS streak_end
            FROM   streaks
            GROUP  BY actor_id, grp
        ),
        top_per_actor AS (
            SELECT DISTINCT ON (actor_id) *
            FROM best ORDER BY actor_id, streak_len DESC
        ),
        bo_coverage AS (
            SELECT ac.actor_id,
                   COUNT(*) FILTER (WHERE m.box_office IS NOT NULL) AS with_bo,
                   COUNT(*)                                          AS total
            FROM   all_credits ac
            JOIN   movies m ON m.id = ac.movie_id
            GROUP  BY ac.actor_id
        )
        SELECT a.id, a.name, a.industry,
               t.streak_len, t.streak_start, t.streak_end,
               bc.with_bo, bc.total
        FROM   top_per_actor t
        JOIN   actors a       ON a.id = t.actor_id
        JOIN   bo_coverage bc ON bc.actor_id = t.actor_id
        WHERE  t.streak_len >= 3
          AND  a.is_primary_actor = TRUE
        ORDER  BY t.streak_len DESC
        LIMIT  %(limit)s
        """

    def rows_to_insights(self, rows: Sequence[dict]) -> list[Insight]:
        out = []
        for r in rows:
            coverage = r["with_bo"] / max(1, r["total"])
            out.append(Insight(
                rule=self.name,
                entities=[Entity(kind="actor", id=r["id"], name=r["name"],
                                 slug=actor_slug(r["name"]))],
                metrics=[
                    Metric(key="streak_years", value=r["streak_len"], unit="years",
                           period=(r["streak_start"], r["streak_end"])),
                ],
                facts={"threshold_cr": 100, "industry": r["industry"],
                       "box_office_coverage_pct": round(coverage * 100)},
                completeness=round(min(1.0, 0.4 + coverage * 0.6), 2),
            ))
        return out
