"""Densest 5-year window of an actor's career — their golden era.

Ported from backend/app/insight_engine.py::_career_peak_window.
"""

from __future__ import annotations

from typing import Sequence

from engine.discovery.base import DiscoveryRule, register
from engine.models import Entity, Insight, Metric
from engine.shared.slugs import actor_slug
from engine.shared.sql import PRIMARY_CREDITS_CTE


@register
class CareerPeakWindow(DiscoveryRule):
    name = "career_peak_window"
    visual_potential = 0.7   # career chart pairs well

    def sql(self) -> str:
        return f"""
        WITH {PRIMARY_CREDITS_CTE},
        yearly AS (
            SELECT ac.actor_id, m.release_year, COUNT(*) AS films_in_year
            FROM   all_credits ac
            JOIN   movies m ON m.id = ac.movie_id
            WHERE  m.release_year IS NOT NULL
            GROUP  BY ac.actor_id, m.release_year
        ),
        windows AS (
            SELECT y1.actor_id, y1.release_year AS win_start,
                   SUM(y2.films_in_year) AS win_films
            FROM   yearly y1
            JOIN   yearly y2 ON y2.actor_id = y1.actor_id
                            AND y2.release_year BETWEEN y1.release_year
                                                    AND y1.release_year + 4
            GROUP  BY y1.actor_id, y1.release_year
        ),
        best_window AS (
            SELECT DISTINCT ON (actor_id) actor_id, win_start, win_films
            FROM windows ORDER BY actor_id, win_films DESC
        )
        SELECT a.id, a.name, a.industry,
               bw.win_start AS peak_start, bw.win_start + 4 AS peak_end,
               bw.win_films, ast.film_count AS total_films
        FROM   best_window bw
        JOIN   actors a        ON a.id = bw.actor_id
        JOIN   actor_stats ast ON ast.actor_id = bw.actor_id
        WHERE  bw.win_films >= 10
          AND  bw.win_films::float / NULLIF(ast.film_count, 0) >= 0.25
        ORDER  BY bw.win_films DESC
        LIMIT  %(limit)s
        """

    def rows_to_insights(self, rows: Sequence[dict]) -> list[Insight]:
        return [
            Insight(
                rule=self.name,
                entities=[Entity(kind="actor", id=r["id"], name=r["name"],
                                 slug=actor_slug(r["name"]))],
                metrics=[
                    Metric(key="films_in_window", value=int(r["win_films"]), unit="films",
                           period=(r["peak_start"], r["peak_end"])),
                    Metric(key="total_films", value=r["total_films"], unit="films"),
                ],
                facts={"window_years": 5, "industry": r["industry"],
                       "share_of_career_pct": round(
                           100 * int(r["win_films"]) / max(1, r["total_films"]))},
            )
            for r in rows
        ]
