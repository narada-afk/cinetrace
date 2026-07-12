"""Directors with 1000+ Cr total box office.

Ported from backend/app/insight_engine.py::_director_box_office.
Box office is sparse — completeness reflects how many of the director's
films actually carry box office data.
"""

from __future__ import annotations

from typing import Sequence

from engine.discovery.base import DiscoveryRule, register
from engine.models import Entity, Insight, Metric
from engine.shared.sql import NOT_BROKEN, SOUTH_INDUSTRIES


@register
class DirectorBoxOffice(DiscoveryRule):
    name = "director_box_office"
    visual_potential = 0.4   # directors have no stat-card avatar

    def sql(self) -> str:
        return f"""
        SELECT m.director,
               ROUND(SUM(m.box_office)) AS total_cr,
               ROUND(MAX(m.box_office)) AS biggest_cr,
               COUNT(*)                 AS films_with_bo,
               (SELECT COUNT(*) FROM movies m3
                WHERE m3.director = m.director
                  AND m3.industry IN {SOUTH_INDUSTRIES}) AS total_films,
               (SELECT m2.title FROM movies m2
                WHERE m2.director = m.director
                  AND m2.box_office IS NOT NULL
                  AND m2.industry IN {SOUTH_INDUSTRIES}
                ORDER BY m2.box_office DESC LIMIT 1) AS biggest_title
        FROM   movies m
        WHERE  m.box_office IS NOT NULL
          AND  m.director   IS NOT NULL
          AND  m.industry   IN {SOUTH_INDUSTRIES}
          AND  {NOT_BROKEN}
        GROUP  BY m.director
        HAVING SUM(m.box_office) >= 1000
        ORDER  BY total_cr DESC
        LIMIT  %(limit)s
        """

    def rows_to_insights(self, rows: Sequence[dict]) -> list[Insight]:
        out = []
        for r in rows:
            bo_coverage = r["films_with_bo"] / max(1, r["total_films"])
            out.append(Insight(
                rule=self.name,
                entities=[Entity(kind="director", name=r["director"])],
                metrics=[
                    Metric(key="total_gross_cr", value=int(r["total_cr"]), unit="₹Cr"),
                    Metric(key="biggest_hit_cr", value=int(r["biggest_cr"]), unit="₹Cr"),
                ],
                facts={"biggest_hit_title": r["biggest_title"],
                       "films_with_box_office_data": r["films_with_bo"]},
                completeness=round(min(1.0, 0.5 + bo_coverage / 2), 2),
            ))
        return out
