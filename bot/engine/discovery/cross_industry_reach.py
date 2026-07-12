"""Primary actors who worked as leads across multiple South Indian industries.

Ported from backend/app/insight_engine.py::_cross_industry_reach.
"""

from __future__ import annotations

from typing import Sequence

from engine.discovery.base import DiscoveryRule, register
from engine.models import Entity, Insight, Metric
from engine.shared.slugs import actor_slug


@register
class CrossIndustryReach(DiscoveryRule):
    name = "cross_industry_reach"
    visual_potential = 0.6

    def sql(self) -> str:
        return """
        SELECT a.id, a.name, a.industry AS home_industry,
               COUNT(DISTINCT LOWER(m.industry)) AS ind_count,
               COUNT(DISTINCT am.movie_id)       AS film_count,
               ARRAY_AGG(DISTINCT m.industry)    AS industries
        FROM   actors a
        JOIN   actor_movies am ON am.actor_id = a.id
        JOIN   movies m        ON m.id = am.movie_id
        WHERE  m.industry IS NOT NULL AND m.industry <> ''
          AND  a.is_primary_actor = TRUE
          AND  am.role_type = 'primary'
        GROUP  BY a.id, a.name, a.industry
        HAVING COUNT(DISTINCT LOWER(m.industry)) >= 3
        ORDER  BY ind_count DESC, film_count DESC
        LIMIT  %(limit)s
        """

    def rows_to_insights(self, rows: Sequence[dict]) -> list[Insight]:
        return [
            Insight(
                rule=self.name,
                entities=[Entity(kind="actor", id=r["id"], name=r["name"],
                                 slug=actor_slug(r["name"]))],
                metrics=[
                    Metric(key="industry_count", value=r["ind_count"], unit="industries"),
                    Metric(key="film_count", value=r["film_count"], unit="films"),
                ],
                facts={"industries": sorted(r["industries"]),
                       "industry": r["home_industry"]},
            )
            for r in rows
        ]
