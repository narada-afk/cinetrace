"""Supporting actors with 150+ films — output rivalling lead actors.

Ported from backend/app/insight_engine.py::_hidden_dominance.
"""

from __future__ import annotations

from typing import Sequence

from engine.discovery.base import DiscoveryRule, register
from engine.models import Entity, Insight, Metric
from engine.shared.slugs import actor_slug


@register
class HiddenDominance(DiscoveryRule):
    name = "hidden_dominance"
    visual_potential = 0.6

    def sql(self) -> str:
        return """
        SELECT a.id, a.name, a.industry, COUNT(am.movie_id) AS film_count,
               (SELECT ROUND(AVG(ast.film_count))
                FROM actor_stats ast JOIN actors ap ON ap.id = ast.actor_id
                WHERE ap.is_primary_actor = TRUE) AS lead_avg
        FROM   actors a
        JOIN   actor_movies am ON am.actor_id = a.id
        WHERE  am.role_type = 'supporting'
          AND  a.is_primary_actor = FALSE
        GROUP  BY a.id, a.name, a.industry
        HAVING COUNT(am.movie_id) >= 150
        ORDER  BY film_count DESC
        LIMIT  %(limit)s
        """

    def rows_to_insights(self, rows: Sequence[dict]) -> list[Insight]:
        return [
            Insight(
                rule=self.name,
                entities=[Entity(kind="actor", id=r["id"], name=r["name"],
                                 slug=actor_slug(r["name"]))],
                metrics=[
                    Metric(key="supporting_film_count", value=r["film_count"], unit="films"),
                    Metric(key="lead_actor_avg_films", value=int(r["lead_avg"] or 0), unit="films"),
                ],
                facts={"role_type": "supporting", "industry": r["industry"]},
            )
            for r in rows
        ]
