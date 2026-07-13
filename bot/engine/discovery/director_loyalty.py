"""Actor-director partnerships covering a large share of the actor's career.

Ported from backend/app/insight_engine.py::_director_loyalty.
"""

from __future__ import annotations

from typing import Sequence

from engine.discovery.base import DiscoveryRule, register
from engine.models import Entity, Insight, Metric
from engine.shared.slugs import actor_slug


@register
class DirectorLoyalty(DiscoveryRule):
    name = "director_loyalty"
    visual_potential = 0.9   # dedicated director-loyalty social card exists

    def sql(self) -> str:
        return """
        SELECT a.id  AS actor_id, a.name AS actor_name, a.industry,
               ads.director AS director_name,
               ads.film_count AS dir_films,
               ast.film_count AS total_films,
               ROUND(ads.film_count * 100.0 / NULLIF(ast.film_count, 0)) AS pct
        FROM   actor_director_stats ads
        JOIN   actor_stats ast ON ast.actor_id = ads.actor_id
        JOIN   actors      a   ON a.id = ads.actor_id
        WHERE  a.is_primary_actor = TRUE
          AND  ads.film_count >= 10
          AND  LOWER(a.name) != LOWER(ads.director)
        ORDER  BY ads.film_count DESC, pct DESC
        LIMIT  %(limit)s
        """

    def rows_to_insights(self, rows: Sequence[dict]) -> list[Insight]:
        return [
            Insight(
                rule=self.name,
                entities=[
                    Entity(kind="actor", id=r["actor_id"], name=r["actor_name"],
                           slug=actor_slug(r["actor_name"])),
                    Entity(kind="director", name=r["director_name"],
                           slug=actor_slug(r["director_name"])),
                ],
                metrics=[
                    Metric(key="films_together", value=r["dir_films"], unit="films"),
                    Metric(key="career_share_pct", value=int(r["pct"] or 0), unit="%"),
                    Metric(key="total_films", value=r["total_films"], unit="films"),
                ],
                facts={"industry": r["industry"]},
                confidence=1.0,   # verified counts from actor_director_stats
            )
            for r in rows
        ]
