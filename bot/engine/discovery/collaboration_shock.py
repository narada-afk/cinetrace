"""Legendary actor duos (10+ films) who haven't shared the screen in 8+ years.

Ported from backend/app/insight_engine.py::_collaboration_shock, prose stripped.
"""

from __future__ import annotations

from datetime import datetime
from typing import Sequence

from engine.discovery.base import DiscoveryRule, register
from engine.models import Entity, Insight, Metric
from engine.shared.slugs import actor_slug
from engine.shared.sql import sane_year

CURRENT_YEAR = datetime.now().year


@register
class CollaborationShock(DiscoveryRule):
    name = "collaboration_shock"
    visual_potential = 0.8   # duo cards render well

    def sql(self) -> str:
        return f"""
        WITH shared_years AS (
            SELECT LEAST(am1.actor_id, am2.actor_id)    AS a1_id,
                   GREATEST(am1.actor_id, am2.actor_id) AS a2_id,
                   MAX(m.release_year)                  AS last_year
            FROM   actor_movies am1
            JOIN   actor_movies am2 ON am2.movie_id = am1.movie_id
                                   AND am2.actor_id != am1.actor_id
            JOIN   movies m ON m.id = am1.movie_id
            WHERE  {sane_year("m.release_year")}
            GROUP  BY 1, 2
            UNION ALL
            SELECT LEAST(c1.actor_id, c2.actor_id),
                   GREATEST(c1.actor_id, c2.actor_id),
                   MAX(m.release_year)
            FROM   "cast" c1
            JOIN   "cast" c2 ON c2.movie_id = c1.movie_id
                            AND c2.actor_id != c1.actor_id
            JOIN   movies m ON m.id = c1.movie_id
            WHERE  {sane_year("m.release_year")}
            GROUP  BY 1, 2
        ),
        best_last AS (
            SELECT a1_id, a2_id, MAX(last_year) AS last_year
            FROM shared_years GROUP BY a1_id, a2_id
        )
        SELECT a1.id AS actor1_id, a1.name AS actor1_name, a1.industry AS industry1,
               a2.id AS actor2_id, a2.name AS actor2_name,
               ac.collaboration_count AS films, bl.last_year
        FROM   actor_collaborations ac
        JOIN   best_last bl ON bl.a1_id = ac.actor1_id AND bl.a2_id = ac.actor2_id
        JOIN   actors a1 ON a1.id = ac.actor1_id
        JOIN   actors a2 ON a2.id = ac.actor2_id
        WHERE  ac.actor1_id < ac.actor2_id
          AND  ac.collaboration_count >= 10
          AND  bl.last_year <= %(cutoff)s
          AND  a1.is_primary_actor = TRUE
          AND  a2.is_primary_actor = TRUE
        ORDER  BY ac.collaboration_count DESC, bl.last_year ASC
        LIMIT  %(limit)s
        """

    def params(self) -> dict:
        return {"cutoff": CURRENT_YEAR - 8, "limit": 100}

    def rows_to_insights(self, rows: Sequence[dict]) -> list[Insight]:
        out = []
        for r in rows:
            gap = CURRENT_YEAR - r["last_year"]
            out.append(Insight(
                rule=self.name,
                entities=[
                    Entity(kind="actor", id=r["actor1_id"], name=r["actor1_name"],
                           slug=actor_slug(r["actor1_name"])),
                    Entity(kind="actor", id=r["actor2_id"], name=r["actor2_name"],
                           slug=actor_slug(r["actor2_name"])),
                ],
                metrics=[
                    Metric(key="collab_count", value=r["films"], unit="films"),
                    Metric(key="years_since_last", value=gap, unit="years"),
                ],
                facts={"last_film_year": r["last_year"],
                       "industry": r["industry1"]},
                completeness=1.0,
                confidence=0.9,   # "last shared film" depends on year coverage
            ))
        return out
