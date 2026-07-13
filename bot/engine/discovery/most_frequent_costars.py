"""Actor pairs with the highest collaboration counts (both currently active)."""

from __future__ import annotations

from typing import Sequence

from engine.discovery.base import DiscoveryRule, register
from engine.models import Entity, Insight, Metric
from engine.shared.slugs import actor_slug


@register
class MostFrequentCostars(DiscoveryRule):
    name = "most_frequent_costars"
    visual_potential = 0.8

    def sql(self) -> str:
        return """
        SELECT a1.id AS actor1_id, a1.name AS actor1_name, a1.industry,
               a2.id AS actor2_id, a2.name AS actor2_name,
               ac.collaboration_count AS films
        FROM   actor_collaborations ac
        JOIN   actors a1 ON a1.id = ac.actor1_id
        JOIN   actors a2 ON a2.id = ac.actor2_id
        WHERE  ac.actor1_id < ac.actor2_id
          AND  ac.collaboration_count >= 15
          AND  a1.is_primary_actor = TRUE
        ORDER  BY ac.collaboration_count DESC
        LIMIT  %(limit)s
        """

    def rows_to_insights(self, rows: Sequence[dict]) -> list[Insight]:
        return [
            Insight(
                rule=self.name,
                entities=[
                    Entity(kind="actor", id=r["actor1_id"], name=r["actor1_name"],
                           slug=actor_slug(r["actor1_name"])),
                    Entity(kind="actor", id=r["actor2_id"], name=r["actor2_name"],
                           slug=actor_slug(r["actor2_name"])),
                ],
                metrics=[Metric(key="collab_count", value=r["films"], unit="films")],
                facts={"industry": r["industry"]},
                confidence=1.0,   # verified count from actor_collaborations
            )
            for r in rows
        ]
