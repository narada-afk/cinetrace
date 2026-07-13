"""Surprising 2-hop connections between famous actors from different industries
who never shared a film — connected through exactly one middle actor.

A degrees-of-separation curiosity: "X and Y never acted together, but Z
worked with both." Restricted to primary actors with substantial filmographies
so both endpoints are recognisable.
"""

from __future__ import annotations

from typing import Sequence

from engine.discovery.base import DiscoveryRule, register
from engine.models import Entity, Insight, Metric
from engine.shared.slugs import actor_slug


@register
class ShortestPath(DiscoveryRule):
    name = "shortest_path"
    visual_potential = 0.9   # connection-finder social card exists

    def sql(self) -> str:
        return """
        WITH famous AS (
            SELECT a.id, a.name, a.industry, ast.film_count
            FROM   actors a
            JOIN   actor_stats ast ON ast.actor_id = a.id
            WHERE  a.is_primary_actor = TRUE
              AND  ast.film_count >= 50
        )
        SELECT f1.id  AS actor1_id, f1.name AS actor1_name, f1.industry AS industry1,
               f2.id  AS actor2_id, f2.name AS actor2_name, f2.industry AS industry2,
               am.name AS middle_name, am.id AS middle_id,
               c1.collaboration_count + c2.collaboration_count AS bridge_strength
        FROM   famous f1
        JOIN   famous f2 ON f2.id > f1.id
                        AND f2.industry != f1.industry
        -- no direct collaboration
        LEFT JOIN actor_collaborations direct
               ON direct.actor1_id = f1.id AND direct.actor2_id = f2.id
        JOIN   actor_collaborations c1 ON c1.actor1_id = f1.id
        JOIN   actor_collaborations c2 ON c2.actor1_id = f2.id
                                      AND c2.actor2_id = c1.actor2_id
        JOIN   actors am ON am.id = c1.actor2_id
        WHERE  direct.actor1_id IS NULL
          AND  c1.collaboration_count >= 3
          AND  c2.collaboration_count >= 3
        ORDER  BY bridge_strength DESC
        LIMIT  %(limit)s
        """

    def params(self) -> dict:
        return {"limit": 50}

    def rows_to_insights(self, rows: Sequence[dict]) -> list[Insight]:
        out = []
        seen_pairs: set[tuple[int, int]] = set()
        for r in rows:
            pair = (r["actor1_id"], r["actor2_id"])
            if pair in seen_pairs:   # keep only strongest bridge per pair
                continue
            seen_pairs.add(pair)
            out.append(Insight(
                rule=self.name,
                entities=[
                    Entity(kind="actor", id=r["actor1_id"], name=r["actor1_name"],
                           slug=actor_slug(r["actor1_name"])),
                    Entity(kind="actor", id=r["actor2_id"], name=r["actor2_name"],
                           slug=actor_slug(r["actor2_name"])),
                    Entity(kind="actor", id=r["middle_id"], name=r["middle_name"],
                           slug=actor_slug(r["middle_name"])),
                ],
                metrics=[
                    Metric(key="degrees_of_separation", value=2, unit="hops"),
                    Metric(key="bridge_strength", value=r["bridge_strength"], unit="films"),
                ],
                facts={"never_acted_together": True,
                       "bridge_actor": r["middle_name"],
                       "industries": [r["industry1"], r["industry2"]],
                       "industry": r["industry1"]},
                # "never acted together" is absence-of-evidence over two
                # incomplete credit sources
                confidence=0.75,
            ))
        return out
