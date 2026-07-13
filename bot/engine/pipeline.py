"""
Pipeline orchestrator: discover → rank → dedup → persist.

Run manually:  python -m engine.pipeline --dry-run   (prints top 20, no writes)
"""

from __future__ import annotations

import argparse
import json

from engine import db as engine_db
from engine.config import get_config
from engine.dedup.deduper import dedup
from engine.discovery import all_rules
from engine.models import RankedInsight
from engine.ranking.features import RankContext
from engine.ranking.ranker import rank
from engine.shared.logging import get_logger

log = get_logger("pipeline")


def _fetch_fame_stats(conn, actor_ids: set[int]) -> dict[int, dict]:
    """One bulk query — port of backend _enrich_with_fame."""
    if not actor_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.id, ast.film_count,
                   COUNT(DISTINCT ac.actor2_id) AS costar_count,
                   a.is_primary_actor
            FROM   actors a
            JOIN   actor_stats ast ON ast.actor_id = a.id
            LEFT JOIN actor_collaborations ac ON ac.actor1_id = a.id
            WHERE  a.id = ANY(%s)
            GROUP  BY a.id, ast.film_count, a.is_primary_actor
            """,
            (list(actor_ids),),
        )
        return {
            r[0]: {"film_count": r[1] or 0, "costar_count": r[2] or 0,
                   "is_primary": r[3]}
            for r in cur.fetchall()
        }


def run_discovery_pipeline(dry_run: bool = False) -> list[RankedInsight]:
    """Discover across all rules, rank, dedup, persist top-N. Returns the
    surviving ranked insights (with db_id set unless dry_run)."""
    config = get_config()

    # 1. Discover — fail-safe per rule: one broken query never kills the run
    insights = []
    health: list[dict] = []
    conn = engine_db.get_conn()
    try:
        for rule in all_rules():
            try:
                insights.extend(rule.discover(conn))
            except Exception as e:
                conn.rollback()
                log.warning("rule %s failed: %s", rule.name, e)
            finally:
                if rule.last_health:
                    health.append(rule.last_health)

        # 2. Build ranking context (bulk fetches)
        actor_ids = {i for ins in insights for i in ins.actor_ids()}
        ctx = RankContext(
            fingerprint_history=engine_db.recent_fingerprint_counts(days=365) if not dry_run else {},
            fame_stats=_fetch_fame_stats(conn, actor_ids),
            rule_visual={r.name: r.visual_potential for r in all_rules()},
        )
    finally:
        conn.close()

    # 3. Rank
    ranked = rank(insights, ctx, config)

    # 4. Dedup
    if dry_run:
        survivors = dedup(ranked, set(), set(), config)
    else:
        survivors = dedup(
            ranked,
            on_cooldown=engine_db.fingerprints_on_cooldown(config.cooldown_days),
            recently_used_actor_ids=engine_db.actors_used_recently(days=1),
            config=config,
        )

    top = survivors[: config.top_n]

    # 5. Persist
    if not dry_run:
        for r in top:
            r.db_id = engine_db.insert_insight(r)
        engine_db.record_rule_health(health)
        log.info("persisted %d insights", len(top))

    return top


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="print top-20 ranked insights as JSON, no DB writes")
    parser.add_argument("-n", type=int, default=20)
    args = parser.parse_args()

    top = run_discovery_pipeline(dry_run=args.dry_run)
    for r in top[: args.n]:
        print(json.dumps({
            "rule": r.insight.rule,
            "score": r.score.total,
            "components": r.score.components,
            "fingerprint": r.fingerprint[:12],
            "entities": [e.name for e in r.insight.entities],
            "metrics": [m.model_dump() for m in r.insight.metrics],
            "facts": r.insight.facts,
        }, default=str))


if __name__ == "__main__":
    main()
