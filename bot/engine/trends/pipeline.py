"""
Orchestrates: GoogleTrendsProvider -> resolver -> reasoning -> Insight.

discover_trending_insight() tries trend candidates in feed order (most
prominent first) and returns the first one that produces a genuine
Cinetrace connection. Returns None when nothing qualifies — callers must
treat that as "skip today's run", not fall back to a different selection
mechanism (see docs/trend-awareness.md). This module does not touch the
database; broadcaster.py persists the result the same way it persists any
other engine Insight.
"""

from __future__ import annotations

import stats_client
from engine.config import TrendConfig, get_trend_config
from engine.models import Entity, Insight, Metric
from engine.shared.logging import get_logger
from engine.trends.provider import GoogleTrendsProvider, TrendProvider
from engine.trends.reasoning import find_trend_connection
from engine.trends.resolver import resolve_trend_to_actor

log = get_logger("trends.pipeline")


async def discover_trending_insight(
    provider: TrendProvider | None = None,
    config: TrendConfig | None = None,
) -> Insight | None:
    config = config or get_trend_config()
    provider = provider or GoogleTrendsProvider(config)

    candidates = await provider.get_trends()
    if not candidates:
        log.info("trend provider returned nothing — skip")
        return None

    checked = 0
    for candidate in candidates[: config.max_candidates]:
        checked += 1

        resolved = await resolve_trend_to_actor(candidate.title)
        if not resolved:
            log.info("trend %r: no Cinetrace actor match — skip", candidate.title)
            continue

        profile = await stats_client.get_full_profile(resolved.name)
        if not profile:
            log.info("trend %r matched %s but no Cinetrace profile — skip",
                     candidate.title, resolved.name)
            continue

        connection = await find_trend_connection(candidate, resolved, profile, config)
        if not connection:
            log.info("trend %r matched %s but no interesting connection — skip",
                     candidate.title, resolved.name)
            continue

        log.info("trend %r -> %s via %s (confidence %.2f)",
                 candidate.title, resolved.name, connection.fact_key, connection.confidence)
        return Insight(
            rule="trending_now",
            entities=[Entity(kind="actor", id=resolved.actor_id, name=resolved.name,
                             slug=resolved.slug)],
            metrics=[Metric(key=connection.fact_key, value=connection.metric_value,
                            unit=connection.metric_unit or None)],
            facts={
                "trend_term": candidate.title,
                "why_trending": connection.why_trending,
                "tweet_angle": connection.tweet_angle,
                "source": "google_trends",
            },
            confidence=connection.confidence,
        )

    log.info("checked %d trend(s), none produced a usable Cinetrace connection — skip", checked)
    return None
