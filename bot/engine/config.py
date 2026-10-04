"""
Engine configuration — ranking weights, cooldowns, feature flags.

Weights are env-overridable (ENGINE_WEIGHT_NOVELTY=0.3 …) so tuning
doesn't require a deploy-with-code-change. weights_version is a hash of
the effective weight dict, stored with every score for auditability.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


DEFAULT_WEIGHTS: dict[str, float] = {
    "novelty":          0.25,
    "surprise":         0.25,
    "popularity":       0.20,
    "visual_potential": 0.10,
    "recency":          0.10,
    "completeness":     0.10,
}


@dataclass
class EngineConfig:
    enabled: bool = os.getenv("INSIGHT_ENGINE_ENABLED", "false").lower() in ("1", "true", "yes")

    weights: dict[str, float] = field(default_factory=lambda: {
        k: _env_float(f"ENGINE_WEIGHT_{k.upper()}", v)
        for k, v in DEFAULT_WEIGHTS.items()
    })

    # Hard filters applied before ranking
    min_completeness: float = _env_float("ENGINE_MIN_COMPLETENESS", 0.5)
    min_fame: float         = _env_float("ENGINE_MIN_FAME", 0.2)

    # Dedup
    cooldown_days: int = int(os.getenv("ENGINE_COOLDOWN_DAYS", "90"))
    # Per-rule overrides, e.g. shortest_path insights can repeat sooner
    rule_cooldown_days: dict[str, int] = field(default_factory=lambda: {
        "shortest_path": 45,
    })

    # How many ranked insights to persist (and hand to the scheduler) per run.
    # Must stay well above the daily slot count so the diversity-aware planner
    # draws from a rule-varied pool — the top of the ranking skews toward the
    # single highest-scoring rule, so a small top_n would starve plan_slots.
    top_n: int = int(os.getenv("ENGINE_TOP_N", "200"))

    # Max insights per actor per day (batch-level diversity)
    max_per_actor_per_day: int = 1

    @property
    def weights_version(self) -> str:
        blob = json.dumps(self.weights, sort_keys=True)
        return hashlib.sha1(blob.encode()).hexdigest()[:10]


@dataclass
class TrendConfig:
    """Trend-awareness layer — runs as its own real-time poller
    (trend_realtime.py), independent of the nightly discovery-pipeline
    schedule. Only takes effect when the insight engine itself is enabled
    (INSIGHT_ENGINE_ENABLED=true). See docs/trend-awareness.md."""

    enabled: bool = os.getenv("TREND_AWARE_ENABLED", "true").lower() in ("1", "true", "yes")

    # Google's public daily-trends RSS feed — no API key, no scraping.
    geo: str = os.getenv("TREND_GEO", "IN")
    rss_url_template: str = os.getenv(
        "TREND_RSS_URL_TEMPLATE", "https://trends.google.com/trending/rss?geo={geo}"
    )
    request_timeout_seconds: float = _env_float("TREND_REQUEST_TIMEOUT_SECONDS", 10.0)

    # How many top trends to try (in feed order) before giving up this poll.
    # Lower than a once-a-day budget would be — this runs every
    # TREND_POLL_INTERVAL_SECONDS (config.py), so cost compounds with
    # frequency, not just with how many candidates one run tries.
    max_candidates: int = int(os.getenv("TREND_MAX_CANDIDATES", "8"))

    # Claude model for the two small reasoning steps (nickname resolution,
    # trend→fact relevance). Deliberately small/cheap — same model used in
    # engine/generators/twitter.py.
    model: str = os.getenv("TREND_MODEL", "claude-haiku-4-5-20251001")

    # Hard ceiling on trend-driven POSTS per day, independent of how often
    # we poll. Data on X/Twitter growth converges on ~3-5 total tweets/day
    # for accounts this size (Rival IQ engagement study; OpenTweet's
    # by-follower-count breakdown puts <10K followers at 2-5/day) — this
    # account already posts one evergreen scheduled tweet/day, so capping
    # trend-driven posts at 3 lands total daily volume in that range rather
    # than flooding the timeline just because trends keep appearing.
    daily_post_cap: int = int(os.getenv("TREND_DAILY_POST_CAP", "3"))


_trend_config: TrendConfig | None = None


def get_trend_config() -> TrendConfig:
    global _trend_config
    if _trend_config is None:
        _trend_config = TrendConfig()
    return _trend_config


_config: EngineConfig | None = None


def get_config() -> EngineConfig:
    global _config
    if _config is None:
        _config = EngineConfig()
    return _config
