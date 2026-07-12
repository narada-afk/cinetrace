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

    # How many ranked insights to persist per discovery run
    top_n: int = int(os.getenv("ENGINE_TOP_N", "40"))

    # Max insights per actor per day (batch-level diversity)
    max_per_actor_per_day: int = 1

    @property
    def weights_version(self) -> str:
        blob = json.dumps(self.weights, sort_keys=True)
        return hashlib.sha1(blob.encode()).hexdigest()[:10]


_config: EngineConfig | None = None


def get_config() -> EngineConfig:
    global _config
    if _config is None:
        _config = EngineConfig()
    return _config
