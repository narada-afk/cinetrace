"""
The one reasoning step this layer is allowed to use an LLM for:

    trend candidate + resolved actor's precomputed Cinetrace facts
        -> is there a genuinely interesting connection, and if so which fact

The whitelist of facts (`_available_facts`) is computed entirely in Python
from live Cinetrace data (reusing generator._compute_cream, the same
deterministic math the existing scheduled-tweet generator already uses).
The model may only choose a key from that whitelist — it cannot invent a
number. This mirrors the rest of the engine's "AI never invents facts" rule
(see docs/insight-engine.md) and lets the result flow into the existing,
unmodified TwitterGenerator + validator unchanged.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass

import anthropic

from config import ANTHROPIC_API_KEY
from engine.config import TrendConfig
from engine.shared.logging import get_logger
from engine.trends.provider import TrendCandidate
from engine.trends.resolver import ResolvedEntity
from generator import _compute_cream

log = get_logger("trends.reasoning")

_client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

SYSTEM_PROMPT = """You are the fact-finder for CineTrace, a South Indian cinema \
analytics account. You are given a term that is CURRENTLY TRENDING, and a list \
of verified Cinetrace facts about the actor it refers to.

Your only job: decide whether any one of the given facts makes a genuinely \
interesting tweet BECAUSE of why this is trending right now — not just a \
generic biography fact that would be true on any other day too.

Rules:
- You may only reference a fact that is in "available_facts" below, by its key.
- Never invent a number, a name, or a claim not present in the data given.
- A good connection ties the fact to the specific reason this is trending
  (a new release, an anniversary, a controversy, a comparison another actor
  trending alongside, a record, a reunion, etc.) using the trend context given.
- If none of the available facts make a genuinely interesting, non-obvious
  connection to why this is trending, say relevant=false. Do not force it —
  a skipped topic is fine; a generic forced tweet is not.

Respond ONLY with JSON:
{
  "relevant": true/false,
  "fact_key": "one of the available_facts keys, or null if relevant=false",
  "why_trending": "one sentence — your best read of why this is trending, from the context given",
  "tweet_angle": "one sentence describing the connection to pass to the copywriter",
  "confidence": 0-100
}"""


@dataclass
class TrendConnection:
    fact_key: str
    metric_value: float
    metric_unit: str
    why_trending: str
    tweet_angle: str
    confidence: float


def _is_clean_number(value) -> bool:
    """cream's fields are formatted for prompt display, so some are plain
    numbers and some are strings/sentinels ("unknown", "none", "?"). Only
    plain numbers are safe as an Insight Metric.value."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _available_facts(movies: list, collaborators: list, directors: list) -> dict[str, dict]:
    """Deterministic, Python-computed whitelist — same math as generator.py's
    scheduled-tweet fact generator. Every value here is traceable to the
    Cinetrace dataset; the LLM only picks a key, never a number."""
    cream = _compute_cream(movies)
    candidates: dict[str, dict] = {
        "total_films":      {"value": cream["total_films"], "unit": "films",
                             "label": "total films in career"},
        "years_active":     {"value": cream["years_active"], "unit": "years",
                             "label": "years active"},
        "films_100cr":      {"value": cream["films_100cr"], "unit": "films",
                             "label": "₹100Cr+ films"},
        "films_200cr":      {"value": cream["films_200cr"], "unit": "films",
                             "label": "₹200Cr+ films"},
        "films_500cr":      {"value": cream["films_500cr"], "unit": "films",
                             "label": "₹500Cr+ films"},
        "max_streak":       {"value": cream["max_streak"], "unit": "films",
                             "label": "longest ₹100Cr+ streak"},
        "peak_decade_films": {"value": cream["peak_decade_count"], "unit": "films",
                              "label": f"films in peak decade ({cream['peak_decade']})"},
    }
    facts = {k: v for k, v in candidates.items() if _is_clean_number(v["value"])}

    if collaborators:
        top = max(collaborators, key=lambda c: c.get("films") or c.get("film_count") or 0)
        count = top.get("films") or top.get("film_count")
        if _is_clean_number(count):
            facts["top_costar"] = {
                "value": count, "unit": "films",
                "label": f"most frequent co-star: {top.get('actor', top.get('name', '?'))}",
            }

    if directors:
        top = max(directors, key=lambda d: d.get("films") or d.get("film_count") or 0)
        count = top.get("films") or top.get("film_count")
        if _is_clean_number(count):
            facts["top_director"] = {
                "value": count, "unit": "films",
                "label": f"most frequent director: {top.get('director', top.get('name', '?'))}",
            }

    industries: Counter = Counter(
        m.get("industry") for m in movies if m.get("industry")
    )
    if len(industries) > 1:
        facts["industries_worked"] = {
            "value": len(industries), "unit": "industries",
            "label": f"industries worked across: {', '.join(industries)}",
        }

    return facts


async def find_trend_connection(
    candidate: TrendCandidate,
    resolved: ResolvedEntity,
    profile: dict,
    config: TrendConfig,
) -> TrendConnection | None:
    movies        = profile.get("movies", [])
    collaborators = profile.get("collaborators", [])
    directors     = profile.get("directors", [])
    if not movies:
        log.info("no movies for %s, skipping", resolved.name)
        return None

    available = _available_facts(movies, collaborators, directors)
    if not available:
        log.info("no usable facts for %s, skipping", resolved.name)
        return None

    payload = {
        "trending_term": candidate.title,
        "trend_context": candidate.context_text,
        "actor_name": resolved.name,
        "actor_industry": resolved.industry,
        "available_facts": {k: {"value": v["value"], "unit": v["unit"], "label": v["label"]}
                            for k, v in available.items()},
    }

    try:
        msg = await _client.messages.create(
            model=config.model,
            max_tokens=400,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": json.dumps(payload, ensure_ascii=False)}],
        )
        text = msg.content[0].text.strip() if msg.content else ""
        if text.startswith("```"):
            text = text.split("```", 2)[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()
        raw = json.loads(text)
    except Exception as e:
        log.warning("reasoning call failed for %r/%s: %s", candidate.title, resolved.name, e)
        return None

    if not raw.get("relevant"):
        log.info("no interesting connection for %r/%s: %s",
                 candidate.title, resolved.name, raw.get("why_trending", ""))
        return None

    fact_key = raw.get("fact_key")
    if fact_key not in available:
        log.warning("model picked an unknown/empty fact_key (%r) for %s — dropping",
                    fact_key, resolved.name)
        return None

    why_trending = (raw.get("why_trending") or "").strip()
    tweet_angle  = (raw.get("tweet_angle") or "").strip()
    if not why_trending or not tweet_angle:
        log.info("incomplete reasoning output for %s — dropping", resolved.name)
        return None

    return TrendConnection(
        fact_key=fact_key,
        metric_value=available[fact_key]["value"],
        metric_unit=available[fact_key]["unit"],
        why_trending=why_trending,
        tweet_angle=tweet_angle,
        confidence=max(0.0, min(1.0, float(raw.get("confidence", 50)) / 100)),
    )
