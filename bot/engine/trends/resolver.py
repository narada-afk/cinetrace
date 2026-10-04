"""
Match a trend candidate's raw title against Cinetrace.

Uses the existing /actors/search endpoint (same one the frontend search box
and stats_client.find_actor use) — no new entity-resolution system. A trend
title rarely equals an actor's name verbatim ("Rajinikanth Jailer 2 teaser"
vs. "Rajinikanth"), so we try, in order, from cheapest to most expensive:

  1. search the raw title
  2. search each individual capitalised word in the title (catches titles
     that just wrap an actor's name in extra words)
  3. one small LLM call for the remaining case — a nickname/alias that
     doesn't literally contain the actor's name ("Thalaivar" -> "Rajinikanth")

Movies and directors are out of scope: Cinetrace does not expose a
movie/director search endpoint today (see docs/trend-awareness.md). A trend
that is a movie or director title simply won't resolve and the pipeline
SKIPs — that's correct behaviour, not a bug to work around here.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

import anthropic
import httpx

from config import ANTHROPIC_API_KEY, CINETRACE_API_URL
from engine.shared.logging import get_logger
from screenshot import actor_slug

log = get_logger("trends.resolver")

_client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

_NICKNAME_SYSTEM_PROMPT = """You map a trending search term to a South Indian \
(Telugu, Tamil, Malayalam, or Kannada) film actor's real, full billing name — \
only when the term is clearly a nickname, alias, or unambiguous reference to \
one specific actor (e.g. "Thalaivar" -> "Rajinikanth", "Lalettan" -> "Mohanlal").

If the term is not a South Indian film actor reference at all (a cricketer, a \
politician, a festival, a different film industry, a movie title, a director, \
or just not identifiable), respond with null.

Respond ONLY with JSON: {"actor_name": "Full Name" or null}"""


@dataclass
class ResolvedEntity:
    actor_id: int
    name: str
    slug: str
    industry: str | None


async def _search_actors(client: httpx.AsyncClient, query: str) -> list[dict]:
    query = query.strip()
    if len(query) < 2:
        return []
    try:
        r = await client.get("/actors/search", params={"q": query, "lead_only": "true"})
        if r.status_code != 200:
            return []
        return r.json()
    except Exception as e:
        log.warning("actor search failed for %r: %s", query, e)
        return []


def _best_exact_match(results: list[dict], query: str) -> dict | None:
    q = query.strip().lower()
    for row in results:
        if row.get("name", "").strip().lower() == q:
            return row
    return None


_NICKNAME_MODEL = "claude-haiku-4-5-20251001"


async def _nickname_to_actor_name(title: str) -> str | None:
    try:
        msg = await _client.messages.create(
            model=_NICKNAME_MODEL,
            max_tokens=100,
            system=_NICKNAME_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": title}],
        )
        if msg.usage:
            from engine import db as engine_db
            engine_db.record_llm_call("nickname", _NICKNAME_MODEL,
                                      msg.usage.input_tokens, msg.usage.output_tokens)
        text = msg.content[0].text.strip() if msg.content else ""
        if text.startswith("```"):
            text = text.strip("`").removeprefix("json").strip()
        raw = json.loads(text)
        name = raw.get("actor_name")
        return name.strip() if isinstance(name, str) and name.strip() else None
    except Exception as e:
        log.warning("nickname resolution failed for %r: %s", title, e)
        return None


async def resolve_trend_to_actor(title: str) -> ResolvedEntity | None:
    """Best-effort match of a trend title onto a Cinetrace actor, or None."""
    async with httpx.AsyncClient(base_url=CINETRACE_API_URL, timeout=10) as client:
        # 1. Raw title
        results = await _search_actors(client, title)
        match = _best_exact_match(results, title)
        if not match and results:
            match = results[0]  # substring hit — still worth a try

        # 2. Individual capitalised words (e.g. "Rajinikanth Jailer 2 teaser")
        if not match:
            for word in re.findall(r"[A-Z][a-zA-Z.]{2,}", title):
                results = await _search_actors(client, word)
                match = _best_exact_match(results, word)
                if match:
                    break

        # 3. Nickname/alias via one small LLM call
        if not match:
            canonical_name = await _nickname_to_actor_name(title)
            if canonical_name:
                results = await _search_actors(client, canonical_name)
                match = _best_exact_match(results, canonical_name) or (
                    results[0] if results else None
                )

        if not match:
            return None

        return ResolvedEntity(
            actor_id=match["id"],
            name=match["name"],
            slug=actor_slug(match["name"]),
            industry=match.get("industry"),
        )
