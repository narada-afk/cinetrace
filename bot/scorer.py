"""
scorer.py
=========
Scores a generated tweet fact against Grok's WOW rubric before it goes
to Telegram for human review.

Six dimensions (0-10 each), total /60:
  40+   → strong, stop-scroll likely
  25-39 → borderline, flag in Telegram
  <25   → weak, skip to inventory fallback

Uses Haiku — this is mechanical rubric application, not creative writing.
"""

from __future__ import annotations

import json
import anthropic
from config import ANTHROPIC_API_KEY

_client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

THRESHOLDS = {"strong": 40, "borderline": 25}

SYSTEM_PROMPT = """You are a tweet quality scorer for a South Indian cinema analytics account on X (Twitter).

Score the given tweet on 6 dimensions (0–10 each) based on what actually drives engagement
among South Indian cinema fans on X — high RT, reply, and bookmark rates.

DIMENSIONS:

1. surprise_rarity (0-10)
   Does this counter common perception or reveal something most fans don't know without doing the math?
   10 = "only/first in history", genuinely non-obvious even to dedicated fans
   5  = mildly interesting, fans might half-know this
   0  = everyone already knows it (e.g. "Rajini is a superstar")

2. debate_comparison (0-10)
   Will this spark quotes, replies, or fan-war arguments?
   10 = directly pits stars against each other, or challenges a dominant narrative
   5  = neutral milestone with mild discussion potential
   0  = pure solo praise, no debate hook

3. specificity_credibility (0-10)
   How precise and verifiable are the numbers?
   10 = exact figures, named films, specific years or territories
   5  = clear numbers but lacks context (no timeframe, no comparison baseline)
   0  = vague ("many films", "biggest ever" without specifics)

4. emotional_resonance (0-10)
   Triggers fan pride, nostalgia, legacy feeling, or underdog emotion?
   10 = generational icon stat, comeback story, or deeply regional pride moment
   5  = respectable milestone, moderate emotional pull
   0  = dry stat, no emotional weight

5. timeliness (0-10)
   Connected to current releases, trending rivalries, or active discourse on X?
   10 = directly tied to a film or event from the last 6 months
   5  = evergreen stat that works any time
   0  = outdated or disconnected from current X discourse

6. visual_shareability (0-10)
   Does this naturally map to a chart, comparison table, or infographic?
   10 = perfect for a bar chart or side-by-side visual
   5  = could work with a simple graphic
   0  = narrative-only, hard to visualise

Respond ONLY with valid JSON. No explanation outside the JSON."""

_SCORE_TEMPLATE = """Actor: {actor_name} | Industry: {industry}

Tweet to score:
HOOK: {hook}
BODY: {body}

Score each dimension 0-10. Compute total (sum of all 6, max 60).

Return JSON:
{{
  "surprise_rarity":         <0-10>,
  "debate_comparison":       <0-10>,
  "specificity_credibility": <0-10>,
  "emotional_resonance":     <0-10>,
  "timeliness":              <0-10>,
  "visual_shareability":     <0-10>,
  "total":                   <sum>,
  "weak_dimensions":         ["list of dimension names scoring 4 or below"],
  "one_line_reason":         "one sentence: why this fact is or isn't stop-scroll worthy"
}}"""


def verdict(total: int) -> str:
    if total >= THRESHOLDS["strong"]:
        return "strong"
    if total >= THRESHOLDS["borderline"]:
        return "borderline"
    return "weak"


async def score_fact(actor_db_name: str, industry: str, fact: dict) -> dict:
    """
    Score a generated fact dict (must have 'hook' and 'body').

    Returns a score dict with all 6 dimensions, total, verdict, and one_line_reason.
    On failure returns a neutral borderline score so generation isn't blocked.
    """
    prompt = _SCORE_TEMPLATE.format(
        actor_name = actor_db_name,
        industry   = industry,
        hook       = fact.get("hook", ""),
        body       = fact.get("body", ""),
    )

    try:
        msg = await _client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        text = msg.content[0].text.strip() if msg.content else ""
        if text.startswith("```"):
            text = text.split("```", 2)[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()
        raw = json.loads(text)

        # Recompute total in case Claude's arithmetic drifted
        dims = [
            "surprise_rarity", "debate_comparison", "specificity_credibility",
            "emotional_resonance", "timeliness", "visual_shareability",
        ]
        total = sum(int(raw.get(d, 0)) for d in dims)
        raw["total"]   = total
        raw["verdict"] = verdict(total)
        return raw

    except Exception as e:
        print(f"[scorer] failed for {actor_db_name}: {e}")
        # Neutral fallback — don't block generation on scorer failure
        return {
            "total":           30,
            "verdict":         "borderline",
            "weak_dimensions": [],
            "one_line_reason": "scorer unavailable",
        }


def format_score_line(score: dict) -> str:
    """One-line summary for the Telegram review header."""
    total   = score.get("total", 0)
    v       = score.get("verdict", "borderline")
    reason  = score.get("reason", score.get("one_line_reason", ""))
    weak    = score.get("weak_dimensions", [])

    icon = "🟢" if v == "strong" else ("🟡" if v == "borderline" else "🔴")
    line = f"{icon} WOW score: *{total}/60* ({v})"
    if weak:
        line += f" — weak: {', '.join(weak)}"
    if reason:
        line += f"\n_{reason}_"
    return line
