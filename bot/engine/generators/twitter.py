"""
Twitter generator — Claude turns an Insight into one tweet.

The prompt never asks the model to find facts; it receives the structured
insight and may only rephrase it. Output is checked by _validator: any
number not present in the payload fails the tweet (one retry with the
violations fed back, then discard).
"""

from __future__ import annotations

import json

import anthropic

from config import ANTHROPIC_API_KEY, CINETRACE_BASE_URL
from engine.generators._validator import validate
from engine.generators.base import ContentGenerator, register
from engine.models import ContentItem, Insight, Platform
from engine.shared.logging import get_logger

log = get_logger("generators.twitter")

_client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

MODEL = "claude-haiku-4-5-20251001"

SYSTEM_PROMPT = """You are a copywriter for a South Indian cinema analytics account.

You will receive a JSON fact package discovered from a film database.
Write ONE tweet that expresses ONLY the facts given.

Hard rules:
- Do not change numbers. Do not invent facts. Do not exaggerate.
- Do not add any statistic, year, count, film name, or claim not present in the JSON.
- Only improve readability and curiosity.
- You may compute a year gap/span from year values that ARE in the JSON.

Voice: sharp, human, understated — a film-obsessed analyst talking to another
film-obsessed person. No fanboy energy, no AI filler ("Interestingly", "Remarkably").

Format:
- Max {char_limit} characters total.
- 1-3 hashtags on their own line (derive from entity names/industry only).
- If a profile URL is provided, it goes alone on the last line.
- No emoji unless the number is genuinely jaw-dropping (one max).

Respond ONLY with the tweet text. No JSON, no explanation."""

# One-line style angle per rule — how to frame this kind of fact
STYLE_HINTS: dict[str, str] = {
    "collaboration_shock":     "Frame the reunion angle: this many films together, yet nothing for years.",
    "hidden_dominance":        "Frame the unsung-hero angle: always in the background, staggering output.",
    "cross_industry_reach":    "Frame the language-barrier angle: most stars stay home, this one didn't.",
    "career_peak_window":      "Frame the golden-era angle: this concentration of work in 5 years.",
    "network_power":           "Frame the connector angle: the actor everyone has worked with.",
    "director_loyalty":        "Frame the creative-partnership angle: one actor, one director, a career together.",
    "director_box_office":     "Frame the hit-machine angle: cumulative earnings across a filmography.",
    "longest_careers":         "Frame the longevity angle: decades between first and latest film.",
    "debut_ages":              "Frame the origin-story angle: when the journey actually started.",
    "most_frequent_costars":   "Frame the on-screen-pair angle: the duo audiences kept seeing.",
    "blockbuster_streaks":     "Frame the consistency angle: hit after hit, year after year.",
    "longest_film_gaps":       "Frame the comeback angle: the silence, then the return.",
    "most_multilingual":       "Frame the polyglot angle: how many languages one career covered.",
    "collaboration_diversity": "Frame the explorer angle: almost never the same director twice.",
    "shortest_path":           "Frame the six-degrees angle: two stars who never shared a frame, one actor apart.",
}


@register
class TwitterGenerator(ContentGenerator):
    platform = Platform.TWITTER
    default_char_limit = 260

    async def generate(self, insight: Insight, insight_id: int,
                       tone: str | None = None,
                       char_limit: int | None = None) -> ContentItem | None:
        limit = char_limit or self.default_char_limit
        primary = insight.primary_entity
        profile_url = (
            f"{CINETRACE_BASE_URL}/actors/{primary.slug}" if primary.slug else ""
        )

        payload = {
            "insight": json.loads(insight.model_dump_json(exclude={"discovered_at"})),
            "style_hint": STYLE_HINTS.get(insight.rule, ""),
            "profile_url": profile_url,
        }
        if tone:
            payload["tone"] = tone

        messages = [{"role": "user", "content": json.dumps(payload, ensure_ascii=False)}]

        for attempt in range(2):
            msg = await _client.messages.create(
                model=MODEL,
                max_tokens=400,
                system=SYSTEM_PROMPT.format(char_limit=limit),
                messages=messages,
            )
            text = msg.content[0].text.strip()

            ok, violations = validate(text, insight)
            if ok and len(text) <= limit + 20:
                return ContentItem(
                    insight_id=insight_id,
                    platform=self.platform,
                    text=text,
                    media_ref=primary.slug,
                    model=MODEL,
                    validated=True,
                )

            if len(text) > limit + 20:
                violations.append(f"tweet is {len(text)} chars, limit {limit}")
            log.warning("attempt %d failed validation for %s: %s",
                        attempt + 1, insight.rule, violations)
            messages += [
                {"role": "assistant", "content": text},
                {"role": "user", "content":
                    "Your tweet violated these rules — fix and resend only the tweet:\n- "
                    + "\n- ".join(violations)},
            ]

        log.error("discarding %s insight %d — validation failed twice",
                  insight.rule, insight_id)
        return None
