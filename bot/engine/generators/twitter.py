"""
Twitter generator — Claude turns an Insight into one tweet.

The prompt never asks the model to find facts; it receives the structured
insight and may only rephrase it. Output is checked by _validator: any
number not present in the payload fails the tweet (one retry with the
violations fed back, then discard).
"""

from __future__ import annotations

import json
import re

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
- Numbers must keep their meaning: a value labelled as a calendar year is a
  year (write "since 2014" or "in 1986"), never a duration; a film count is
  films, never years. If unsure how to use a number, leave it out.

Voice: follow the "editorial_voice" given in the request. All voices are sharp,
human, understated — a film-obsessed analyst talking to another film-obsessed
person. No fanboy energy, no AI filler ("Interestingly", "Remarkably").

BANNED constructions (overused templates — never produce these shapes):
- "That's not a X — that's a Y"
- "Let that sink in"
- "Read that again"
- "X isn't just Y, it's Z"
- rhetorical questions as openers
Also avoid any framing listed under "avoid_phrasings" in the request.

Format:
- Max {char_limit} characters total.
- 1-3 hashtags on their own line (derive from entity names/industry only).
- If a profile URL is provided, it goes alone on the last line.
- No emoji unless the number is genuinely jaw-dropping (one max).

Respond ONLY with the tweet text. No JSON, no explanation."""

# Rotating editorial voices — the account should feel like a small desk of
# editors, not one template. Chosen deterministically per insight so retries
# keep the same voice.
EDITORIAL_VOICES = [
    "The archivist: lead with the number, no adjectives, let the stat sit alone. Dry, factual, almost deadpan.",
    "The storyteller: one tiny narrative beat — setup, then the number as the payoff. Warm but restrained.",
    "The stat-nerd: compare or contextualize the number against something in the data. Precise, playful.",
    "The minimalist: shortest possible sentences. Fragments allowed. White space does the work.",
    "The historian: anchor the fact in its era using years from the data. Measured, respectful tone.",
]

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


_BANNED_TEMPLATE_RE = re.compile(
    r"that'?s not (a |just )?\w+.{0,30}?[—–-]|isn'?t just|let that sink|read that again",
    re.IGNORECASE,
)


def _has_banned_template(text: str) -> bool:
    return bool(_BANNED_TEMPLATE_RE.search(text))


@register
class TwitterGenerator(ContentGenerator):
    platform = Platform.TWITTER
    default_char_limit = 260

    async def generate(self, insight: Insight, insight_id: int,
                       tone: str | None = None,
                       char_limit: int | None = None,
                       avoid_texts: list[str] | None = None) -> ContentItem | None:
        limit = char_limit or self.default_char_limit
        primary = insight.primary_entity
        profile_url = (
            f"{CINETRACE_BASE_URL}/actors/{primary.slug}" if primary.slug else ""
        )

        # Deterministic voice per insight (stable across retries) so the desk
        # of editors rotates but a given fact keeps one voice.
        voice = tone or EDITORIAL_VOICES[insight_id % len(EDITORIAL_VOICES)]

        payload = {
            "insight": json.loads(insight.model_dump_json(exclude={"discovered_at"})),
            "style_hint": STYLE_HINTS.get(insight.rule, ""),
            "editorial_voice": voice,
            "profile_url": profile_url,
        }
        # Recent tweets whose framing must not be reused (anti-repetition)
        if avoid_texts:
            payload["avoid_phrasings"] = avoid_texts[:20]

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
            # Style-only soft check: banned rhetorical templates trigger a
            # rewrite on the first attempt, but never cause a discard —
            # factual correctness is the only hard gate.
            style_issue = attempt == 0 and _has_banned_template(text)
            if ok and not style_issue and len(text) <= limit + 20:
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
            if style_issue and not violations:
                violations.append(
                    "avoid the 'That's not X — that's Y' / 'isn't just' template; "
                    "rephrase with a different structure")
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
