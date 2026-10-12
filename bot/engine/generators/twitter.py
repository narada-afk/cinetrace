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
Write a TWO-PART post that expresses ONLY the facts given:

PART 1 — THE HOOK (a standalone tweet):
- Short lines. One idea per line. White space does the work.
- Build curiosity with a setup, but WITHHOLD the payoff — hold back the key
  number, the name, or the twist for Part 2.
- End on a cliffhanger line, then a final line that is exactly: ↓
- NO hashtags, NO URL, NO image reference in Part 1.
- It must make a film fan stop scrolling and want the answer.

PART 2 — THE REVEAL (posted as a reply to the hook):
- Deliver the payoff: the specific number(s) and the name(s).
- Short lines again — keep the rhythm.
- Last lines: 1-3 hashtags on their own line, then the profile URL (if given)
  alone on the very last line.

Hard rules (both parts):
- Do not change numbers. Do not invent facts. Do not exaggerate.
- Do not add any statistic, year, count, film name, or claim not in the JSON.
- You may compute a year gap/span from year values that ARE in the JSON.
- Numbers keep their meaning: a calendar year is a year ("since 2014"), never a
  duration; a film count is films, never years. If unsure, leave it out.
- The primary entity's name must appear somewhere (usually the reveal).

Voice: follow the "editorial_voice" in the request. Sharp, human, understated —
a film-obsessed analyst talking to another. No fanboy energy, no AI filler
("Interestingly", "Remarkably"). At most one emoji, only if truly jaw-dropping.

BANNED constructions (never produce these shapes):
- "That's not a X — that's a Y"
- "Let that sink in" / "Read that again"
- "X isn't just Y, it's Z"
Also avoid any framing listed under "avoid_phrasings" in the request.

LENGTH: each part must be at most {char_limit} characters.

OUTPUT FORMAT — respond with EXACTLY this, nothing else:
<hook text>
===REPLY===
<reply text>"""

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
        # Card slug: the primary entity's slug, else the first entity that has
        # one — guarantees every tweet gets a stat-card image, including
        # director-primary insights (the card renders an initials fallback).
        card_slug = primary.slug or next(
            (e.slug for e in insight.entities if e.slug), None)
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
                max_tokens=500,
                system=SYSTEM_PROMPT.format(char_limit=limit),
                messages=messages,
            )
            raw = msg.content[0].text.strip()
            hook, reply = _split_parts(raw)

            violations: list[str] = []
            if not reply:
                violations.append(
                    "missing the ===REPLY=== separator — output the hook, then a "
                    "line with exactly ===REPLY===, then the reply")
            # Validate facts across BOTH parts combined (numbers + primary name)
            combined = f"{hook}\n{reply}".strip()
            ok, fact_violations = validate(combined, insight)
            violations += fact_violations

            if len(hook) > limit + 20:
                violations.append(f"hook is {len(hook)} chars, limit {limit}")
            if reply and len(reply) > limit + 20:
                violations.append(f"reply is {len(reply)} chars, limit {limit}")

            # Style-only soft check: banned templates trigger a rewrite on the
            # first attempt, but never cause a discard — facts are the only gate.
            style_issue = attempt == 0 and _has_banned_template(combined)

            if not violations and not style_issue:
                return ContentItem(
                    insight_id=insight_id,
                    platform=self.platform,
                    text=hook,
                    reply_text=reply,
                    media_ref=card_slug,
                    model=MODEL,
                    validated=True,
                )

            if style_issue and not violations:
                violations.append(
                    "avoid the 'That's not X — that's Y' / 'isn't just' template; "
                    "rephrase with a different structure")
            log.warning("attempt %d failed for %s: %s",
                        attempt + 1, insight.rule, violations)
            messages += [
                {"role": "assistant", "content": raw},
                {"role": "user", "content":
                    "Your post violated these rules — fix and resend in the same "
                    "hook/===REPLY===/reply format, nothing else:\n- "
                    + "\n- ".join(violations)},
            ]

        log.error("discarding %s insight %d — validation failed twice",
                  insight.rule, insight_id)
        return None


def _split_parts(raw: str) -> tuple[str, str]:
    """Split the model output into (hook, reply) on the ===REPLY=== marker.
    Tolerates minor variants (extra =, spaces, case)."""
    m = re.split(r"\n\s*=+\s*REPLY\s*=+\s*\n", raw, maxsplit=1, flags=re.IGNORECASE)
    if len(m) == 2:
        return m[0].strip(), m[1].strip()
    return raw.strip(), ""
