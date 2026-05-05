import json
import re
import anthropic
from config import ANTHROPIC_API_KEY
from actors import ACTORS

_client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

SYSTEM_PROMPT = """You are the intelligence layer for CineTrace — a South Indian cinema analytics account.
Your job: decide whether to engage with a stat reply, and what angle to use.

Be conservative — when in doubt, do NOT engage.

NEVER engage if the tweet involves:
- Personal tragedy, death, grief, illness
- Political controversy or statements
- Family matters, relationships, personal life
- Inter-fan fights or rivalry baiting
- Brand endorsements or paid promotions
- Anything negative, critical, or sensitive

ENGAGE if the tweet is:
- Celebratory (milestone, announcement, achievement)
- Film-related (new project, release, award, teaser, trailer)
- Positive and personal (gratitude, fan interaction)
- A trending topic clearly about cinema or box office
- A signal from a director/composer/analyst praising or associating with the actor

For stat_angle, prefer angles that produce a Fact → Pattern → Meaning reply:
- "box_office_avg" — average box office across career
- "collab_record" — actor's streak with a specific director/co-star
- "cross_industry" — how many industries they've worked in
- "longevity" — career span with consistent performance
- "debut_vs_now" — contrast between first film and latest
- "genre_breakdown" — what genre they dominate
- "director_collab" — specific director partnership record

Respond ONLY with valid JSON. No explanation outside the JSON."""

ANALYSE_TEMPLATE = """Actor: {actor_name} (@{handle})
Their tweet: "{tweet_text}"
Trigger context: {trigger_context}

Respond with JSON:
{{
  "should_engage": true/false,
  "reason": "one sentence",
  "stat_angle": "one of the stat_angle types listed, or null",
  "safety_score": 0-100
}}"""

# ── Keyword-based trigger classifier ─────────────────────────────────────────

_TRIGGER_STAT_MAP = {
    "director":   "collab_record",
    "composer":   "collab_record",
    "trade":      "box_office_avg",
}

_KEYWORD_OVERRIDES: list[tuple[list[str], str]] = [
    (["teaser", "trailer", "release", "first look", "launch"],  "box_office_avg"),
    (["blockbuster", "hit", "gross", "collection", "record"],   "box_office_avg"),
    (["director", "directed", "helmed", "filmmaker"],           "director_collab"),
    (["music", "bgm", "soundtrack", "score", "composed"],       "collab_record"),
    (["debut", "first film", "early career"],                   "debut_vs_now"),
    (["cross", "multi", "industries", "pan-india"],             "cross_industry"),
]

def classify_signal_angle(signal_role: str, signal_tweet: str) -> str:
    text = signal_tweet.lower()
    for keywords, angle in _KEYWORD_OVERRIDES:
        if any(kw in text for kw in keywords):
            return angle
    return _TRIGGER_STAT_MAP.get(signal_role, "box_office_avg")

# ── Actor name detector (for signal tweets) ───────────────────────────────────

def detect_actor_in_text(text: str) -> dict | None:
    text_lower = text.lower()
    for actor in ACTORS:
        name_lower = actor["name"].lower()
        handle_lower = actor["handle"].lower()
        # match full name (word boundary) or @handle
        if re.search(rf"\b{re.escape(name_lower)}\b", text_lower) or \
           handle_lower in text_lower:
            return actor
    return None

# ── Main analysis ─────────────────────────────────────────────────────────────

async def analyse_tweet(actor_name: str, handle: str,
                        tweet_text: str, trend_context: str = "") -> dict:
    trigger_context = trend_context if trend_context else "direct tweet from actor"
    prompt = ANALYSE_TEMPLATE.format(
        actor_name      = actor_name,
        handle          = handle,
        tweet_text      = tweet_text,
        trigger_context = trigger_context,
    )
    msg = await _client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=300,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    try:
        return json.loads(msg.content[0].text)
    except Exception:
        return {"should_engage": False, "reason": "parse error", "safety_score": 0}
