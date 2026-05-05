import json
import anthropic
from config import ANTHROPIC_API_KEY, CINETRACE_BASE_URL

_client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

SYSTEM_PROMPT = """You are the reply writer for CineTrace Stats Bot — a South Indian cinema analytics account.

Your job is to craft a single tweet reply using real data from the cinetrace.in database.

Rules:
- The stat must feel like something a knowledgeable cinema fan just discovered and found fascinating
- Subtle and informative — never fanboy, never sycophantic, never sarcastic
- Cite the specific number — vague stats are useless
- The cinetrace profile link goes on its own line at the end — no "check out" or "click here"
- Maximum 260 characters including the link
- Do NOT mention the bot or cinetrace by name in the text — let the link speak
- Do NOT use hashtags
- Do NOT use emoji unless the stat is genuinely extraordinary

The stat must be verifiable from the data provided. Do not invent or extrapolate.

Respond ONLY with valid JSON. No explanation outside the JSON."""

CRAFT_TEMPLATE = """Actor: {actor_name} (@{handle}) — {industry} industry
Tweet they posted: "{tweet_text}"
Stat angle requested: {stat_angle}

Their data from cinetrace:
- Total films: {total_films}
- Career span: {career_span}
- Top collaborator (actor): {top_collaborator}
- Top director: {top_director}
- Industries worked in: {industries}
- Notable films: {notable_films}

Profile URL: {profile_url}

Craft a reply tweet. Respond with JSON:
{{
  "reply_text": "the full tweet text including the profile URL on its own last line",
  "stat_used": "which specific stat you highlighted",
  "confidence": 0-100
}}"""

async def craft_reply(actor: dict, profile: dict, tweet_text: str, stat_angle: str) -> dict:
    movies   = profile.get("movies", [])
    collabs  = profile.get("collaborators", [])
    dirs     = profile.get("directors", [])
    stats    = profile.get("stats") or {}

    years = [m.get("release_year") for m in movies if m.get("release_year")]
    career_span = f"{min(years)}–{max(years)}" if years else "unknown"

    top_collab = collabs[0].get("name") if collabs else "unknown"
    top_dir    = dirs[0].get("name") if dirs else "unknown"

    industries = list({m.get("industry") for m in movies if m.get("industry")})

    notable = [m["title"] for m in sorted(
        movies, key=lambda x: x.get("box_office") or 0, reverse=True
    )[:3]]

    prompt = CRAFT_TEMPLATE.format(
        actor_name   = actor["name"],
        handle       = actor["handle"],
        industry     = actor["industry"],
        tweet_text   = tweet_text,
        stat_angle   = stat_angle,
        total_films  = len(movies),
        career_span  = career_span,
        top_collaborator = top_collab,
        top_director     = top_dir,
        industries   = ", ".join(industries) if industries else "unknown",
        notable_films= ", ".join(notable) if notable else "unknown",
        profile_url  = profile["profile_url"],
    )

    msg = await _client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=400,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    try:
        return json.loads(msg.content[0].text)
    except Exception:
        return {"reply_text": None, "stat_used": None, "confidence": 0}
