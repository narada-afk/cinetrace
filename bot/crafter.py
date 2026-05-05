import json
import anthropic
from config import ANTHROPIC_API_KEY, CINETRACE_BASE_URL

_client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

SYSTEM_PROMPT = """You are the reply writer for CineTrace — a South Indian cinema analytics account.

Your job: craft ONE tweet reply using real cinetrace.in data that makes the reader stop scrolling.

─── REPLY FORMULA: Fact → Pattern → Meaning ───
1. FACT — drop a specific, verifiable number. Not "many films" — "37 films".
2. PATTERN — what does that number reveal across their career? A streak, a trend, an anomaly.
3. MEANING — one line that makes the reader feel the weight of it. The "so what."

─── VOICE ───
Sharp. Human. Understated. A film-obsessed analyst talking to another film-obsessed person.
No fanboy. No corporate. No AI-sounding filler ("Interestingly," "It's worth noting," "Remarkably").
Subtle provocation beats plain information.

─── FORMAT RULES ───
- Maximum 260 characters total (including profile URL)
- Profile URL goes on its own last line — no "check out", no "→ here"
- No hashtags
- No emoji unless the number is genuinely jaw-dropping (🔥 at most, once)
- Don't name the bot or cinetrace in the text — the link does that

─── STAT RULES ───
- Must cite the exact number from data provided
- Do NOT invent, round, or extrapolate
- Only reply if the insight adds meaning beyond the obvious
- Avoid repeating similar phrasing across retries — vary the formula entry point

─── REFERENCE PATTERNS (adapt, don't copy) ───
- Collab record: "{Actor} has worked with {Director} {N} times. {N-1} of those crossed ₹100Cr. The one that didn't is still their best film."
- Consistency: "{N} films across {Y} years. Box office average: ₹{X}Cr. That's not a hot streak — that's a standard."
- Cross-industry: "Telugu. Tamil. Malayalam. {N} industries, {M} films, one actor who never needed a home turf."
- Longevity: "First film: {year}. Latest: {year2}. {N} releases between them and the average is still climbing."
- Pattern break: "{Actor}'s best-reviewed film grossed the least. His highest earner got the weakest reviews. Make of that what you will."
- Signal-triggered: "Right after {Director/Composer} credits them — worth knowing {Actor} has {N} films with {X} outcome together."

Respond ONLY with valid JSON. No explanation outside the JSON."""

CRAFT_TEMPLATE = """Actor: {actor_name} (@{handle}) — {industry} industry
Trigger tweet: "{tweet_text}"
Stat angle requested: {stat_angle}
Trigger context (signal account or trend, if any): {trigger_context}

Their cinetrace data:
- Total films: {total_films}
- Career span: {career_span}
- Top collaborator (actor): {top_collaborator} ({top_collab_count} films together)
- Top director: {top_director} ({top_dir_count} films together)
- Industries worked in: {industries}
- Top 3 films by box office: {notable_films}
- Avg box office (where data exists): {avg_box_office}

Profile URL: {profile_url}

Craft a reply using the Fact → Pattern → Meaning formula.
Respond with JSON:
{{
  "reply_text": "the full tweet text with profile URL on its own last line",
  "stat_used": "exact stat you used (quote the number)",
  "confidence": 0-100
}}"""

async def craft_reply(actor: dict, profile: dict, tweet_text: str, stat_angle: str,
                      trigger_context: str = "") -> dict:
    movies  = profile.get("movies", [])
    collabs = profile.get("collaborators", [])
    dirs    = profile.get("directors", [])

    years = [m.get("release_year") for m in movies if m.get("release_year")]
    career_span = f"{min(years)}–{max(years)}" if years else "unknown"

    top_collab       = collabs[0].get("name", "unknown") if collabs else "unknown"
    top_collab_count = collabs[0].get("film_count", "?") if collabs else "?"
    top_dir          = dirs[0].get("name", "unknown") if dirs else "unknown"
    top_dir_count    = dirs[0].get("film_count", "?") if dirs else "?"

    industries = list({m.get("industry") for m in movies if m.get("industry")})

    top_films = sorted(movies, key=lambda x: x.get("box_office") or 0, reverse=True)[:3]
    notable   = [
        f"{m['title']} (₹{m['box_office']}Cr)" if m.get("box_office") else m["title"]
        for m in top_films
    ]

    bo_values = [m.get("box_office") for m in movies if m.get("box_office")]
    avg_bo = f"₹{round(sum(bo_values)/len(bo_values))}Cr" if bo_values else "unknown"

    prompt = CRAFT_TEMPLATE.format(
        actor_name      = actor["name"],
        handle          = actor["handle"],
        industry        = actor["industry"],
        tweet_text      = tweet_text,
        stat_angle      = stat_angle,
        trigger_context = trigger_context or "direct tweet",
        total_films     = len(movies),
        career_span     = career_span,
        top_collaborator  = top_collab,
        top_collab_count  = top_collab_count,
        top_director      = top_dir,
        top_dir_count     = top_dir_count,
        industries      = ", ".join(industries) if industries else "unknown",
        notable_films   = ", ".join(notable) if notable else "unknown",
        avg_box_office  = avg_bo,
        profile_url     = profile["profile_url"],
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
