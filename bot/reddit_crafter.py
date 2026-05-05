import json
import anthropic
from config import ANTHROPIC_API_KEY

_client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

SYSTEM_PROMPT = """You are the comment writer for CineTrace — a South Indian cinema analytics account on Reddit.

Your job: write a Reddit comment that adds genuine value to a discussion using verified data from cinetrace.in.

─── FORMULA: Fact → Pattern → Meaning (expanded for Reddit) ───
1. FACT — one specific, verifiable number that reframes the conversation
2. PATTERN — what does that number reveal across their career? A trend, anomaly, or streak
3. MEANING — why does this matter? What does it say about the actor, the industry, or cinema broadly
4. LINK — profile URL on its own last line

─── VOICE ───
Sounds like a passionate cinema nerd who happens to have data — not a bot, not a press release.
Confident. Slightly provocative. The kind of comment that gets upvoted because it adds something real.
No sycophancy, no fanboy energy, no corporate language.

─── FORMAT ───
- 2-4 paragraphs (Reddit has no character limit — use the space)
- Reddit markdown: **bold** for key stats and names
- No hashtags
- Last line: the cinetrace profile URL, nothing else
- Don't introduce yourself or mention CineTrace in the text — the link does that

─── STAT RULES ───
- Cite exact numbers from data provided — do NOT invent or round aggressively
- Only engage if the insight adds something beyond the obvious
- Tailor the angle to what the Reddit thread is actually discussing

Respond ONLY with valid JSON. No explanation outside the JSON."""

CRAFT_TEMPLATE = """Actor: {actor_name} — {industry} industry
Reddit post title: "{post_title}"
Post content (first 400 chars): "{post_body}"
Subreddit: r/{subreddit}
Stat angle: {stat_angle}

Their cinetrace data:
- Total films: {total_films}
- Career span: {career_span}
- Top collaborator (actor): {top_collaborator} ({top_collab_count} films together)
- Top director: {top_director} ({top_dir_count} films together)
- Industries: {industries}
- Top 3 films by box office: {notable_films}
- Avg box office (where data exists): {avg_box_office}

Profile URL: {profile_url}

Write a Reddit comment using the Fact → Pattern → Meaning formula.
Respond with JSON:
{{
  "comment_text": "the full Reddit comment in markdown, profile URL on last line",
  "stat_used": "exact stat and number you used",
  "confidence": 0-100
}}"""

async def craft_reddit_comment(actor: dict, profile: dict, post_title: str,
                                post_body: str, subreddit: str,
                                stat_angle: str = "box_office_avg") -> dict:
    movies  = profile.get("movies", [])
    collabs = profile.get("collaborators", [])
    dirs    = profile.get("directors", [])

    years       = [m.get("release_year") for m in movies if m.get("release_year")]
    career_span = f"{min(years)}–{max(years)}" if years else "unknown"

    top_collab       = collabs[0].get("name", "unknown") if collabs else "unknown"
    top_collab_count = collabs[0].get("film_count", "?") if collabs else "?"
    top_dir          = dirs[0].get("name", "unknown") if dirs else "unknown"
    top_dir_count    = dirs[0].get("film_count", "?") if dirs else "?"

    industries = list({m.get("industry") for m in movies if m.get("industry")})
    top_films  = sorted(movies, key=lambda x: x.get("box_office") or 0, reverse=True)[:3]
    notable    = [
        f"{m['title']} (₹{m['box_office']}Cr)" if m.get("box_office") else m["title"]
        for m in top_films
    ]

    bo_values = [m.get("box_office") for m in movies if m.get("box_office")]
    avg_bo    = f"₹{round(sum(bo_values)/len(bo_values))}Cr" if bo_values else "unknown"

    prompt = CRAFT_TEMPLATE.format(
        actor_name       = actor["name"],
        industry         = actor["industry"],
        post_title       = post_title[:200],
        post_body        = (post_body or "")[:400],
        subreddit        = subreddit,
        stat_angle       = stat_angle,
        total_films      = len(movies),
        career_span      = career_span,
        top_collaborator = top_collab,
        top_collab_count = top_collab_count,
        top_director     = top_dir,
        top_dir_count    = top_dir_count,
        industries       = ", ".join(industries) if industries else "unknown",
        notable_films    = ", ".join(notable) if notable else "unknown",
        avg_box_office   = avg_bo,
        profile_url      = profile["profile_url"],
    )

    msg = await _client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=800,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    try:
        return json.loads(msg.content[0].text)
    except Exception:
        return {"comment_text": None, "stat_used": None, "confidence": 0}
