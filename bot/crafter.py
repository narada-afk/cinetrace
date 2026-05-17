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
- Must cite the exact number from the cream stats provided
- Do NOT invent, round, or extrapolate
- Use the single most impressive or surprising stat from the cream list
- Only reply if the insight adds meaning beyond the obvious
- Avoid repeating similar phrasing across retries — vary the formula entry point

─── WHAT MAKES A CREAM STAT ───
Prefer stats that reveal something non-obvious:
- A streak nobody expected to last that long
- A hit rate that defies the industry average
- A trajectory that contradicts the narrative (declining? actually rising)
- A specific milestone (first ₹500Cr, 10th consecutive hit)
- An anomaly (lowest-earning film is the most praised, or vice versa)
- A director pairing with an unusually high win rate
Avoid: basic career totals, vague averages, anything the casual fan already knows

─── REFERENCE PATTERNS (adapt, don't copy) ───
- Streak: "{Actor} has delivered {N} consecutive ₹100Cr+ films. In {industry}, that's not normal."
- Hit rate: "{N} out of {M} films crossed ₹100Cr. That's a {X}% hit rate — highest in {industry} over the last decade."
- Trajectory: "Career average ₹{X}Cr. Last 5 films average ₹{Y}Cr. The ceiling keeps moving."
- Collab record: "{Actor} + {Director}: {N} films, {M} of them crossed ₹100Cr. One partnership, no misses."
- Milestone: "Took {N} years to reach their first ₹500Cr film. The next one came {X} months later."
- Anomaly: "Highest earner: ₹{X}Cr. Lowest rating. Best-reviewed: ₹{Y}Cr. Worst earner. Go figure."
- Cross-industry: "{N} industries. {M} of them, they hold the all-time BO record."

Respond ONLY with valid JSON. No explanation outside the JSON."""

CRAFT_TEMPLATE = """Actor: {actor_name} (@{handle}) — {industry} industry
Trigger tweet: "{tweet_text}"
Stat angle requested: {stat_angle}
Trigger context: {trigger_context}

── CREAM STATS (use the most impressive one) ──
Career: {total_films} films over {career_span} ({years_active} years)
Films with BO data: {films_with_bo_data}
BO milestones: {films_100cr}× ₹100Cr+ | {films_200cr}× ₹200Cr+ | {films_500cr}× ₹500Cr+
Career BO average: {avg_box_office}
Last-5-films average: {recent_avg} ({trajectory})
Longest ₹100Cr+ hit streak: {max_hit_streak}
Hit rate (₹100Cr+ / total with BO data): {hit_rate}
Highest-grossing film: {top_film}
Best-reviewed film: {best_reviewed}
Highest earner vs best reviewed divergence: {divergence}
Top director pairing: {top_director}
Top co-star pairing: {top_collab}
Industries worked in: {industries}
First ₹100Cr film: {first_100cr}
First ₹500Cr film: {first_500cr}
Biggest single-year output (year, films): {best_year}

Profile URL: {profile_url}

Pick the single most striking stat from the cream above. Build Fact → Pattern → Meaning around it.
Respond with JSON:
{{
  "reply_text": "the full tweet text with profile URL on its own last line",
  "stat_used": "exact stat you used (quote the number)",
  "confidence": 0-100
}}"""


# ── Cream stats extractor ─────────────────────────────────────────────────────

def _compute_cream(movies: list, collaborators: list, directors: list) -> dict:
    """Pre-compute the most tweet-worthy stats from raw profile data."""

    bo_movies  = sorted(
        [m for m in movies if m.get("box_office")],
        key=lambda x: x.get("release_year") or 0,
    )
    all_years  = sorted([m.get("release_year") for m in movies if m.get("release_year")])

    # ── Milestones ────────────────────────────────────────────────────────────
    films_100  = [m for m in bo_movies if m["box_office"] >= 100]
    films_200  = [m for m in bo_movies if m["box_office"] >= 200]
    films_500  = [m for m in bo_movies if m["box_office"] >= 500]

    top_film = max(bo_movies, key=lambda x: x["box_office"], default=None)

    # First ₹100Cr and ₹500Cr milestone films
    first_100 = min(films_100, key=lambda x: x.get("release_year") or 9999, default=None)
    first_500 = min(films_500, key=lambda x: x.get("release_year") or 9999, default=None)

    # ── Averages & trajectory ─────────────────────────────────────────────────
    avg_bo     = round(sum(m["box_office"] for m in bo_movies) / len(bo_movies)) if bo_movies else None
    recent_5   = sorted(bo_movies, key=lambda x: x.get("release_year") or 0, reverse=True)[:5]
    recent_avg = round(sum(m["box_office"] for m in recent_5) / len(recent_5)) if recent_5 else None

    if recent_avg and avg_bo:
        diff_pct = round((recent_avg - avg_bo) / avg_bo * 100)
        trajectory = f"↑ {diff_pct}% above career avg" if diff_pct > 0 else f"↓ {abs(diff_pct)}% below career avg"
    else:
        trajectory = "unknown"

    # ── Hit streak ────────────────────────────────────────────────────────────
    max_streak = cur_streak = 0
    for m in bo_movies:
        if m["box_office"] >= 100:
            cur_streak += 1
            max_streak = max(max_streak, cur_streak)
        else:
            cur_streak = 0

    # ── Hit rate ──────────────────────────────────────────────────────────────
    hit_rate = (
        f"{len(films_100)}/{len(bo_movies)} ({round(len(films_100)/len(bo_movies)*100)}%)"
        if bo_movies else "unknown"
    )

    # ── Best-reviewed vs highest-earner divergence ────────────────────────────
    rated = [m for m in movies if m.get("rating") and m.get("box_office")]
    if len(rated) >= 3:
        best_rated  = max(rated, key=lambda x: x["rating"])
        worst_rated = min(rated, key=lambda x: x["rating"])
        top_earner  = max(rated, key=lambda x: x["box_office"])
        if best_rated != top_earner:
            divergence = (
                f"Best reviewed: {best_rated['title']} (⭐{best_rated['rating']}, "
                f"₹{best_rated.get('box_office','?')}Cr) | "
                f"Highest earner: {top_earner['title']} (₹{top_earner['box_office']}Cr, "
                f"⭐{top_earner.get('rating','?')})"
            )
            best_reviewed_str = f"{best_rated['title']} (⭐{best_rated['rating']})"
        else:
            divergence = "no notable divergence"
            best_reviewed_str = f"{best_rated['title']} (⭐{best_rated['rating']})"
    else:
        divergence        = "insufficient data"
        best_reviewed_str = "unknown"

    # ── Best single year ──────────────────────────────────────────────────────
    from collections import Counter
    year_counts = Counter(m.get("release_year") for m in movies if m.get("release_year"))
    best_year_val = year_counts.most_common(1)
    best_year_str = f"{best_year_val[0][0]} ({best_year_val[0][1]} films)" if best_year_val else "unknown"

    # ── Industry breakdown ────────────────────────────────────────────────────
    industries = sorted({m.get("industry") for m in movies if m.get("industry")})

    # ── Career span ───────────────────────────────────────────────────────────
    career_span   = f"{all_years[0]}–{all_years[-1]}" if all_years else "unknown"
    years_active  = (all_years[-1] - all_years[0] + 1) if len(all_years) >= 2 else "?"

    return {
        "total_films":      len(movies),
        "career_span":      career_span,
        "years_active":     years_active,
        "films_with_bo_data": len(bo_movies),
        "films_100cr":      len(films_100),
        "films_200cr":      len(films_200),
        "films_500cr":      len(films_500),
        "avg_box_office":   f"₹{avg_bo}Cr" if avg_bo else "unknown",
        "recent_avg":       f"₹{recent_avg}Cr" if recent_avg else "unknown",
        "trajectory":       trajectory,
        "max_hit_streak":   max_streak if max_streak > 1 else "none",
        "hit_rate":         hit_rate,
        "top_film":         f"{top_film['title']} (₹{top_film['box_office']}Cr)" if top_film else "unknown",
        "best_reviewed":    best_reviewed_str,
        "divergence":       divergence,
        "first_100cr":      f"{first_100['title']} ({first_100.get('release_year','?')})" if first_100 else "none yet",
        "first_500cr":      f"{first_500['title']} ({first_500.get('release_year','?')})" if first_500 else "none yet",
        "best_year":        best_year_str,
        "top_collab":       (
            f"{collaborators[0]['name']} ({collaborators[0].get('film_count','?')} films)"
            if collaborators else "unknown"
        ),
        "top_director":     (
            f"{directors[0]['name']} ({directors[0].get('film_count','?')} films)"
            if directors else "unknown"
        ),
        "industries":       ", ".join(industries) if industries else "unknown",
    }


# ── Main crafter ──────────────────────────────────────────────────────────────

async def craft_reply(actor: dict, profile: dict, tweet_text: str, stat_angle: str,
                      trigger_context: str = "") -> dict:
    movies       = profile.get("movies", [])
    collaborators = profile.get("collaborators", [])
    directors    = profile.get("directors", [])

    cream = _compute_cream(movies, collaborators, directors)

    prompt = CRAFT_TEMPLATE.format(
        actor_name      = actor["name"],
        handle          = actor["handle"],
        industry        = actor["industry"],
        tweet_text      = tweet_text,
        stat_angle      = stat_angle,
        trigger_context = trigger_context or "direct tweet",
        profile_url     = profile["profile_url"],
        **cream,
    )

    msg = await _client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=400,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    try:
        return json.loads(msg.content[0].text)
    except Exception:
        return {"reply_text": None, "stat_used": None, "confidence": 0}
