"""
generator.py
============
AI-powered scheduled tweet fact generator.

Fetches live actor data and uses Claude to surface facts that only reveal
themselves through computation across the full dataset — streaks, ratios,
decade contrasts, trajectory inversions, partnership outliers.

Returns a fact dict compatible with broadcaster._format_tweet():
    {hook, body, hashtags, section, compare_with, stat_key, confidence}
"""

from __future__ import annotations

import json
from collections import Counter

import anthropic
from config import ANTHROPIC_API_KEY
from screenshot import actor_slug

_client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

# Fact types Claude outputs → sections we already have
# Keeps section logic in Python, not in Claude's head
FACT_TYPE_TO_SECTION: dict[str, str] = {
    "director_one":      "director-loyalty",  # dedicated director loyalty social card
    "director_spread":   "directors",          # Directors Worked With section (chip clicked)
    "collab":            "compare",            # compare page — collab always names a co-star
    "streak_trajectory": "career",             # career chart — Hit Rate shows the streak
    "milestone":         "blockbusters",       # Blockbusters section lists milestone films
    "decade_longevity":  "career",             # career chart — Films/yr shows decade peaks
    "comparison":        "compare",            # compare page
}

# Career chart mode to pre-select via ?mode= URL param
# Only relevant when section == "career"
FACT_TYPE_TO_CHART_MODE: dict[str, str] = {
    "streak_trajectory": "hit_rate",  # hit rate line makes the streak visible
    "decade_longevity":  "films",     # film count by year makes decade peaks visible
}

VALID_FACT_TYPES = set(FACT_TYPE_TO_SECTION)

# Build slug set once at import time for compare_with validation
from actors import ACTORS as _ACTORS
_ACTOR_SLUGS: set[str] = {actor_slug(a["db_name"]) for a in _ACTORS}

# ── Prompts ───────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are the fact-finder for CineTrace — a South Indian cinema analytics account.

Given raw actor data, find the single most surprising, hidden, or counterintuitive fact
that only reveals itself through computation across the full dataset.

─── WHAT MAKES A HIDDEN FACT ───
Surface things that require looking across the WHOLE dataset:
- A streak nobody expected: N consecutive ₹100Cr+ hits, or a comeback after N misses
- A ratio that defies expectation: % of films featuring the same co-star or director
- A timing anomaly: years between debut and first ₹100Cr, gap between first and second ₹500Cr
- A decade contrast: their 1980s output vs a modern star's entire career
- A trajectory inversion: early-career average vs recent-5 average and what that gap means
- A partnership outlier: one director or co-star in a disproportionate share of films
- A hidden peak: an era that outperformed their "iconic" period by the raw numbers

Avoid: basic career totals, obvious milestones, "most films in X language" surface facts,
anything a casual fan already knows without doing the math

─── TWEET FORMULA ───
hook     → one line, the jaw-dropping number or contrast. Make the reader stop.
body     → 2-3 SHORT lines. The pattern. The "so what." Do NOT restate the hook.
           Keep the whole body under 200 characters.
hashtags → 2-3 relevant tags only. No spaces between tags.

─── VOICE ───
Sharp. Human. Understated. A film analyst talking to another obsessive.
No AI filler ("Interestingly", "It's worth noting"). No fanboy gushing.
Counterintuitive beats obvious. Precise beats vague.

─── FACT TYPE (controls which screenshot is used) ───
Pick the ONE fact_type that best describes the stat you found:
  "director_one"      → one director worked with this actor far more than any other
  "director_spread"   → stats about directors in general (counts, variety, patterns)
  "collab"            → co-star appeared in X% or N films alongside them — set compare_with to the co-star's slug
  "streak_trajectory" → consecutive hit/miss streak, or career BO trajectory over years
  "milestone"         → specific ₹100Cr / ₹200Cr / ₹500Cr films and when they happened — ONLY if the fact is purely about this actor with NO named comparison to another actor
  "decade_longevity"  → decade film counts, career span, active years — ONLY if no named actor comparison
  "comparison"        → REQUIRED whenever you name another specific actor in the hook or body (e.g. "more than X's career", "unlike Y", "compared to Z"). Always set compare_with to their slug.

RULE: If your hook or body mentions another actor by name for a head-to-head career comparison, use "comparison". If it's specifically about how often two actors co-starred in films together, use "collab". Both require compare_with to be set to the other actor's slug.

Respond ONLY with valid JSON. No explanation outside the JSON."""

_GENERATE_TEMPLATE = """Actor: {actor_name} | Industry: {industry}
Profile URL: {profile_url}

── FILMOGRAPHY ──
{movies_block}

── TOP CO-STARS (by films together) ──
{collaborators_block}

── TOP DIRECTORS (by films together) ──
{directors_block}

── COMPUTED STATS ──
Career: {total_films} films | {career_span} ({years_active} yrs active)
Films with BO data: {films_with_bo}
Career BO average: {avg_bo} | Last-5 average: {recent_avg} ({trajectory})
Hit rate (₹100Cr+): {hit_rate} | Longest ₹100Cr+ streak: {max_streak}
₹100Cr films: {films_100cr} | ₹200Cr+: {films_200cr} | ₹500Cr+: {films_500cr}
Peak decade: {peak_decade} ({peak_decade_count} films)
First ₹100Cr: {first_100cr} | First ₹500Cr: {first_500cr}
Best-reviewed vs highest-earner: {divergence}

── AVAILABLE ACTORS FOR COMPARE (use slug for compare_with) ──
{actor_slugs}

── RECENT TWEETS FOR THIS ACTOR (avoid repeating these angles) ──
{recent_tweets}

Find the most surprising hidden fact. Respond with JSON:
{{
  "hook": "one-line opener — the jaw-dropping number or contrast",
  "body": "2-3 short lines — the pattern and meaning. Do NOT restate the hook.",
  "hashtags": "#Tag1 #Tag2",
  "fact_type": "one of: director_one | director_spread | collab | streak_trajectory | milestone | decade_longevity | comparison",
  "compare_with": "other-actor-slug if fact_type is comparison, else empty string",
  "director_name": "exact director name (from TOP DIRECTORS list) if fact_type is director_one or director_spread and fact is about one specific director, else empty string",
  "stat_key": "short_snake_case key describing this angle (e.g. hit_streak_2019_2023)",
  "confidence": 0-100
}}"""


# ── Data formatters ───────────────────────────────────────────────────────────

def _movies_block(movies: list) -> str:
    """
    Full list for actors with <80 films.
    Decade summary + top 30 BO + recent 10 for prolific actors.
    """
    bo_movies = sorted(
        [m for m in movies if m.get("box_office")],
        key=lambda x: x.get("release_year") or 0,
    )
    all_sorted = sorted(movies, key=lambda x: x.get("release_year") or 0)

    def _fmt(m: dict) -> str:
        bo = f" ₹{m['box_office']}Cr" if m.get("box_office") else ""
        rt = f" ⭐{m['rating']}" if m.get("rating") else ""
        return f"  {m.get('release_year', '?')}: {m['title']}{bo}{rt}"

    if len(all_sorted) < 80:
        return "\n".join(_fmt(m) for m in all_sorted)

    # Prolific actor — summarise by decade + highlights
    by_decade: dict[int, int] = Counter(
        (m["release_year"] // 10) * 10
        for m in all_sorted if m.get("release_year")
    )
    decade_line = " | ".join(
        f"{d}s: {c} films" for d, c in sorted(by_decade.items())
    )
    top_bo = sorted(bo_movies, key=lambda x: x["box_office"], reverse=True)[:30]
    recent_10 = all_sorted[-10:]

    return (
        f"BY DECADE: {decade_line}\n\n"
        "TOP 30 BY BOX OFFICE:\n"
        + "\n".join(_fmt(m) for m in sorted(top_bo, key=lambda x: x.get("release_year") or 0))
        + "\n\nRECENT 10:\n"
        + "\n".join(_fmt(m) for m in recent_10)
    )


def _collabs_block(items: list, limit: int = 20) -> str:
    lines = []
    for c in items[:limit]:
        name  = c.get("actor") or c.get("director") or c.get("name", "?")
        count = c.get("films") or c.get("film_count", "?")
        lines.append(f"  {name}: {count} films")
    return "\n".join(lines) or "  (no data)"


def _compute_cream(movies: list) -> dict:
    bo_movies = sorted(
        [m for m in movies if m.get("box_office")],
        key=lambda x: x.get("release_year") or 0,
    )
    all_years = sorted(m["release_year"] for m in movies if m.get("release_year"))

    films_100 = [m for m in bo_movies if m["box_office"] >= 100]
    films_200 = [m for m in bo_movies if m["box_office"] >= 200]
    films_500 = [m for m in bo_movies if m["box_office"] >= 500]

    avg_bo = (
        round(sum(m["box_office"] for m in bo_movies) / len(bo_movies))
        if bo_movies else None
    )
    recent_5 = sorted(bo_movies, key=lambda x: x.get("release_year") or 0, reverse=True)[:5]
    recent_avg = (
        round(sum(m["box_office"] for m in recent_5) / len(recent_5))
        if recent_5 else None
    )
    if recent_avg and avg_bo:
        diff = round((recent_avg - avg_bo) / avg_bo * 100)
        traj = f"↑{diff}% above career avg" if diff > 0 else f"↓{abs(diff)}% below career avg"
    else:
        traj = "unknown"

    max_streak = cur = 0
    for m in bo_movies:
        if m["box_office"] >= 100:
            cur += 1
            max_streak = max(max_streak, cur)
        else:
            cur = 0

    hit_rate = (
        f"{len(films_100)}/{len(bo_movies)} ({round(len(films_100)/len(bo_movies)*100)}%)"
        if bo_movies else "unknown"
    )

    rated = [m for m in movies if m.get("rating") and m.get("box_office")]
    if len(rated) >= 3:
        best = max(rated, key=lambda x: x["rating"])
        top_earner = max(rated, key=lambda x: x["box_office"])
        if best != top_earner:
            divergence = (
                f"Best reviewed: {best['title']} "
                f"(⭐{best['rating']}, ₹{best.get('box_office','?')}Cr) | "
                f"Highest earner: {top_earner['title']} "
                f"(₹{top_earner['box_office']}Cr, ⭐{top_earner.get('rating','?')})"
            )
        else:
            divergence = "no notable divergence"
    else:
        divergence = "insufficient data"

    by_decade: Counter = Counter(
        (m["release_year"] // 10) * 10
        for m in movies if m.get("release_year")
    )
    peak_dec, peak_count = by_decade.most_common(1)[0] if by_decade else (None, 0)

    first_100 = min(films_100, key=lambda x: x.get("release_year") or 9999, default=None)
    first_500 = min(films_500, key=lambda x: x.get("release_year") or 9999, default=None)

    return dict(
        total_films       = len(movies),
        career_span       = f"{all_years[0]}–{all_years[-1]}" if all_years else "unknown",
        years_active      = (all_years[-1] - all_years[0] + 1) if len(all_years) >= 2 else "?",
        films_with_bo     = len(bo_movies),
        avg_bo            = f"₹{avg_bo}Cr" if avg_bo else "unknown",
        recent_avg        = f"₹{recent_avg}Cr" if recent_avg else "unknown",
        trajectory        = traj,
        hit_rate          = hit_rate,
        max_streak        = max_streak if max_streak > 1 else "none",
        films_100cr       = len(films_100),
        films_200cr       = len(films_200),
        films_500cr       = len(films_500),
        peak_decade       = f"{peak_dec}s" if peak_dec else "unknown",
        peak_decade_count = peak_count,
        divergence        = divergence,
        first_100cr       = (
            f"{first_100['title']} ({first_100.get('release_year','?')})"
            if first_100 else "none yet"
        ),
        first_500cr       = (
            f"{first_500['title']} ({first_500.get('release_year','?')})"
            if first_500 else "none yet"
        ),
    )


# ── Main entry point ──────────────────────────────────────────────────────────

async def generate_fact(
    actor_db_name: str,
    industry: str,
    profile: dict,
    recent_tweets: list[str],
) -> dict | None:
    """
    Generate a fresh tweet fact for the given actor.

    profile must contain: movies, collaborators, directors, profile_url
    recent_tweets: last ~30 days of tweet texts for this actor (for angle dedup).

    Returns dict with: hook, body, hashtags, section, compare_with, stat_key, confidence
    Returns None on failure.
    """
    movies       = profile.get("movies", [])
    collaborators = profile.get("collaborators", [])
    directors    = profile.get("directors", [])

    if not movies:
        print(f"[generator] no movies for {actor_db_name}, skipping")
        return None

    cream = _compute_cream(movies)

    # Limit slug list to keep prompt size reasonable
    slug_sample = sorted(_ACTOR_SLUGS)[:50]

    recent_block = (
        "\n".join(f"  - {t[:120]}" for t in recent_tweets[-10:])
        if recent_tweets else "  (none yet)"
    )

    prompt = _GENERATE_TEMPLATE.format(
        actor_name          = actor_db_name,
        industry            = industry,
        profile_url         = profile["profile_url"],
        movies_block        = _movies_block(movies),
        collaborators_block = _collabs_block(collaborators),
        directors_block     = _collabs_block(directors),
        actor_slugs         = ", ".join(slug_sample),
        recent_tweets       = recent_block,
        **cream,
    )

    try:
        msg = await _client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=800,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        text = msg.content[0].text.strip() if msg.content else ""
        print(f"[generator] stop_reason={msg.stop_reason} len={len(text)} preview={text[:120]}")
        # Strip markdown code fences if Claude wrapped the JSON
        if text.startswith("```"):
            text = text.split("```", 2)[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()
        raw = json.loads(text)
    except Exception as e:
        print(f"[generator] Claude call failed for {actor_db_name}: {e}")
        return None

    # Map fact_type → section deterministically — Claude decides WHAT, Python decides WHERE
    fact_type = raw.get("fact_type", "decade_longevity")
    if fact_type not in VALID_FACT_TYPES:
        fact_type = "decade_longevity"
    section = FACT_TYPE_TO_SECTION[fact_type]

    compare_with = raw.get("compare_with", "").strip()
    if compare_with and compare_with not in _ACTOR_SLUGS:
        compare_with = ""
    if fact_type in ("comparison", "collab") and not compare_with:
        # Can't do compare/collab without a valid second actor — fall back
        fact_type = "decade_longevity"
        section   = FACT_TYPE_TO_SECTION[fact_type]

    hook = (raw.get("hook") or "").strip()
    body = (raw.get("body") or "").strip()
    if not hook or not body:
        print(f"[generator] incomplete output for {actor_db_name}: {raw}")
        return None

    director_name = (raw.get("director_name") or "").strip()

    return {
        "hook":          hook,
        "body":          body,
        "hashtags":      (raw.get("hashtags") or "").strip(),
        "section":       section,
        "chart_mode":    FACT_TYPE_TO_CHART_MODE.get(fact_type, "rating"),
        "compare_with":  compare_with,
        "director_name": director_name,
        "stat_key":      f"gen_{raw.get('stat_key', 'unknown')}",
        "confidence":    int(raw.get("confidence", 50)),
    }
