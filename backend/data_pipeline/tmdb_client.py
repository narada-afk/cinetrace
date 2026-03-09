"""
tmdb_client.py
==============
TMDB (The Movie Database) API client for South Cinema Analytics.

Public functions
----------------
    search_movie_tmdb(title, year) -> dict | None
        Search for a movie by title/year and return poster, rating, etc.

    fetch_movie_credits(tmdb_id, top_n=10) -> list[dict]
        Fetch the top-billed cast for a movie using its TMDB ID.
        Returns up to top_n members with name, character and billing order.
        Added in Sprint 8 to power ingest_supporting_actors.py.

Authentication
--------------
Set the environment variable TMDB_API_KEY to your TMDB v3 API key before
running any pipeline script that imports this module.

    export TMDB_API_KEY=your_key_here

Get a free key at: https://www.themoviedb.org/settings/api

Rate limiting
-------------
TMDB allows ~40 requests per 10 seconds for free-tier keys.  This client
enforces a conservative REQUEST_DELAY (0.25 s) between calls — roughly 4
req/s — well within the limit and safe for overnight batch runs.

Retry logic
-----------
Uses a requests.Session with urllib3 Retry:
    total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]

A 429 (rate-limited) response triggers an automatic retry with exponential
back-off, so transient throttling is handled transparently.

Search strategy
---------------
1. Search with title + year (primary_release_year).
2. If TMDB returns zero results, retry without the year constraint.
   This helps when the Wikidata release year differs by one from TMDB's
   primary_release_year (e.g. late-December theatrical vs. wide release).
3. The first result in the ranked list is used; TMDB orders results by
   relevance + popularity, so result[0] is almost always correct.

Image URL formats
-----------------
    Poster   : https://image.tmdb.org/t/p/w500/<poster_path>
    Backdrop : https://image.tmdb.org/t/p/w780/<backdrop_path>
"""

import os
import time
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TMDB_BASE          = "https://api.themoviedb.org/3"
_SEARCH_URL         = f"{_TMDB_BASE}/search/movie"
_CREDITS_URL        = f"{_TMDB_BASE}/movie/{{tmdb_id}}/credits"  # .format(tmdb_id=…)
_POSTER_BASE_URL    = "https://image.tmdb.org/t/p/w500"
_BACKDROP_BASE_URL  = "https://image.tmdb.org/t/p/w780"

REQUEST_DELAY = 0.25   # minimum seconds between API calls


# ---------------------------------------------------------------------------
# HTTP session (module-level singleton — one session for the whole process)
# ---------------------------------------------------------------------------

def _build_session() -> requests.Session:
    """Return a requests.Session with retry/backoff configured."""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session


_SESSION = _build_session()

# Monotonic timestamp of the last outbound request — used to enforce
# REQUEST_DELAY without blocking longer than necessary.
_last_request_ts: float = 0.0


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_api_key() -> str:
    """Return the TMDB API key from the environment, or raise clearly."""
    key = os.getenv("TMDB_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "TMDB_API_KEY environment variable is not set.\n"
            "Get a free key at https://www.themoviedb.org/settings/api\n"
            "Then run:  export TMDB_API_KEY=your_key_here"
        )
    return key


def _api_get(url: str, params: dict) -> dict:
    """
    Rate-limited GET to any TMDB API endpoint.

    Enforces REQUEST_DELAY between consecutive calls so we stay well
    under TMDB's 40 req/10 s free-tier limit.  Raises requests.HTTPError
    on 4xx/5xx after the configured retries are exhausted.
    """
    global _last_request_ts

    elapsed = time.monotonic() - _last_request_ts
    if elapsed < REQUEST_DELAY:
        time.sleep(REQUEST_DELAY - elapsed)

    resp = _SESSION.get(url, params=params, timeout=10)
    _last_request_ts = time.monotonic()
    resp.raise_for_status()
    return resp.json()


def _rate_limited_get(params: dict) -> dict:
    """
    Backward-compatible wrapper — calls _api_get against _SEARCH_URL.
    New code should call _api_get directly with an explicit URL.
    """
    return _api_get(_SEARCH_URL, params)


def _build_image_url(base: str, path: Optional[str]) -> Optional[str]:
    """Return a full TMDB image URL, or None if path is missing."""
    if not path:
        return None
    return f"{base}{path}"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def search_movie_tmdb(title: str, year: int) -> Optional[dict]:
    """
    Search TMDB for a movie and return its metadata.

    Parameters
    ----------
    title : str
        Movie title as stored in the database (may be in any language).
    year  : int
        Release year from the database.  Pass 0 if unknown (sentinel value
        used by the ingestion pipeline); the year filter will be skipped.

    Returns
    -------
    dict with keys:
        tmdb_id      : int   — TMDB movie ID
        poster_url   : str | None
        backdrop_url : str | None
        vote_average : float | None  — community vote average (0.0–10.0)
        popularity   : float | None  — TMDB popularity score

    Returns None if no match is found or if the API call fails.

    Example
    -------
    >>> result = search_movie_tmdb("Jailer", 2023)
    >>> result["tmdb_id"]
    1037011
    >>> result["poster_url"]
    'https://image.tmdb.org/t/p/w500/abc123.jpg'
    """
    api_key = _get_api_key()

    base_params = {
        "api_key":        api_key,
        "query":          title,
        "language":       "en-US",
        "page":           1,
        "include_adult":  False,
    }

    # --- Strategy: try with year, then without (see module docstring) -------
    search_attempts: list[dict] = []

    if year and year > 0:
        # Primary: exact year match increases precision significantly
        search_attempts.append({**base_params, "primary_release_year": year})

    # Fallback: no year filter (catches ±1 year discrepancies between Wikidata
    # and TMDB, or films released across year boundaries)
    search_attempts.append(base_params)

    for attempt_params in search_attempts:
        try:
            data = _rate_limited_get(attempt_params)
        except requests.RequestException:
            # Network / API error — give up rather than burn retries
            return None

        results = data.get("results") or []
        if results:
            best = results[0]   # TMDB ranks by relevance + popularity
            return {
                "tmdb_id":      best.get("id"),
                "poster_url":   _build_image_url(_POSTER_BASE_URL,   best.get("poster_path")),
                "backdrop_url": _build_image_url(_BACKDROP_BASE_URL, best.get("backdrop_path")),
                "vote_average": best.get("vote_average"),
                "popularity":   best.get("popularity"),
            }

    # Both attempts returned zero results
    return None


# ---------------------------------------------------------------------------
# Sprint 8 — Cast credits
# ---------------------------------------------------------------------------

def fetch_movie_credits(tmdb_id: int, top_n: int = 10) -> list[dict]:
    """
    Fetch the cast credits for a movie from TMDB.

    Calls:
        GET https://api.themoviedb.org/3/movie/{tmdb_id}/credits

    TMDB returns cast members pre-sorted by billing order (``order`` field,
    0-based).  This function returns the first ``top_n`` entries.

    Parameters
    ----------
    tmdb_id : int
        The TMDB numeric movie ID (stored in movies.tmdb_id after Sprint 7).
    top_n   : int
        Maximum number of cast members to return (default 10).

    Returns
    -------
    list of dict, each containing:
        tmdb_person_id : int   — TMDB's unique ID for this person
        name           : str   — actor's full name
        character      : str | None  — character name in this film
        cast_order     : int   — billing position (0 = top-billed)

    Returns an empty list on API error or if no cast data is available.

    Example
    -------
    >>> credits = fetch_movie_credits(1153399)   # Coolie (2025)
    >>> credits[0]
    {'tmdb_person_id': 120983, 'name': 'Rajinikanth', 'character': 'Coolie', 'cast_order': 0}
    """
    api_key = _get_api_key()
    url     = _CREDITS_URL.format(tmdb_id=tmdb_id)
    params  = {"api_key": api_key, "language": "en-US"}

    try:
        data = _api_get(url, params)
    except requests.RequestException:
        return []

    raw_cast = data.get("cast") or []

    results: list[dict] = []
    for member in raw_cast[:top_n]:
        person_id = member.get("id")
        name      = (member.get("name") or "").strip()
        if not person_id or not name:
            continue                         # skip malformed entries
        results.append({
            "tmdb_person_id": person_id,
            "name":           name,
            "character":      member.get("character") or None,
            "cast_order":     member.get("order", 0),
        })

    return results
