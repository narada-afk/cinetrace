"""
tmdb_client.py
==============
TMDB (The Movie Database) API client for South Cinema Analytics.

Provides a single public function:

    search_movie_tmdb(title, year) -> dict | None

which calls the TMDB search/movie endpoint and returns structured metadata
for the best-matching film.

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

_SEARCH_URL        = "https://api.themoviedb.org/3/search/movie"
_POSTER_BASE_URL   = "https://image.tmdb.org/t/p/w500"
_BACKDROP_BASE_URL = "https://image.tmdb.org/t/p/w780"

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


def _rate_limited_get(params: dict) -> dict:
    """
    Fire a GET to _SEARCH_URL with rate limiting enforced.

    Sleeps for the remaining fraction of REQUEST_DELAY since the last call
    before sending the next request.  Raises requests.HTTPError on 4xx/5xx
    after retries are exhausted.
    """
    global _last_request_ts

    elapsed = time.monotonic() - _last_request_ts
    if elapsed < REQUEST_DELAY:
        time.sleep(REQUEST_DELAY - elapsed)

    resp = _SESSION.get(_SEARCH_URL, params=params, timeout=10)
    _last_request_ts = time.monotonic()
    resp.raise_for_status()
    return resp.json()


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
