"""
wikidata_client.py
==================
Fetches South Indian cinema data from the Wikidata public SPARQL endpoint.

Sprint 3 change — QID-based lookups
-------------------------------------
The previous implementation resolved actors by matching their English rdfs:label,
which was fragile:  common names ("Vijay") matched multiple entities, and
diacritics / alternate spellings caused missed results.

This version resolves actors via their canonical Wikidata QID (e.g. Q352416
for Allu Arjun).  Querying `wd:Q352416` is unambiguous, faster, and immune
to label-language issues.

Sprint 4 change — Retry logic (Task 4)
----------------------------------------
A urllib3 Retry strategy is now mounted on the shared requests.Session:
  - 3 total retries
  - Exponential backoff: 1 s → 2 s → 4 s
  - Retries on HTTP 429, 500, 502, 503, 504
This handles Wikidata rate-limit bursts and transient server errors without
any changes to calling code.

Polite usage rules (unchanged from Sprint 2):
  - Descriptive User-Agent header identifies our service to Wikidata ops.
  - REQUEST_DELAY (1 s) between every HTTP call respects Wikidata's fair-use
    guidelines.  https://www.mediawiki.org/wiki/Wikidata_Query_Service/User_Manual
  - 30-second timeout prevents hung connections from stalling the pipeline.

Usage (standalone test):
    python -m data_pipeline.wikidata_client Q352416            # Allu Arjun
    python -m data_pipeline.wikidata_client Q297491 "Prabhas"  # named output
"""

import json
import re
import sys
import time
from typing import Optional

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

# 1 request per second — required by Wikidata's terms of service.
REQUEST_DELAY: float = 1.0

USER_AGENT = (
    "SouthCinemaAnalytics/1.0 "
    "(https://github.com/south-cinema-analytics; "
    "contact@south-cinema-analytics.example) "
    "Python/3.11 requests"
)

# Valid QID pattern: "Q" followed by one or more digits.
_QID_RE = re.compile(r"^Q\d+$")


# ---------------------------------------------------------------------------
# Session with retry logic (Task 4)
# ---------------------------------------------------------------------------

def _build_session() -> Session:
    """
    Create a requests.Session with automatic retry on transient errors.

    Retry configuration:
      - total=3          : up to 3 retry attempts per request
      - backoff_factor=1 : sleep 1 s, 2 s, 4 s between attempts
      - status_forcelist : retry on 429 (rate limited), 5xx server errors
      - raise_on_status=False : let _sparql_query() call raise_for_status()

    Returns:
        Configured Session instance.
    """
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = Session()
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session


# Module-level session: shared and reused across all calls in a process.
_SESSION: Session = _build_session()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _validate_qid(wikidata_id: str) -> str:
    """
    Validate and normalise a Wikidata QID string.

    Strips leading/trailing whitespace and uppercases the "Q" prefix so that
    inputs like "q352416" or " Q352416 " are silently corrected.

    Args:
        wikidata_id: Raw QID string from the caller.

    Returns:
        Normalised QID string, e.g. "Q352416".

    Raises:
        ValueError: if the string does not match the QID format after normalisation.
    """
    qid = wikidata_id.strip().upper()
    if not _QID_RE.match(qid):
        raise ValueError(
            f"Invalid Wikidata QID {wikidata_id!r}.  "
            f"Expected format: Q followed by digits, e.g. 'Q352416'."
        )
    return qid


def _sparql_query(query: str) -> dict:
    """
    Execute a SPARQL SELECT query against the Wikidata endpoint.

    Sleeps REQUEST_DELAY seconds *before* the HTTP call so every caller
    automatically respects the rate limit without extra bookkeeping.
    The session-level Retry adapter handles transient failures automatically.

    Args:
        query: SPARQL SELECT query string.

    Returns:
        Parsed JSON response dict (SPARQL 1.1 results format).

    Raises:
        requests.HTTPError: on 4xx / 5xx status codes (after retries exhausted).
        requests.Timeout:   if the server doesn't respond within 30 s.
    """
    time.sleep(REQUEST_DELAY)

    headers = {
        "User-Agent": USER_AGENT,
        "Accept":     "application/sparql-results+json",
    }
    response = _SESSION.get(
        SPARQL_ENDPOINT,
        params={"query": query, "format": "json"},
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def _is_unresolved_qid(value: str) -> bool:
    """
    Return True if *value* is an unresolved Wikidata entity ID (e.g. "Q123456").

    When the label service cannot resolve a label it falls back to the raw
    entity IRI.  We filter these out so junk never reaches the database.
    """
    return bool(value) and _QID_RE.match(value) is not None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_actor_filmography(wikidata_id: str, actor_name: str = "") -> dict:
    """
    Fetch every film in which the given actor appeared, using their Wikidata
    QID for a precise, unambiguous lookup.

    SPARQL strategy
    ---------------
    The query uses the *reverse property path* ``^wdt:P161`` which reads as
    "find all entities X such that X has cast member = wd:<QID>", i.e.
    "all films this actor appeared in".  This is equivalent to::

        ?film wdt:P161 wd:<QID>

    but written in the compact Turtle form preferred by the task spec.

    Additional constraints:
      - ``?film wdt:P31/wdt:P279* wd:Q11424``  — restrict to theatrical films
        and their subclasses; excludes TV episodes, shorts, documentaries, etc.
      - OPTIONAL release date and director — not every film has these on Wikidata.

    Deduplication
    -------------
    A single film can appear multiple times in SPARQL results when it has more
    than one director listed on Wikidata (one binding per director).  We
    deduplicate by ``(title, releaseYear)`` in Python and keep only the first
    director encountered for each pair.

    Args:
        wikidata_id: Wikidata entity ID of the actor, e.g. ``"Q352416"``
                     (Allu Arjun).  Case-insensitive; leading/trailing
                     whitespace is stripped automatically.
        actor_name:  Optional human-readable name used only for the ``"actor"``
                     field in the returned dict.  Defaults to *wikidata_id*
                     when empty.

    Returns:
        Dict with the structure::

            {
                "actor": "Allu Arjun",       # actor_name or wikidata_id
                "wikidata_id": "Q352416",
                "movies": [
                    {
                        "title":    "Pushpa: The Rise",
                        "year":     2021,        # int or None
                        "director": "Sukumar"    # str or None
                    },
                    ...
                ]
            }

    Raises:
        ValueError:           if *wikidata_id* is not a valid QID.
        requests.HTTPError:   on Wikidata API errors.
        requests.Timeout:     if the SPARQL endpoint is unresponsive.
    """
    qid = _validate_qid(wikidata_id)
    display_name = actor_name.strip() or qid

    query = f"""
    SELECT ?film ?filmLabel ?releaseYear ?directorLabel WHERE {{

      # ── Films this actor appeared in ──────────────────────────────────────
      # wd:{qid} ^wdt:P161 ?film  is shorthand for:  ?film wdt:P161 wd:{qid}
      wd:{qid} ^wdt:P161 ?film .

      # Restrict to theatrical films and their Wikidata subclasses.
      # This excludes TV episodes, documentaries, web series, etc.
      ?film wdt:P31/wdt:P279* wd:Q11424 .

      # ── Optional metadata ─────────────────────────────────────────────────
      OPTIONAL {{
        ?film wdt:P577 ?releaseDate .
        BIND(YEAR(?releaseDate) AS ?releaseYear)
      }}
      OPTIONAL {{
        ?film wdt:P57 ?director .
      }}

      # Resolve labels in English.
      SERVICE wikibase:label {{
        bd:serviceParam wikibase:language "en" .
      }}
    }}
    ORDER BY ?releaseYear
    LIMIT 300
    """

    print(f"  Querying Wikidata : {display_name} ({qid})")
    results  = _sparql_query(query)
    bindings = results.get("results", {}).get("bindings", [])

    # Deduplicate by (title, year) — one row per unique film.
    seen:   set[tuple]  = set()
    movies: list[dict]  = []

    for binding in bindings:
        title        = binding.get("filmLabel",      {}).get("value", "")
        year_raw     = binding.get("releaseYear",     {}).get("value")
        director_raw = binding.get("directorLabel",   {}).get("value", "")

        # Drop rows where the label service returned a raw entity ID.
        if not title or _is_unresolved_qid(title):
            continue

        year: Optional[int] = int(year_raw) if year_raw else None
        director: Optional[str] = (
            director_raw
            if director_raw and not _is_unresolved_qid(director_raw)
            else None
        )

        key = (title, year)
        if key in seen:
            continue   # already have this film — skip extra director binding
        seen.add(key)

        movies.append({"title": title, "year": year, "director": director})

    return {
        "actor":       display_name,
        "wikidata_id": qid,
        "movies":      movies,
    }


# ---------------------------------------------------------------------------
# Standalone test entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Usage:
    #   python -m data_pipeline.wikidata_client Q352416
    #   python -m data_pipeline.wikidata_client Q352416 "Allu Arjun"
    if len(sys.argv) < 2:
        print("Usage: python -m data_pipeline.wikidata_client <QID> [actor_name]")
        print()
        print("Examples:")
        print("  python -m data_pipeline.wikidata_client Q352416")
        print('  python -m data_pipeline.wikidata_client Q352416 "Allu Arjun"')
        print('  python -m data_pipeline.wikidata_client Q351478 "Rajinikanth"')
        sys.exit(1)

    _qid  = sys.argv[1]
    _name = sys.argv[2] if len(sys.argv) > 2 else ""
    _data = fetch_actor_filmography(_qid, _name)
    print(json.dumps(_data, indent=2, ensure_ascii=False))
