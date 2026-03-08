"""
wikidata_batch_client.py
========================
Fetches South Indian cinema data from Wikidata in *batch* mode.

Why batching?
-------------
The single-actor client (wikidata_client.py) issues one SPARQL request per
actor.  With 13 actors that is 13 sequential HTTP calls and 13+ seconds of
wall-clock time just in rate-limit delays.

This module issues ONE SPARQL request per batch of up to BATCH_SIZE actors by
using the SPARQL ``VALUES`` clause to enumerate multiple actor QIDs in a single
query.  A batch of 20 actors that would have cost 20 s now costs ~1 s of delay
plus one network round-trip.

Public API
----------
    fetch_filmography_batch(actor_qids, label="") -> list[dict]

        Each dict represents one (actor, film) row:
            {
                "actor_qid":   "Q352416",
                "actor_name":  "Allu Arjun",      # from Wikidata label service
                "film_title":  "Pushpa: The Rise",
                "release_year": 2021,              # int or None
                "director":    "Sukumar"           # str or None
            }

        Duplicate rows (caused by films with multiple directors) are collapsed
        in Python before the list is returned.  Only the first director seen for
        each (actor_qid, film_title, release_year) triple is kept.

Compatibility
-------------
This module is a sibling of wikidata_client.py, NOT a replacement.  The single-
actor client remains available for one-off debugging:
    python -m data_pipeline.wikidata_client Q352416 "Allu Arjun"

Sprint 4 change — Retry logic (Task 4)
----------------------------------------
A urllib3 Retry strategy is now mounted on the shared requests.Session:
  - 3 total retries
  - Exponential backoff: 1 s → 2 s → 4 s
  - Retries on HTTP 429 (rate limited), 500, 502, 503, 504
Batch SPARQL queries are longer-running than single-actor queries, making
them more susceptible to transient timeouts; retries provide resilience.

Polite usage rules
------------------
  - Descriptive User-Agent header identifies this service to Wikidata ops.
  - REQUEST_DELAY (1 s) sleep *between batches* (not per-actor) keeps throughput
    high while respecting Wikidata's fair-use policy.
    https://www.mediawiki.org/wiki/Wikidata_Query_Service/User_Manual
  - 60-second timeout per request; batch queries can be slower than single ones.
  - LIMIT 5000 per query guards against unexpectedly large result sets.

Standalone test:
    python -m data_pipeline.wikidata_batch_client Q352416 Q297491 Q536725
"""

import json
import re
import sys
import time
from collections import defaultdict
from typing import Optional

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

# Delay between *batch* requests (not per-actor — that's the whole point).
REQUEST_DELAY: float = 1.0

# Default actors per SPARQL query.  Wikidata handles 20 comfortably; going
# higher risks query timeouts on complex filmographies.
BATCH_SIZE: int = 20

# Safety ceiling: warn if results hit this limit (possible truncation).
RESULT_LIMIT: int = 5000

USER_AGENT = (
    "SouthCinemaAnalytics/1.0 "
    "(https://github.com/south-cinema-analytics; "
    "contact@south-cinema-analytics.example) "
    "Python/3.11 requests"
)

# Valid QID: "Q" followed by one or more digits.
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

    Batch SPARQL queries are slower than single-actor queries, so transient
    timeouts are more likely; retries provide an important safety net.

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


# Module-level session: shared and reused across all batch calls in a process.
_SESSION: Session = _build_session()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _validate_qids(qids: list[str]) -> list[str]:
    """
    Validate and normalise a list of Wikidata QIDs.

    Uppercases the "Q" prefix and strips whitespace so inputs like
    "q352416" or " Q297491 " are silently corrected.

    Args:
        qids: Raw QID strings from the caller.

    Returns:
        List of normalised QID strings.

    Raises:
        ValueError: if any element does not match the QID format after
                    normalisation, or if the list is empty.
    """
    if not qids:
        raise ValueError("fetch_filmography_batch: actor_qids list must not be empty.")

    result = []
    for raw in qids:
        qid = raw.strip().upper()
        if not _QID_RE.match(qid):
            raise ValueError(
                f"Invalid Wikidata QID {raw!r}.  "
                f"Expected 'Q' followed by digits, e.g. 'Q352416'."
            )
        result.append(qid)
    return result


def _build_values_block(qids: list[str]) -> str:
    """
    Build the SPARQL VALUES block for a list of QIDs.

    Example output for ["Q352416", "Q297491"]:
        wd:Q352416
        wd:Q297491

    Args:
        qids: Normalised QID strings.

    Returns:
        Multi-line string for insertion into the VALUES clause.
    """
    return "\n    ".join(f"wd:{qid}" for qid in qids)


def _qid_from_uri(uri: str) -> str:
    """
    Extract the QID from a Wikidata entity URI.

    Example:
        "http://www.wikidata.org/entity/Q352416"  →  "Q352416"

    Args:
        uri: Full Wikidata entity IRI from the SPARQL JSON response.

    Returns:
        QID string, e.g. "Q352416".
    """
    return uri.rsplit("/", 1)[-1]


def _is_unresolved_qid(value: str) -> bool:
    """
    Return True if *value* is a raw Wikidata entity ID (e.g. "Q123456").

    The label service falls back to the entity ID when no English label
    exists.  We filter these out so unresolved IDs never reach the database.
    """
    return bool(value) and _QID_RE.match(value) is not None


def _sparql_query(query: str) -> dict:
    """
    Execute a SPARQL SELECT query against the Wikidata endpoint.

    Sleeps REQUEST_DELAY seconds *before* sending so every batch call
    automatically respects the rate limit without extra bookkeeping.
    The session-level Retry adapter handles transient failures automatically.

    Args:
        query: SPARQL SELECT query string.

    Returns:
        Parsed JSON response (SPARQL 1.1 results format).

    Raises:
        requests.HTTPError: on 4xx / 5xx status codes (after retries exhausted).
        requests.Timeout:   if the server doesn't respond within 60 s.
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
        timeout=60,   # batch queries can be slower than single-actor ones
    )
    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_filmography_batch(
    actor_qids: list[str],
    label:      str = "",
) -> list[dict]:
    """
    Fetch filmographies for multiple actors in a single SPARQL query.

    The query uses a ``VALUES`` clause to enumerate all actor QIDs at once.
    Results are returned as a flat list of (actor, film) row dicts.

    SPARQL strategy
    ---------------
    ``?film wdt:P161 ?actor`` finds all films in which *?actor* was a cast
    member.  Filtering ``?film wdt:P31/wdt:P279* wd:Q11424`` keeps only
    theatrical films (excluding TV episodes, documentaries, shorts, etc.).

    Deduplication
    -------------
    A film with multiple Wikidata directors produces one result row per
    director.  This function deduplicates on ``(actor_qid, film_title,
    release_year)`` and keeps only the first director encountered per triple.

    Args:
        actor_qids: List of Wikidata QIDs, e.g. ["Q352416", "Q297491"].
                    Case-insensitive; whitespace is stripped automatically.
                    Maximum recommended size: BATCH_SIZE (20).
        label:      Optional human-readable label printed in progress output,
                    e.g. "batch 1/3".  Defaults to "batch of N actors".

    Returns:
        Flat list of dicts, one per unique (actor, film) pair::

            [
                {
                    "actor_qid":    "Q352416",
                    "actor_name":   "Allu Arjun",
                    "film_title":   "Pushpa: The Rise",
                    "release_year": 2021,       # int or None
                    "director":     "Sukumar"   # str or None
                },
                ...
            ]

    Raises:
        ValueError:           if *actor_qids* is empty or contains invalid QIDs.
        requests.HTTPError:   on Wikidata API errors.
        requests.Timeout:     if the SPARQL endpoint is unresponsive.
    """
    validated_qids = _validate_qids(actor_qids)
    values_block   = _build_values_block(validated_qids)
    display_label  = label or f"batch of {len(validated_qids)} actor(s)"

    query = f"""
    SELECT ?actor ?actorLabel ?film ?filmLabel ?releaseYear ?directorLabel WHERE {{

      # ── Actor set — enumerate all QIDs in a single VALUES clause ───────────
      VALUES ?actor {{
        {values_block}
      }}

      # ── Films each actor appeared in ────────────────────────────────────────
      ?film wdt:P161 ?actor .

      # Restrict to theatrical films and their Wikidata subclasses.
      # Excludes: TV episodes, documentaries, web series, short films, etc.
      ?film wdt:P31/wdt:P279* wd:Q11424 .

      # ── Optional metadata ──────────────────────────────────────────────────
      OPTIONAL {{
        ?film wdt:P577 ?releaseDate .
        BIND(YEAR(?releaseDate) AS ?releaseYear)
      }}
      OPTIONAL {{
        ?film wdt:P57 ?director .
      }}

      # Resolve human-readable labels in English.
      SERVICE wikibase:label {{
        bd:serviceParam wikibase:language "en" .
      }}
    }}
    ORDER BY ?actor ?releaseYear
    LIMIT {RESULT_LIMIT}
    """

    print(f"  → Querying Wikidata ({display_label}) ...")
    raw      = _sparql_query(query)
    bindings = raw.get("results", {}).get("bindings", [])

    if len(bindings) >= RESULT_LIMIT:
        print(
            f"  ⚠  Result count hit the LIMIT of {RESULT_LIMIT}.  "
            f"Some films may be missing.  Consider reducing BATCH_SIZE."
        )

    # ── Parse and deduplicate ─────────────────────────────────────────────────
    # Key: (actor_qid, film_title, release_year) — one row per unique triple.
    seen:  set[tuple]  = set()
    rows:  list[dict]  = []

    for binding in bindings:
        actor_uri    = binding.get("actor",         {}).get("value", "")
        actor_name   = binding.get("actorLabel",    {}).get("value", "")
        film_title   = binding.get("filmLabel",     {}).get("value", "")
        year_raw     = binding.get("releaseYear",   {}).get("value")
        director_raw = binding.get("directorLabel", {}).get("value", "")

        # Extract QID from the entity URI.
        actor_qid = _qid_from_uri(actor_uri) if actor_uri else ""
        if not actor_qid or not _QID_RE.match(actor_qid):
            continue   # skip malformed actor URI

        # Skip rows where the label service returned a raw entity ID.
        if not film_title or _is_unresolved_qid(film_title):
            continue

        year: Optional[int] = int(year_raw) if year_raw else None

        director: Optional[str] = (
            director_raw
            if director_raw and not _is_unresolved_qid(director_raw)
            else None
        )

        key = (actor_qid, film_title, year)
        if key in seen:
            continue   # duplicate from multi-director film — skip
        seen.add(key)

        rows.append({
            "actor_qid":    actor_qid,
            "actor_name":   actor_name,
            "film_title":   film_title,
            "release_year": year,
            "director":     director,
        })

    return rows


# ---------------------------------------------------------------------------
# Standalone test entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m data_pipeline.wikidata_batch_client <QID1> [QID2 ...]")
        print()
        print("Examples:")
        print("  python -m data_pipeline.wikidata_batch_client Q352416 Q297491")
        print("  python -m data_pipeline.wikidata_batch_client Q536725 Q351478 Q330829")
        sys.exit(1)

    _qids  = sys.argv[1:]
    _rows  = fetch_filmography_batch(_qids, label="test")
    print(f"\nReturned {len(_rows)} row(s):\n")
    print(json.dumps(_rows[:20], indent=2, ensure_ascii=False))
    if len(_rows) > 20:
        print(f"  … and {len(_rows) - 20} more rows")
