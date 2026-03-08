"""
wikipedia_client.py
===================
Fetches film metadata from the public English Wikipedia MediaWiki API and
parses the structured film infobox to extract:
  - runtime           (int, minutes)
  - production_company (str, first listed company)
  - language          (str, first listed language)

Sprint 4 additions
------------------
  HTTP caching (Task 3):
    requests-cache is used to avoid re-fetching Wikipedia pages that were
    already retrieved in a previous run.  Pages are cached for 24 hours in
    a local SQLite file named ``wiki_cache.sqlite``.  On a re-run of
    enrich_movies.py the cache is consulted first; only cache misses generate
    real HTTP calls, dramatically speeding up subsequent enrichment passes.

  Retry logic (Task 4):
    A ``urllib3.util.retry.Retry`` strategy is mounted on the session with:
      - 3 total retries
      - Exponential backoff: 1 s → 2 s → 4 s between attempts
      - Retries on HTTP 429 (rate limited), 500, 502, 503, 504
    Transient network blips or Wikipedia rate-limit bursts are handled
    transparently without any changes to calling code.

Design rules (unchanged):
  - One public function:  fetch_movie_metadata(title) -> dict
  - Every HTTP call goes through _get(), which enforces a 1-second delay and
    a descriptive User-Agent so Wikipedia can identify and contact us.
  - On *any* error (network, parse, missing data) the function returns a dict
    with None values rather than raising — the pipeline must never crash on a
    single bad page.
  - Two HTTP calls per movie: one search call, one parse call.

Requires:
  pip install requests beautifulsoup4 requests-cache

Usage (standalone test):
    python -m data_pipeline.wikipedia_client "Pushpa: The Rise"
    python -m data_pipeline.wikipedia_client "Baahubali: The Beginning"
"""

import json
import re
import sys
import time
from typing import Optional

import requests_cache
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

WIKI_API_URL = "https://en.wikipedia.org/w/api.php"

# Polite rate limit: every live HTTP call sleeps this many seconds first.
# Cached responses do NOT sleep (they are served from disk instantly).
REQUEST_DELAY: float = 1.0

# Cache settings
CACHE_NAME = "wiki_cache"        # SQLite file: wiki_cache.sqlite (in CWD)
CACHE_EXPIRE = 86_400            # 24 hours — Wikipedia film pages are stable

# Wikipedia's bot policy requires a meaningful User-Agent.
USER_AGENT = (
    "SouthCinemaAnalytics/1.0 "
    "(https://github.com/south-cinema-analytics; "
    "contact@south-cinema-analytics.example) "
    "Python/3.11 requests-cache/beautifulsoup4"
)

_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json",
}

# Infobox row labels we care about (lowercased for case-insensitive matching).
_RUNTIME_LABELS    = {"running time", "runtime"}
_PRODUCTION_LABELS = {"production company", "production companies"}
_LANGUAGE_LABELS   = {"language", "languages"}

# Empty result returned whenever metadata cannot be found.
_EMPTY: dict = {"runtime": None, "production_company": None, "language": None}


# ---------------------------------------------------------------------------
# Session: caching + retry (Task 3 & Task 4)
# ---------------------------------------------------------------------------

def _build_session() -> requests_cache.CachedSession:
    """
    Create a requests session with:
      - 24-hour SQLite cache (requests-cache)
      - Automatic retry on transient errors (urllib3 Retry)

    The CachedSession is a drop-in replacement for requests.Session.
    Cached responses are returned immediately without touching the network,
    so REQUEST_DELAY is only applied for real (un-cached) HTTP calls.

    Returns:
        Configured CachedSession instance.
    """
    session = requests_cache.CachedSession(
        CACHE_NAME,
        expire_after=CACHE_EXPIRE,
        # Cache only successful (2xx) responses.
        allowable_codes=[200],
        # Cache GET requests only.
        allowable_methods=["GET"],
        # Thread-safe SQLite backend (uses WAL mode internally).
        backend="sqlite",
    )

    # Retry strategy: 3 attempts with exponential backoff.
    # Delays: 1 s, 2 s, 4 s (backoff_factor=1 → factor * (2^attempt)).
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False,  # let raise_for_status() in _get() handle it
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)

    return session


# Module-level session: shared across all calls within a process.
# Thread-safe: each thread gets its own SQLite connection via the cache backend.
_SESSION: requests_cache.CachedSession = _build_session()


# ---------------------------------------------------------------------------
# Internal HTTP helper
# ---------------------------------------------------------------------------

def _get(params: dict) -> dict:
    """
    Rate-limited GET request to the Wikipedia MediaWiki API.

    Cached responses are served from disk without any delay.
    Live (un-cached) requests sleep REQUEST_DELAY seconds before sending so
    that repeated cache misses respect Wikipedia's rate-limit guidelines.

    Args:
        params: Query-string parameters for the MediaWiki API.

    Returns:
        Parsed JSON response dict.

    Raises:
        requests.HTTPError: on 4xx / 5xx status codes (after retries are
                            exhausted for eligible codes).
        requests.Timeout:   if the server doesn't respond within 30 s.
    """
    # Only sleep for real (live) requests; skip delay for cache hits.
    # We probe the cache before sending to decide whether to sleep.
    cached_response = _SESSION.cache.get_response(
        _SESSION.prepare_request(
            requests_cache.AnyRequest(  # type: ignore[attr-defined]
                "GET", WIKI_API_URL, params=params
            )
        )
    ) if hasattr(_SESSION, "cache") else None

    if cached_response is None:
        # Live request — sleep to respect rate limit.
        time.sleep(REQUEST_DELAY)

    response = _SESSION.get(
        WIKI_API_URL,
        params=params,
        headers=_HEADERS,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------------------------
# Step 1 — Search for the Wikipedia page
# ---------------------------------------------------------------------------

def _search_page_id(movie_title: str) -> Optional[int]:
    """
    Search Wikipedia full-text for the most relevant film article and return
    its numeric page ID.

    Appending " film" to the query biases results toward film articles and
    away from disambiguation pages or unrelated uses of the title.

    We inspect up to 5 results and prefer any result whose title contains
    the original movie title (case-insensitive).  If none match, we fall
    back to the first result.

    Args:
        movie_title: Film title as stored in the DB, e.g. "Pushpa: The Rise".

    Returns:
        Integer Wikipedia page ID, or None if the search found nothing.
    """
    params = {
        "action":   "query",
        "list":     "search",
        "srsearch": f"{movie_title} film",
        "srlimit":  5,
        "srinfo":   "",           # suppress extra metadata we don't need
        "srprop":   "titlesnippet",
        "format":   "json",
    }

    try:
        data    = _get(params)
        results = data.get("query", {}).get("search", [])
    except Exception:
        return None

    if not results:
        return None

    # Prefer a result whose title contains the original query (minus colons,
    # dashes etc.) — helps with titles like "Pushpa: The Rise – Part 1".
    normalised = movie_title.lower().split(":")[0].strip()
    for result in results:
        if normalised in result["title"].lower():
            return result["pageid"]

    # Fall back to the top search result.
    return results[0]["pageid"]


# ---------------------------------------------------------------------------
# Step 2 — Fetch and parse the page HTML
# ---------------------------------------------------------------------------

def _fetch_page_html(page_id: int) -> Optional[str]:
    """
    Fetch the fully rendered HTML of a Wikipedia page by its page ID.

    We use action=parse rather than action=query+prop=revisions so that
    Wikipedia renders all templates (including the infobox) into HTML before
    we receive it — no need to parse raw wikitext.

    Args:
        page_id: Integer Wikipedia page ID from _search_page_id().

    Returns:
        HTML string of the page body, or None on any error.
    """
    params = {
        "action": "parse",
        "pageid": page_id,
        "prop":   "text",
        "format": "json",
    }

    try:
        data = _get(params)
        return data.get("parse", {}).get("text", {}).get("*")
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Infobox parsing helpers
# ---------------------------------------------------------------------------

def _clean_cell(td: Tag) -> str:
    """
    Extract readable text from a table data cell (<td>), stripping:
      - Superscript citations ([1], [2] …)
      - Invisible spans used for sorting (e.g. <span style="display:none">)
      - Excess whitespace

    We also normalise internal newlines / <br> tags to spaces so that
    multi-line values (e.g. lists of companies) become a single string
    that we can then split on commas or newlines.

    Args:
        td: BeautifulSoup Tag object for the <td> element.

    Returns:
        Cleaned plain-text string.
    """
    # Work on a copy so we don't mutate the shared parse tree.
    cell = BeautifulSoup(str(td), "html.parser")

    # Drop citation superscripts: <sup class="reference">…</sup>
    for sup in cell.find_all("sup"):
        sup.decompose()

    # Drop hidden sorting spans.
    for span in cell.find_all("span", style=lambda s: s and "display:none" in s):
        span.decompose()

    # Replace <br> and <li> boundaries with a sentinel we can split on later.
    for tag in cell.find_all(["br", "li"]):
        tag.replace_with("\n")

    text = cell.get_text(separator=" ")

    # Collapse runs of whitespace (spaces, tabs) but keep newlines.
    text = re.sub(r"[ \t]+", " ", text)
    # Remove leading/trailing whitespace from each logical line.
    lines = [line.strip() for line in text.splitlines()]
    # Drop empty lines.
    lines = [line for line in lines if line]

    return "\n".join(lines)


def _first_non_empty(values: list[str]) -> Optional[str]:
    """Return the first non-empty, non-whitespace string, or None."""
    for v in values:
        v = v.strip()
        if v:
            return v
    return None


def _parse_runtime_text(text: str) -> Optional[int]:
    """
    Convert a runtime text fragment into an integer number of minutes.

    Handles the common Wikipedia formats:
      "179 minutes"              →  179
      "179"                      →  179
      "2 hours 59 minutes"       →  179
      "2 hours"                  →  120
      "2 hr 59 min"              →  179
      "179 min"                  →  179

    Args:
        text: Raw text value from the "Running time" infobox row.

    Returns:
        Integer minutes, or None if the format is not recognised.
    """
    text = text.strip()

    # "X hours Y minutes" or "X hr Y min" variants.
    m = re.search(
        r"(\d+)\s*h(?:ours?|r)?\s*(\d+)\s*m(?:inutes?|in)?",
        text,
        re.IGNORECASE,
    )
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))

    # "X hours" (no minutes component).
    m = re.search(r"(\d+)\s*h(?:ours?|r)?", text, re.IGNORECASE)
    if m:
        return int(m.group(1)) * 60

    # "X minutes" or "X min".
    m = re.search(r"(\d+)\s*m(?:inutes?|in)?", text, re.IGNORECASE)
    if m:
        return int(m.group(1))

    # Bare integer (e.g. just "179").
    m = re.fullmatch(r"\d+", text)
    if m:
        return int(text)

    return None


def _split_list_value(text: str) -> list[str]:
    """
    Split a multi-valued infobox cell (e.g. a list of companies or languages)
    into individual cleaned items.

    Splits on newlines (already inserted by _clean_cell for <br>/<li>) and
    on commas, then strips each token.

    Args:
        text: Multi-line / comma-separated text from _clean_cell().

    Returns:
        List of non-empty stripped strings.
    """
    # Split on newlines first (already our primary separator from _clean_cell).
    parts: list[str] = []
    for segment in text.splitlines():
        # Further split by comma within each line.
        for item in segment.split(","):
            item = item.strip()
            if item:
                parts.append(item)
    return parts


# ---------------------------------------------------------------------------
# Main infobox parser
# ---------------------------------------------------------------------------

def _parse_infobox(html: str) -> dict:
    """
    Parse the film infobox from a rendered Wikipedia page HTML string.

    Wikipedia film infoboxes are HTML <table> elements whose class list
    contains "infobox".  Each row has a <th> label and a <td> value.
    We build a label → value map and pick out the three fields we need.

    Args:
        html: Full HTML string of the Wikipedia page body.

    Returns:
        Dict with keys runtime (int|None), production_company (str|None),
        language (str|None).
    """
    result: dict = dict(_EMPTY)   # start with all-None copy

    soup = BeautifulSoup(html, "html.parser")

    # Locate the infobox table (class contains "infobox").
    infobox: Optional[Tag] = soup.find(
        "table",
        class_=lambda c: c and "infobox" in c.split(),
    )
    if infobox is None:
        return result   # page exists but has no infobox (e.g. redirect target)

    for row in infobox.find_all("tr"):
        th: Optional[Tag] = row.find("th")
        td: Optional[Tag] = row.find("td")
        if not th or not td:
            continue

        label = th.get_text(separator=" ", strip=True).lower()
        value = _clean_cell(td)

        # ── Runtime ──────────────────────────────────────────────────────────
        if result["runtime"] is None and label in _RUNTIME_LABELS:
            # Use only the first line (some pages add clarifications after).
            first_line = value.splitlines()[0] if value else ""
            parsed = _parse_runtime_text(first_line)
            if parsed is None and value:
                # Try parsing the whole text in case there's no newline.
                parsed = _parse_runtime_text(value)
            result["runtime"] = parsed

        # ── Production company ───────────────────────────────────────────────
        elif result["production_company"] is None and label in _PRODUCTION_LABELS:
            companies = _split_list_value(value)
            result["production_company"] = _first_non_empty(companies)

        # ── Language ─────────────────────────────────────────────────────────
        elif result["language"] is None and label in _LANGUAGE_LABELS:
            langs = _split_list_value(value)
            result["language"] = _first_non_empty(langs)

        # Stop early once all three fields are populated.
        if all(v is not None for v in result.values()):
            break

    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_movie_metadata(title: str) -> dict:
    """
    Fetch Wikipedia metadata for a single film.

    Makes at most two HTTP calls (search + parse), each subject to the
    REQUEST_DELAY rate limit.  Cached responses are served from disk
    instantly without sleeping.  Returns a dict with None values on any
    failure so that callers never need to guard against exceptions.

    Args:
        title: Film title as stored in the movies table, e.g. "Pushpa: The Rise".

    Returns:
        {
            "runtime":            179,                    # int minutes, or None
            "production_company": "Mythri Movie Makers",  # str or None
            "language":           "Telugu",               # str or None
        }

    Example:
        >>> meta = fetch_movie_metadata("Pushpa: The Rise")
        >>> meta["runtime"]
        179
        >>> meta["language"]
        'Telugu'
    """
    # Step 1 — Find the Wikipedia page.
    page_id = _search_page_id(title)
    if page_id is None:
        return dict(_EMPTY)

    # Step 2 — Fetch and parse its infobox.
    html = _fetch_page_html(page_id)
    if html is None:
        return dict(_EMPTY)

    return _parse_infobox(html)


# ---------------------------------------------------------------------------
# Standalone test entry-point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    _title = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "Pushpa: The Rise"
    print(f"Fetching metadata for: {_title!r}")
    _meta = fetch_movie_metadata(_title)
    print(json.dumps(_meta, indent=2, ensure_ascii=False))
