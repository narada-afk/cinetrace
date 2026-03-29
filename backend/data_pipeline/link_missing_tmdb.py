"""
link_missing_tmdb.py
====================
For movies with no tmdb_id, tries two sources in order:

  1. TMDB title search  — links tmdb_id + fetches primary cast
  2. Wikipedia API      — fetches cast from film's Wikipedia infobox as fallback

Outcome per movie:
  A) TMDB match found   → update movies.tmdb_id, insert primary cast
  B) Wikipedia cast found (no TMDB) → insert cast actors, mark source='wikipedia'
  C) Neither found      → keep existing confirmed_none placeholder, log for manual review

Usage
-----
    cd backend
    export TMDB_API_KEY=your_key
    python -m data_pipeline.link_missing_tmdb             # full run
    python -m data_pipeline.link_missing_tmdb --dry-run   # no DB writes
"""

from __future__ import annotations

import logging
import re
import sys
import time
from typing import Optional

import requests
from sqlalchemy import text
from sqlalchemy.orm import Session

from data_pipeline.tmdb_client import (
    _TMDB_BASE, _api_get, _get_api_key, search_movie_tmdb,
)

logger = logging.getLogger(__name__)

RATE_LIMIT_S     = 0.26
WIKI_API_URL     = "https://en.wikipedia.org/api/rest_v1/page/summary/{}"
WIKI_SEARCH_URL  = "https://en.wikipedia.org/w/api.php"
WIKI_PARSE_URL   = "https://en.wikipedia.org/w/api.php"


# ─── TMDB helpers ─────────────────────────────────────────────────────────────

def _fetch_tmdb_primary_cast(tmdb_id: int, api_key: str) -> list[dict]:
    """Fetch billing positions 0-2 from TMDB credits."""
    try:
        data = _api_get(
            f"{_TMDB_BASE}/movie/{tmdb_id}/credits",
            {"api_key": api_key, "language": "en-US"},
        )
    except Exception as exc:
        logger.warning("TMDB credits failed for tmdb_id=%s: %s", tmdb_id, exc)
        return []

    raw = sorted(data.get("cast") or [], key=lambda c: c.get("order", 999))
    return [
        {
            "name":           c["name"].strip(),
            "tmdb_person_id": c["id"],
            "billing_order":  c.get("order", 0),
            "character_name": (c.get("character") or "").strip(),
            "gender":         {1: "F", 2: "M"}.get(c.get("gender")),
        }
        for c in raw[:3]
        if c.get("name") and c.get("id")
    ]


# ─── Wikipedia helpers ────────────────────────────────────────────────────────

def _wiki_search(title: str, year: int) -> Optional[str]:
    """
    Search Wikipedia for a film page. Returns the best-matching page title or None.
    Tries '<title> film' and '<title> (<year> film)' variants.
    """
    queries = [f"{title} ({year} film)", f"{title} film", title]

    for q in queries:
        try:
            resp = requests.get(
                WIKI_SEARCH_URL,
                params={
                    "action":   "query",
                    "list":     "search",
                    "srsearch": q,
                    "srnamespace": 0,
                    "srlimit":  3,
                    "format":   "json",
                },
                timeout=8,
            )
            results = resp.json().get("query", {}).get("search", [])
            for r in results:
                t = r.get("title", "")
                # Only accept pages that look like film articles
                if any(kw in t.lower() for kw in ["film", title.lower()[:6]]):
                    return t
        except Exception as exc:
            logger.warning("Wikipedia search failed for '%s': %s", q, exc)

    return None


def _wiki_cast(page_title: str) -> list[str]:
    """
    Parse the Starring / Cast section of a Wikipedia film infobox.
    Returns a list of actor name strings (plain text, no wiki markup).
    """
    try:
        resp = requests.get(
            WIKI_PARSE_URL,
            params={
                "action":  "parse",
                "page":    page_title,
                "prop":    "wikitext",
                "section": 0,          # Lead + infobox only
                "format":  "json",
            },
            timeout=10,
        )
        wikitext = resp.json().get("parse", {}).get("wikitext", {}).get("*", "")
    except Exception as exc:
        logger.warning("Wikipedia parse failed for '%s': %s", page_title, exc)
        return []

    # Extract starring field from infobox
    match = re.search(
        r'\|\s*starring\s*=\s*(.*?)(?=\n\s*\||\n\s*\}\})',
        wikitext,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return []

    raw = match.group(1)

    # Remove wiki markup: [[Actor Name|display]] → Actor Name
    raw = re.sub(r'\[\[([^|\]]+)\|([^\]]+)\]\]', r'\2', raw)
    raw = re.sub(r'\[\[([^\]]+)\]\]', r'\1', raw)
    # Remove templates {{...}}
    raw = re.sub(r'\{\{[^}]+\}\}', '', raw)
    # Remove HTML tags
    raw = re.sub(r'<[^>]+>', '', raw)
    # Split on newlines, bullets, commas, <br>
    names = re.split(r'[\n,*•]+', raw)
    # Clean each name
    names = [n.strip().strip("'\"") for n in names if n.strip()]
    # Filter out blanks and obvious non-names (< 3 chars or all digits)
    names = [n for n in names if len(n) >= 3 and not n.isdigit()]

    return names[:5]   # cap at top 5


# ─── Actor insert helper ──────────────────────────────────────────────────────

def _get_or_create_actor(
    name: str, industry: str, db: Session,
    tmdb_person_id: Optional[int] = None,
    gender: Optional[str] = None,
) -> Optional[int]:
    """Return actor.id — create minimal record if not present."""
    if tmdb_person_id:
        row = db.execute(
            text("SELECT id FROM actors WHERE tmdb_person_id = :tid"),
            {"tid": tmdb_person_id},
        ).fetchone()
        if row:
            return row.id

    row = db.execute(
        text("SELECT id FROM actors WHERE LOWER(TRIM(name)) = LOWER(TRIM(:n))"),
        {"n": name},
    ).fetchone()
    if row:
        if tmdb_person_id:
            db.execute(
                text("UPDATE actors SET tmdb_person_id = :tid WHERE id = :aid AND tmdb_person_id IS NULL"),
                {"tid": tmdb_person_id, "aid": row.id},
            )
        return row.id

    result = db.execute(
        text("""
            INSERT INTO actors (name, tmdb_person_id, industry, gender)
            VALUES (:name, :tid, :industry, :gender)
            ON CONFLICT DO NOTHING RETURNING id
        """),
        {"name": name.strip(), "tid": tmdb_person_id, "industry": industry, "gender": gender},
    ).fetchone()

    if result:
        return result.id

    row = db.execute(
        text("SELECT id FROM actors WHERE LOWER(TRIM(name)) = LOWER(TRIM(:n))"),
        {"n": name},
    ).fetchone()
    return row.id if row else None


def _insert_primary_cast(
    movie_id: int, cast: list[dict], industry: str,
    db: Session, dry_run: bool,
) -> int:
    """Insert list of cast dicts into actor_movies. Returns count inserted."""
    inserted = 0
    for i, actor in enumerate(cast):
        actor_id = _get_or_create_actor(
            actor["name"], industry, db,
            tmdb_person_id=actor.get("tmdb_person_id"),
            gender=actor.get("gender"),
        )
        if not actor_id:
            continue
        if not dry_run:
            db.execute(
                text("""
                    INSERT INTO actor_movies
                        (actor_id, movie_id, role_type, billing_order, character_name)
                    VALUES (:aid, :mid, 'primary', :order, :char)
                    ON CONFLICT (actor_id, movie_id) DO UPDATE
                        SET role_type      = 'primary',
                            billing_order  = EXCLUDED.billing_order,
                            character_name = EXCLUDED.character_name
                """),
                {
                    "aid":   actor_id,
                    "mid":   movie_id,
                    "order": actor.get("billing_order", i),
                    "char":  actor.get("character_name", ""),
                },
            )
        inserted += 1
    return inserted


def _clear_override(movie_id: int, field: str, db: Session) -> None:
    """Remove a confirmed_none override once we've found real data."""
    db.execute(
        text("""
            UPDATE movie_validation_results
            SET validation_overrides = validation_overrides - :field
            WHERE movie_id = :mid
        """),
        {"field": field, "mid": movie_id},
    )


# ─── Main ─────────────────────────────────────────────────────────────────────

def link_missing_tmdb(db: Session, *, dry_run: bool = False) -> dict:
    """
    Process all movies with no tmdb_id.
    Returns summary dict.
    """
    rows = db.execute(text("""
        SELECT id, title, release_year, industry
        FROM movies
        WHERE tmdb_id IS NULL
        ORDER BY release_year DESC NULLS LAST
    """)).fetchall()

    total        = len(rows)
    tmdb_linked  = 0
    wiki_cast    = 0
    not_found    = 0
    errors       = 0

    api_key = _get_api_key()

    logger.info("[link_missing_tmdb] %d movies to process", total)

    for i, row in enumerate(rows, 1):
        movie_id = row.id
        title    = row.title
        year     = row.release_year or 0
        industry = row.industry

        logger.info("  [%d/%d] '%s' (%s, %s)", i, total, title, industry, year or "?")

        try:
            # ── SOURCE 1: TMDB title search ───────────────────────────────
            tmdb_result = search_movie_tmdb(title, year)
            time.sleep(RATE_LIMIT_S)

            if tmdb_result and tmdb_result.get("tmdb_id"):
                new_tmdb_id = int(tmdb_result["tmdb_id"])
                logger.info("    ✓ TMDB match → tmdb_id=%d", new_tmdb_id)

                if not dry_run:
                    db.execute(
                        text("""
                            UPDATE movies SET
                                tmdb_id      = :tid,
                                poster_url   = COALESCE(poster_url, :poster),
                                backdrop_url = COALESCE(backdrop_url, :backdrop),
                                vote_average = COALESCE(vote_average, :va),
                                popularity   = COALESCE(popularity, :pop)
                            WHERE id = :mid
                        """),
                        {
                            "tid":     new_tmdb_id,
                            "poster":  tmdb_result.get("poster_url"),
                            "backdrop":tmdb_result.get("backdrop_url"),
                            "va":      tmdb_result.get("vote_average"),
                            "pop":     tmdb_result.get("popularity"),
                            "mid":     movie_id,
                        },
                    )

                # Fetch primary cast using new tmdb_id
                cast = _fetch_tmdb_primary_cast(new_tmdb_id, api_key)
                time.sleep(RATE_LIMIT_S)

                if cast:
                    n = _insert_primary_cast(movie_id, cast, industry, db, dry_run)
                    logger.info("    ✓ Inserted %d primary cast actors", n)
                    if not dry_run:
                        _clear_override(movie_id, "primary_cast", db)
                else:
                    logger.info("    ⚠ TMDB linked but no cast found — keeping placeholder")

                tmdb_linked += 1
                if not dry_run:
                    db.commit()
                continue

            # ── SOURCE 2: Wikipedia fallback ──────────────────────────────
            logger.info("    → No TMDB match. Trying Wikipedia...")
            page_title = _wiki_search(title, year)

            if page_title:
                names = _wiki_cast(page_title)
                if names:
                    logger.info("    ✓ Wikipedia cast found: %s", names)
                    cast_dicts = [
                        {"name": n, "billing_order": idx, "character_name": ""}
                        for idx, n in enumerate(names[:3])
                    ]
                    n = _insert_primary_cast(movie_id, cast_dicts, industry, db, dry_run)
                    logger.info("    ✓ Inserted %d actors from Wikipedia", n)
                    if not dry_run:
                        _clear_override(movie_id, "primary_cast", db)
                        db.commit()
                    wiki_cast += 1
                    continue
                else:
                    logger.info("    ⚠ Wikipedia page found ('%s') but no cast parsed", page_title)
            else:
                logger.info("    ✗ No Wikipedia page found")

            # ── Neither source worked ─────────────────────────────────────
            logger.info("    ✗ No data found — keeping confirmed_none placeholder")
            not_found += 1

        except Exception as exc:
            logger.error("  [%d/%d] error for movie %d '%s': %s", i, total, movie_id, title, exc)
            db.rollback()
            errors += 1

    if not dry_run:
        db.commit()

    summary = {
        "total":       total,
        "tmdb_linked": tmdb_linked,
        "wiki_cast":   wiki_cast,
        "not_found":   not_found,
        "errors":      errors,
        "dry_run":     dry_run,
    }
    logger.info(
        "[link_missing_tmdb] Done — tmdb_linked=%d wiki_cast=%d not_found=%d errors=%d",
        tmdb_linked, wiki_cast, not_found, errors,
    )
    return summary


# ─── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    from app.database import SessionLocal

    parser = argparse.ArgumentParser(description="Link missing tmdb_ids via TMDB search + Wikipedia")
    parser.add_argument("--dry-run", action="store_true", help="No DB writes")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )

    db = SessionLocal()
    try:
        summary = link_missing_tmdb(db, dry_run=args.dry_run)
        print("\n" + "─" * 50)
        print(f"  Total           : {summary['total']}")
        print(f"  TMDB linked     : {summary['tmdb_linked']}  (tmdb_id found + cast fetched)")
        print(f"  Wikipedia cast  : {summary['wiki_cast']}   (no TMDB, but wiki cast found)")
        print(f"  Not found       : {summary['not_found']}  (placeholder kept — manual review)")
        print(f"  Errors          : {summary['errors']}")
        print(f"  Dry run         : {summary['dry_run']}")
        print("─" * 50)
    finally:
        db.close()
