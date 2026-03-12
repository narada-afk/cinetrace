"""
audit_tmdb_posters.py
=====================
Audits all movies that already have a tmdb_id and flags ones where the
stored ID points to the wrong film (e.g. a foreign film with a matching
English title instead of the correct South Indian film).

Root cause of wrong posters
---------------------------
enrich_tmdb_movies.py searches by title + year with NO language filter.
For common titles that overlap with foreign films (e.g. "Happy" matched a
Japanese TV movie, "Arya" matched a WWII drama), TMDB's popularity ranking
puts the wrong film first.

How this audit works
--------------------
Two-phase approach — accurate because it checks facts, not search rankings:

  Phase 1 — Verify the stored ID
      Call GET /movie/{tmdb_id} and read the actual ``original_language``
      field.  If it doesn't match the expected language for the industry
      (te/ta/ml/kn/hi), the stored ID is CONFIRMED wrong.

  Phase 2 — Find the correct ID  (only for confirmed bad IDs)
      Search TMDB by title + year (no API-side language filter), fetch up
      to the first page of results, then filter *client-side* by
      ``original_language == lang_code``.  Take the first match.

      Note: ``with_original_language`` is a /discover/movie parameter and is
      silently ignored by /search/movie — client-side filtering is the only
      reliable way to restrict by language in the search endpoint.

Industry → TMDB original_language mapping
-----------------------------------------
    Telugu    → te
    Tamil     → ta
    Malayalam → ml
    Kannada   → kn
    Hindi     → hi

Outcome codes per movie
-----------------------
    ✓ OK           — stored tmdb_id has the correct original_language
    ✗ MISMATCH     — stored ID has wrong language; correct ID found
    ✗ WRONG/UNFIXABLE — stored ID has wrong language; no replacement found
    ? Unverifiable — couldn't fetch stored ID details (API error)
    ~ No mapping   — industry has no language code (skipped)

Usage (safe by default — no DB writes unless --fix is passed)
-------------------------------------------------------------
    # From backend/ directory:
    python -m data_pipeline.audit_tmdb_posters
    python -m data_pipeline.audit_tmdb_posters --industry Telugu
    python -m data_pipeline.audit_tmdb_posters --industry Telugu --fix
    python -m data_pipeline.audit_tmdb_posters --fix --report-csv fixes.csv
    python -m data_pipeline.audit_tmdb_posters --batch-size 50 --verbose

Flags
-----
    --fix              Apply corrections: update tmdb_id + poster/backdrop/ratings.
    --industry X       Restrict to one industry (Telugu/Tamil/Malayalam/Kannada/Hindi).
    --batch-size N     Process at most N movies per run (default: 0 = unlimited).
    --verbose          Print a status line for every movie (default: mismatches only).
    --report-csv PATH  Write mismatch rows to a CSV file for offline review.

Environment
-----------
    DATABASE_URL   PostgreSQL DSN (default: postgresql://sca:sca@postgres:5432/sca)
    TMDB_API_KEY   Your TMDB v3 API key (required)

Estimated runtime
-----------------
    All ~1 700 movies: ~7–8 min (1 API call/movie for OK; 2–3 for bad ones).
    Single industry:   ~2–3 min.
"""

import argparse
import csv
import os
import sys
import time
from typing import Optional

# ---------------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------------
_BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _BACKEND_DIR not in sys.path:
    sys.path.insert(0, _BACKEND_DIR)

from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Movie

# Reuse the rate-limited session from tmdb_client.
from data_pipeline.tmdb_client import (  # noqa: F401
    _api_get,
    _SEARCH_URL,
    _get_api_key,
    _build_image_url,
    _POSTER_BASE_URL,
    _BACKDROP_BASE_URL,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

INDUSTRY_LANG: dict[str, str] = {
    "Telugu":    "te",
    "Tamil":     "ta",
    "Malayalam": "ml",
    "Kannada":   "kn",
    "Hindi":     "hi",
}

_MOVIE_DETAIL_URL = "https://api.themoviedb.org/3/movie/{tmdb_id}"

_SEP_THIN = "-" * 64
_SEP_BOLD = "=" * 64

_CSV_FIELDS = [
    "db_id", "title", "release_year", "industry", "lang_code",
    "stored_tmdb_id", "stored_lang",
    "correct_tmdb_id", "tmdb_title_en", "tmdb_original_title", "tmdb_release_date",
    "old_poster_url", "new_poster_url",
    "status",   # mismatch | fixed | fix_failed | wrong_unfixable
]


# ---------------------------------------------------------------------------
# Phase 1 — Verify the stored tmdb_id
# ---------------------------------------------------------------------------

def _verify_stored_id(tmdb_id: int) -> Optional[dict]:
    """
    Fetch movie detail for the stored tmdb_id and return its metadata.

    Returns dict with: original_language, title_en, original_title,
    release_date.  Returns None on API error.
    """
    api_key = _get_api_key()
    url = _MOVIE_DETAIL_URL.format(tmdb_id=tmdb_id)
    try:
        data = _api_get(url, {"api_key": api_key, "language": "en-US"})
    except Exception:
        return None

    return {
        "original_language": data.get("original_language", ""),
        "title_en":          data.get("title", ""),
        "original_title":    data.get("original_title", ""),
        "release_date":      data.get("release_date", ""),
    }


# ---------------------------------------------------------------------------
# Phase 2 — Find the correct ID (client-side language filtering)
# ---------------------------------------------------------------------------

def _find_correct_id(title: str, year: int, lang_code: str) -> Optional[dict]:
    """
    Search TMDB by title + year, then filter results *client-side* by
    original_language == lang_code.

    This is the correct way to restrict by language in /search/movie.
    The ``with_original_language`` query param is a /discover/movie feature
    and is silently ignored by /search/movie.

    Returns dict with tmdb_id, poster/backdrop URLs, ratings, titles.
    Returns None if no language-matching result is found.
    """
    api_key = _get_api_key()

    base: dict = {
        "api_key":       api_key,
        "query":         title,
        "language":      "en-US",
        "page":          1,
        "include_adult": False,
    }

    attempts: list[dict] = []
    if year and year > 0:
        attempts.append({**base, "primary_release_year": year})
    attempts.append(base)   # year-free fallback

    for params in attempts:
        try:
            data = _api_get(_SEARCH_URL, params)
        except Exception:
            return None

        results = data.get("results") or []

        # Client-side filter: keep only films whose original_language matches
        matching = [r for r in results if r.get("original_language") == lang_code]

        if matching:
            best = matching[0]
            tmdb_id = best.get("id")
            if not tmdb_id:
                continue
            return {
                "tmdb_id":        tmdb_id,
                "poster_url":     _build_image_url(_POSTER_BASE_URL,   best.get("poster_path")),
                "backdrop_url":   _build_image_url(_BACKDROP_BASE_URL, best.get("backdrop_path")),
                "vote_average":   best.get("vote_average"),
                "popularity":     best.get("popularity"),
                "title_en":       best.get("title", ""),
                "original_title": best.get("original_title", ""),
                "release_date":   best.get("release_date", ""),
            }

    return None


# ---------------------------------------------------------------------------
# DB fix helper
# ---------------------------------------------------------------------------

def _apply_fix(movie_id: int, new: dict) -> tuple[bool, str]:
    """
    Overwrite tmdb_id + poster/backdrop/ratings for one movie row.
    Returns (success: bool, error_message: str).
    """
    try:
        db: Session = SessionLocal()
        try:
            m = db.query(Movie).filter(Movie.id == movie_id).first()
            if not m:
                return False, f"Movie id={movie_id} not found in DB"

            m.tmdb_id      = new["tmdb_id"]
            m.poster_url   = new.get("poster_url")
            m.backdrop_url = new.get("backdrop_url")
            if new.get("vote_average") is not None:
                m.vote_average = new["vote_average"]
            if new.get("popularity") is not None:
                m.popularity = new["popularity"]

            db.commit()
            return True, ""
        finally:
            db.close()
    except Exception as exc:
        return False, str(exc)


# ---------------------------------------------------------------------------
# Console formatting
# ---------------------------------------------------------------------------

def _trunc(s: str, n: int = 70) -> str:
    return s if len(s) <= n else s[:n - 3] + "..."


def _print_header(total: int, fix: bool, industry: str) -> None:
    mode = "[FIX — DB writes enabled]" if fix else "[REPORT ONLY — no DB writes]"
    print(f"\n{_SEP_BOLD}")
    print(f"  TMDB Poster Audit  {mode}")
    print(f"  Strategy        : verify stored ID language, then find replacement")
    print(f"  Movies to audit : {total}")
    if industry:
        print(f"  Industry filter : {industry}")
    print(f"{_SEP_BOLD}\n")


def _print_mismatch_block(
    idx: int, total: int,
    movie: Movie,
    lang_code: str,
    stored_info: dict,
    new: Optional[dict],
    fix: bool,
    fix_ok: bool,
    fix_err: str,
) -> None:
    width = len(str(total))
    print(f"[{idx:>{width}}/{total}] {movie.title} ({movie.release_year})"
          f"  [{movie.industry}/{lang_code}]")
    print(_SEP_THIN)
    print(f"  ✗ WRONG LANGUAGE STORED")
    print(f"    Stored tmdb_id  : {movie.tmdb_id}"
          f"  (original_language={stored_info['original_language']!r}"
          f", expected={lang_code!r})")
    print(f"    Stored film     : {stored_info['title_en']} / {stored_info['original_title']}"
          f"  ({stored_info['release_date'][:4] if stored_info['release_date'] else '?'})")
    print(f"    TMDB link (bad) : https://www.themoviedb.org/movie/{movie.tmdb_id}")

    if new:
        yr = new['release_date'][:4] if new.get('release_date') else '?'
        print(f"    Correct tmdb_id : {new['tmdb_id']}")
        print(f"    Correct film    : {new['title_en']} / {new['original_title']} ({yr})")
        print(f"    TMDB link (new) : https://www.themoviedb.org/movie/{new['tmdb_id']}")
        print(f"    Old poster_url  : {_trunc(movie.poster_url or '—')}")
        print(f"    New poster_url  : {_trunc(new.get('poster_url') or '—')}")
        if fix:
            verb = "✓ Fixed" if fix_ok else f"✗ Fix failed: {fix_err}"
            print(f"    → {verb}")
        else:
            print(f"    → Run with --fix to apply this correction.")
    else:
        print(f"    ! No {lang_code!r} replacement found on TMDB — manual fix needed.")
    print()


def _print_verbose_line(
    idx: int, total: int,
    movie: Movie,
    lang_code: str,
    status: str,
    detail: str = "",
) -> None:
    width = len(str(total))
    icons  = {"ok": "✓", "unverifiable": "?", "no_map": "~", "error": "!"}
    labels = {
        "ok":           "OK",
        "unverifiable": f"Unverifiable (API error fetching tmdb_id={movie.tmdb_id})",
        "no_map":       f"No language mapping for '{movie.industry}'",
        "error":        f"Error: {detail}",
    }
    icon  = icons.get(status, "?")
    label = labels.get(status, status)
    tag   = f"[{movie.industry}/{lang_code}]" if lang_code else f"[{movie.industry}]"
    print(f"[{idx:>{width}}/{total}] {movie.title} ({movie.release_year})"
          f"  {tag}  {icon} {label}")


def _print_progress(idx: int, total: int) -> None:
    print(f"  … {idx}/{total} ({idx / total * 100:.0f}%)", flush=True)


def _print_summary(
    processed: int,
    n_ok: int,
    n_mismatch: int,
    n_unfixable: int,
    n_fixed: int,
    n_fix_failed: int,
    n_unverifiable: int,
    n_errors: int,
    elapsed: float,
    fix: bool,
) -> None:
    print(f"\n{_SEP_BOLD}")
    print(f"  Audit Summary{'  [FIX MODE]' if fix else '  [REPORT ONLY]'}")
    print(_SEP_THIN)
    print(f"  Processed          : {processed}")
    print(f"  ✓ OK               : {n_ok}")
    print(f"  ✗ Wrong language   : {n_mismatch + n_unfixable}")
    if n_mismatch or n_unfixable:
        print(f"    ↳ Replacement found   : {n_mismatch}")
        print(f"    ↳ No replacement found: {n_unfixable}  (manual fix needed)")
    if fix:
        print(f"  ✓ Fixed            : {n_fixed}")
        print(f"  ✗ Fix failed       : {n_fix_failed}")
    else:
        if n_mismatch:
            print(f"  → Re-run with --fix to correct the {n_mismatch} fixable row(s).")
    print(f"  ? Unverifiable     : {n_unverifiable}  (API error on stored ID)")
    print(f"  ! Errors           : {n_errors}")
    print(f"  Elapsed            : {elapsed:.1f} s")
    print(f"{_SEP_BOLD}\n")


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def _open_csv_writer(path: str):
    if not path:
        return None, None
    try:
        fh = open(path, "w", newline="", encoding="utf-8")
        writer = csv.DictWriter(fh, fieldnames=_CSV_FIELDS)
        writer.writeheader()
        return fh, writer
    except OSError as exc:
        print(f"  ! Cannot open CSV at '{path}': {exc}")
        return None, None


# ---------------------------------------------------------------------------
# Main audit function
# ---------------------------------------------------------------------------

def audit_tmdb_posters(
    batch_size: int = 0,
    fix: bool = False,
    industry: str = "",
    verbose: bool = False,
    report_csv: str = "",
) -> int:
    """
    Audit existing TMDB IDs by verifying their original_language.

    Returns 0 on success, 1 on fatal error.
    """
    t_start = time.monotonic()

    try:
        _get_api_key()
    except RuntimeError as exc:
        print(f"\n✗ {exc}\n")
        return 1

    # ------------------------------------------------------------------
    # Load movies
    # ------------------------------------------------------------------
    db: Session = SessionLocal()
    try:
        q = db.query(Movie).filter(Movie.tmdb_id.isnot(None))
        if industry:
            q = q.filter(Movie.industry == industry)
        q = q.order_by(Movie.release_year.desc(), Movie.title)
        if batch_size > 0:
            q = q.limit(batch_size)
        movies = q.all()
    finally:
        db.close()

    total = len(movies)
    if total == 0:
        print("\n✓ No movies with tmdb_id found — nothing to audit.\n")
        return 0

    _print_header(total=total, fix=fix, industry=industry)

    csv_fh, csv_writer = _open_csv_writer(report_csv)

    # ------------------------------------------------------------------
    # Counters
    # ------------------------------------------------------------------
    n_ok           = 0
    n_mismatch     = 0    # wrong lang, replacement found
    n_unfixable    = 0    # wrong lang, no replacement found
    n_fixed        = 0
    n_fix_failed   = 0
    n_unverifiable = 0    # couldn't fetch stored ID details
    n_errors       = 0

    width = len(str(total))

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    for idx, movie in enumerate(movies, start=1):

        if not verbose and idx % 50 == 0:
            _print_progress(idx, total)

        # Skip if no language mapping for this industry
        lang_code = INDUSTRY_LANG.get(movie.industry or "", "")
        if not lang_code:
            if verbose:
                _print_verbose_line(idx, total, movie, "", "no_map")
            n_unverifiable += 1
            continue

        # ----------------------------------------------------------
        # Phase 1: verify the stored tmdb_id
        # ----------------------------------------------------------
        try:
            stored_info = _verify_stored_id(movie.tmdb_id)
        except RuntimeError:
            print("\n✗ TMDB_API_KEY missing mid-run. Aborting.\n")
            if csv_fh:
                csv_fh.close()
            return 1
        except Exception as exc:
            if verbose:
                _print_verbose_line(idx, total, movie, lang_code, "error", str(exc))
            n_errors += 1
            continue

        if stored_info is None:
            # API error — can't verify
            if verbose:
                _print_verbose_line(idx, total, movie, lang_code, "unverifiable")
            n_unverifiable += 1
            continue

        stored_lang = stored_info.get("original_language", "")

        if stored_lang == lang_code:
            # ✓ Stored ID has the correct language — all good
            if verbose:
                _print_verbose_line(idx, total, movie, lang_code, "ok")
            n_ok += 1
            continue

        # ----------------------------------------------------------
        # Phase 2: stored ID has wrong language — find replacement
        # ----------------------------------------------------------
        try:
            new = _find_correct_id(movie.title, movie.release_year or 0, lang_code)
        except Exception as exc:
            if verbose:
                _print_verbose_line(idx, total, movie, lang_code, "error", str(exc))
            n_errors += 1
            continue

        if new is None:
            n_unfixable += 1
        else:
            n_mismatch += 1

        fix_ok  = False
        fix_err = ""
        if fix and new is not None:
            fix_ok, fix_err = _apply_fix(movie.id, new)
            if fix_ok:
                n_fixed += 1
            else:
                n_fix_failed += 1

        _print_mismatch_block(
            idx=idx, total=total,
            movie=movie,
            lang_code=lang_code,
            stored_info=stored_info,
            new=new,
            fix=fix,
            fix_ok=fix_ok,
            fix_err=fix_err,
        )

        if csv_writer:
            status_str = (
                "fixed"          if (fix and fix_ok)
                else "fix_failed"    if (fix and not fix_ok and new)
                else "wrong_unfixable" if new is None
                else "mismatch"
            )
            csv_writer.writerow({
                "db_id":              movie.id,
                "title":              movie.title,
                "release_year":       movie.release_year,
                "industry":           movie.industry,
                "lang_code":          lang_code,
                "stored_tmdb_id":     movie.tmdb_id,
                "stored_lang":        stored_lang,
                "correct_tmdb_id":    new["tmdb_id"] if new else "",
                "tmdb_title_en":      new.get("title_en", "") if new else "",
                "tmdb_original_title": new.get("original_title", "") if new else "",
                "tmdb_release_date":  new.get("release_date", "") if new else "",
                "old_poster_url":     movie.poster_url or "",
                "new_poster_url":     new.get("poster_url") or "" if new else "",
                "status":             status_str,
            })

    # Final progress tick
    if not verbose:
        _print_progress(total, total)

    if csv_fh:
        csv_fh.close()
        if n_mismatch or n_unfixable:
            print(f"\n  📄 Report saved → {report_csv}")

    elapsed = time.monotonic() - t_start

    _print_summary(
        processed=total,
        n_ok=n_ok,
        n_mismatch=n_mismatch,
        n_unfixable=n_unfixable,
        n_fixed=n_fixed,
        n_fix_failed=n_fix_failed,
        n_unverifiable=n_unverifiable,
        n_errors=n_errors,
        elapsed=elapsed,
        fix=fix,
    )

    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Audit TMDB poster matches for all South Cinema Analytics movies.\n"
            "Verifies each stored tmdb_id by checking its actual original_language,\n"
            "then searches for a correct replacement if the language is wrong."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  # Report-only scan of all Telugu movies:\n"
            "  python -m data_pipeline.audit_tmdb_posters --industry Telugu\n\n"
            "  # Full audit, verbose, save report:\n"
            "  python -m data_pipeline.audit_tmdb_posters --verbose"
            " --report-csv audit.csv\n\n"
            "  # Find & fix all mismatches:\n"
            "  python -m data_pipeline.audit_tmdb_posters --fix"
            " --report-csv fixes.csv\n\n"
            "  # Quick 50-movie test:\n"
            "  python -m data_pipeline.audit_tmdb_posters --batch-size 50 --verbose\n"
        ),
    )
    p.add_argument(
        "--fix",
        action="store_true",
        help="Write corrections to the database (default: report only).",
    )
    p.add_argument(
        "--batch-size", "-n",
        type=int, default=0, metavar="N",
        help="Process at most N movies (default: 0 = no limit).",
    )
    p.add_argument(
        "--industry",
        type=str, default="", metavar="INDUSTRY",
        help="Restrict to one industry: Telugu, Tamil, Malayalam, Kannada, Hindi.",
    )
    p.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print a line for every movie, not just mismatches.",
    )
    p.add_argument(
        "--report-csv",
        type=str, default="", metavar="PATH",
        help="Write mismatch rows to a CSV file.",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    sys.exit(
        audit_tmdb_posters(
            batch_size=args.batch_size,
            fix=args.fix,
            industry=args.industry,
            verbose=args.verbose,
            report_csv=args.report_csv,
        )
    )
