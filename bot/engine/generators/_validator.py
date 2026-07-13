"""
Output validator — the anti-hallucination gate.

Two layers:

1. PRESENCE — every number in generated copy must exist in the insight
   payload (formatting-tolerant: "1,200" == 1200), or be derivable year
   arithmetic (gap/span between payload years, current year).

2. SEMANTICS — a number paired with a unit word must belong to the matching
   semantic class. "44 films" is valid only if 44 is a film-count in the
   payload; "1982 years" is invalid when 1982 is a calendar year, even
   though the number itself is present. This catches the year-as-duration
   misuse class of hallucination ("vanished for 1982 years").
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from engine.models import Insight

# Canonical semantic class per metric unit string used by discovery rules
_UNIT_TO_CLASS = {
    "films": "count_films", "film": "count_films",
    "years": "duration_years",
    "year": "calendar_year",
    "co-stars": "count_costars",
    "languages": "count_languages",
    "industries": "count_industries",
    "directors": "count_directors",
    "hops": "count_hops",
    "%": "percent",
    "₹cr": "money_cr", "₹Cr": "money_cr", "cr": "money_cr",
}

# Unit words as they appear in generated text → semantic class they demand
_TEXT_UNIT_WORDS = {
    "year": "duration_years", "years": "duration_years",
    "yr": "duration_years", "yrs": "duration_years",
    "decade": "duration_decades", "decades": "duration_decades",
    "film": "count_films", "films": "count_films",
    "movie": "count_films", "movies": "count_films",
    "co-star": "count_costars", "co-stars": "count_costars",
    "costar": "count_costars", "costars": "count_costars",
    "language": "count_languages", "languages": "count_languages",
    "industry": "count_industries", "industries": "count_industries",
    "director": "count_directors", "directors": "count_directors",
    "crore": "money_cr", "crores": "count_films_or_money",  # see check below
    "cr": "money_cr",
    "hop": "count_hops", "hops": "count_hops",
}

def _class_for_fact_key(key: str) -> str | None:
    """Classify a numeric fact by its key name.

    Suffix-sensitive: '..._year' is a calendar year (last_film_year,
    comeback_year), but '..._years' is a duration (window_years, gap_years)
    and must NOT be treated as a year — otherwise duration arithmetic across
    it produces bogus allowed values.
    """
    low = key.lower()
    if low.endswith("_years") or low == "years":
        return "duration_years"
    if low.endswith("_year") or low == "year":
        return "calendar_year"
    if "pct" in low or "percent" in low:
        return "percent"
    if low.endswith("_cr") or "gross_cr" in low or "box_office" in low:
        return "money_cr"
    if "language" in low:
        return "count_languages"
    if "industr" in low:
        return "count_industries"
    if "director" in low:
        return "count_directors"
    if "film" in low:
        return "count_films"
    return None


def _collect_numbers(obj: Any, out: set[float]) -> None:
    if isinstance(obj, bool):
        return
    if isinstance(obj, (int, float)):
        out.add(float(obj))
    elif isinstance(obj, str):
        for m in re.findall(r"\d+(?:\.\d+)?", obj):
            out.add(float(m))
    elif isinstance(obj, dict):
        for v in obj.values():
            _collect_numbers(v, out)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            _collect_numbers(v, out)


def semantic_classes(insight: Insight) -> dict[str, set[float]]:
    """Map semantic class → set of payload values belonging to it."""
    classes: dict[str, set[float]] = {}

    def add(cls: str, value: float | int) -> None:
        classes.setdefault(cls, set()).add(float(value))

    for m in insight.metrics:
        cls = _UNIT_TO_CLASS.get((m.unit or "").strip())
        if cls is None and m.key.endswith("_year"):
            cls = "calendar_year"
        if cls:
            add(cls, m.value)
        if m.period:
            add("calendar_year", m.period[0])
            add("calendar_year", m.period[1])
            span = abs(m.period[1] - m.period[0])
            add("duration_years", span)
            add("duration_years", span + 1)   # inclusive count

    for k, v in insight.facts.items():
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            cls = _class_for_fact_key(k)
            if cls:
                add(cls, v)

    # Durations derivable from any two calendar years in the payload
    years = sorted(classes.get("calendar_year", set()))
    for i, a in enumerate(years):
        for b in years[i + 1:]:
            add("duration_years", b - a)
            add("duration_years", b - a + 1)

    return classes


def allowed_numbers(insight: Insight) -> set[float]:
    nums: set[float] = set()
    _collect_numbers(insight.model_dump(exclude={"discovered_at"}), nums)
    years = {n for n in nums if 1900 <= n <= 2100}
    for a in years:
        for b in years:
            if a > b:
                nums.add(a - b)
                nums.add(a - b + 1)
    nums.add(float(datetime.now().year))
    return nums


_NUM_RE = re.compile(r"\d[\d,]*(?:\.\d+)?")
# number followed by a unit word ("44 films", "12-year gap", "1982 years")
_NUM_UNIT_RE = re.compile(
    r"(\d[\d,]*(?:\.\d+)?)[\s -]*"
    r"(years?|yrs?|decades?|films?|movies?|co-?stars?|costars?|"
    r"languages?|industr(?:y|ies)|directors?|crores?|cr\b|hops?)",
    re.IGNORECASE,
)


def extract_numbers(text: str) -> list[float]:
    out = []
    for m in _NUM_RE.findall(text):
        try:
            out.append(float(m.replace(",", "")))
        except ValueError:
            pass
    return out


def _check_semantics(text: str, insight: Insight) -> list[str]:
    classes = semantic_classes(insight)
    violations = []
    for raw, unit_word in _NUM_UNIT_RE.findall(text):
        n = float(raw.replace(",", ""))
        cls = _TEXT_UNIT_WORDS.get(unit_word.lower().rstrip("."))
        if cls is None:
            continue

        if cls == "duration_decades":
            ok = any(abs(n * 10 - d) <= 1 for d in classes.get("duration_years", ()))
            if not ok:
                violations.append(
                    f'"{raw} {unit_word}" — no {int(n * 10)}-year duration in the data')
            continue

        if cls == "count_films_or_money":   # "crores" ambiguity
            ok = n in classes.get("money_cr", set()) or n in classes.get("count_films", set())
            if not ok:
                violations.append(f'"{raw} {unit_word}" — value not a ₹Cr figure in the data')
            continue

        allowed = classes.get(cls, set())
        if n not in allowed:
            hint = ""
            if cls == "duration_years" and n in classes.get("calendar_year", set()):
                hint = f" ({raw} is a calendar year in the data, not a duration)"
            violations.append(
                f'"{raw} {unit_word}" — {n:g} is not a valid {cls.replace("_", " ")}'
                f" in the insight data{hint}")
    return violations


def validate(text: str, insight: Insight) -> tuple[bool, list[str]]:
    """Returns (ok, violations): presence check + semantic unit check +
    primary-entity name check."""
    allowed = allowed_numbers(insight)
    violations = []
    for n in extract_numbers(text):
        if n not in allowed:
            violations.append(f"number {n:g} not present in insight data")

    violations += _check_semantics(text, insight)

    if insight.entities and insight.primary_entity.name not in text:
        violations.append(
            f"primary entity name '{insight.primary_entity.name}' missing from output"
        )
    return (not violations, violations)
