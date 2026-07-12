"""
Output validator — the anti-hallucination gate.

Every number in generated copy must exist in the insight payload (with
formatting tolerance: "1,200" == 1200, "44" == 44.0). Derived values the
LLM may legitimately compute are whitelisted: differences and sums of
payload year-pairs (e.g. "12-year gap" from period (2001, 2013)) and the
current year.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from engine.models import Insight


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


def allowed_numbers(insight: Insight) -> set[float]:
    nums: set[float] = set()
    _collect_numbers(insight.model_dump(exclude={"discovered_at"}), nums)

    # Derived values: year arithmetic on periods and year-valued numbers
    years = {n for n in nums if 1900 <= n <= 2100}
    for a in years:
        for b in years:
            if a > b:
                nums.add(a - b)          # gap/span
                nums.add(a - b + 1)      # inclusive span
    nums.add(float(datetime.now().year))
    return nums


_NUM_RE = re.compile(r"\d[\d,]*(?:\.\d+)?")


def extract_numbers(text: str) -> list[float]:
    out = []
    for m in _NUM_RE.findall(text):
        try:
            out.append(float(m.replace(",", "")))
        except ValueError:
            pass
    return out


def validate(text: str, insight: Insight) -> tuple[bool, list[str]]:
    """Returns (ok, violations). Every numeral in text must be an allowed number."""
    allowed = allowed_numbers(insight)
    violations = []
    for n in extract_numbers(text):
        if n not in allowed:
            violations.append(f"number {n:g} not present in insight data")

    # Entity names must not be mangled: any capitalised multi-char token
    # sequence check is too brittle; instead require the primary entity's
    # name to appear verbatim (it always should).
    if insight.entities and insight.primary_entity.name not in text:
        violations.append(
            f"primary entity name '{insight.primary_entity.name}' missing from output"
        )
    return (not violations, violations)
