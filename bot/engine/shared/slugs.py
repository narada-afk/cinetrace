"""Actor name → frontend slug ('Jr. NTR' → 'jr-ntr'). Mirrors bot/screenshot.py::actor_slug."""

import re


def actor_slug(name: str) -> str:
    s = name.lower()
    s = re.sub(r"[^a-z0-9\s]", "", s)
    return re.sub(r"\s+", "-", s.strip())
