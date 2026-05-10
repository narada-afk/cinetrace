#!/usr/bin/env python3
"""
test_screenshots.py
===================
Validates all screenshot section types by capturing a real PNG for each
and saving them to /tmp/ss_test/. Run inside the bot container after deploy.

Usage:
    python test_screenshots.py

Output:
    /tmp/ss_test/<actor>_<section>.png  — inspect these visually
    Summary table printed to stdout
"""

import asyncio
import os
import sys
from pathlib import Path

import screenshot as ss

# ── Test cases ────────────────────────────────────────────────────────────────
# (label, slug, section, compare_with)
TEST_CASES = [
    # Section-heading based
    ("Directors",        "mohanlal",       "directors",     ""),
    ("Blockbusters",     "rajinikanth",    "blockbusters",  ""),
    ("By the Numbers",   "prabhas",        "overview",      ""),
    ("Collaborators",    "mahesh-babu",    "collaborators", ""),

    # Career chart (client-rendered, needs frontend deployed)
    ("Career chart",     "kamal-haasan",   "career",        ""),
    ("Filmography→chart","mammootty",      "filmography",   ""),

    # Compare page chart
    ("Compare chart",    "kamal-haasan",   "compare",       "rajinikanth"),
    ("Compare chart 2",  "prabhas",        "compare",       "mahesh-babu"),

    # Fallback hero
    ("Hero fallback",    "vijay",          "unknown",       ""),
]

OUT_DIR = Path("/tmp/ss_test")


async def run_tests():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    results = []
    for label, slug, section, compare_with in TEST_CASES:
        print(f"  → {label} ({slug}/{section}) … ", end="", flush=True)
        try:
            png = await ss.capture_section_snapshot(slug, section, compare_with=compare_with)
            if png:
                fname = OUT_DIR / f"{slug}_{section}.png"
                fname.write_bytes(png)
                kb    = len(png) // 1024
                # Decode PNG header to get dimensions
                import struct
                w = struct.unpack(">I", png[16:20])[0]
                h = struct.unpack(">I", png[20:24])[0]
                status = f"✅  {w}×{h}px  {kb}KB  → {fname.name}"
            else:
                status = "❌  returned None"
        except Exception as e:
            status = f"💥  {e}"

        print(status)
        results.append((label, slug, section, status))

    print("\n── Summary ──────────────────────────────────────────────────────")
    passed = sum(1 for *_, s in results if s.startswith("✅"))
    failed = len(results) - passed
    for label, slug, section, status in results:
        mark = "✅" if status.startswith("✅") else "❌"
        print(f"  {mark}  {label:<20}  {slug}/{section}")
    print(f"\n  {passed}/{len(results)} passed", "🎉" if failed == 0 else "— check failures above")

    if failed:
        print("\n  To inspect visually, copy PNGs off the container:")
        print(f"  docker cp <container>:{OUT_DIR} ./ss_test_output/")
        sys.exit(1)


if __name__ == "__main__":
    print(f"\nScreenshot test — saving PNGs to {OUT_DIR}\n")
    asyncio.run(run_tests())
