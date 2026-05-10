"""
screenshot.py
=============
Captures actor page sections as PNGs for tweet attachments.

Section → page strategy:
  "directors"    → actor page "Directors Worked With" heading, 600px window
  "blockbusters" → actor page "Blockbusters" heading, 600px window
  "collaborators"→ actor page "By the Numbers" (insight cards)
  "overview"     → actor page "By the Numbers" (insight cards)
  "filmography"  → actor page "By the Numbers" (All Films has unloaded images)
  "compare"      → /compare/{slug}-vs-{compare_with} page, above-fold
  fallback       → actor page hero above-fold (670px)
"""

import re
import asyncio
from playwright.async_api import async_playwright
from config import CINETRACE_SCREENSHOT_URL

# Maps section → H2 heading text to scroll to on the actor page
_SECTION_HEADINGS = {
    "directors":     "Directors Worked With",
    "blockbusters":  "Blockbusters",
    # These all show the "By the Numbers" insight cards — best visual for data tweets
    "collaborators": "By the Numbers",
    "overview":      "By the Numbers",
    "filmography":   "By the Numbers",
}

# Fallback height when section bounds can't be calculated
_SECTION_HEIGHT = 500

_FIND_HEADING_JS = """(heading) => {
    const hs = Array.from(document.querySelectorAll("h2"));
    const idx = hs.findIndex(el => el.textContent.includes(heading));
    if (idx < 0) return null;
    const h = hs[idx];
    const next = hs[idx + 1];
    const r = h.getBoundingClientRect();
    const sectionTop = r.top + window.scrollY;
    // Height = distance to next heading, capped at 650px, minimum 200px
    const sectionH = next
        ? Math.min(Math.max(next.getBoundingClientRect().top + window.scrollY - sectionTop, 200), 650)
        : 500;
    return {y: sectionTop, x: r.x, w: r.width, h: sectionH};
}"""


async def capture_section_snapshot(slug: str, section: str,
                                   compare_with: str = "") -> bytes | None:
    """
    Returns PNG bytes for the given actor + section.
    Falls back to actor hero if the section heading isn't found.

    compare_with: second actor slug, required when section == "compare".
    """
    # ── Compare page ──────────────────────────────────────────────────────────
    if section == "compare" and compare_with:
        return await _capture_compare(slug, compare_with)

    # ── Actor page sections ───────────────────────────────────────────────────
    url = f"{CINETRACE_SCREENSHOT_URL}/actors/{slug}"
    heading_text = _SECTION_HEADINGS.get(section)

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox"],
            )
            page = await browser.new_page(
                viewport={"width": 1280, "height": 900},
                color_scheme="dark",
            )
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(4000)

            if heading_text:
                pos = await page.evaluate(_FIND_HEADING_JS, heading_text)
                if pos:
                    await page.evaluate("(y) => window.scrollTo(0, y)",
                                        max(0, pos["y"] - 12))
                    await page.wait_for_timeout(400)
                    section_h = int(pos.get("h", _SECTION_HEIGHT))
                    png = await page.screenshot(
                        type="png",
                        clip={
                            "x":      max(0, pos["x"] - 16),
                            "y":      0,
                            "width":  min(pos["w"] + 32, 1280),
                            "height": section_h,
                        },
                    )
                    await browser.close()
                    return png
                else:
                    print(f"[screenshot] '{heading_text}' not found for {slug}, "
                          f"falling back to hero")

            return await _hero_fallback(page, browser)

    except Exception as e:
        print(f"[screenshot] failed for {slug}/{section}: {e}")
        return None


async def _capture_compare(slug1: str, slug2: str) -> bytes | None:
    """Screenshot the /compare/{slug1}-vs-{slug2} page above-fold."""
    url = f"{CINETRACE_SCREENSHOT_URL}/compare/{slug1}-vs-{slug2}"
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox"],
            )
            page = await browser.new_page(
                viewport={"width": 1280, "height": 800},
                color_scheme="dark",
            )
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(4000)
            png = await page.screenshot(
                type="png",
                clip={"x": 0, "y": 0, "width": 1280, "height": 700},
            )
            await browser.close()
            return png
    except Exception as e:
        print(f"[screenshot] compare failed for {slug1}-vs-{slug2}: {e}")
        return None


async def _hero_fallback(page, browser) -> bytes | None:
    """Screenshot the actor hero / above-fold area."""
    try:
        png = await page.screenshot(
            type="png",
            clip={"x": 0, "y": 0, "width": 1280, "height": 670},
        )
        await browser.close()
        return png
    except Exception as e:
        print(f"[screenshot] fallback failed: {e}")
        return None


def actor_slug(db_name: str) -> str:
    """'Jr. NTR' → 'jr-ntr', 'Ram Charan' → 'ram-charan'"""
    s = db_name.lower()
    s = re.sub(r'[^a-z0-9\s]', '', s)
    s = re.sub(r'\s+', '-', s.strip())
    return s
