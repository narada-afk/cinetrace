"""
screenshot.py
=============
Captures the actor page section as a PNG for attaching to tweets.

Strategy: navigate to the actor page, find the H2 section heading,
scroll to it, then clip a fixed-height window starting from the heading.
This avoids relying on container sizing which varies by data quantity.
"""

import re
import asyncio
from playwright.async_api import async_playwright
from config import CINETRACE_SCREENSHOT_URL

# Maps inventory section → text fragment in the H2 heading on the actor page.
# "collaborators" → "By the Numbers" shows the insight cards (Iconic Pair, Leading Ladies, etc.)
# which is the best visual for connection/co-star tweets.
_SECTION_HEADINGS = {
    "directors":     "Directors Worked With",
    "collaborators": "By the Numbers",
    "blockbusters":  "Blockbusters",
}

# How tall a window to capture below the heading (px).
# 600px covers the tallest section (By the Numbers insight cards ~370px)
# without bleeding into the next section for shorter ones.
_SECTION_HEIGHT = 600


async def capture_section_snapshot(slug: str, section: str) -> bytes | None:
    """
    Returns PNG bytes for the given actor + section.
    Falls back to the actor hero above-fold if the section heading isn't found.
    """
    url = f"{CINETRACE_SCREENSHOT_URL}/actors/{slug}"
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox"],
            )
            page = await browser.new_page(
                viewport={"width": 1280, "height": _SECTION_HEIGHT + 200},
                color_scheme="dark",
            )
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(4000)  # let lazy API sections render

            heading_text = _SECTION_HEADINGS.get(section)

            if heading_text:
                JS = (
                    "() => {"
                    "  const hs = Array.from(document.querySelectorAll('h2'));"
                    "  const h = hs.find(el => el.textContent.includes('" + heading_text + "'));"
                    "  if (!h) return null;"
                    "  const rect = h.getBoundingClientRect();"
                    "  return { x: rect.x, y: rect.y + window.scrollY, w: rect.width };"
                    "}"
                )
                pos = await page.evaluate(JS)

                if pos:
                    # Scroll so the heading lands near the top of the viewport
                    await page.evaluate(f"window.scrollTo(0, {max(0, pos['y'] - 12)})")
                    await page.wait_for_timeout(400)
                    # clip coords are viewport-relative — y=0 is the top of the
                    # visible area after the scroll above
                    png = await page.screenshot(
                        type="png",
                        clip={
                            "x":      max(0, pos["x"] - 16),
                            "y":      0,
                            "width":  min(pos["w"] + 32, 1280),
                            "height": _SECTION_HEIGHT,
                        },
                    )
                    await browser.close()
                    return png
                else:
                    print(f"[screenshot] heading '{heading_text}' not found for {slug}, falling back")

            return await _fallback(page, browser)

    except Exception as e:
        print(f"[screenshot] failed for {slug}/{section}: {e}")
        return None


async def _fallback(page, browser) -> bytes | None:
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
    """Convert DB name to URL slug: 'Jr. NTR' → 'jr-ntr', 'Ram Charan' → 'ram-charan'"""
    s = db_name.lower()
    s = re.sub(r'[^a-z0-9\s]', '', s)
    s = re.sub(r'\s+', '-', s.strip())
    return s
