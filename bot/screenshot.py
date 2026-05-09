"""
screenshot.py
=============
Captures the actor page section as a PNG for attaching to tweets.

Strategy: navigate to the actor page, find the section by its heading text,
scroll to it, screenshot the containing card element directly.
No html2canvas, no share modal — pure Playwright element screenshot.
"""

import re
import asyncio
from playwright.async_api import async_playwright
from config import CINETRACE_BASE_URL

# Maps inventory section → heading text to locate on the page
_SECTION_HEADINGS = {
    "directors":    "Directors Worked With",
    "collaborators": "Lead Actresses",
    "blockbusters": "Blockbusters",
}


async def capture_section_snapshot(slug: str, section: str) -> bytes | None:
    """
    Returns PNG bytes for the given actor + section card.
    Falls back to the actor hero above-fold if section not found.
    """
    url = f"{CINETRACE_BASE_URL}/actors/{slug}"
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
            await page.wait_for_timeout(3000)  # let lazy sections + data fetches render

            heading_text = _SECTION_HEADINGS.get(section)

            if heading_text:
                # Find the section by its H2 heading text, walk up 3 levels to the card container
                JS = f"""() => {{
                    const headings = Array.from(document.querySelectorAll("h2"));
                    const h = headings.find(el => el.textContent.includes("{heading_text}"));
                    if (!h) return null;
                    // Walk up 3 ancestors to reach the section card div
                    let el = h.parentElement?.parentElement?.parentElement;
                    if (!el) return null;
                    const rect = el.getBoundingClientRect();
                    return {{ x: rect.x, y: rect.y + window.scrollY, w: rect.width, h: rect.height }};
                }}"""
                box = await page.evaluate(JS)

                if box and box["h"] > 80:
                    # Scroll element into view, then clip-screenshot it
                    await page.evaluate(f"window.scrollTo(0, {max(0, box['y'] - 40)})")
                    await page.wait_for_timeout(500)
                    png = await page.screenshot(
                        type="png",
                        clip={
                            "x":      max(0, box["x"] - 16),
                            "y":      max(0, box["y"] - 16),
                            "width":  min(box["w"] + 32, 1280),
                            "height": min(box["h"] + 32, 1400),
                        },
                    )
                    await browser.close()
                    return png
                else:
                    print(f"[screenshot] section '{heading_text}' not found or too small (box={box}), falling back")

            # Fallback: screenshot the hero / above-fold area
            return await _fallback(page, browser)

    except Exception as e:
        print(f"[screenshot] failed for {slug}/{section}: {e}")
        return None


async def _fallback(page, browser) -> bytes | None:
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
    s = re.sub(r'[^a-z0-9\s]', '', s)   # strip punctuation (dots, apostrophes, etc.)
    s = re.sub(r'\s+', '-', s.strip())   # spaces → hyphens
    return s
