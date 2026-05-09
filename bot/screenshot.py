"""
screenshot.py
=============
Captures the share snapshot image from a cinetrace actor page section —
the same PNG the user sees when they click "Share snapshot" on the site.

Flow:
  1. Navigate to /actors/{slug}
  2. Click the share button for the target section (directors / collaborators / blockbusters / overview)
  3. Wait for the modal + html2canvas to finish rendering
  4. Extract the data URL from the <img> preview inside the modal
  5. Return as PNG bytes → attach to tweet via Twitter media upload
"""

import base64
import asyncio
from playwright.async_api import async_playwright
from config import CINETRACE_BASE_URL

# Maps inventory `section` value → aria-label of the share button on the page
_SECTION_LABELS = {
    "directors":    "Share Directors Worked With",
    "collaborators":"Share ✨ Lead Actresses",
    "blockbusters": "Share Blockbusters",
}

async def capture_section_snapshot(actor_slug: str, section: str) -> bytes | None:
    """
    Returns PNG bytes of the share snapshot card for the given actor + section.
    Falls back to a viewport screenshot of the hero area if anything fails.
    """
    url = f"{CINETRACE_BASE_URL}/actors/{actor_slug}"
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
            await page.wait_for_timeout(3000)   # let lazy sections + data fetches render

            label = _SECTION_LABELS.get(section)

            if label:
                # Find and click the share button for this section
                btn = page.get_by_role("button", name=label)
                if await btn.count() == 0:
                    print(f"[screenshot] share button not found: '{label}', falling back")
                    return await _fallback(page, browser)

                await btn.scroll_into_view_if_needed()
                await btn.click()

                # Wait for modal img with a data URL src (html2canvas output)
                img = page.locator('img[alt="Section preview"]')
                await img.wait_for(timeout=12000)

                # Extract data URL → decode to bytes
                data_url = await img.get_attribute("src")
                await browser.close()

                if data_url and data_url.startswith("data:image/png;base64,"):
                    return base64.b64decode(data_url.split(",", 1)[1])

                print(f"[screenshot] unexpected img src format, falling back")
                return None

            else:
                # "overview" — screenshot the hero / above-fold area
                result = await _fallback(page, browser)
                return result

    except Exception as e:
        print(f"[screenshot] failed for {actor_slug}/{section}: {e}")
        return None


async def _fallback(page, browser) -> bytes | None:
    try:
        screenshot = await page.screenshot(
            type="png",
            clip={"x": 0, "y": 0, "width": 1280, "height": 670},
        )
        await browser.close()
        return screenshot
    except Exception as e:
        print(f"[screenshot] fallback failed: {e}")
        return None


def actor_slug(db_name: str) -> str:
    """Convert DB name to URL slug: 'Jr. NTR' → 'jr-ntr', 'Ram Charan' → 'ram-charan'"""
    import re
    s = db_name.lower()
    s = re.sub(r'[^a-z0-9\s]', '', s)   # strip punctuation (dots, apostrophes, etc.)
    s = re.sub(r'\s+', '-', s.strip())   # spaces → hyphens
    return s
