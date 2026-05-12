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
import socket
import asyncio
from playwright.async_api import async_playwright
from config import CINETRACE_SCREENSHOT_URL

def _backend_host_resolver_rules() -> str:
    """
    Playwright's Chromium subprocess doesn't use Docker DNS.
    Resolve both 'backend' and 'frontend' to their container IPs so that:
      - direct backend API calls work (used by some screenshot paths)
      - client-side XHRs back to the frontend origin work (career chart uses
        /api/backend/... proxy, which requires resolving 'frontend')
    """
    rules: list[str] = []
    for host in ("backend", "frontend"):
        try:
            ip = socket.gethostbyname(host)
            rules.append(f"MAP {host} {ip}")
        except OSError:
            pass
    return ", ".join(rules)

# Maps section → H2 heading text to scroll to on the actor page.
# "career" is handled separately via [data-section="career-chart"] attribute.
_SECTION_HEADINGS = {
    "directors":     "Directors Worked With",
    "blockbusters":  "Blockbusters",
    "collaborators": "By the Numbers",
    "overview":      "By the Numbers",
    # filmography tweets now use the career chart
    "filmography":   None,
}

# Viewport width for all screenshots.
# 800 px — wide enough for tablet layout, narrow enough that Twitter/X renders
# the image at full width on a phone without letterboxing. Components reflow
# to a comfortable single-column at this breakpoint.
_VIEWPORT_W = 800

# Fallback height when section bounds can't be calculated
_SECTION_HEIGHT = 500

# Finds the [data-section] element and returns its bounding rect (document coords)
_FIND_DATA_SECTION_JS = """(name) => {
    const el = document.querySelector(`[data-section="${name}"]`);
    if (!el) return null;
    const r = el.getBoundingClientRect();
    return {y: r.top + window.scrollY, x: r.left, w: r.width, h: Math.min(r.height, 700)};
}"""

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

    # ── Career chart (data-section attribute, client-rendered) ────────────────
    if section in ("career", "filmography"):
        return await _capture_career_chart(slug)

    # ── Actor page sections (H2 heading approach) ─────────────────────────────
    url = f"{CINETRACE_SCREENSHOT_URL}/actors/{slug}"
    heading_text = _SECTION_HEADINGS.get(section)

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox"],
            )
            page = await browser.new_page(
                viewport={"width": _VIEWPORT_W, "height": 900},
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
                            "width":  min(pos["w"] + 32, _VIEWPORT_W),
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


async def _capture_career_chart(slug: str) -> bytes | None:
    """Screenshot the ActorCareerChart section.

    We load /actors/{slug}/chart — a minimal page that renders ONLY the career
    chart component, making SSR fast and React hydration near-instant (one
    component vs the full actor page with 10+ sections).
    """
    url = f"{CINETRACE_SCREENSHOT_URL}/actors/{slug}/chart"
    resolver = _backend_host_resolver_rules()
    chromium_args = ["--no-sandbox", "--disable-setuid-sandbox"]
    if resolver:
        chromium_args.append(f"--host-resolver-rules={resolver}")

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=chromium_args,
            )
            page = await browser.new_page(
                viewport={"width": _VIEWPORT_W, "height": 900},
                color_scheme="dark",
            )
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)

            # Wait for the career chart SVG to render.
            # The component is dynamically imported (ssr: false) so we wait for:
            #   1. React hydration + dynamic chunk download
            #   2. useEffect fires → /api/backend/stats/chart-data fetched
            #   3. DualChart SVG painted
            # With Docker DNS resolution in place, the /api/backend proxy call
            # resolves correctly and the SVG typically appears within 15 s.
            try:
                await page.wait_for_selector(
                    '[data-section="career-chart"] svg',
                    timeout=35000,
                )
            except Exception:
                print(f"[screenshot] career chart SVG not found for {slug}, falling back to hero")
                return await _hero_fallback(page, browser)

            # Extra tick so the animated line draw starts
            await page.wait_for_timeout(1200)

            pos = await page.evaluate(_FIND_DATA_SECTION_JS, "career-chart")
            if not pos:
                return await _hero_fallback(page, browser)

            await page.evaluate("(y) => window.scrollTo(0, y)", max(0, pos["y"] - 16))
            await page.wait_for_timeout(300)

            png = await page.screenshot(
                type="png",
                clip={
                    "x":      max(0, pos["x"] - 16),
                    "y":      0,
                    "width":  min(pos["w"] + 32, _VIEWPORT_W),
                    "height": min(int(pos["h"]) + 32, 560),
                },
            )
            await browser.close()
            return png
    except Exception as e:
        print(f"[screenshot] career chart failed for {slug}: {e}")
        return None


async def _capture_compare(slug1: str, slug2: str) -> bytes | None:
    """Screenshot the /compare/{slug1}-vs-{slug2} chart section.

    Scrolls past the actor header / filter controls so the Year vs Metric
    line chart is the hero of the image.
    """
    url = f"{CINETRACE_SCREENSHOT_URL}/compare/{slug1}-vs-{slug2}"
    # JS: find the recharts/svg chart container and return its document Y
    _FIND_CHART_JS = """() => {
        const el = document.querySelector('.recharts-wrapper, svg.recharts-surface');
        if (!el) return null;
        const r = el.getBoundingClientRect();
        return r.top + window.scrollY;
    }"""
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox"],
            )
            page = await browser.new_page(
                viewport={"width": _VIEWPORT_W, "height": 900},
                color_scheme="dark",
            )
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            # Extra wait so the interactive chart renders
            await page.wait_for_timeout(5000)

            chart_y = await page.evaluate(_FIND_CHART_JS)
            if chart_y and chart_y > 20:
                # Scroll so chart top is near the top of the viewport
                scroll_to = max(0, int(chart_y) - 20)
                await page.evaluate("(y) => window.scrollTo(0, y)", scroll_to)
                await page.wait_for_timeout(400)
                png = await page.screenshot(
                    type="png",
                    clip={"x": 0, "y": 0, "width": _VIEWPORT_W, "height": 500},
                )
            else:
                # Fallback: capture the full above-fold area
                png = await page.screenshot(
                    type="png",
                    clip={"x": 0, "y": 0, "width": _VIEWPORT_W, "height": 500},
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
            clip={"x": 0, "y": 0, "width": _VIEWPORT_W, "height": 500},
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
