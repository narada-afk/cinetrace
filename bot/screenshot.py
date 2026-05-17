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

# Same as above but taller cap — used after a director chip is expanded
_FIND_HEADING_TALL_JS = """(heading) => {
    const hs = Array.from(document.querySelectorAll("h2"));
    const idx = hs.findIndex(el => el.textContent.includes(heading));
    if (idx < 0) return null;
    const h = hs[idx];
    const next = hs[idx + 1];
    const r = h.getBoundingClientRect();
    const sectionTop = r.top + window.scrollY;
    const sectionH = next
        ? Math.min(Math.max(next.getBoundingClientRect().top + window.scrollY - sectionTop, 200), 900)
        : 700;
    return {y: sectionTop, x: r.x, w: r.width, h: sectionH};
}"""

# Clicks the director chip matching director_name (or the first chip if empty)
_CLICK_DIRECTOR_CHIP_JS = """(directorName) => {
    const h2s = Array.from(document.querySelectorAll("h2"));
    const h2 = h2s.find(el => el.textContent.includes("Directors Worked With"));
    if (!h2) return false;
    // Walk siblings until we find a container with buttons (the chip row)
    let el = h2.nextElementSibling;
    while (el) {
        const buttons = Array.from(el.querySelectorAll("button"));
        if (buttons.length > 0) {
            const target = directorName
                ? buttons.find(b => b.textContent.includes(directorName))
                : buttons[0];
            if (target) { target.click(); return true; }
            // Fallback to first if named director not found
            buttons[0].click();
            return true;
        }
        el = el.nextElementSibling;
    }
    return false;
}"""


async def capture_section_snapshot(slug: str, section: str,
                                   compare_with: str = "",
                                   chart_mode: str = "rating",
                                   director_name: str = "",
                                   chart_metric: str = "film_count") -> bytes | None:
    """
    Returns PNG bytes for the given actor + section.
    Falls back to actor hero if the section heading isn't found.

    compare_with:  second actor slug, required when section == "compare".
    director_name: specific director to expand in the directors chip list.
                   If empty, expands the first (most collaborated) director.
    chart_metric:  Y-axis metric for compare chart screenshots.
    """
    # ── Compare chart (dedicated minimal page, same pattern as career chart) ──
    if section == "compare" and compare_with:
        return await _capture_compare_chart(slug, compare_with, metric=chart_metric)

    # ── Connection Finder social card (SSR, data-section attribute) ───────────
    if section == "connections" and compare_with:
        return await _capture_connections(slug, compare_with)

    # ── Career chart (data-section attribute, client-rendered) ────────────────
    if section in ("career", "filmography"):
        return await _capture_career_chart(slug, mode=chart_mode)

    # ── Director loyalty social card (SSR, data-section attribute) ────────────
    if section == "director-loyalty":
        return await _capture_director_loyalty(slug)

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

                    # For directors: click a chip so the film grid expands
                    if section == "directors":
                        clicked = await page.evaluate(_CLICK_DIRECTOR_CHIP_JS, director_name)
                        if clicked:
                            await page.wait_for_timeout(600)
                            # Re-measure after expansion (taller cap)
                            pos = await page.evaluate(_FIND_HEADING_TALL_JS, heading_text) or pos

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


async def _capture_career_chart(slug: str, mode: str = "rating") -> bytes | None:
    """Screenshot the ActorCareerChart section in the requested mode.

    We load /actors/{slug}/chart?mode={mode} — a minimal page that renders ONLY
    the career chart component. The mode param pre-selects the chart view so the
    screenshot shows the right data dimension for the tweet.
    """
    url = f"{CINETRACE_SCREENSHOT_URL}/actors/{slug}/chart?mode={mode}"
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


async def _capture_director_loyalty(slug: str) -> bytes | None:
    """Screenshot the /social/director-loyalty/{slug} social card.

    Pure SSR — all data is rendered server-side, no client fetches.
    DOM is complete after domcontentloaded; 1.5 s covers web-font loading
    and the actor avatar image settling.
    """
    url = f"{CINETRACE_SCREENSHOT_URL}/social/director-loyalty/{slug}"
    resolver = _backend_host_resolver_rules()
    chromium_args = ["--no-sandbox", "--disable-setuid-sandbox"]
    if resolver:
        chromium_args.append(f"--host-resolver-rules={resolver}")

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=chromium_args)
            page = await browser.new_page(
                viewport={"width": _VIEWPORT_W, "height": 900},
                color_scheme="dark",
            )
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            # Fonts + avatar image
            await page.wait_for_timeout(1500)

            pos = await page.evaluate(_FIND_DATA_SECTION_JS, "director-loyalty")
            if not pos:
                print(f"[screenshot] director-loyalty section not found for {slug}")
                return await _hero_fallback(page, browser)

            # Scroll the card to viewport top, then clip full-width
            await page.evaluate("(y) => window.scrollTo(0, y)", max(0, int(pos["y"]) - 16))
            await page.wait_for_timeout(200)

            png = await page.screenshot(
                type="png",
                clip={
                    "x":      0,
                    "y":      0,
                    "width":  _VIEWPORT_W,
                    "height": min(int(pos["h"]) + 40, 600),
                },
            )
            await browser.close()
            return png
    except Exception as e:
        print(f"[screenshot] director loyalty failed for {slug}: {e}")
        return None


async def _capture_connections(slug1: str, slug2: str) -> bytes | None:
    """Screenshot the /social/connections/{slug1}/{slug2} social card.

    Pure SSR — all data fetched server-side. domcontentloaded + 1.5 s covers
    font loading and avatar images. Card height scales with chain depth;
    clip height is derived from the data-section bounding box (cap 700 px).
    """
    url = f"{CINETRACE_SCREENSHOT_URL}/social/connections/{slug1}/{slug2}"
    resolver = _backend_host_resolver_rules()
    chromium_args = ["--no-sandbox", "--disable-setuid-sandbox"]
    if resolver:
        chromium_args.append(f"--host-resolver-rules={resolver}")

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=chromium_args)
            page = await browser.new_page(
                viewport={"width": _VIEWPORT_W, "height": 900},
                color_scheme="dark",
            )
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            # Fonts + avatar images
            await page.wait_for_timeout(1500)

            pos = await page.evaluate(_FIND_DATA_SECTION_JS, "connection-finder")
            if not pos:
                print(f"[screenshot] connection-finder section not found for {slug1}/{slug2}")
                return await _hero_fallback(page, browser)

            # Scroll card to viewport top, then clip full width
            await page.evaluate("(y) => window.scrollTo(0, y)", max(0, int(pos["y"]) - 16))
            await page.wait_for_timeout(200)

            png = await page.screenshot(
                type="png",
                clip={
                    "x":      0,
                    "y":      0,
                    "width":  _VIEWPORT_W,
                    "height": min(int(pos["h"]) + 40, 700),
                },
            )
            await browser.close()
            return png
    except Exception as e:
        print(f"[screenshot] connections failed for {slug1}/{slug2}: {e}")
        return None


async def _capture_compare_chart(slug1: str, slug2: str,
                                  metric: str = "film_count") -> bytes | None:
    """Screenshot the /compare/{slug1}-vs-{slug2}/chart minimal page.

    Equivalent to _capture_career_chart but for two-actor comparisons.
    Waits for the SVG line chart inside [data-section="compare-chart"] to render.
    """
    url = f"{CINETRACE_SCREENSHOT_URL}/compare/{slug1}-vs-{slug2}/chart?metric={metric}"
    resolver = _backend_host_resolver_rules()
    chromium_args = ["--no-sandbox", "--disable-setuid-sandbox"]
    if resolver:
        chromium_args.append(f"--host-resolver-rules={resolver}")

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=chromium_args)
            page = await browser.new_page(
                viewport={"width": _VIEWPORT_W, "height": 900},
                color_scheme="dark",
            )
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)

            # Wait for the SVG chart to render (client-side fetch + draw)
            try:
                await page.wait_for_selector(
                    '[data-section="compare-chart"] svg',
                    timeout=35000,
                )
            except Exception:
                print(f"[screenshot] compare chart SVG not found for {slug1}-vs-{slug2}, falling back to hero")
                return await _hero_fallback(page, browser)

            # Extra tick for animated line draw to complete
            await page.wait_for_timeout(1200)

            pos = await page.evaluate(_FIND_DATA_SECTION_JS, "compare-chart")
            if not pos:
                return await _hero_fallback(page, browser)

            await page.evaluate("(y) => window.scrollTo(0, y)", max(0, pos["y"] - 16))
            await page.wait_for_timeout(300)

            png = await page.screenshot(
                type="png",
                clip={
                    "x":      0,
                    "y":      0,
                    "width":  _VIEWPORT_W,
                    "height": min(int(pos["h"]) + 32, 600),
                },
            )
            await browser.close()
            return png
    except Exception as e:
        print(f"[screenshot] compare chart failed for {slug1}-vs-{slug2}: {e}")
        return None


async def _capture_compare(slug1: str, slug2: str) -> bytes | None:
    """Kept for backwards compatibility — routes to the chart page."""
    return await _capture_compare_chart(slug1, slug2)


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
