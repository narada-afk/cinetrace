import asyncio
from playwright.async_api import async_playwright
from config import CINETRACE_BASE_URL

async def capture_actor_page(actor_slug: str) -> bytes | None:
    url = f"{CINETRACE_BASE_URL}/actors/{actor_slug}"
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox"]
            )
            page = await browser.new_page(
                viewport={"width": 1200, "height": 800},
                color_scheme="dark",
            )
            await page.goto(url, wait_until="networkidle", timeout=20000)
            await page.wait_for_timeout(2000)

            # Try to screenshot the first shareable section
            section = await page.query_selector("[data-share-section], .glass, section")
            if section:
                screenshot = await section.screenshot(type="png")
            else:
                # Fall back to above-the-fold viewport
                screenshot = await page.screenshot(
                    type="png",
                    clip={"x": 0, "y": 0, "width": 1200, "height": 630}
                )

            await browser.close()
            return screenshot
    except Exception as e:
        print(f"[screenshot] failed for {actor_slug}: {e}")
        return None

def actor_slug(db_name: str) -> str:
    return db_name.lower().replace(" ", "").replace(".", "")
