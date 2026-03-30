/**
 * Automated screenshot script — South Cinema Analytics
 * Run: node scripts/screenshot.js
 *
 * Captures key pages AND interacts with every major section so screenshots
 * show the UI in a realistic, populated state (not just static empty shells).
 *
 * Saved to /screenshots/YYYY-MM-DD_HH-mm/
 */

const { chromium } = require('playwright')
const fs   = require('fs')
const path = require('path')

// ── Config ────────────────────────────────────────────────────────────────────

const BASE_URL   = process.env.SCREENSHOT_URL || 'http://localhost:3000'
const VIEWPORT   = { width: 1280, height: 800 }
const SETTLE_MS  = 2500   // wait after network idle for animations / lazy images
const SEARCH_MS  = 1800   // wait after typing for debounce + results

// ── Helpers ───────────────────────────────────────────────────────────────────

function timestampFolder() {
  const now = new Date()
  const pad = n => String(n).padStart(2, '0')
  return [
    now.getFullYear(),
    pad(now.getMonth() + 1),
    pad(now.getDate()),
  ].join('-') + '_' + pad(now.getHours()) + '-' + pad(now.getMinutes())
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms))
}

async function gotoAndSettle(page, url) {
  await page.goto(url, { waitUntil: 'networkidle', timeout: 45_000 })
  await page.waitForSelector('main', { timeout: 15_000 })
  await sleep(SETTLE_MS)
}

async function scrollTo(page, text) {
  try {
    await page.getByText(text, { exact: false }).first().scrollIntoViewIfNeeded()
    await sleep(600)
  } catch { /* section may not exist on this page */ }
}

// ── Scenarios ─────────────────────────────────────────────────────────────────
// Each scenario is { name, run(page, folder) }
// They share a single browser context so cookies / state carry over where needed.

const SCENARIOS = [

  // ── 1. Home — default view ─────────────────────────────────────────────────
  {
    name: 'home',
    async run(page, folder) {
      await gotoAndSettle(page, `${BASE_URL}/`)
      await page.screenshot({ path: `${folder}/home.png`, fullPage: true })
    },
  },

  // ── 2. Home — search dropdown open with live results ──────────────────────
  {
    name: 'home_search_suggestions',
    async run(page, folder) {
      await gotoAndSettle(page, `${BASE_URL}/`)
      // Focus the hero search bar
      const searchInput = page.locator('#hero-search-input')
      await searchInput.click()
      await sleep(400)
      await searchInput.type('allu', { delay: 60 })
      await sleep(SEARCH_MS)
      await page.screenshot({ path: `${folder}/home_search_suggestions.png`, fullPage: false })
    },
  },

  // ── 3. Home — Tamil industry tab ──────────────────────────────────────────
  {
    name: 'home_tamil_tab',
    async run(page, folder) {
      await gotoAndSettle(page, `${BASE_URL}/?industry=tamil`)
      await page.screenshot({ path: `${folder}/home_tamil_tab.png`, fullPage: true })
    },
  },

  // ── 4. Header search dropdown (on actor page) ─────────────────────────────
  {
    name: 'header_search_suggestions',
    async run(page, folder) {
      await gotoAndSettle(page, `${BASE_URL}/actors/allu-arjun`)
      // Focus the header search bar
      const headerSearch = page.locator('input[placeholder="Search actors…"]')
      await headerSearch.click()
      await sleep(400)
      await headerSearch.type('vijay', { delay: 60 })
      await sleep(SEARCH_MS)
      await page.screenshot({ path: `${folder}/header_search_suggestions.png`, fullPage: false })
    },
  },

  // ── 5. Actor page — Allu Arjun — full page ────────────────────────────────
  {
    name: 'actor_alluarjun',
    async run(page, folder) {
      await gotoAndSettle(page, `${BASE_URL}/actors/allu-arjun`)
      await page.screenshot({ path: `${folder}/actor_alluarjun.png`, fullPage: true })
    },
  },

  // ── 6. Actor page — Compare widget: search dropdown open ─────────────────
  {
    name: 'actor_compare_widget',
    async run(page, folder) {
      await gotoAndSettle(page, `${BASE_URL}/actors/allu-arjun`)
      await scrollTo(page, 'Compare with another actor')
      // Type in the compare search input
      const compareInput = page.locator('input[placeholder*="Search any actor"]')
      await compareInput.click()
      await sleep(400)
      await compareInput.type('vijay', { delay: 60 })
      await sleep(SEARCH_MS)
      await page.screenshot({ path: `${folder}/actor_compare_widget.png`, fullPage: false })
    },
  },

  // ── 7. Actor page — Connection Finder: actor2 selected, ready to submit ──
  {
    name: 'actor_connection_finder_filled',
    async run(page, folder) {
      await gotoAndSettle(page, `${BASE_URL}/actors/allu-arjun`)
      await scrollTo(page, 'Connection Finder')
      // Find the actor-2 search box (actor1 is pre-locked to Allu Arjun)
      const actor2Input = page.locator('input[placeholder="Search actor…"]').first()
      await actor2Input.click()
      await sleep(400)
      await actor2Input.type('vijay', { delay: 60 })
      await sleep(SEARCH_MS)
      // Pick first result
      const firstResult = page.locator('button').filter({ hasText: 'Vijay' }).first()
      await firstResult.click()
      await sleep(600)
      await page.screenshot({ path: `${folder}/actor_connection_finder_filled.png`, fullPage: false })
    },
  },

  // ── 8. Actor page — Connection Finder: result after clicking Find ────────
  {
    name: 'actor_connection_finder_result',
    async run(page, folder) {
      await gotoAndSettle(page, `${BASE_URL}/actors/allu-arjun`)
      await scrollTo(page, 'Connection Finder')
      const actor2Input = page.locator('input[placeholder="Search actor…"]').first()
      await actor2Input.click()
      await sleep(400)
      await actor2Input.type('vijay', { delay: 60 })
      await sleep(SEARCH_MS)
      const firstResult = page.locator('button').filter({ hasText: 'Vijay' }).first()
      await firstResult.click()
      await sleep(600)
      // Click Find Connection
      await page.getByText('Find Connection').click()
      await sleep(3000)   // wait for BFS result
      await page.screenshot({ path: `${folder}/actor_connection_finder_result.png`, fullPage: false })
    },
  },

  // ── 9. Actor page — Vijay ─────────────────────────────────────────────────
  {
    name: 'actor_vijay',
    async run(page, folder) {
      await gotoAndSettle(page, `${BASE_URL}/actors/vijay`)
      await page.screenshot({ path: `${folder}/actor_vijay.png`, fullPage: true })
    },
  },

  // ── 10. Compare page — full view ──────────────────────────────────────────
  {
    name: 'compare_alluarjun_vs_vijay',
    async run(page, folder) {
      await gotoAndSettle(page, `${BASE_URL}/compare/1-vs-2`)
      await page.screenshot({ path: `${folder}/compare_alluarjun_vs_vijay.png`, fullPage: true })
    },
  },

  // ── 11. Compare page — Career Showdown section scrolled into view ─────────
  {
    name: 'compare_career_showdown',
    async run(page, folder) {
      await gotoAndSettle(page, `${BASE_URL}/compare/1-vs-2`)
      await scrollTo(page, 'Career Showdown')
      await sleep(1000)
      await page.screenshot({ path: `${folder}/compare_career_showdown.png`, fullPage: false })
    },
  },

  // ── 12. Compare page — Top Collaborations section ────────────────────────
  {
    name: 'compare_top_collaborations',
    async run(page, folder) {
      await gotoAndSettle(page, `${BASE_URL}/compare/1-vs-2`)
      await scrollTo(page, 'Top Collaborations')
      await sleep(800)
      await page.screenshot({ path: `${folder}/compare_top_collaborations.png`, fullPage: false })
    },
  },

]

// ── Main ──────────────────────────────────────────────────────────────────────

;(async () => {
  const folder = path.join(__dirname, '..', 'screenshots', timestampFolder())
  fs.mkdirSync(folder, { recursive: true })
  console.log(`📁 Saving to: ${folder}`)

  const browser = await chromium.launch()
  const context = await browser.newContext({ viewport: VIEWPORT })
  const page    = await context.newPage()

  for (const scenario of SCENARIOS) {
    console.log(`📸 ${scenario.name}`)
    try {
      await scenario.run(page, folder)
      console.log(`   ✓ saved ${scenario.name}.png`)
    } catch (err) {
      console.warn(`   ⚠ ${scenario.name} failed: ${err.message}`)
      try { await page.screenshot({ path: `${folder}/${scenario.name}.png`, fullPage: false }) } catch {}
    }
  }

  await browser.close()
  console.log('✅ Done')
})()
