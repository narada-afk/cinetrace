/**
 * Automated screenshot script — South Cinema Analytics
 * Run: node scripts/screenshot.js
 *
 * Captures key pages and saves them into /screenshots/YYYY-MM-DD_HH-mm/
 */

const { chromium } = require('playwright')
const fs   = require('fs')
const path = require('path')

// ── Config ────────────────────────────────────────────────────────────────────

const BASE_URL  = process.env.SCREENSHOT_URL || 'http://localhost:3000'
const VIEWPORT  = { width: 1280, height: 800 }
const DELAY_MS  = 3500   // wait after page load for animations + lazy images

const PAGES = [
  {
    name:     'home',
    path:     '/',
    waitFor:  'main',
  },
  {
    // Allu Arjun — seeded by app.seed_data with ~9 movies
    name:     'actor_alluarjun',
    path:     '/actors/allu-arjun',
    waitFor:  'main',
  },
  {
    // Vijay — seeded by app.seed_data with ~10 movies
    name:     'actor_vijay',
    path:     '/actors/vijay',
    waitFor:  'main',
  },
  {
    // Compare: uses numeric IDs — always resolves correctly regardless of name slug format
    name:     'compare_alluarjun_vs_vijay',
    path:     '/compare/1-vs-2',
    waitFor:  'main',
  },
]

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

// ── Main ──────────────────────────────────────────────────────────────────────

;(async () => {
  const folder = path.join(__dirname, '..', 'screenshots', timestampFolder())
  fs.mkdirSync(folder, { recursive: true })
  console.log(`📁 Saving to: ${folder}`)

  const browser = await chromium.launch()
  const context = await browser.newContext({ viewport: VIEWPORT })
  const page    = await context.newPage()

  for (const { name, path: pagePath, waitFor } of PAGES) {
    const url  = `${BASE_URL}${pagePath}`
    const file = path.join(folder, `${name}.png`)

    console.log(`📸 ${name} → ${url}`)
    try {
      await page.goto(url, { waitUntil: 'networkidle', timeout: 45_000 })
      await page.waitForSelector(waitFor, { timeout: 15_000 })
      await sleep(DELAY_MS)
      await page.screenshot({ path: file, fullPage: true })
      console.log(`   ✓ saved ${name}.png`)
    } catch (err) {
      console.warn(`   ⚠ ${name} failed: ${err.message}`)
      // Still attempt a screenshot of whatever rendered
      try { await page.screenshot({ path: file, fullPage: true }) } catch {}
    }
  }

  await browser.close()
  console.log('✅ Done')
})()
