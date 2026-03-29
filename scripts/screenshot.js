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
const DELAY_MS  = 1500   // wait after page load for animations

const PAGES = [
  {
    name:     'home',
    path:     '/',
    waitFor:  'text=Actor Connection Finder',
  },
  {
    name:     'network_actor_1',
    path:     '/?actor=1',
    waitFor:  'text=Network',
  },
  {
    name:     'actor_1',
    path:     '/actors/1',
    waitFor:  'main',
  },
  {
    name:     'actor_2',
    path:     '/actors/2',
    waitFor:  'main',
  },
  {
    name:     'compare_1_2',
    path:     '/actors/1?compare=2',
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
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30_000 })
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
