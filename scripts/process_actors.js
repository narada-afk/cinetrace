/**
 * process_actors.js
 *
 * Removes background from actor images using remove.bg API.
 *
 * Input:  data/top_actors/{anything}.jpg|jpeg|png|webp
 * Output: frontend/public/avatars/{slug}.png
 *
 * Slug logic matches the frontend:
 *   name.toLowerCase().replace(/[\s_]+/g, '')
 *   e.g. "Allu Arjun.jpg" → "alluarjun.png"
 *        "allu_arjun.jpg" → "alluarjun.png"
 *
 * Usage:
 *   REMOVE_BG_API_KEY=xxx node scripts/process_actors.js
 *   or add key to .env at repo root
 */

require('dotenv').config()
const axios    = require('axios')
const FormData = require('form-data')
const fs       = require('fs')
const path     = require('path')
const https    = require('https')

// Bypass corporate proxy SSL issues
const httpsAgent = new https.Agent({ rejectUnauthorized: false })

const INPUT_DIR  = path.join(__dirname, '..', 'data', 'top_actors')
const OUTPUT_DIR = path.join(__dirname, '..', 'frontend', 'public', 'avatars')
const API_KEY    = process.env.REMOVE_BG_API_KEY

const SUPPORTED = new Set(['.jpg', '.jpeg', '.png', '.webp'])

// ── Slug: match frontend ActorAvatar logic ────────────────────────
function toSlug(filename) {
  const ext  = path.extname(filename).toLowerCase()
  const base = path.basename(filename, ext)
  return base.toLowerCase().replace(/[\s_]+/g, '')
}

// ── remove.bg API call ────────────────────────────────────────────
async function removeBg(inputPath, outputPath) {
  const form = new FormData()
  form.append('image_file', fs.createReadStream(inputPath))
  form.append('size', 'auto')

  const response = await axios.post(
    'https://api.remove.bg/v1.0/removebg',
    form,
    {
      headers: {
        ...form.getHeaders(),
        'X-Api-Key': API_KEY,
      },
      responseType: 'arraybuffer',
      timeout: 30000,
      httpsAgent,
    }
  )

  fs.writeFileSync(outputPath, response.data)
}

// ── Main ──────────────────────────────────────────────────────────
async function processAll() {
  if (!API_KEY) {
    console.error('❌  REMOVE_BG_API_KEY not set. Add it to .env or export it.')
    process.exit(1)
  }

  if (!fs.existsSync(INPUT_DIR)) {
    console.error(`❌  Input folder not found: ${INPUT_DIR}`)
    process.exit(1)
  }

  fs.mkdirSync(OUTPUT_DIR, { recursive: true })

  const files = fs.readdirSync(INPUT_DIR).filter(f => {
    const ext = path.extname(f).toLowerCase()
    return SUPPORTED.has(ext) && !f.startsWith('.')
  })

  if (files.length === 0) {
    console.log('⚠️   No images found in data/top_actors/ — add your actor images there.')
    return
  }

  console.log(`\n🎬  Processing ${files.length} actor image(s)…\n`)

  let success = 0
  let skipped = 0
  let failed  = 0
  const failures = []

  for (const file of files) {
    const inputPath  = path.join(INPUT_DIR, file)
    const slug       = toSlug(file)
    const outputPath = path.join(OUTPUT_DIR, `${slug}.png`)

    // Skip if already processed
    if (fs.existsSync(outputPath)) {
      console.log(`⏭️   Skip   ${file} → ${slug}.png (already exists)`)
      skipped++
      continue
    }

    process.stdout.write(`⏳  Processing ${file} → ${slug}.png … `)

    try {
      await removeBg(inputPath, outputPath)
      console.log('✅')
      success++
    } catch (err) {
      const msg = err.response
        ? `HTTP ${err.response.status}: ${Buffer.from(err.response.data).toString().slice(0, 120)}`
        : err.message
      console.log(`❌  ${msg}`)
      failures.push({ file, slug, msg })
      failed++
    }
  }

  // ── Summary ───────────────────────────────────────────────────
  console.log('\n─────────────────────────────────')
  console.log(`  Processed : ${files.length}`)
  console.log(`  ✅ Success : ${success}`)
  console.log(`  ⏭️  Skipped : ${skipped}`)
  console.log(`  ❌ Failed  : ${failed}`)
  if (failures.length > 0) {
    console.log('\nFailed files:')
    failures.forEach(f => console.log(`  • ${f.file} → ${f.slug}.png  (${f.msg})`))
  }
  console.log('─────────────────────────────────\n')
}

processAll()
