const axios = require('axios')
const fs = require('fs')
const path = require('path')
const https = require('https')
const FormData = require('form-data')

const API_KEY = 'fzotK6cmX6mEkRyr5ma8aiFy'
const SOURCE_DIR = '/Users/macmini/Documents/Primary_Images'
const OUT_DIR = '/Users/macmini/south-cinema-analytics/frontend/public/avatars'
const WORKTREE_OUT_DIR = '/Users/macmini/south-cinema-analytics/.claude/worktrees/relaxed-euler/frontend/public/avatars'

const httpsAgent = new https.Agent({ rejectUnauthorized: false })

// Map: source filename (lowercase, no ext) → target slug (matches existing site convention)
const NEW_ACTORS = {
  'arjunsarja':          'arjunsarja',
  'chiranjeevi':         'chiranjeevi',
  'nagarjunaakkineni':   'nagarjunaakkineni',
  'nandamuribalakrishna':'nandamuribalakrishna',
  'ranadaggubati':       'ranadaggubati',
  'raviteja':            'raviteja',
  'sharwanand':          'sharwanand',
  'siddhujonnalagadda':  'siddhujonnalagadda',
  'varunsandesh':        'varunsandesh',
  'varuntej':            'varuntej',
  'venkateshdaggubati':  'venkateshdaggubati',
  'vishwaksen':          'vishwaksen',
}

async function processActor(sourceSlug, targetSlug) {
  // Find source file (case-insensitive)
  const files = fs.readdirSync(SOURCE_DIR)
  const match = files.find(f => f.replace(/\.png$/i, '').toLowerCase() === sourceSlug.toLowerCase())
  if (!match) {
    console.log(`✗ ${sourceSlug}: source file not found`)
    return false
  }

  const sourcePath = path.join(SOURCE_DIR, match)
  const outPath    = path.join(OUT_DIR, `${targetSlug}.png`)
  const worktreePath = path.join(WORKTREE_OUT_DIR, `${targetSlug}.png`)

  console.log(`⏳ ${targetSlug}: sending to remove.bg...`)

  try {
    const form = new FormData()
    form.append('image_file', fs.createReadStream(sourcePath))
    form.append('size', 'auto')

    const response = await axios.post('https://api.remove.bg/v1.0/removebg', form, {
      headers: { ...form.getHeaders(), 'X-Api-Key': API_KEY },
      responseType: 'arraybuffer',
      httpsAgent,
      timeout: 30000,
    })

    fs.writeFileSync(outPath, response.data)
    fs.writeFileSync(worktreePath, response.data)
    console.log(`✓ ${targetSlug}: saved (${Math.round(response.data.length / 1024)}KB)`)
    return true
  } catch (err) {
    const msg = err.response
      ? `HTTP ${err.response.status}: ${Buffer.from(err.response.data).toString().slice(0, 120)}`
      : err.message
    console.log(`✗ ${targetSlug}: ${msg}`)
    return false
  }
}

async function main() {
  const entries = Object.entries(NEW_ACTORS)
  console.log(`Processing ${entries.length} new actors...\n`)
  let ok = 0, fail = 0
  for (const [src, tgt] of entries) {
    const success = await processActor(src, tgt)
    success ? ok++ : fail++
    await new Promise(r => setTimeout(r, 600)) // gentle rate limit
  }
  console.log(`\nDone — ${ok} succeeded, ${fail} failed`)
}

main()
