'use client'
// Canvas-based share card generator.
// Draws a 1200×630 OG-image-sized card, then:
//   • mobile : triggers native Web Share Sheet (files)
//   • desktop: downloads the PNG

import { useState } from 'react'

interface ShareCardData {
  name1: string
  name2: string
  industry1: string
  industry2: string
  films1: number
  films2: number
  collabs1: number
  collabs2: number
  dirs1: number
  dirs2: number
  /** null = tie */
  winner: string | null
  winnerLeads: number
}

function drawRoundRect(
  ctx: CanvasRenderingContext2D,
  x: number, y: number, w: number, h: number, r: number,
) {
  ctx.beginPath()
  ctx.moveTo(x + r, y)
  ctx.lineTo(x + w - r, y)
  ctx.quadraticCurveTo(x + w, y, x + w, y + r)
  ctx.lineTo(x + w, y + h - r)
  ctx.quadraticCurveTo(x + w, y + h, x + w - r, y + h)
  ctx.lineTo(x + r, y + h)
  ctx.quadraticCurveTo(x, y + h, x, y + h - r)
  ctx.lineTo(x, y + r)
  ctx.quadraticCurveTo(x, y, x + r, y)
  ctx.closePath()
}

function buildCanvas(d: ShareCardData): HTMLCanvasElement {
  const C = document.createElement('canvas')
  C.width = 1200
  C.height = 630
  const ctx = C.getContext('2d')!

  // ── Background ────────────────────────────────────────────────
  ctx.fillStyle = '#0a0a0f'
  ctx.fillRect(0, 0, 1200, 630)

  // Subtle dot grid
  ctx.fillStyle = 'rgba(255,255,255,0.025)'
  for (let x = 0; x < 1200; x += 48) {
    for (let y = 0; y < 630; y += 48) {
      ctx.beginPath()
      ctx.arc(x, y, 1, 0, Math.PI * 2)
      ctx.fill()
    }
  }

  // Left amber glow
  const g1 = ctx.createRadialGradient(0, 315, 0, 0, 315, 500)
  g1.addColorStop(0, 'rgba(245,158,11,0.18)')
  g1.addColorStop(1, 'rgba(245,158,11,0)')
  ctx.fillStyle = g1
  ctx.fillRect(0, 0, 1200, 630)

  // Right cyan glow
  const g2 = ctx.createRadialGradient(1200, 315, 0, 1200, 315, 500)
  g2.addColorStop(0, 'rgba(6,182,212,0.18)')
  g2.addColorStop(1, 'rgba(6,182,212,0)')
  ctx.fillStyle = g2
  ctx.fillRect(0, 0, 1200, 630)

  // Top accent lines
  ctx.fillStyle = '#f59e0b'
  ctx.fillRect(0, 0, 555, 3)
  ctx.fillStyle = '#06b6d4'
  ctx.fillRect(645, 0, 555, 3)

  // Center divider
  ctx.strokeStyle = 'rgba(255,255,255,0.07)'
  ctx.lineWidth = 1
  ctx.setLineDash([4, 4])
  ctx.beginPath()
  ctx.moveTo(600, 40)
  ctx.lineTo(600, 560)
  ctx.stroke()
  ctx.setLineDash([])

  // VS pill
  ctx.fillStyle = 'rgba(255,255,255,0.07)'
  drawRoundRect(ctx, 574, 44, 52, 28, 14)
  ctx.fill()
  ctx.fillStyle = 'rgba(255,255,255,0.3)'
  ctx.font = 'bold 14px -apple-system, system-ui, sans-serif'
  ctx.textAlign = 'center'
  ctx.fillText('VS', 600, 63)

  // ── Actor 1 (left) ─────────────────────────────────────────────
  ctx.textAlign = 'left'
  ctx.fillStyle = 'rgba(245,158,11,0.7)'
  ctx.font = '13px -apple-system, system-ui, sans-serif'
  ctx.fillText((d.industry1 || '').toUpperCase(), 60, 98)

  ctx.fillStyle = '#ffffff'
  const f1 = d.name1.length > 16 ? 44 : d.name1.length > 12 ? 52 : 60
  ctx.font = `bold ${f1}px -apple-system, system-ui, sans-serif`
  ctx.fillText(d.name1, 60, 165)

  // ── Actor 2 (right) ────────────────────────────────────────────
  ctx.textAlign = 'right'
  ctx.fillStyle = 'rgba(6,182,212,0.7)'
  ctx.font = '13px -apple-system, system-ui, sans-serif'
  ctx.fillText((d.industry2 || '').toUpperCase(), 1140, 98)

  ctx.fillStyle = '#ffffff'
  const f2 = d.name2.length > 16 ? 44 : d.name2.length > 12 ? 52 : 60
  ctx.font = `bold ${f2}px -apple-system, system-ui, sans-serif`
  ctx.fillText(d.name2, 1140, 165)

  // ── Stat bars ──────────────────────────────────────────────────
  const stats = [
    { label: 'Films',         v1: d.films1,   v2: d.films2 },
    { label: 'Collaborators', v1: d.collabs1, v2: d.collabs2 },
    { label: 'Directors',     v1: d.dirs1,    v2: d.dirs2 },
  ]

  const BAR_W = 430      // max bar width (per side)
  const BAR_H = 12
  const ROW_H = 80
  let rowY = 240

  for (const stat of stats) {
    const lead = stat.v1 > stat.v2 ? 1 : stat.v2 > stat.v1 ? 2 : 0
    const maxV = Math.max(stat.v1, stat.v2) || 1

    // Stat label (center)
    ctx.fillStyle = 'rgba(255,255,255,0.3)'
    ctx.font = '12px -apple-system, system-ui, sans-serif'
    ctx.textAlign = 'center'
    ctx.fillText(stat.label.toUpperCase(), 600, rowY + 6)

    // Actor 1 bar (grows left-to-right toward center)
    const w1 = Math.round((stat.v1 / maxV) * BAR_W)
    ctx.fillStyle = 'rgba(255,255,255,0.07)'
    drawRoundRect(ctx, 600 - BAR_W - 10, rowY + 16, BAR_W, BAR_H, 6)
    ctx.fill()
    ctx.fillStyle = lead === 1 ? '#f59e0b' : 'rgba(255,255,255,0.18)'
    drawRoundRect(ctx, 600 - w1 - 10, rowY + 16, w1, BAR_H, 6)
    ctx.fill()

    // Actor 1 value
    ctx.fillStyle = lead === 1 ? '#f59e0b' : 'rgba(255,255,255,0.45)'
    ctx.font = `bold ${lead === 1 ? 26 : 22}px -apple-system, system-ui, sans-serif`
    ctx.textAlign = 'right'
    ctx.fillText(stat.v1.toLocaleString(), 600 - BAR_W - 20, rowY + 30)

    // Actor 2 bar (grows right from center)
    const w2 = Math.round((stat.v2 / maxV) * BAR_W)
    ctx.fillStyle = 'rgba(255,255,255,0.07)'
    drawRoundRect(ctx, 610, rowY + 16, BAR_W, BAR_H, 6)
    ctx.fill()
    ctx.fillStyle = lead === 2 ? '#06b6d4' : 'rgba(255,255,255,0.18)'
    drawRoundRect(ctx, 610, rowY + 16, w2, BAR_H, 6)
    ctx.fill()

    // Actor 2 value
    ctx.fillStyle = lead === 2 ? '#06b6d4' : 'rgba(255,255,255,0.45)'
    ctx.font = `bold ${lead === 2 ? 26 : 22}px -apple-system, system-ui, sans-serif`
    ctx.textAlign = 'left'
    ctx.fillText(stat.v2.toLocaleString(), 610 + BAR_W + 20, rowY + 30)

    rowY += ROW_H
  }

  // ── Verdict ────────────────────────────────────────────────────
  if (d.winner) {
    const verdictColor = d.winner === d.name1 ? '#f59e0b' : '#06b6d4'
    ctx.fillStyle = 'rgba(255,255,255,0.05)'
    drawRoundRect(ctx, 100, rowY + 8, 1000, 52, 12)
    ctx.fill()

    ctx.fillStyle = verdictColor
    ctx.font = 'bold 19px -apple-system, system-ui, sans-serif'
    ctx.textAlign = 'center'
    ctx.fillText(
      `\u{1F3C6} ${d.winner} leads in ${d.winnerLeads} of 3 metrics`,
      600,
      rowY + 40,
    )
  } else {
    ctx.fillStyle = 'rgba(255,255,255,0.3)'
    ctx.font = '17px -apple-system, system-ui, sans-serif'
    ctx.textAlign = 'center'
    ctx.fillText('All square \u2014 perfectly matched', 600, rowY + 40)
  }

  // ── Branding ───────────────────────────────────────────────────
  ctx.fillStyle = 'rgba(255,255,255,0.18)'
  ctx.font = '13px -apple-system, system-ui, sans-serif'
  ctx.textAlign = 'center'
  ctx.fillText('southcinemaanalytics.com', 600, 616)

  return C
}

export default function ShareButton(props: ShareCardData) {
  const [status, setStatus] = useState<'idle' | 'working' | 'done'>('idle')

  async function handleClick() {
    setStatus('working')
    try {
      const canvas = buildCanvas(props)
      await new Promise<void>((resolve, reject) => {
        canvas.toBlob(async (blob) => {
          if (!blob) { reject(new Error('canvas.toBlob failed')); return }
          const slug = `${props.name1.replace(/\s+/g, '-')}-vs-${props.name2.replace(/\s+/g, '-')}`
          const file = new File([blob], `${slug}.png`, { type: 'image/png' })
          try {
            if (navigator.canShare?.({ files: [file] })) {
              await navigator.share({
                files: [file],
                title: `${props.name1} vs ${props.name2} · South Cinema Analytics`,
              })
            } else {
              const url = URL.createObjectURL(blob)
              const a = document.createElement('a')
              a.href = url
              a.download = `${slug}.png`
              a.click()
              setTimeout(() => URL.revokeObjectURL(url), 5000)
            }
          } catch {
            // Share cancelled — not an error
          }
          resolve()
        }, 'image/png')
      })
      setStatus('done')
      setTimeout(() => setStatus('idle'), 3000)
    } catch (err) {
      console.error('Share card error:', err)
      setStatus('idle')
    }
  }

  return (
    <button
      onClick={handleClick}
      disabled={status === 'working'}
      className={`
        inline-flex items-center gap-2.5 px-7 py-3.5 rounded-full
        font-semibold text-sm transition-all duration-200 active:scale-95
        ${status === 'done'
          ? 'bg-emerald-500/15 text-emerald-400 border border-emerald-500/20'
          : 'bg-white text-black hover:bg-white/90 shadow-lg shadow-white/10'
        }
        disabled:opacity-60 disabled:cursor-not-allowed
      `}
    >
      {status === 'working' ? (
        <><span className="inline-block animate-spin">⏳</span> Generating…</>
      ) : status === 'done' ? (
        <>✓ Share card downloaded!</>
      ) : (
        <>📸 Generate Share Card</>
      )}
    </button>
  )
}
