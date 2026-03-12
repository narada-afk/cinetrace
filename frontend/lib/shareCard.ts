/**
 * shareCard.ts — shared canvas share-card generator.
 * Imported by both ShareButton (download) and ShareSheet (Instagram download).
 */

export interface ShareCardData {
  name1: string
  name2: string
  industry1: string
  industry2: string
  films1: number
  films2: number
  yearsActive1: number
  yearsActive2: number
  avgRating1: number
  avgRating2: number
  uniqueDirs1: number
  uniqueDirs2: number
  coStars1: number
  coStars2: number
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

export function buildCanvas(d: ShareCardData): HTMLCanvasElement {
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
  ctx.lineTo(600, 540)
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
  const f1 = d.name1.length > 16 ? 38 : d.name1.length > 12 ? 46 : 54
  ctx.font = `bold ${f1}px -apple-system, system-ui, sans-serif`
  ctx.fillText(d.name1, 60, 152)

  // ── Actor 2 (right) ────────────────────────────────────────────
  ctx.textAlign = 'right'
  ctx.fillStyle = 'rgba(6,182,212,0.7)'
  ctx.font = '13px -apple-system, system-ui, sans-serif'
  ctx.fillText((d.industry2 || '').toUpperCase(), 1140, 98)

  ctx.fillStyle = '#ffffff'
  const f2 = d.name2.length > 16 ? 38 : d.name2.length > 12 ? 46 : 54
  ctx.font = `bold ${f2}px -apple-system, system-ui, sans-serif`
  ctx.fillText(d.name2, 1140, 152)

  // ── Stat bars ──────────────────────────────────────────────────
  const stats = [
    { label: 'FILMS',            v1: d.films1,        v2: d.films2,        fmt: (v: number) => String(v) },
    { label: 'YEARS ACTIVE',     v1: d.yearsActive1,  v2: d.yearsActive2,  fmt: (v: number) => String(v) },
    { label: 'AVG RATING',       v1: d.avgRating1,    v2: d.avgRating2,    fmt: (v: number) => v.toFixed(1) },
    { label: 'UNIQUE DIRECTORS', v1: d.uniqueDirs1,   v2: d.uniqueDirs2,   fmt: (v: number) => String(v) },
    { label: 'CO-STARS',         v1: d.coStars1,      v2: d.coStars2,      fmt: (v: number) => String(v) },
  ]

  const BAR_W = 430
  const BAR_H = 10
  const ROW_H = 62
  let rowY = 192

  for (const stat of stats) {
    const lead = stat.v1 > stat.v2 ? 1 : stat.v2 > stat.v1 ? 2 : 0
    const maxV = Math.max(stat.v1, stat.v2) || 1

    ctx.fillStyle = 'rgba(255,255,255,0.3)'
    ctx.font = '11px -apple-system, system-ui, sans-serif'
    ctx.textAlign = 'center'
    ctx.fillText(stat.label, 600, rowY + 6)

    const w1 = Math.round((stat.v1 / maxV) * BAR_W)
    ctx.fillStyle = 'rgba(255,255,255,0.07)'
    drawRoundRect(ctx, 600 - BAR_W - 10, rowY + 14, BAR_W, BAR_H, 5)
    ctx.fill()
    ctx.fillStyle = lead === 1 ? '#f59e0b' : 'rgba(255,255,255,0.18)'
    drawRoundRect(ctx, 600 - w1 - 10, rowY + 14, w1, BAR_H, 5)
    ctx.fill()

    ctx.fillStyle = lead === 1 ? '#f59e0b' : 'rgba(255,255,255,0.45)'
    ctx.font = `bold ${lead === 1 ? 22 : 18}px -apple-system, system-ui, sans-serif`
    ctx.textAlign = 'right'
    ctx.fillText(stat.fmt(stat.v1), 600 - BAR_W - 20, rowY + 28)

    const w2 = Math.round((stat.v2 / maxV) * BAR_W)
    ctx.fillStyle = 'rgba(255,255,255,0.07)'
    drawRoundRect(ctx, 610, rowY + 14, BAR_W, BAR_H, 5)
    ctx.fill()
    ctx.fillStyle = lead === 2 ? '#06b6d4' : 'rgba(255,255,255,0.18)'
    drawRoundRect(ctx, 610, rowY + 14, w2, BAR_H, 5)
    ctx.fill()

    ctx.fillStyle = lead === 2 ? '#06b6d4' : 'rgba(255,255,255,0.45)'
    ctx.font = `bold ${lead === 2 ? 22 : 18}px -apple-system, system-ui, sans-serif`
    ctx.textAlign = 'left'
    ctx.fillText(stat.fmt(stat.v2), 610 + BAR_W + 20, rowY + 28)

    rowY += ROW_H
  }

  // ── Verdict ────────────────────────────────────────────────────
  if (d.winner) {
    const verdictColor = d.winner === d.name1 ? '#f59e0b' : '#06b6d4'
    ctx.fillStyle = 'rgba(255,255,255,0.05)'
    drawRoundRect(ctx, 100, rowY + 6, 1000, 48, 12)
    ctx.fill()

    ctx.fillStyle = verdictColor
    ctx.font = 'bold 18px -apple-system, system-ui, sans-serif'
    ctx.textAlign = 'center'
    ctx.fillText(`\u{1F3C6} ${d.winner} leads in ${d.winnerLeads} of 5 metrics`, 600, rowY + 36)
  } else {
    ctx.fillStyle = 'rgba(255,255,255,0.3)'
    ctx.font = '16px -apple-system, system-ui, sans-serif'
    ctx.textAlign = 'center'
    ctx.fillText('All square \u2014 perfectly matched', 600, rowY + 36)
  }

  // ── Branding ───────────────────────────────────────────────────
  ctx.fillStyle = 'rgba(255,255,255,0.18)'
  ctx.font = '13px -apple-system, system-ui, sans-serif'
  ctx.textAlign = 'center'
  ctx.fillText('southcinemaanalytics.com', 600, 616)

  return C
}

/** Download the canvas as a PNG file. */
export async function downloadCanvas(canvas: HTMLCanvasElement, filename: string) {
  await new Promise<void>((resolve, reject) => {
    canvas.toBlob((blob) => {
      if (!blob) { reject(new Error('toBlob failed')); return }
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = filename
      a.click()
      setTimeout(() => URL.revokeObjectURL(url), 5000)
      resolve()
    }, 'image/png')
  })
}
