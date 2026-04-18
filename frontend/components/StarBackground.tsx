'use client'

import { useEffect, useRef } from 'react'

// ─── Types ────────────────────────────────────────────────────────────────────

interface Star {
  x:             number
  y:             number
  radius:        number
  opacity:       number
  speed:         number
  twinkleSpeed:  number
  twinkleOffset: number
  parallaxFactor:number
  layer:         0 | 1 | 2   // 0 = bg, 1 = mid, 2 = fg
  glow:          number       // shadowBlur — only non-zero on fg stars
}

// density=1 → full counts; density<1 → reduced (inner pages)
type Density = 'full' | 'reduced'

const BASE_LAYERS = [
  // background: dense, tiny, barely visible, very slow
  { count: 100, rMin: 0.3, rMax: 0.7,  oMin: 0.08, oMax: 0.22, sMin: 0.02, sMax: 0.07, pMin: 0.003, pMax: 0.010, twinkle: false, glowMax: 0 },
  // mid: medium, subtle twinkle, slight parallax
  { count:  50, rMin: 0.7, rMax: 1.4,  oMin: 0.20, oMax: 0.45, sMin: 0.05, sMax: 0.14, pMin: 0.008, pMax: 0.025, twinkle: true,  glowMax: 0 },
  // foreground: few, slightly larger, soft glow
  { count:  15, rMin: 1.4, rMax: 2.2,  oMin: 0.40, oMax: 0.70, sMin: 0.02, sMax: 0.06, pMin: 0.015, pMax: 0.040, twinkle: true,  glowMax: 5 },
] as const

// ─── Helpers ──────────────────────────────────────────────────────────────────

function rand(min: number, max: number) { return Math.random() * (max - min) + min }

function buildStars(width: number, height: number, scale: number): Star[] {
  const stars: Star[] = []
  for (let layer = 0; layer < BASE_LAYERS.length; layer++) {
    const L     = BASE_LAYERS[layer]
    const count = Math.round(L.count * scale)
    for (let i = 0; i < count; i++) {
      stars.push({
        x:             rand(0, width),
        y:             rand(0, height),
        radius:        rand(L.rMin, L.rMax),
        opacity:       rand(L.oMin, L.oMax),
        speed:         rand(L.sMin, L.sMax),
        twinkleSpeed:  L.twinkle ? rand(0.004, 0.016) : 0,
        twinkleOffset: Math.random() * Math.PI * 2,
        parallaxFactor:rand(L.pMin, L.pMax),
        layer:         layer as 0 | 1 | 2,
        glow:          L.glowMax > 0 ? rand(2, L.glowMax) : 0,
      })
    }
  }
  return stars
}

// ─── Component ────────────────────────────────────────────────────────────────

/**
 * Global 3-layer canvas starfield.
 *
 * Place ONCE in app/layout.tsx with `fixed inset-0 -z-10`.
 * Persists across all page navigations — never re-mounts.
 *
 * Optional `density` prop:
 *   "full"    → homepage (100 % star count, default)
 *   "reduced" → inner pages (70 % star count)
 *
 * Note: changing `density` after first mount has no effect by design —
 * it is intentionally read only once to avoid re-building stars on navigation.
 */
export default function StarBackground({ density = 'full' }: { density?: Density }) {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const rafRef    = useRef<number>()

  // Capture density at mount time — we deliberately do NOT want this in the
  // effect deps array, so the canvas is only ever initialised once.
  const densityScale = density === 'reduced' ? 0.7 : 1.0

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')
    if (!ctx) return

    // Size the canvas to the full viewport
    let width  = window.innerWidth
    let height = window.innerHeight
    canvas.width  = width
    canvas.height = height

    let stars = buildStars(width, height, densityScale)

    // ── Constellation connections (mid-layer only, pre-computed) ─────────────
    const midStars = stars.filter(s => s.layer === 1)
    const connections: [Star, Star][] = []
    const MAX_DIST = 140
    const MAX_CONN = 14
    outer: for (let i = 0; i < midStars.length; i++) {
      for (let j = i + 1; j < midStars.length; j++) {
        if (connections.length >= MAX_CONN) break outer
        const dx = midStars[i].x - midStars[j].x
        const dy = midStars[i].y - midStars[j].y
        if (Math.sqrt(dx * dx + dy * dy) < MAX_DIST) {
          connections.push([midStars[i], midStars[j]])
        }
      }
    }

    // ── Mouse parallax (ultra-slow lerp = premium feel) ───────────────────────
    const target = { x: 0, y: 0 }
    const smooth = { x: 0, y: 0 }

    const onMouseMove = (e: MouseEvent) => {
      target.x = (e.clientX - width  / 2) * 0.4
      target.y = (e.clientY - height / 2) * 0.4
    }

    const onResize = () => {
      width  = window.innerWidth
      height = window.innerHeight
      canvas.width  = width
      canvas.height = height
      stars = buildStars(width, height, densityScale)
    }

    window.addEventListener('mousemove', onMouseMove, { passive: true })
    window.addEventListener('resize',    onResize,    { passive: true })

    // ── Render loop ───────────────────────────────────────────────────────────
    let frame = 0

    const draw = () => {
      ctx.clearRect(0, 0, width, height)
      frame++

      smooth.x += (target.x - smooth.x) * 0.035
      smooth.y += (target.y - smooth.y) * 0.035

      // 0. Center ambient glow — very faint blue-purple radial, sits behind hero text
      const cx = width / 2, cy = height * 0.36
      const grd = ctx.createRadialGradient(cx, cy, 0, cx, cy, Math.min(width, height) * 0.48)
      grd.addColorStop(0,    'rgba(79, 172, 254, 0.032)')
      grd.addColorStop(0.42, 'rgba(168, 85, 247, 0.020)')
      grd.addColorStop(1,    'rgba(0, 0, 0, 0)')
      ctx.fillStyle = grd
      ctx.fillRect(0, 0, width, height)

      // 1. Constellation lines (drawn first, beneath stars)
      ctx.lineWidth = 0.6
      for (const [a, b] of connections) {
        ctx.beginPath()
        ctx.moveTo(a.x + smooth.x * a.parallaxFactor, a.y + smooth.y * a.parallaxFactor)
        ctx.lineTo(b.x + smooth.x * b.parallaxFactor, b.y + smooth.y * b.parallaxFactor)
        ctx.strokeStyle = 'rgba(160, 200, 255, 0.07)'
        ctx.stroke()
      }

      // 1b. Node dots at constellation endpoints — "network" feel
      for (const [a, b] of connections) {
        for (const s of [a, b]) {
          ctx.beginPath()
          ctx.arc(
            s.x + smooth.x * s.parallaxFactor,
            s.y + smooth.y * s.parallaxFactor,
            1.4, 0, Math.PI * 2,
          )
          ctx.fillStyle = 'rgba(160, 200, 255, 0.13)'
          ctx.fill()
        }
      }

      // 2. BG + MID stars — no shadow for performance
      ctx.shadowBlur = 0
      for (const s of stars) {
        if (s.layer === 2) continue

        s.y -= s.speed
        if (s.y < -2) { s.y = height + 2; s.x = Math.random() * width }

        const twinkle = s.twinkleSpeed > 0
          ? Math.sin(frame * s.twinkleSpeed + s.twinkleOffset) * 0.10 : 0
        const alpha = Math.max(0.03, Math.min(0.75, s.opacity + twinkle))

        ctx.beginPath()
        ctx.arc(
          s.x + smooth.x * s.parallaxFactor,
          s.y + smooth.y * s.parallaxFactor,
          s.radius, 0, Math.PI * 2,
        )
        ctx.fillStyle = `rgba(215, 232, 255, ${alpha})`
        ctx.fill()
      }

      // 3. FG stars — shadowBlur only for this small group (15 stars max)
      ctx.shadowColor = 'rgba(180, 220, 255, 0.8)'
      for (const s of stars) {
        if (s.layer !== 2) continue

        s.y -= s.speed
        if (s.y < -2) { s.y = height + 2; s.x = Math.random() * width }

        const twinkle = Math.sin(frame * s.twinkleSpeed + s.twinkleOffset) * 0.12
        const alpha   = Math.max(0.2, Math.min(0.85, s.opacity + twinkle))
        ctx.shadowBlur = s.glow * (0.7 + Math.sin(frame * s.twinkleSpeed + s.twinkleOffset) * 0.3)

        ctx.beginPath()
        ctx.arc(
          s.x + smooth.x * s.parallaxFactor,
          s.y + smooth.y * s.parallaxFactor,
          s.radius, 0, Math.PI * 2,
        )
        ctx.fillStyle = `rgba(230, 242, 255, ${alpha})`
        ctx.fill()
      }
      ctx.shadowBlur = 0  // always reset — prevents bleed into other draws

      rafRef.current = requestAnimationFrame(draw)
    }

    rafRef.current = requestAnimationFrame(draw)

    return () => {
      if (rafRef.current) cancelAnimationFrame(rafRef.current)
      window.removeEventListener('mousemove', onMouseMove)
      window.removeEventListener('resize',    onResize)
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []) // intentionally empty — initialise once, persist across navigations

  return (
    <canvas
      ref={canvasRef}
      aria-hidden="true"
      // fixed + full-viewport so it covers every page without re-mounting
      className="fixed inset-0 w-full h-full pointer-events-none"
      style={{ zIndex: 0 }}
    />
  )
}
