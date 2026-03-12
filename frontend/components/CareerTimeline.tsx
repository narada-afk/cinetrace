'use client'
/**
 * CareerTimeline — animated SVG line chart (Films Per Year)
 *
 * Animation sequence:
 *   1. IntersectionObserver fires when card enters viewport (threshold 0.2)
 *   2. Both lines draw left → right via strokeDashoffset 0.0s – 1.2s
 *   3. Data points fade + scale in with 200ms delay after lines finish (1.4s total)
 *   4. Hover: point grows r 3.5 → 5, SVG tooltip appears
 */

import { useRef, useEffect, useState } from 'react'
import type { ActorMovie } from '@/lib/api'

/** Count films per release year (1975–2026 window). */
function buildTimeline(movies: ActorMovie[]): Map<number, number> {
  const map = new Map<number, number>()
  for (const m of movies) {
    const y = m.release_year
    if (y >= 1975 && y <= 2026) map.set(y, (map.get(y) ?? 0) + 1)
  }
  return map
}

interface HoveredPoint {
  actorIdx: 1 | 2
  year: number
  cx: number   // SVG x coordinate
  cy: number   // SVG y coordinate
  count: number
  color: string
}

export default function CareerTimeline({
  movies1,
  movies2,
  name1,
  name2,
}: {
  movies1: ActorMovie[]
  movies2: ActorMovie[]
  name1: string
  name2: string
}) {
  const containerRef = useRef<HTMLDivElement>(null)
  const line1Ref    = useRef<SVGPolylineElement>(null)
  const line2Ref    = useRef<SVGPolylineElement>(null)
  const [animated, setAnimated] = useState(false)
  const [hovered,  setHovered]  = useState<HoveredPoint | null>(null)

  // ── Data ──────────────────────────────────────────────────────────────────
  const c1 = buildTimeline(movies1)
  const c2 = buildTimeline(movies2)
  const allYears = [...new Set([...c1.keys(), ...c2.keys()])].sort()

  if (allYears.length < 3) return null

  const minYear  = allYears[0]
  const maxYear  = allYears[allYears.length - 1]
  const yearSpan = maxYear - minYear || 1
  const maxCount = Math.max(...allYears.flatMap((y) => [c1.get(y) ?? 0, c2.get(y) ?? 0]))
  if (maxCount === 0) return null

  const W   = 600
  const H   = 150
  const PAD = { t: 10, r: 16, b: 28, l: 24 }
  const cW  = W - PAD.l - PAD.r
  const cH  = H - PAD.t - PAD.b

  const toX = (y: number) => PAD.l + ((y - minYear) / yearSpan) * cW
  const toY = (c: number) => PAD.t + cH - (c / maxCount) * cH

  const pts1 = allYears.map((y) => `${toX(y)},${toY(c1.get(y) ?? 0)}`).join(' ')
  const pts2 = allYears.map((y) => `${toX(y)},${toY(c2.get(y) ?? 0)}`).join(' ')

  const firstLabel = Math.ceil(minYear / 5) * 5
  const xLabels: number[] = []
  for (let y = firstLabel; y <= maxYear; y += 5) xLabels.push(y)
  const yLabels = [0, Math.ceil(maxCount / 2), maxCount]

  // ── IntersectionObserver ──────────────────────────────────────────────────
  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    const observer = new IntersectionObserver(
      ([entry]) => { if (entry.isIntersecting) { setAnimated(true); observer.disconnect() } },
      { threshold: 0.2 },
    )
    observer.observe(el)
    return () => observer.disconnect()
  }, [])

  // ── Line draw animation via strokeDashoffset ──────────────────────────────
  useEffect(() => {
    const els = [line1Ref.current, line2Ref.current].filter(Boolean) as SVGPolylineElement[]
    if (els.length === 0) return

    // Initialise: hide lines without transition
    els.forEach((el) => {
      const len = el.getTotalLength()
      el.style.strokeDasharray  = `${len}`
      el.style.strokeDashoffset = `${len}`
      el.style.transition       = 'none'
    })

    if (!animated) return

    // Double-RAF: commits initial styles before transition starts
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        els.forEach((el) => {
          el.style.transition       = 'stroke-dashoffset 1200ms ease-in-out'
          el.style.strokeDashoffset = '0'
        })
      })
    })
  }, [animated])

  // ── Shared dot styles ─────────────────────────────────────────────────────
  // Dots appear after lines finish: 1200ms line + 200ms buffer = 1400ms delay
  const dotStyle = animated
    ? {
        opacity:         0.9,
        transform:       'scale(1)',
        transformBox:    'fill-box' as const,
        transformOrigin: 'center',
        transition:      'opacity 300ms ease-out 1400ms, transform 300ms ease-out 1400ms',
      }
    : {
        opacity:         0,
        transform:       'scale(0)',
        transformBox:    'fill-box' as const,
        transformOrigin: 'center',
        transition:      'none',
      }

  // ── Tooltip geometry ──────────────────────────────────────────────────────
  // Clamp so tooltip never overflows the SVG viewBox
  const tipW = 82
  const tipH = 22
  let tipX = hovered ? hovered.cx - tipW / 2 : 0
  let tipY = hovered ? hovered.cy - tipH - 6 : 0
  if (hovered) {
    tipX = Math.max(PAD.l, Math.min(W - PAD.r - tipW, tipX))
    tipY = tipY < PAD.t ? hovered.cy + 6 : tipY   // flip below if too high
  }

  return (
    <div ref={containerRef} className="glass rounded-3xl p-6">
      {/* Legend */}
      <div className="flex gap-6 mb-4">
        <div className="flex items-center gap-2">
          <div className="w-5 h-0.5 rounded-full" style={{ background: '#f59e0b' }} />
          <span className="text-xs text-white/45">{name1}</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-5 h-0.5 rounded-full" style={{ background: '#06b6d4' }} />
          <span className="text-xs text-white/45">{name2}</span>
        </div>
      </div>

      <svg
        viewBox={`0 0 ${W} ${H}`}
        className="w-full h-auto overflow-visible"
        aria-label={`Career timeline: ${name1} vs ${name2}`}
      >
        {/* Horizontal grid */}
        {yLabels.map((v) => (
          <line
            key={v}
            x1={PAD.l} y1={toY(v)} x2={W - PAD.r} y2={toY(v)}
            stroke="rgba(255,255,255,0.05)" strokeWidth="1"
          />
        ))}

        {/* Y-axis labels */}
        {yLabels.map((v) => (
          <text key={v} x={PAD.l - 3} y={toY(v) + 4}
            textAnchor="end" fill="rgba(255,255,255,0.2)" fontSize="8">
            {v}
          </text>
        ))}

        {/* X-axis labels */}
        {xLabels.map((y) => (
          <text key={y} x={toX(y)} y={H - 4}
            textAnchor="middle" fill="rgba(255,255,255,0.2)" fontSize="8">
            {y}
          </text>
        ))}

        {/* ── Actor 1 line (amber) ────────────────────────────────────── */}
        <polyline
          ref={line1Ref}
          points={pts1}
          fill="none"
          stroke="#f59e0b"
          strokeWidth="2.5"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeOpacity="0.75"
        />

        {/* Actor 1 data points */}
        {allYears
          .filter((y) => (c1.get(y) ?? 0) > 0)
          .map((y) => {
            const cx     = toX(y)
            const cy     = toY(c1.get(y)!)
            const isHov  = hovered?.actorIdx === 1 && hovered.year === y
            return (
              <circle
                key={y}
                cx={cx} cy={cy}
                r={isHov ? 5 : 3.5}
                fill="#f59e0b"
                style={{ ...dotStyle, cursor: 'crosshair', transition: isHov ? 'r 150ms ease' : dotStyle.transition }}
                onMouseEnter={() => setHovered({ actorIdx: 1, year: y, cx, cy, count: c1.get(y)!, color: '#f59e0b' })}
                onMouseLeave={() => setHovered(null)}
              />
            )
          })}

        {/* ── Actor 2 line (cyan) ─────────────────────────────────────── */}
        <polyline
          ref={line2Ref}
          points={pts2}
          fill="none"
          stroke="#06b6d4"
          strokeWidth="2.5"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeOpacity="0.75"
        />

        {/* Actor 2 data points */}
        {allYears
          .filter((y) => (c2.get(y) ?? 0) > 0)
          .map((y) => {
            const cx    = toX(y)
            const cy    = toY(c2.get(y)!)
            const isHov = hovered?.actorIdx === 2 && hovered.year === y
            return (
              <circle
                key={y}
                cx={cx} cy={cy}
                r={isHov ? 5 : 3.5}
                fill="#06b6d4"
                style={{ ...dotStyle, cursor: 'crosshair', transition: isHov ? 'r 150ms ease' : dotStyle.transition }}
                onMouseEnter={() => setHovered({ actorIdx: 2, year: y, cx, cy, count: c2.get(y)!, color: '#06b6d4' })}
                onMouseLeave={() => setHovered(null)}
              />
            )
          })}

        {/* ── Hover tooltip ────────────────────────────────────────────── */}
        {hovered && (
          <g pointerEvents="none">
            <rect
              x={tipX} y={tipY}
              width={tipW} height={tipH}
              rx="5"
              fill="rgba(10,10,20,0.88)"
              stroke={hovered.color}
              strokeWidth="0.8"
              strokeOpacity="0.5"
            />
            <text
              x={tipX + tipW / 2}
              y={tipY + 14}
              textAnchor="middle"
              fill="white"
              fontSize="9"
              fontWeight="600"
            >
              {hovered.year} · {hovered.count} film{hovered.count !== 1 ? 's' : ''}
            </text>
          </g>
        )}
      </svg>
    </div>
  )
}
