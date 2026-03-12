'use client'
/**
 * VerdictCard — client component
 *
 * Renders a single shared comparison bar per metric.
 * Each bar grows from 0 → target width via CSS transition triggered by
 * IntersectionObserver, so the animation fires the first time the card
 * scrolls into view.
 *
 * Bar anatomy (per actor):
 *
 *   ┌─────────────────────────────────────────────────────┐
 *   │ Actor Name                               Value       │  ← text layer (z-10)
 *   └─────────────────────────────────────────────────────┘
 *     ████████████████████████████████░░░░░░░░░░░░░░░░░░░   ← fill (proportional)
 *
 * The fill is absolutely positioned behind the text.
 * Leading actor → accent color; trailing actor → dim grey.
 * Tied actors → both get their accent color at 100% width.
 */

import { useRef, useEffect, useState } from 'react'
import type { ActorProfile, ActorMovie, Collaborator, DirectorCollab } from '@/lib/api'
import { calcYearsActive, calcAvgRating } from '@/lib/metrics'

// ── Types ─────────────────────────────────────────────────────────────────────

interface ActorData {
  profile: ActorProfile
  movies: ActorMovie[]
  collaborators: Collaborator[]
  directors: DirectorCollab[]
}

// ── MetricBar ─────────────────────────────────────────────────────────────────

function MetricBar({
  name,
  displayValue,
  pct,
  color,
  isLeading,
  accentColor,
  animated,
  delay,
}: {
  name: string
  displayValue: string
  /** 0–100: proportional to max value across both actors */
  pct: number
  /** Fill color (accent or dim grey) */
  color: string
  /** Whether this bar is the leader (affects text contrast) */
  isLeading: boolean
  /** Accent color for this actor — used on dim fills */
  accentColor: string
  animated: boolean
  /** CSS transition-delay in seconds */
  delay: number
}) {
  // On a bright colored fill → white text. On a dim grey fill → accent-tinted text.
  const nameColor  = isLeading ? 'rgba(255,255,255,0.95)' : 'rgba(255,255,255,0.45)'
  const valueColor = isLeading ? '#ffffff'                 : 'rgba(255,255,255,0.38)'

  return (
    <div
      className="relative h-11 rounded-xl overflow-hidden"
      style={{ background: 'rgba(255,255,255,0.04)' }}
    >
      {/* Animated fill */}
      <div
        className="absolute left-0 top-0 bottom-0 rounded-xl"
        style={{
          width: animated ? `${pct}%` : '0%',
          background: color,
          transition: `width 0.8s ease-out ${delay}s`,
          minWidth: animated ? '2px' : '0',
        }}
      />

      {/* Text layer — always fully visible above the fill */}
      <div className="absolute inset-0 flex items-center justify-between px-4 z-10">
        <span
          className="text-sm font-semibold truncate pr-3 leading-none"
          style={{ color: nameColor }}
        >
          {name}
        </span>
        <span
          className="text-sm font-bold tabular-nums flex-shrink-0 leading-none"
          style={{ color: valueColor }}
        >
          {displayValue}
        </span>
      </div>
    </div>
  )
}

// ── VerdictCard ───────────────────────────────────────────────────────────────

export default function VerdictCard({ data1, data2 }: { data1: ActorData; data2: ActorData }) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [animated, setAnimated] = useState(false)

  useEffect(() => {
    const el = containerRef.current
    if (!el) return

    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setAnimated(true)
          observer.disconnect()
        }
      },
      { threshold: 0.25 },
    )
    observer.observe(el)
    return () => observer.disconnect()
  }, [])

  const p1 = data1.profile
  const p2 = data2.profile

  const yrs1 = calcYearsActive(p1)
  const yrs2 = calcYearsActive(p2)
  const rat1 = calcAvgRating(data1.movies)
  const rat2 = calcAvgRating(data2.movies)

  // Sequential reveal delays, one step per metric
  const METRICS = [
    { label: 'Films',            v1: p1.film_count,              v2: p2.film_count,              d1: String(p1.film_count),          d2: String(p2.film_count),          delay: 0.1 },
    { label: 'Years Active',     v1: yrs1,                       v2: yrs2,                       d1: String(yrs1),                   d2: String(yrs2),                   delay: 0.2 },
    { label: 'Avg Rating',       v1: rat1,                       v2: rat2,                       d1: rat1.toFixed(1),                d2: rat2.toFixed(1),                delay: 0.3 },
    { label: 'Unique Directors', v1: data1.directors.length,     v2: data2.directors.length,     d1: String(data1.directors.length), d2: String(data2.directors.length), delay: 0.4 },
    { label: 'Co-Stars',         v1: data1.collaborators.length, v2: data2.collaborators.length, d1: String(data1.collaborators.length), d2: String(data2.collaborators.length), delay: 0.5 },
  ]

  const wins1 = METRICS.filter((m) => m.v1 > m.v2).length
  const wins2 = METRICS.filter((m) => m.v2 > m.v1).length
  const winner = wins1 > wins2 ? p1 : wins2 > wins1 ? p2 : null
  const winnerLeads = Math.max(wins1, wins2)
  const winnerColor = winner?.name === p1.name ? '#f59e0b' : '#06b6d4'

  return (
    <div ref={containerRef} className="glass rounded-3xl p-6 sm:p-8 flex flex-col gap-8">

      {/* Trophy header */}
      <div className="flex flex-col items-center gap-2 text-center">
        <p className="text-2xl">🏆</p>
        <p className="text-[10px] font-bold uppercase tracking-[0.2em] text-white/30">Verdict</p>
        {winner ? (
          <p className="text-lg font-bold" style={{ color: winnerColor }}>
            {winner.name} leads in {winnerLeads} of 5 metrics
          </p>
        ) : (
          <p className="text-lg font-bold text-white/60">All square — perfectly matched</p>
        )}
      </div>

      {/* Metric rows */}
      <div className="flex flex-col gap-5">
        {METRICS.map((m) => {
          const maxV   = Math.max(m.v1, m.v2) || 1
          const pct1   = (m.v1 / maxV) * 100
          const pct2   = (m.v2 / maxV) * 100
          const lead   = m.v1 > m.v2 ? 1 : m.v2 > m.v1 ? 2 : 0   // 0 = tie

          // Leading actor gets accent fill; trailing gets very dim grey
          const leading1 = lead === 0 || lead === 1   // tie counts as both leading
          const leading2 = lead === 0 || lead === 2
          const fill1    = leading1 ? '#f59e0b'               : 'rgba(255,255,255,0.08)'
          const fill2    = leading2 ? '#06b6d4'               : 'rgba(255,255,255,0.08)'

          return (
            <div key={m.label} className="flex flex-col gap-2">
              {/* Metric label */}
              <p className="text-[11px] text-white/35 uppercase tracking-widest text-center">
                {m.label}
              </p>

              {/* Actor 1 bar */}
              <MetricBar
                name={p1.name}
                displayValue={m.d1}
                pct={pct1}
                color={fill1}
                isLeading={leading1}
                accentColor="#f59e0b"
                animated={animated}
                delay={m.delay}
              />

              {/* Actor 2 bar */}
              <MetricBar
                name={p2.name}
                displayValue={m.d2}
                pct={pct2}
                color={fill2}
                isLeading={leading2}
                accentColor="#06b6d4"
                animated={animated}
                delay={m.delay}
              />
            </div>
          )
        })}
      </div>
    </div>
  )
}
