'use client'
/**
 * VerdictCard — client component
 *
 * ONE bar per metric, split by share:
 *
 *  ┌─────────────────────────────────────────────────────────────────┐
 *  │  Prabhas     75  ████████████████████████████████████████████   │ ← amber, 75 %
 *  │                  ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░████  25  Allu  │ ← cyan,  25 %
 *  └─────────────────────────────────────────────────────────────────┘
 *
 *  pct1 = v1 / (v1 + v2)   pct2 = v2 / (v1 + v2)   → always sum to 100 %
 *
 * Amber fills left → right; cyan fills right → left.
 * Both animate 0 → target on first IntersectionObserver trigger.
 *
 * Victory reveal sequence (all triggered once by IntersectionObserver):
 *  ① Bars grow          0.1 – ~1.4s
 *  ② Winner bar sweeps  bar.delay + 0.7s each
 *  ③ Trophy glow        1.0s delay, 900ms
 *  ④ Verdict pulse      1.2s delay, 800ms
 */

import { useRef, useEffect, useState } from 'react'
import type { ActorProfile, ActorMovie, Collaborator, DirectorCollab } from '@/lib/api'
import { calcYearsActive, calcAvgRating } from '@/lib/metrics'

interface ActorData {
  profile: ActorProfile
  movies: ActorMovie[]
  collaborators: Collaborator[]
  directors: DirectorCollab[]
}

export default function VerdictCard({ data1, data2 }: { data1: ActorData; data2: ActorData }) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [animated, setAnimated] = useState(false)

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

  const p1 = data1.profile
  const p2 = data2.profile

  const yrs1 = calcYearsActive(p1)
  const yrs2 = calcYearsActive(p2)
  const rat1 = calcAvgRating(data1.movies)
  const rat2 = calcAvgRating(data2.movies)

  const METRICS = [
    { label: 'Films',            v1: p1.film_count,              v2: p2.film_count,              d1: String(p1.film_count),              d2: String(p2.film_count),              delay: 0.1 },
    { label: 'Years Active',     v1: yrs1,                       v2: yrs2,                       d1: String(yrs1),                       d2: String(yrs2),                       delay: 0.2 },
    { label: 'Avg Rating',       v1: rat1,                       v2: rat2,                       d1: rat1.toFixed(1),                    d2: rat2.toFixed(1),                    delay: 0.3 },
    { label: 'Unique Directors', v1: data1.directors.length,     v2: data2.directors.length,     d1: String(data1.directors.length),     d2: String(data2.directors.length),     delay: 0.4 },
    { label: 'Co-Stars',         v1: data1.collaborators.length, v2: data2.collaborators.length, d1: String(data1.collaborators.length), d2: String(data2.collaborators.length), delay: 0.5 },
  ]

  const wins1 = METRICS.filter((m) => m.v1 > m.v2).length
  const wins2 = METRICS.filter((m) => m.v2 > m.v1).length
  const winner = wins1 > wins2 ? p1 : wins2 > wins1 ? p2 : null
  const winnerLeads = Math.max(wins1, wins2)
  const winnerColor = winner?.name === p1.name ? '#f59e0b' : '#06b6d4'

  // ── Animation styles (applied only after IO fires) ──────────────────────────

  // ④ Verdict text: scale 0.96→1.05→1, opacity 0.7→1 — delayed until bars settle
  const verdictStyle = animated
    ? { animation: 'verdictPulse 800ms ease-out 1.2s both' }
    : { opacity: 0.8, transform: 'scale(0.96)' }

  // ③ Trophy: drop-shadow glow burst then settles to a whisper
  const trophyStyle = animated
    ? { animation: 'trophyGlow 900ms ease-out 1.0s both', display: 'inline-block' }
    : { display: 'inline-block' }

  return (
    <>
      {/* ── Keyframes injected once alongside the component ─────────────── */}
      <style>{`
        @keyframes verdictPulse {
          0%   { transform: scale(0.96); opacity: 0.7; }
          55%  { transform: scale(1.05); opacity: 1;   }
          100% { transform: scale(1);   opacity: 1;   }
        }
        @keyframes trophyGlow {
          0%   { filter: drop-shadow(0 0 0px  rgba(255,215,0,0));    }
          40%  { filter: drop-shadow(0 0 14px rgba(255,215,0,0.85)); }
          100% { filter: drop-shadow(0 0 4px  rgba(255,215,0,0.2));  }
        }
        @keyframes barSweep {
          0%   { transform: translateX(-100%); opacity: 1; }
          100% { transform: translateX(200%);  opacity: 0; }
        }
      `}</style>

      <div ref={containerRef} className="glass rounded-3xl p-6 sm:p-8 flex flex-col gap-8">

        {/* Trophy header */}
        <div className="flex flex-col items-center gap-2 text-center">
          {/* ③ Trophy glow */}
          <p className="text-2xl" style={trophyStyle}>🏆</p>
          <p className="text-[10px] font-bold uppercase tracking-[0.2em] text-white/30">Verdict</p>
          {/* ④ Verdict pulse */}
          {winner ? (
            <p className="text-lg font-bold" style={{ color: winnerColor, ...verdictStyle }}>
              {winner.name} leads in {winnerLeads} of 5 metrics
            </p>
          ) : (
            <p className="text-lg font-bold text-white/60" style={verdictStyle}>
              All square — perfectly matched
            </p>
          )}
        </div>

        {/* One split bar per metric */}
        <div className="flex flex-col gap-5">
          {METRICS.map((m) => {
            const total = m.v1 + m.v2 || 1            // avoid ÷0
            const pct1  = (m.v1 / total) * 100        // actor 1 share (left)
            const pct2  = (m.v2 / total) * 100        // actor 2 share (right)

            // 0 = tie, 1 = actor1 wins, 2 = actor2 wins
            const lead = m.v1 > m.v2 ? 1 : m.v2 > m.v1 ? 2 : 0

            const fill1 = (lead === 0 || lead === 1) ? '#f59e0b' : 'rgba(255,255,255,0.15)'
            const fill2 = (lead === 0 || lead === 2) ? '#06b6d4' : 'rgba(255,255,255,0.15)'
            const dur1  = lead === 1 ? '0.7s' : lead === 2 ? '0.9s' : '0.8s'
            const dur2  = lead === 2 ? '0.7s' : lead === 1 ? '0.9s' : '0.8s'

            const glow =
              lead === 1 ? '0 0 12px rgba(245,158,11,0.35)' :
              lead === 2 ? '0 0 12px rgba(6,182,212,0.35)'  : 'none'

            // Text colours: full white on accent fills; muted on grey fills
            const text1 = lead === 2 ? 'rgba(255,255,255,0.45)' : '#ffffff'
            const text2 = lead === 1 ? 'rgba(255,255,255,0.45)' : '#ffffff'

            // ② Sweep shimmer: fires right after the winning bar finishes growing
            //    Winner bar duration is always 0.7s, so sweep delay = bar delay + 0.7s
            const sweepDelay1 = `${m.delay + 0.7}s`
            const sweepDelay2 = `${m.delay + 0.7}s`

            return (
              <div key={m.label} className="flex flex-col gap-2">

                {/* Metric label */}
                <p className="text-[11px] text-white/35 uppercase tracking-widest text-center">
                  {m.label}
                </p>

                {/* ── Single split bar ───────────────────────────────── */}
                <div
                  className="relative h-11 rounded-xl overflow-hidden"
                  style={{ background: 'rgba(255,255,255,0.06)', boxShadow: animated ? glow : 'none' }}
                >
                  {/* Actor 1 — amber when winning, grey when losing */}
                  <div
                    className="absolute left-0 top-0 bottom-0 flex items-center overflow-hidden"
                    style={{
                      width: animated ? `${pct1}%` : '0%',
                      background: fill1,
                      transition: `width ${dur1} ease-out ${m.delay}s`,
                    }}
                  >
                    <span className="pl-4 text-sm font-semibold whitespace-nowrap leading-none" style={{ color: text1 }}>
                      {p1.name}
                    </span>
                    <span className="ml-auto pr-3 text-sm font-bold tabular-nums leading-none" style={{ color: text1 }}>
                      {m.d1}
                    </span>

                    {/* ② Sweep shimmer on winner bar only */}
                    {animated && lead === 1 && (
                      <div
                        aria-hidden="true"
                        className="absolute inset-0 pointer-events-none"
                        style={{
                          background: 'linear-gradient(90deg, transparent 0%, rgba(255,255,255,0.28) 50%, transparent 100%)',
                          animation: `barSweep 900ms ease-in-out ${sweepDelay1} both`,
                        }}
                      />
                    )}
                  </div>

                  {/* Actor 2 — cyan when winning, grey when losing */}
                  <div
                    className="absolute right-0 top-0 bottom-0 flex items-center overflow-hidden"
                    style={{
                      width: animated ? `${pct2}%` : '0%',
                      background: fill2,
                      transition: `width ${dur2} ease-out ${m.delay}s`,
                    }}
                  >
                    <span className="pl-3 text-sm font-semibold whitespace-nowrap leading-none" style={{ color: text2 }}>
                      {m.d2}
                    </span>
                    <span className="mr-auto pl-1 pr-4 text-sm font-bold tabular-nums leading-none" style={{ color: text2 }}>
                      {p2.name}
                    </span>

                    {/* ② Sweep shimmer on winner bar only */}
                    {animated && lead === 2 && (
                      <div
                        aria-hidden="true"
                        className="absolute inset-0 pointer-events-none"
                        style={{
                          background: 'linear-gradient(90deg, transparent 0%, rgba(255,255,255,0.28) 50%, transparent 100%)',
                          animation: `barSweep 900ms ease-in-out ${sweepDelay2} both`,
                        }}
                      />
                    )}
                  </div>
                </div>
                {/* ─────────────────────────────────────────────────────── */}

              </div>
            )
          })}
        </div>
      </div>
    </>
  )
}
