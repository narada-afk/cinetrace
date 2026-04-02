'use client'

/**
 * TrustBadge
 * ----------
 * Displays the data confidence score, last verified timestamp,
 * and data sources.
 *
 * Modes:
 *   compact  — single pill, e.g. "✓ 92.4% verified"   (for header / footer)
 *   full     — card with all details                   (for stats / about page)
 *
 * Data is fetched once on mount from GET /trust.
 * Falls back silently to null state if the endpoint is unavailable
 * (migration not yet run, backend down, etc.).
 */

import { useEffect, useState } from 'react'

interface TrustData {
  data_confidence_score:  number | null
  avg_actor_score:        number | null
  avg_movie_score:        number | null
  collab_integrity:       number | null
  validation_passed:      boolean | null
  ghost_collab_count:     number
  duplicate_count:        number
  total_actors:           number
  total_movies:           number
  total_collab_pairs:     number
  sources_used:           string[]
  last_verified:          string | null
  last_verified_human:    string | null
}

function useTrust() {
  const [data, setData]       = useState<TrustData | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const api = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000'
    fetch(`${api}/trust`)
      .then(r => r.ok ? r.json() : null)
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [])

  return { data, loading }
}

// ── Score colour ──────────────────────────────────────────────────────────────

function scoreColor(score: number | null): string {
  if (score === null) return 'rgba(255,255,255,0.3)'
  if (score >= 85)    return '#4ade80'   // green
  if (score >= 65)    return '#fbbf24'   // amber
  return '#f87171'                        // red
}

function scoreLabel(score: number | null, passed: boolean | null): string {
  if (score === null) return 'Unscored'
  if (!passed)        return 'Issues found'
  if (score >= 85)    return 'Production ready'
  if (score >= 65)    return 'Needs attention'
  return 'Critical issues'
}

// ── Compact pill ──────────────────────────────────────────────────────────────

export function TrustBadgeCompact() {
  const { data, loading } = useTrust()

  if (loading) {
    return (
      <div
        className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs"
        style={{ background: 'rgba(255,255,255,0.05)', color: 'rgba(255,255,255,0.25)' }}
      >
        <span className="w-1.5 h-1.5 rounded-full bg-white/20 animate-pulse" />
        Checking…
      </div>
    )
  }

  const score  = data?.data_confidence_score ?? null
  const passed = data?.validation_passed     ?? null
  const human  = data?.last_verified_human   ?? null
  const color  = scoreColor(score)
  const icon   = passed === false ? '⚠' : '✓'

  return (
    <div
      className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-medium transition-opacity duration-300"
      style={{
        background: `${color}15`,
        border:     `1px solid ${color}40`,
        color,
      }}
      title={human ? `Last verified: ${human}` : 'Data confidence score'}
    >
      <span>{icon}</span>
      <span>
        {score !== null ? `${score.toFixed(1)}%` : '—'}
        {' '}verified
      </span>
    </div>
  )
}

// ── Full card ─────────────────────────────────────────────────────────────────

export function TrustBadgeFull() {
  const { data, loading } = useTrust()

  if (loading) {
    return (
      <div
        className="rounded-2xl p-6 animate-pulse"
        style={{ background: 'rgba(255,255,255,0.04)', minHeight: 160 }}
      />
    )
  }

  if (!data) return null

  const score  = data.data_confidence_score
  const passed = data.validation_passed
  const color  = scoreColor(score)
  const label  = scoreLabel(score, passed)
  const sources = data.sources_used?.length
    ? data.sources_used.join(' + ')
    : 'TMDB + Wikidata'

  return (
    <div
      className="rounded-2xl p-6 border"
      style={{
        background:   'rgba(255,255,255,0.03)',
        borderColor:  `${color}30`,
        boxShadow:    `0 0 32px ${color}08`,
      }}
    >
      {/* Header row */}
      <div className="flex items-start justify-between mb-5">
        <div>
          <p className="text-[10px] font-bold uppercase tracking-widest mb-1"
             style={{ color }}>
            Data Confidence
          </p>
          {/* Big score number */}
          <div className="flex items-baseline gap-2">
            <span className="text-4xl font-black text-white leading-none">
              {score !== null ? score.toFixed(1) : '—'}
            </span>
            <span className="text-sm font-semibold" style={{ color }}>/ 100</span>
          </div>
          <p className="text-xs mt-1" style={{ color: `${color}cc` }}>{label}</p>
        </div>

        {/* Pass / fail badge */}
        <div
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-bold"
          style={{
            background: passed === false ? 'rgba(248,113,113,0.15)' : 'rgba(74,222,128,0.12)',
            color:      passed === false ? '#f87171' : '#4ade80',
            border:     `1px solid ${passed === false ? '#f8717130' : '#4ade8030'}`,
          }}
        >
          {passed === false ? '⚠ Issues' : '✓ Verified'}
        </div>
      </div>

      {/* Score breakdown */}
      <div className="grid grid-cols-3 gap-3 mb-5">
        {[
          { label: 'Actors',      value: data.avg_actor_score },
          { label: 'Movies',      value: data.avg_movie_score },
          { label: 'Connections', value: data.collab_integrity },
        ].map(({ label, value }) => (
          <div key={label}
               className="rounded-xl p-3 text-center"
               style={{ background: 'rgba(255,255,255,0.04)' }}>
            <p className="text-[10px] text-white/40 uppercase tracking-wider mb-1">{label}</p>
            <p className="text-lg font-bold" style={{ color: scoreColor(value) }}>
              {value !== null ? `${value?.toFixed(0)}` : '—'}
            </p>
          </div>
        ))}
      </div>

      {/* Warnings if any */}
      {(data.ghost_collab_count > 0 || data.duplicate_count > 0) && (
        <div className="mb-4 rounded-xl px-4 py-3 text-xs"
             style={{ background: 'rgba(251,191,36,0.08)', color: '#fbbf24',
                      border: '1px solid rgba(251,191,36,0.2)' }}>
          {data.ghost_collab_count > 0 &&
            <p>⚠ {data.ghost_collab_count} ghost collaboration{data.ghost_collab_count !== 1 ? 's' : ''} detected</p>}
          {data.duplicate_count > 0 &&
            <p>⚠ {data.duplicate_count} duplicate movie group{data.duplicate_count !== 1 ? 's' : ''} detected</p>}
        </div>
      )}

      {/* Footer meta */}
      <div className="flex items-center justify-between text-[10px] text-white/30 pt-4 border-t border-white/[0.06]">
        <span>Source: {sources}</span>
        <span>{data.last_verified_human ? `Last verified: ${data.last_verified_human}` : 'Not yet scored'}</span>
      </div>
    </div>
  )
}

// Default export = full card
export default TrustBadgeFull
