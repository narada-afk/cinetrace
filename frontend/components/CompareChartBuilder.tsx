'use client'

/**
 * CompareChartBuilder — ChartBuilder embedded in the compare page.
 * Both actors are pre-seeded and locked (not removable).
 * Users only pick X axis, Y axis, and optional filters.
 */

import { useState, useEffect, useRef, useCallback } from 'react'
import { useRouter } from 'next/navigation'
import { getChartData, toActorSlug, type Actor, type ChartData } from '@/lib/api'
import ActorAvatar from '@/components/ActorAvatar'

// ── Constants ──────────────────────────────────────────────────────────────────

// X is always 'year' in the compare chart — no selector shown
const FIXED_X = 'year'

const Y_OPTIONS = [
  // Career volume
  { value: 'film_count',              label: 'Films per Year',           group: 'Career' },
  { value: 'unique_directors',        label: 'Unique Directors',         group: 'Career' },
  // Ratings & quality
  { value: 'avg_rating',              label: 'Avg Rating (0–10)',         group: 'Quality' },
  { value: 'hit_rate',                label: 'Hit Rate % (≥7.0)',        group: 'Quality' },
  { value: 'avg_popularity',          label: 'Avg Popularity Score',     group: 'Quality' },
  // Box office
  { value: 'avg_box_office',          label: 'Avg Box Office (₹ Cr)',    group: 'Box Office' },
  { value: 'total_box_office',        label: 'Total Box Office (₹ Cr)',  group: 'Box Office' },
  { value: 'avg_budget',              label: 'Avg Budget (₹ Cr)',        group: 'Box Office' },
  // Collaborations
  { value: 'unique_costars',          label: 'Unique Co-Stars',          group: 'Network' },
  { value: 'total_collaborations',    label: 'Total Collaborations',     group: 'Network' },
  { value: 'director_collaborations', label: 'Director Collaborations',  group: 'Network' },
]

const INDUSTRY_OPTIONS = ['All', 'Tamil', 'Telugu', 'Malayalam', 'Kannada']

const ACTOR_COLORS = ['#f59e0b', '#06b6d4']

// ── SVG Line Chart ─────────────────────────────────────────────────────────────

interface TooltipState {
  x: number       // SVG x coord of hovered point
  y: number       // SVG y coord of hovered point
  xVal: number    // raw X value (e.g. year)
  rows: { name: string; value: number; color: string }[]
}

function LineChart({ data }: { data: ChartData }) {
  const [animated, setAnimated] = useState(false)
  const [tooltip, setTooltip] = useState<TooltipState | null>(null)
  useEffect(() => {
    setAnimated(false)           // reset — lines snap back to 0
    setTooltip(null)
    const t = setTimeout(() => setAnimated(true), 60)  // then draw
    return () => clearTimeout(t)
  }, [data])

  const W = 800, H = 320, PAD = { top: 24, right: 20, bottom: 40, left: 52 }
  const innerW = W - PAD.left - PAD.right, innerH = H - PAD.top - PAD.bottom
  const allX = [...new Set(data.series.flatMap(s => s.points.map(p => Number(p.x))))].sort((a, b) => a - b)
  const allY = data.series.flatMap(s => s.points.map(p => p.y))
  const maxY = Math.max(...allY) * 1.1 || 1
  const xScale = (x: number) => PAD.left + ((x - allX[0]) / ((allX[allX.length - 1] - allX[0]) || 1)) * innerW
  const yScale = (y: number) => PAD.top + innerH - (y / maxY) * innerH
  const yTicks = Array.from({ length: 5 }, (_, i) => maxY * i / 4)
  const step = Math.max(1, Math.ceil(allX.length / 10))
  const xTicks = allX.filter((_, i) => i % step === 0)

  // Build a map: xVal → all series values, for merged tooltip
  const xValMap = new Map<number, { name: string; value: number; color: string }[]>()
  data.series.forEach((s, si) => {
    s.points.forEach(p => {
      const xv = Number(p.x)
      if (!xValMap.has(xv)) xValMap.set(xv, [])
      xValMap.get(xv)!.push({ name: s.actor_name, value: p.y, color: ACTOR_COLORS[si % ACTOR_COLORS.length] })
    })
  })

  // Tooltip box dimensions in SVG units
  const TT_W = 130, TT_H = 20 + data.series.length * 16

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ maxHeight: 320 }}
      onMouseLeave={() => setTooltip(null)}>
      {yTicks.map((t, i) => (
        <g key={i}>
          <line x1={PAD.left} x2={W - PAD.right} y1={yScale(t)} y2={yScale(t)} stroke="white" strokeOpacity={0.06} />
          <text x={PAD.left - 6} y={yScale(t) + 4} textAnchor="end" fontSize={10} fill="rgba(255,255,255,0.35)">
            {t % 1 === 0 ? t : t.toFixed(1)}
          </text>
        </g>
      ))}
      {xTicks.map((t, i) => (
        <text key={i} x={xScale(t)} y={H - PAD.bottom + 16} textAnchor="middle" fontSize={10} fill="rgba(255,255,255,0.35)">{t}</text>
      ))}

      {data.series.map((s, si) => {
        const color = ACTOR_COLORS[si % ACTOR_COLORS.length]
        const pts = s.points.map(p => ({ x: xScale(Number(p.x)), y: yScale(p.y), xVal: Number(p.x), yVal: p.y }))
        const pathD = pts.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(' ')
        const totalLen = pts.reduce((acc, p, i) => i === 0 ? 0 : acc + Math.hypot(p.x - pts[i - 1].x, p.y - pts[i - 1].y), 0)
        return (
          <g key={s.actor_id}>
            <path d={pathD} fill="none" stroke={color} strokeWidth={2.5} strokeLinecap="round" strokeLinejoin="round"
              strokeDasharray={totalLen} strokeDashoffset={animated ? 0 : totalLen}
              style={{ transition: `stroke-dashoffset ${0.8 + si * 0.2}s ease-out` }} />
            {pts.map((p, i) => {
              const isHov = tooltip?.xVal === p.xVal
              return (
                <circle key={i} cx={p.x} cy={p.y}
                  r={isHov ? 5 : 2.5}
                  fill={isHov ? 'white' : color}
                  stroke={isHov ? color : 'none'}
                  strokeWidth={isHov ? 2 : 0}
                  opacity={animated ? 1 : 0}
                  style={{ transition: `opacity 0.3s ${0.8 + si * 0.2}s, r 0.1s`, cursor: 'crosshair' }}
                  onMouseEnter={() => setTooltip({
                    x: p.x, y: p.y, xVal: p.xVal,
                    rows: xValMap.get(p.xVal) ?? [],
                  })}
                />
              )
            })}
          </g>
        )
      })}

      {/* Vertical crosshair line */}
      {tooltip && (
        <line
          x1={xScale(tooltip.xVal)} x2={xScale(tooltip.xVal)}
          y1={PAD.top} y2={PAD.top + innerH}
          stroke="white" strokeOpacity={0.15} strokeWidth={1} strokeDasharray="4 3"
          style={{ pointerEvents: 'none' }}
        />
      )}

      {/* Tooltip box */}
      {tooltip && (() => {
        const tx = xScale(tooltip.xVal)
        // Flip to left side if too close to right edge
        const boxX = tx + TT_W + 12 > W - PAD.right ? tx - TT_W - 8 : tx + 8
        const boxY = Math.max(PAD.top, Math.min(tooltip.y - TT_H / 2, PAD.top + innerH - TT_H))
        return (
          <g style={{ pointerEvents: 'none' }}>
            <rect x={boxX} y={boxY} width={TT_W} height={TT_H} rx={6}
              fill="#1e1e2e" stroke="rgba(255,255,255,0.12)" strokeWidth={1} />
            {/* Year label */}
            <text x={boxX + 10} y={boxY + 14} fontSize={10} fontWeight="600" fill="rgba(255,255,255,0.5)">
              {tooltip.xVal}
            </text>
            {/* Per-actor rows */}
            {tooltip.rows.map((row, i) => (
              <g key={row.name}>
                <circle cx={boxX + 14} cy={boxY + 26 + i * 16} r={3.5} fill={row.color} />
                <text x={boxX + 24} y={boxY + 30 + i * 16} fontSize={10} fill="rgba(255,255,255,0.85)">
                  {row.name.split(' ')[0]}
                </text>
                <text x={boxX + TT_W - 8} y={boxY + 30 + i * 16} fontSize={10}
                  fontWeight="600" fill="white" textAnchor="end">
                  {row.value % 1 === 0 ? row.value : row.value.toFixed(2)}
                </text>
              </g>
            ))}
          </g>
        )
      })()}
    </svg>
  )
}


// ── Legend ─────────────────────────────────────────────────────────────────────

function Legend({ series }: { series: ChartData['series'] }) {
  return (
    <div className="flex gap-4 mt-3">
      {series.map((s, i) => (
        <div key={s.actor_id} className="flex items-center gap-1.5 text-xs text-white/60">
          <div className="w-3 h-3 rounded-sm" style={{ background: ACTOR_COLORS[i % ACTOR_COLORS.length] }} />
          {s.actor_name}
        </div>
      ))}
    </div>
  )
}

// ── Main component ─────────────────────────────────────────────────────────────

interface CompareChartBuilderProps {
  actor1: Actor
  actor2: Actor
}

export default function CompareChartBuilder({ actor1, actor2 }: CompareChartBuilderProps) {
  const router = useRouter()
  const [yAxis, setYAxis] = useState('avg_rating')
  const [industry, setIndustry] = useState('All')
  const [yearFrom, setYearFrom] = useState(1970)
  const [attentionPulse, setAttentionPulse] = useState(false)

  // After 3 s of idle, briefly boost the glow once to draw attention
  useEffect(() => {
    const t1 = setTimeout(() => setAttentionPulse(true),  3000)
    const t2 = setTimeout(() => setAttentionPulse(false), 4800)
    return () => { clearTimeout(t1); clearTimeout(t2) }
  }, [])
  const [yearTo, setYearTo] = useState(2026)
  const [chartData, setChartData] = useState<ChartData | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const debounceRef = useRef<ReturnType<typeof setTimeout>>()

  const buildChart = useCallback(async (y: string, ind: string, yf: number, yt: number) => {
    if (yt <= yf || yf < 1950 || yt > 2026) return
    setLoading(true)
    setError(null)
    try {
      const data = await getChartData(
        FIXED_X, y,
        [actor1.id, actor2.id],
        ind === 'All' ? undefined : ind,
        yf,
        yt,
      )
      setChartData(data)
    } catch {
      setError('Failed to fetch chart data.')
    } finally {
      setLoading(false)
    }
  }, [actor1.id, actor2.id])

  // Auto-fetch whenever any control changes — debounced so year typing doesn't spam
  useEffect(() => {
    clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => {
      buildChart(yAxis, industry, yearFrom, yearTo)
    }, 350)
    return () => clearTimeout(debounceRef.current)
  }, [yAxis, industry, yearFrom, yearTo, buildChart])

  const yLabel = Y_OPTIONS.find(o => o.value === yAxis)?.label ?? yAxis

  // Group Y options for the select
  const yGroups = Y_OPTIONS.reduce<Record<string, typeof Y_OPTIONS>>((acc, o) => {
    if (!acc[o.group]) acc[o.group] = []
    acc[o.group].push(o)
    return acc
  }, {})

  return (
    <div className="glass rounded-3xl p-6 sm:p-8">
      {/* Header */}
      <div className="flex items-center gap-3 mb-6">
        <span className="text-2xl">🔥</span>
        <div>
          <h2 className="text-white font-bold text-lg">Career Showdown</h2>
          <p className="text-white/40 text-sm">Compare two actors across time</p>
        </div>
      </div>

      {/* Actor selector bar — interactive CTA */}
      <style>{`
        @keyframes glowPulse {
          0%, 100% { box-shadow: 0 0 0 0 rgba(100, 120, 255, 0); }
          50%       { box-shadow: 0 0 14px 3px rgba(100, 120, 255, 0.10); }
        }
        @keyframes glowPulseStrong {
          0%, 100% { box-shadow: 0 0 0 0 rgba(100, 120, 255, 0); }
          50%       { box-shadow: 0 0 22px 5px rgba(100, 120, 255, 0.22); }
        }
        .actor-bar:hover {
          box-shadow: 0 0 18px 4px rgba(100, 120, 255, 0.16) !important;
          animation: none !important;
        }
        .actor-bar:focus-visible {
          outline: 2px solid rgba(150, 170, 255, 0.35);
          outline-offset: 2px;
        }
      `}</style>
      <button
        className="actor-bar w-full flex items-center gap-3 mb-6 p-4 rounded-2xl border border-white/[0.08] cursor-pointer transition-all duration-300 hover:-translate-y-0.5 hover:border-white/[0.20] text-left group"
        style={{
          background: '#0d0d15',
          animation: attentionPulse
            ? 'glowPulseStrong 0.9s ease-in-out 2'
            : 'glowPulse 2.8s ease-in-out infinite',
        }}
        onClick={() => router.push(`/actors/${toActorSlug(actor1.name)}`)}
        aria-label={`Change matchup — currently ${actor1.name} vs ${actor2.name}`}
      >
        {/* Actor 1 */}
        <div className="flex items-center gap-2">
          <div className="w-2.5 h-2.5 rounded-full flex-shrink-0" style={{ background: ACTOR_COLORS[0] }} />
          <ActorAvatar name={actor1.name} size={28} />
          <span className="text-white text-sm font-medium">{actor1.name}</span>
        </div>

        <span className="text-white/20 text-xs font-bold mx-1">VS</span>

        {/* Actor 2 */}
        <div className="flex items-center gap-2">
          <div className="w-2.5 h-2.5 rounded-full flex-shrink-0" style={{ background: ACTOR_COLORS[1] }} />
          <ActorAvatar name={actor2.name} size={28} />
          <span className="text-white text-sm font-medium">{actor2.name}</span>
        </div>

        {/* Microcopy hint — right side */}
        <div className="ml-auto flex items-center gap-1.5 flex-shrink-0">
          <span className="text-white/35 text-[11px] group-hover:text-white/55 transition-colors duration-200">
            Change matchup
          </span>
          <svg width="11" height="11" viewBox="0 0 11 11" fill="none"
            className="opacity-30 group-hover:opacity-55 transition-opacity duration-200 group-hover:translate-x-0.5 transition-transform">
            <path d="M2 5.5h7M6.5 3l2.5 2.5L6.5 8" stroke="white" strokeWidth="1.3"
              strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
        </div>
      </button>

      {/* Controls */}
      <div className="flex flex-wrap gap-4 items-end mb-6 pb-6 border-b border-white/[0.07]">
        {/* Metric — grouped select */}
        <div className="flex-1 min-w-[200px]">
          <label className="block text-xs text-white/40 uppercase tracking-wider mb-1.5">Metric</label>
          <select value={yAxis} onChange={e => setYAxis(e.target.value)}
            className="w-full bg-white/[0.05] border border-white/[0.10] rounded-xl px-3 py-2.5 text-sm text-white outline-none focus:border-white/25 transition-colors">
            {Object.entries(yGroups).map(([group, opts]) => (
              <optgroup key={group} label={`— ${group} —`}>
                {opts.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
              </optgroup>
            ))}
          </select>
        </div>

        {/* Industry */}
        <div className="min-w-[130px]">
          <label className="block text-xs text-white/40 uppercase tracking-wider mb-1.5">Industry</label>
          <select value={industry} onChange={e => setIndustry(e.target.value)}
            className="w-full bg-white/[0.05] border border-white/[0.10] rounded-xl px-3 py-2.5 text-sm text-white outline-none focus:border-white/25 transition-colors">
            {INDUSTRY_OPTIONS.map(i => <option key={i} value={i}>{i}</option>)}
          </select>
        </div>

        {/* Year range */}
        <div>
          <label className="block text-xs text-white/40 uppercase tracking-wider mb-1.5">Year Range</label>
          <div className="flex items-center gap-1.5">
            <input type="number" min={1950} max={2025} value={yearFrom}
              onChange={e => setYearFrom(Number(e.target.value))}
              className="w-20 bg-white/[0.05] border border-white/[0.10] rounded-lg px-2 py-2.5 text-sm text-white outline-none text-center" />
            <span className="text-white/30 text-xs">–</span>
            <input type="number" min={1950} max={2026} value={yearTo}
              onChange={e => setYearTo(Number(e.target.value))}
              className="w-20 bg-white/[0.05] border border-white/[0.10] rounded-lg px-2 py-2.5 text-sm text-white outline-none text-center" />
          </div>
        </div>

        {/* Loading indicator */}
        {loading && (
          <div className="flex items-end pb-2.5">
            <div className="flex gap-1">
              {[0,1,2].map(i => (
                <div key={i} className="w-1.5 h-1.5 rounded-full bg-violet-400 animate-bounce"
                  style={{ animationDelay: `${i * 0.15}s` }} />
              ))}
            </div>
          </div>
        )}
      </div>

      {error && (
        <div className="mb-4 px-4 py-3 rounded-xl bg-red-500/10 border border-red-500/20 text-red-400 text-sm">{error}</div>
      )}

      {/* Chart area */}
      {chartData ? (
        <div
          key={`${yAxis}-${industry}-${yearFrom}-${yearTo}`}
          style={{ animation: 'chartFadeIn 0.35s ease-out both' }}
        >
          <style>{`
            @keyframes chartFadeIn {
              from { opacity: 0; transform: translateY(10px); }
              to   { opacity: 1; transform: translateY(0); }
            }
          `}</style>
          <div className="text-xs text-white/30 mb-4">
            Year vs {yLabel}{industry !== 'All' ? ` · ${industry}` : ''} · {yearFrom}–{yearTo}
          </div>
          <LineChart data={chartData} />
          <Legend series={chartData.series} />
        </div>
      ) : !loading && (
        <div className="flex flex-col items-center justify-center py-12 text-white/20">
          <div className="text-4xl mb-3">📈</div>
          <p className="text-sm">Loading chart…</p>
        </div>
      )}
    </div>
  )
}
