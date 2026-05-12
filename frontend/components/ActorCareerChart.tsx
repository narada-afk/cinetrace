'use client'

/**
 * ActorCareerChart — multi-metric career trajectory chart.
 *
 * Four toggle modes (Grok-recommended ranking by tweet visibility):
 *   "rating"     — Avg rating line  (amber, 0–10)   + film count bars (subtle)
 *   "hit_rate"   — Hit rate % line  (amber, 0–100)  + film count bars (subtle)
 *   "box_office" — Total box office bars (amber, ₹cr) + avg rating line (muted)
 *   "films"      — Film count bars  (amber)          + avg rating line (muted)
 *
 * All four datasets are fetched in parallel on mount / filter change.
 * The chart is screenshot-able by the bot via [data-section="career-chart"].
 */

import { useState, useEffect, useRef, useCallback } from 'react'
import { getChartData, type ChartData } from '@/lib/api'

// ── Constants ──────────────────────────────────────────────────────────────────

const FIXED_X          = 'year'
const INDUSTRY_OPTIONS = ['All', 'Tamil', 'Telugu', 'Malayalam', 'Kannada']
const AMBER            = '#f59e0b'

type Mode = 'rating' | 'hit_rate' | 'box_office' | 'films'

interface ModeConfig {
  label:      string
  lineKey:    string          // dataset used for the line
  barKey:     string          // dataset used for bars
  lineAmber:  boolean         // true = line is the primary (amber)
  lineMax:    number | null   // null = dynamic
  barMax:     null            // always dynamic
  formatY:    (v: number) => string
  formatTT:   (v: number) => string  // tooltip format
  yLabel:     string          // axis legend label
}

const MODE_CONFIG: Record<Mode, ModeConfig> = {
  rating: {
    label:     'Rating',
    lineKey:   'avg_rating',
    barKey:    'film_count',
    lineAmber: true,
    lineMax:   10,
    barMax:    null,
    formatY:   (v) => v % 1 === 0 ? String(v) : v.toFixed(1),
    formatTT:  (v) => v.toFixed(1),
    yLabel:    'Avg Rating',
  },
  hit_rate: {
    label:     'Hit Rate',
    lineKey:   'hit_rate',
    barKey:    'film_count',
    lineAmber: true,
    lineMax:   100,
    barMax:    null,
    formatY:   (v) => `${Math.round(v)}%`,
    formatTT:  (v) => `${v.toFixed(0)}%`,
    yLabel:    'Hit Rate (rating ≥7)',
  },
  box_office: {
    label:     'Box Office',
    lineKey:   'avg_rating',
    barKey:    'total_box_office',
    lineAmber: false,
    lineMax:   10,
    barMax:    null,
    formatY:   (v) => v >= 1000 ? `₹${(v / 1000).toFixed(1)}K` : `₹${Math.round(v)}`,
    formatTT:  (v) => `₹${v.toFixed(0)} Cr`,
    yLabel:    'Total Box Office (₹ Cr)',
  },
  films: {
    label:     'Films / yr',
    lineKey:   'avg_rating',
    barKey:    'film_count',
    lineAmber: false,
    lineMax:   10,
    barMax:    null,
    formatY:   (v) => String(Math.round(v)),
    formatTT:  (v) => `${Math.round(v)} films`,
    yLabel:    'Films / yr',
  },
}

const MODES: Mode[] = ['rating', 'hit_rate', 'box_office', 'films']

// ── Dataset store ──────────────────────────────────────────────────────────────

type DataStore = Record<string, ChartData | null>

// ── Dual SVG Chart ─────────────────────────────────────────────────────────────

interface TooltipState {
  xVal:     number
  pivotX:   number
  pivotY:   number
  lineVal?: number
  barVal?:  number
}

function DualChart({
  lineData,
  barData,
  mode,
}: {
  lineData: ChartData | null
  barData:  ChartData | null
  mode:     Mode
}) {
  const cfg = MODE_CONFIG[mode]
  const [animated, setAnimated] = useState(false)
  const [tooltip,  setTooltip]  = useState<TooltipState | null>(null)

  useEffect(() => {
    setAnimated(false)
    setTooltip(null)
    const t = setTimeout(() => setAnimated(true), 60)
    return () => clearTimeout(t)
  }, [lineData, barData, mode])

  const W   = 800
  const H   = 300
  const PAD = { top: 24, right: 24, bottom: 40, left: 56 }
  const iW  = W - PAD.left - PAD.right
  const iH  = H - PAD.top  - PAD.bottom

  const lPts = lineData?.series[0]?.points ?? []
  const bPts = barData?.series[0]?.points  ?? []
  if (!lPts.length && !bPts.length) return null

  const allX = [...new Set([...lPts, ...bPts].map(p => Number(p.x)))].sort((a, b) => a - b)
  if (!allX.length) return null

  const minX   = allX[0]
  const maxX   = allX[allX.length - 1]
  const xRange = (maxX - minX) || 1

  const lMap = new Map(lPts.map(p => [Number(p.x), p.y]))
  const bMap = new Map(bPts.map(p => [Number(p.x), p.y]))

  // Y scales
  const lineMax = cfg.lineMax ?? (Math.max(...[...lMap.values()], 1) * 1.25)
  const barMax  = Math.max(...[...bMap.values()], 1) * 1.25

  const xScale  = (x: number) => PAD.left + ((x - minX) / xRange) * iW
  const yLine   = (v: number) => PAD.top + iH - (v / lineMax) * iH
  const yBar    = (v: number) => PAD.top + iH - (v / barMax)  * iH

  // Primary Y axis: line axis (left side)
  const primaryMax = lineMax
  const yPrimary   = yLine
  const yTicks     = Array.from({ length: 5 }, (_, i) => primaryMax * i / 4)
  const xStep      = Math.max(1, Math.ceil(allX.length / 10))
  const xTicks     = allX.filter((_, i) => i % xStep === 0)

  const barW = Math.max(4, (iW / Math.max(allX.length, 1)) * 0.6)

  // Line path (always avg_rating or hit_rate or similar continuous metric)
  const linePts = [...lMap.entries()]
    .sort((a, b) => a[0] - b[0])
    .filter(([, y]) => y > 0)   // skip zero-data years for cleaner line
    .map(([x, y]) => ({ sx: xScale(x), sy: yLine(y), xVal: x, yVal: y }))
  const pathD    = linePts.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.sx.toFixed(1)},${p.sy.toFixed(1)}`).join(' ')
  const totalLen = linePts.reduce((acc, p, i) =>
    i === 0 ? 0 : acc + Math.hypot(p.sx - linePts[i - 1].sx, p.sy - linePts[i - 1].sy), 0)
  const floorY = (PAD.top + iH).toFixed(1)
  const areaD  = linePts.length > 1
    ? `${pathD} L${linePts[linePts.length - 1].sx.toFixed(1)},${floorY} L${linePts[0].sx.toFixed(1)},${floorY} Z`
    : ''

  const lineColor  = cfg.lineAmber ? AMBER : `${AMBER}55`
  const lineWidth  = cfg.lineAmber ? 2.5 : 1.5

  const TT_W = 150
  const TT_H = lMap.size > 0 && bMap.size > 0 ? 72 : 50

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ maxHeight: 300 }}
      onMouseLeave={() => setTooltip(null)}>

      {/* Grid + Y labels */}
      {yTicks.map((t, i) => (
        <g key={i}>
          <line x1={PAD.left} x2={W - PAD.right} y1={yPrimary(t)} y2={yPrimary(t)}
            stroke="white" strokeOpacity={0.06} />
          <text x={PAD.left - 6} y={yPrimary(t) + 4} textAnchor="end" fontSize={10}
            fill="rgba(255,255,255,0.35)">
            {cfg.formatY(t)}
          </text>
        </g>
      ))}

      {/* X labels */}
      {xTicks.map((t, i) => (
        <text key={i} x={xScale(t)} y={H - PAD.bottom + 16} textAnchor="middle"
          fontSize={10} fill="rgba(255,255,255,0.35)">{t}</text>
      ))}

      {/* ── BARS ────────────────────────────────────────────────────── */}
      {[...bMap.entries()].map(([year, count]) => {
        if (count <= 0) return null
        const bh  = (count / barMax) * iH
        const bx  = xScale(year) - barW / 2
        const by  = yBar(count)
        const isH = tooltip?.xVal === year
        const fill    = !cfg.lineAmber ? AMBER : 'white'
        const opacity = !cfg.lineAmber
          ? (animated ? (isH ? 0.80 : 0.60) : 0)
          : (animated ? (isH ? 0.22 : 0.10) : 0)
        return (
          <rect key={year} x={bx} y={by} width={barW} height={bh}
            fill={fill} opacity={opacity} rx={2}
            style={{ transition: 'opacity 0.4s ease-out', cursor: 'crosshair' }}
            onMouseEnter={() => setTooltip({
              xVal: year, pivotX: xScale(year), pivotY: by,
              lineVal: lMap.get(year), barVal: count,
            })}
          />
        )
      })}

      {/* ── AREA fill (when line is primary) ────────────────────────── */}
      {cfg.lineAmber && areaD && (
        <path d={areaD} fill={AMBER}
          fillOpacity={animated ? 0.07 : 0}
          style={{ transition: 'fill-opacity 0.6s ease-out' }} />
      )}

      {/* ── LINE ────────────────────────────────────────────────────── */}
      {linePts.length > 1 && (
        <path d={pathD} fill="none"
          stroke={lineColor} strokeWidth={lineWidth}
          strokeLinecap="round" strokeLinejoin="round"
          strokeDasharray={totalLen}
          strokeDashoffset={animated ? 0 : totalLen}
          style={{ transition: 'stroke-dashoffset 1.0s ease-out' }} />
      )}

      {/* ── DOTS (when line is primary) ─────────────────────────────── */}
      {cfg.lineAmber && linePts.map((p, i) => {
        const isH = tooltip?.xVal === p.xVal
        return (
          <circle key={i} cx={p.sx} cy={p.sy}
            r={isH ? 5 : 2.5}
            fill={isH ? 'white' : AMBER}
            stroke={isH ? AMBER : 'none'} strokeWidth={isH ? 2 : 0}
            opacity={animated ? 1 : 0}
            style={{ transition: 'opacity 0.3s 0.8s, r 0.1s', cursor: 'crosshair' }}
            onMouseEnter={() => setTooltip({
              xVal: p.xVal, pivotX: p.sx, pivotY: p.sy,
              lineVal: p.yVal, barVal: bMap.get(p.xVal),
            })}
          />
        )
      })}

      {/* ── CROSSHAIR ───────────────────────────────────────────────── */}
      {tooltip && (
        <line
          x1={xScale(tooltip.xVal)} x2={xScale(tooltip.xVal)}
          y1={PAD.top} y2={PAD.top + iH}
          stroke="white" strokeOpacity={0.15} strokeWidth={1} strokeDasharray="4 3"
          style={{ pointerEvents: 'none' }} />
      )}

      {/* ── TOOLTIP ─────────────────────────────────────────────────── */}
      {tooltip && (() => {
        const tx = xScale(tooltip.xVal)
        const bx = tx + TT_W + 12 > W - PAD.right ? tx - TT_W - 8 : tx + 8
        const by = Math.max(PAD.top, Math.min(
          (tooltip.pivotY ?? PAD.top + iH / 2) - TT_H / 2,
          PAD.top + iH - TT_H,
        ))
        // Primary is line when lineAmber, bar otherwise
        const primary   = cfg.lineAmber ? tooltip.lineVal  : tooltip.barVal
        const secondary = cfg.lineAmber ? tooltip.barVal   : tooltip.lineVal
        const primLabel = cfg.lineAmber ? cfg.yLabel        : cfg.yLabel
        const secLabel  = cfg.lineAmber ? 'Films'           : 'Rating'
        const primFmt   = cfg.lineAmber
          ? (v: number) => cfg.formatTT(v)
          : (v: number) => cfg.formatTT(v)
        const secFmt    = cfg.lineAmber
          ? (v: number) => `${Math.round(v)}`
          : (v: number) => `${v.toFixed(1)}/10`
        return (
          <g style={{ pointerEvents: 'none' }}>
            <rect x={bx} y={by} width={TT_W} height={TT_H} rx={6}
              fill="#1e1e2e" stroke="rgba(255,255,255,0.12)" strokeWidth={1} />
            <text x={bx + 10} y={by + 14} fontSize={10} fontWeight="600"
              fill="rgba(255,255,255,0.45)">{tooltip.xVal}</text>
            {primary !== undefined && (
              <g>
                <circle cx={bx + 14} cy={by + 30} r={3.5} fill={AMBER} />
                <text x={bx + 26} y={by + 34} fontSize={10} fill="rgba(255,255,255,0.75)">{primLabel}</text>
                <text x={bx + TT_W - 8} y={by + 34} fontSize={11} fontWeight="700"
                  fill={AMBER} textAnchor="end">{primFmt(primary)}</text>
              </g>
            )}
            {secondary !== undefined && secondary > 0 && (
              <g>
                <circle cx={bx + 14} cy={by + 52} r={3.5} fill="rgba(255,255,255,0.35)" />
                <text x={bx + 26} y={by + 56} fontSize={10} fill="rgba(255,255,255,0.75)">{secLabel}</text>
                <text x={bx + TT_W - 8} y={by + 56} fontSize={11} fontWeight="700"
                  fill="rgba(255,255,255,0.7)" textAnchor="end">{secFmt(secondary)}</text>
              </g>
            )}
          </g>
        )
      })()}
    </svg>
  )
}

// ── Legend ─────────────────────────────────────────────────────────────────────

function Legend({ mode }: { mode: Mode }) {
  const cfg = MODE_CONFIG[mode]
  return (
    <div className="flex gap-5 mt-3">
      <div className="flex items-center gap-1.5 text-xs text-white/50">
        <div className="w-6 h-0.5 rounded-full" style={{ background: AMBER }} />
        {cfg.lineAmber ? cfg.yLabel : 'Avg Rating'}
      </div>
      <div className="flex items-center gap-1.5 text-xs text-white/50">
        <div className="w-3 h-3 rounded-sm"
          style={{ background: !cfg.lineAmber ? AMBER : 'rgba(255,255,255,0.25)' }} />
        {cfg.lineAmber ? 'Films / yr' : cfg.yLabel}
      </div>
    </div>
  )
}

// ── Main component ─────────────────────────────────────────────────────────────

interface ActorCareerChartProps {
  actorId:        number
  actorName:      string
  firstFilmYear?: number
}

export default function ActorCareerChart({
  actorId,
  actorName,
  firstFilmYear = 1970,
}: ActorCareerChartProps) {
  const [mode,     setMode]     = useState<Mode>('rating')
  const [industry, setIndustry] = useState('All')
  const [yearFrom, setYearFrom] = useState(firstFilmYear)
  const [yearTo,   setYearTo]   = useState(2026)
  const [data,     setData]     = useState<DataStore>({})
  const [loading,  setLoading]  = useState(false)
  const [error,    setError]    = useState<string | null>(null)
  const debounceRef = useRef<ReturnType<typeof setTimeout>>()

  const buildChart = useCallback(async (ind: string, yf: number, yt: number) => {
    if (yt <= yf || yf < 1950 || yt > 2026) return
    setLoading(true)
    setError(null)
    try {
      const industry = ind === 'All' ? undefined : ind
      // Fetch all 4 datasets in parallel — fast since they share the same DB query shape
      const [rating, films, hitRate, boxOffice] = await Promise.all([
        getChartData(FIXED_X, 'avg_rating',       [actorId], industry, yf, yt),
        getChartData(FIXED_X, 'film_count',        [actorId], industry, yf, yt),
        getChartData(FIXED_X, 'hit_rate',          [actorId], industry, yf, yt),
        getChartData(FIXED_X, 'total_box_office',  [actorId], industry, yf, yt),
      ])
      setData({ avg_rating: rating, film_count: films, hit_rate: hitRate, total_box_office: boxOffice })
    } catch {
      setError('Failed to load chart data.')
    } finally {
      setLoading(false)
    }
  }, [actorId])

  useEffect(() => {
    clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => buildChart(industry, yearFrom, yearTo), 350)
    return () => clearTimeout(debounceRef.current)
  }, [industry, yearFrom, yearTo, buildChart])

  const cfg      = MODE_CONFIG[mode]
  const lineData = data[cfg.lineKey] ?? null
  const barData  = data[cfg.barKey]  ?? null
  const hasData  = lineData || barData

  return (
    <div data-section="career-chart" className="glass rounded-3xl p-6 sm:p-8">

      {/* ── Header + mode toggles ────────────────────────────────────── */}
      <div className="flex items-start justify-between gap-4 mb-6 flex-wrap">
        <div>
          <h2 className="text-white font-bold text-xl tracking-tight">Career at a Glance</h2>
          <p className="text-white/40 text-sm mt-1">{actorName}'s year-by-year trajectory</p>
        </div>

        {/* 4-way toggle */}
        <div className="flex gap-1 bg-white/[0.05] rounded-xl p-1 border border-white/[0.08] flex-wrap flex-shrink-0">
          {MODES.map(m => (
            <button key={m} onClick={() => setMode(m)}
              className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all duration-200 whitespace-nowrap ${
                mode === m
                  ? 'bg-amber-500/20 text-amber-400 border border-amber-500/30'
                  : 'text-white/40 hover:text-white/60'
              }`}>
              {MODE_CONFIG[m].label}
            </button>
          ))}
        </div>
      </div>

      {/* ── Filters ─────────────────────────────────────────────────── */}
      <div className="flex flex-wrap gap-4 items-end mb-6 pb-6 border-b border-white/[0.07]">
        <div className="min-w-[130px]">
          <label className="block text-xs text-white/40 uppercase tracking-wider mb-1.5">Industry</label>
          <select value={industry} onChange={e => setIndustry(e.target.value)}
            className="w-full bg-white/[0.05] border border-white/[0.10] rounded-xl px-3 py-2.5 text-sm text-white outline-none focus:border-white/25 transition-colors">
            {INDUSTRY_OPTIONS.map(i => <option key={i} value={i}>{i}</option>)}
          </select>
        </div>

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

        {loading && (
          <div className="flex items-end pb-2.5">
            <div className="flex gap-1">
              {[0, 1, 2].map(i => (
                <div key={i} className="w-1.5 h-1.5 rounded-full bg-amber-400 animate-bounce"
                  style={{ animationDelay: `${i * 0.15}s` }} />
              ))}
            </div>
          </div>
        )}
      </div>

      {error && (
        <div className="mb-4 px-4 py-3 rounded-xl bg-red-500/10 border border-red-500/20 text-red-400 text-sm">
          {error}
        </div>
      )}

      {/* ── Chart ───────────────────────────────────────────────────── */}
      {hasData ? (
        <div key={`${mode}-${industry}-${yearFrom}-${yearTo}`}
          style={{ animation: 'careerFadeIn 0.35s ease-out both' }}>
          <style>{`
            @keyframes careerFadeIn {
              from { opacity: 0; transform: translateY(8px); }
              to   { opacity: 1; transform: translateY(0); }
            }
          `}</style>
          <div className="text-xs text-white/30 mb-3">
            {cfg.yLabel} by year
            {industry !== 'All' ? ` · ${industry}` : ''} · {yearFrom}–{yearTo}
          </div>
          <DualChart lineData={lineData} barData={barData} mode={mode} />
          <Legend mode={mode} />
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
