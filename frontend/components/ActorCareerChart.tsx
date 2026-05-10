'use client'

/**
 * ActorCareerChart — dual-metric career trajectory chart.
 *
 * Default ("Rating" mode):
 *   • Amber line  = avg rating by year (left Y axis, 0–10)
 *   • Subtle bars = film count per year (background, own scale)
 *
 * "Films / yr" mode:
 *   • Amber bars  = film count per year (prominent)
 *   • Muted line  = avg rating (secondary reference)
 *
 * Both datasets are fetched in parallel on mount / filter change.
 * The chart is server-screenshot-able via [data-section="career-chart"].
 */

import { useState, useEffect, useRef, useCallback } from 'react'
import { getChartData, type ChartData } from '@/lib/api'

// ── Constants ──────────────────────────────────────────────────────────────────

const FIXED_X          = 'year'
const INDUSTRY_OPTIONS = ['All', 'Tamil', 'Telugu', 'Malayalam', 'Kannada']
const AMBER            = '#f59e0b'
type Mode = 'rating' | 'films'

// ── Dual SVG Chart ─────────────────────────────────────────────────────────────

interface TooltipState {
  xVal: number
  pivotX: number   // SVG x of hovered column
  pivotY: number   // SVG y (for positioning)
  rating?: number
  films?: number
}

function DualChart({
  ratingData,
  filmData,
  mode,
}: {
  ratingData: ChartData | null
  filmData:   ChartData | null
  mode:       Mode
}) {
  const [animated, setAnimated] = useState(false)
  const [tooltip,  setTooltip]  = useState<TooltipState | null>(null)

  useEffect(() => {
    setAnimated(false)
    setTooltip(null)
    const t = setTimeout(() => setAnimated(true), 60)
    return () => clearTimeout(t)
  }, [ratingData, filmData, mode])

  const W   = 800
  const H   = 300
  const PAD = { top: 24, right: 24, bottom: 40, left: 52 }
  const iW  = W - PAD.left - PAD.right
  const iH  = H - PAD.top  - PAD.bottom

  const rPts = ratingData?.series[0]?.points ?? []
  const fPts = filmData?.series[0]?.points   ?? []
  if (!rPts.length && !fPts.length) return null

  // Union of all years, sorted
  const allX = [...new Set([...rPts, ...fPts].map(p => Number(p.x)))].sort((a, b) => a - b)
  if (!allX.length) return null

  const minX   = allX[0]
  const maxX   = allX[allX.length - 1]
  const xRange = (maxX - minX) || 1

  const rMap = new Map(rPts.map(p => [Number(p.x), p.y]))
  const fMap = new Map(fPts.map(p => [Number(p.x), p.y]))

  const maxRating = 10
  const maxFilms  = Math.max(...[...fMap.values()], 1) * 1.25

  const xScale   = (x: number) => PAD.left + ((x - minX) / xRange) * iW
  const yRating  = (v: number) => PAD.top + iH - (v / maxRating) * iH
  const yFilms   = (v: number) => PAD.top + iH - (v / maxFilms)  * iH

  // Primary Y axis ticks depend on mode
  const primaryMax   = mode === 'rating' ? maxRating : maxFilms
  const yPrimary     = (v: number) => PAD.top + iH - (v / primaryMax) * iH
  const yTicks       = Array.from({ length: 5 }, (_, i) => primaryMax * i / 4)
  const xStep        = Math.max(1, Math.ceil(allX.length / 10))
  const xTicks       = allX.filter((_, i) => i % xStep === 0)

  // Bar width: 60 % of year slot, min 4 px
  const barW = Math.max(4, (iW / Math.max(allX.length, 1)) * 0.6)

  // Rating line path
  const linePts = [...rMap.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([x, y]) => ({ sx: xScale(x), sy: yRating(y), xVal: x, yVal: y }))
  const pathD    = linePts.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.sx.toFixed(1)},${p.sy.toFixed(1)}`).join(' ')
  const totalLen = linePts.reduce((acc, p, i) =>
    i === 0 ? 0 : acc + Math.hypot(p.sx - linePts[i - 1].sx, p.sy - linePts[i - 1].sy), 0)
  const floorY   = (PAD.top + iH).toFixed(1)
  const areaD    = linePts.length > 1
    ? `${pathD} L${linePts[linePts.length - 1].sx.toFixed(1)},${floorY} L${linePts[0].sx.toFixed(1)},${floorY} Z`
    : ''

  const TT_W = 130
  const TT_H = rMap.size > 0 && fMap.size > 0 ? 62 : 44

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ maxHeight: 300 }}
      onMouseLeave={() => setTooltip(null)}>

      {/* Grid + primary Y labels */}
      {yTicks.map((t, i) => (
        <g key={i}>
          <line x1={PAD.left} x2={W - PAD.right} y1={yPrimary(t)} y2={yPrimary(t)}
            stroke="white" strokeOpacity={0.06} />
          <text x={PAD.left - 6} y={yPrimary(t) + 4} textAnchor="end" fontSize={10}
            fill="rgba(255,255,255,0.35)">
            {t % 1 === 0 ? t : t.toFixed(1)}
          </text>
        </g>
      ))}

      {/* X labels */}
      {xTicks.map((t, i) => (
        <text key={i} x={xScale(t)} y={H - PAD.bottom + 16} textAnchor="middle"
          fontSize={10} fill="rgba(255,255,255,0.35)">{t}</text>
      ))}

      {/* ── BARS (film count) ───────────────────────────────────────── */}
      {[...fMap.entries()].map(([year, count]) => {
        const bh  = (count / maxFilms) * iH
        const bx  = xScale(year) - barW / 2
        const by  = PAD.top + iH - bh
        const isH = tooltip?.xVal === year
        // Rating mode: very subtle white bars in background
        // Films  mode: amber bars, prominent
        const fill    = mode === 'films' ? AMBER : 'white'
        const opacity = mode === 'films'
          ? (animated ? (isH ? 0.75 : 0.55) : 0)
          : (animated ? (isH ? 0.22 : 0.10) : 0)
        return (
          <rect key={year} x={bx} y={by} width={barW} height={bh}
            fill={fill} opacity={opacity} rx={2}
            style={{ transition: 'opacity 0.4s ease-out', cursor: 'crosshair' }}
            onMouseEnter={() => setTooltip({
              xVal: year, pivotX: xScale(year), pivotY: by,
              rating: rMap.get(year), films: count,
            })}
          />
        )
      })}

      {/* ── AREA fill (rating, only in rating mode) ─────────────────── */}
      {mode === 'rating' && areaD && (
        <path d={areaD} fill={AMBER}
          fillOpacity={animated ? 0.07 : 0}
          style={{ transition: 'fill-opacity 0.6s ease-out' }} />
      )}

      {/* ── LINE (rating) ───────────────────────────────────────────── */}
      {linePts.length > 1 && (
        <path d={pathD} fill="none"
          stroke={mode === 'rating' ? AMBER : `${AMBER}55`}
          strokeWidth={mode === 'rating' ? 2.5 : 1.5}
          strokeLinecap="round" strokeLinejoin="round"
          strokeDasharray={totalLen}
          strokeDashoffset={animated ? 0 : totalLen}
          style={{ transition: 'stroke-dashoffset 1.0s ease-out' }} />
      )}

      {/* ── DOTS (rating mode only) ─────────────────────────────────── */}
      {mode === 'rating' && linePts.map((p, i) => {
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
              rating: p.yVal, films: fMap.get(p.xVal),
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
        return (
          <g style={{ pointerEvents: 'none' }}>
            <rect x={bx} y={by} width={TT_W} height={TT_H} rx={6}
              fill="#1e1e2e" stroke="rgba(255,255,255,0.12)" strokeWidth={1} />
            <text x={bx + 10} y={by + 14} fontSize={10} fontWeight="600"
              fill="rgba(255,255,255,0.45)">{tooltip.xVal}</text>
            {tooltip.rating !== undefined && (
              <g>
                <circle cx={bx + 14} cy={by + 30} r={3.5} fill={AMBER} />
                <text x={bx + 24} y={by + 34} fontSize={10} fill="rgba(255,255,255,0.75)">Rating</text>
                <text x={bx + TT_W - 8} y={by + 34} fontSize={11} fontWeight="700"
                  fill={AMBER} textAnchor="end">{tooltip.rating.toFixed(1)}</text>
              </g>
            )}
            {tooltip.films !== undefined && (
              <g>
                <circle cx={bx + 14} cy={by + 48} r={3.5} fill="rgba(255,255,255,0.35)" />
                <text x={bx + 24} y={by + 52} fontSize={10} fill="rgba(255,255,255,0.75)">Films</text>
                <text x={bx + TT_W - 8} y={by + 52} fontSize={11} fontWeight="700"
                  fill="white" textAnchor="end">{tooltip.films}</text>
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
  return (
    <div className="flex gap-5 mt-3">
      <div className="flex items-center gap-1.5 text-xs text-white/50">
        <div className="w-6 h-0.5 rounded-full" style={{ background: AMBER }} />
        Avg Rating
      </div>
      <div className="flex items-center gap-1.5 text-xs text-white/50">
        <div className="w-3 h-3 rounded-sm"
          style={{ background: mode === 'films' ? AMBER : 'rgba(255,255,255,0.25)' }} />
        Films / yr
      </div>
    </div>
  )
}

// ── Main component ─────────────────────────────────────────────────────────────

interface ActorCareerChartProps {
  actorId:       number
  actorName:     string
  firstFilmYear?: number
}

export default function ActorCareerChart({
  actorId,
  actorName,
  firstFilmYear = 1970,
}: ActorCareerChartProps) {
  const [mode,       setMode]       = useState<Mode>('rating')
  const [industry,   setIndustry]   = useState('All')
  const [yearFrom,   setYearFrom]   = useState(firstFilmYear)
  const [yearTo,     setYearTo]     = useState(2026)
  const [ratingData, setRatingData] = useState<ChartData | null>(null)
  const [filmData,   setFilmData]   = useState<ChartData | null>(null)
  const [loading,    setLoading]    = useState(false)
  const [error,      setError]      = useState<string | null>(null)
  const debounceRef = useRef<ReturnType<typeof setTimeout>>()

  const buildChart = useCallback(async (ind: string, yf: number, yt: number) => {
    if (yt <= yf || yf < 1950 || yt > 2026) return
    setLoading(true)
    setError(null)
    try {
      const opts = { actorIds: [actorId], industry: ind === 'All' ? undefined : ind, yf, yt }
      const [rating, films] = await Promise.all([
        getChartData(FIXED_X, 'avg_rating',  [actorId], opts.industry, yf, yt),
        getChartData(FIXED_X, 'film_count',  [actorId], opts.industry, yf, yt),
      ])
      setRatingData(rating)
      setFilmData(films)
    } catch {
      setError('Failed to load chart.')
    } finally {
      setLoading(false)
    }
  }, [actorId])

  useEffect(() => {
    clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => buildChart(industry, yearFrom, yearTo), 350)
    return () => clearTimeout(debounceRef.current)
  }, [industry, yearFrom, yearTo, buildChart])

  const hasData = ratingData || filmData

  return (
    <div data-section="career-chart" className="glass rounded-3xl p-6 sm:p-8">

      {/* ── Header + mode toggle ─────────────────────────────────────── */}
      <div className="flex items-start justify-between mb-6">
        <div>
          <h2 className="text-white font-bold text-xl tracking-tight">Career at a Glance</h2>
          <p className="text-white/40 text-sm mt-1">{actorName}'s year-by-year trajectory</p>
        </div>

        {/* Primary toggle: Rating | Films / yr */}
        <div className="flex gap-1 bg-white/[0.05] rounded-xl p-1 border border-white/[0.08] flex-shrink-0">
          {(['rating', 'films'] as Mode[]).map(m => (
            <button key={m} onClick={() => setMode(m)}
              className={`px-4 py-1.5 rounded-lg text-sm font-medium transition-all duration-200 ${
                mode === m
                  ? 'bg-amber-500/20 text-amber-400 border border-amber-500/30'
                  : 'text-white/40 hover:text-white/60'
              }`}>
              {m === 'rating' ? 'Rating' : 'Films / yr'}
            </button>
          ))}
        </div>
      </div>

      {/* ── Filters ─────────────────────────────────────────────────── */}
      <div className="flex flex-wrap gap-4 items-end mb-6 pb-6 border-b border-white/[0.07]">
        {/* Industry */}
        <div className="min-w-[130px]">
          <label className="block text-xs text-white/40 uppercase tracking-wider mb-1.5">
            Industry
          </label>
          <select value={industry} onChange={e => setIndustry(e.target.value)}
            className="w-full bg-white/[0.05] border border-white/[0.10] rounded-xl px-3 py-2.5 text-sm text-white outline-none focus:border-white/25 transition-colors">
            {INDUSTRY_OPTIONS.map(i => <option key={i} value={i}>{i}</option>)}
          </select>
        </div>

        {/* Year range */}
        <div>
          <label className="block text-xs text-white/40 uppercase tracking-wider mb-1.5">
            Year Range
          </label>
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
            {mode === 'rating' ? 'Avg Rating' : 'Films / yr'} by year
            {industry !== 'All' ? ` · ${industry}` : ''} · {yearFrom}–{yearTo}
          </div>
          <DualChart ratingData={ratingData} filmData={filmData} mode={mode} />
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
