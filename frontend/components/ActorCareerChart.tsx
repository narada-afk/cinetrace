'use client'

/**
 * ActorCareerChart — single-actor career trajectory chart.
 * Year on X, selectable metric on Y (rating, films/year, box office…).
 * Uses the same animated SVG line as CompareChartBuilder, amber colour.
 */

import { useState, useEffect, useRef, useCallback } from 'react'
import { getChartData, type ChartData } from '@/lib/api'

// ── Constants ──────────────────────────────────────────────────────────────────

const FIXED_X = 'year'

const Y_OPTIONS = [
  { value: 'avg_rating',       label: 'Avg Rating (0–10)',      group: 'Quality'    },
  { value: 'hit_rate',         label: 'Hit Rate % (≥7.0)',     group: 'Quality'    },
  { value: 'avg_popularity',   label: 'Avg Popularity Score',   group: 'Quality'    },
  { value: 'film_count',       label: 'Films per Year',         group: 'Career'     },
  { value: 'unique_directors', label: 'Unique Directors',       group: 'Career'     },
  { value: 'avg_box_office',   label: 'Avg Box Office (₹ Cr)', group: 'Box Office' },
  { value: 'total_box_office', label: 'Total Box Office (₹ Cr)', group: 'Box Office' },
]

const INDUSTRY_OPTIONS = ['All', 'Tamil', 'Telugu', 'Malayalam', 'Kannada']
const LINE_COLOR = '#f59e0b'   // amber

// ── SVG Line Chart ─────────────────────────────────────────────────────────────

interface TooltipState {
  x: number; y: number; xVal: number; value: number
}

function LineChart({ data }: { data: ChartData }) {
  const [animated, setAnimated] = useState(false)
  const [tooltip, setTooltip]   = useState<TooltipState | null>(null)

  useEffect(() => {
    setAnimated(false)
    setTooltip(null)
    const t = setTimeout(() => setAnimated(true), 60)
    return () => clearTimeout(t)
  }, [data])

  const W = 800, H = 300, PAD = { top: 24, right: 20, bottom: 40, left: 52 }
  const innerW = W - PAD.left - PAD.right
  const innerH = H - PAD.top - PAD.bottom

  const series = data.series[0]
  if (!series || series.points.length === 0) return null

  const rawPts = series.points.map(p => ({ x: Number(p.x), y: p.y }))
  const allX   = rawPts.map(p => p.x)
  const allY   = rawPts.map(p => p.y)
  const minX   = allX[0], maxX = allX[allX.length - 1]
  const maxY   = Math.max(...allY) * 1.1 || 1

  const xScale = (x: number) => PAD.left + ((x - minX) / ((maxX - minX) || 1)) * innerW
  const yScale = (y: number) => PAD.top + innerH - (y / maxY) * innerH
  const yTicks = Array.from({ length: 5 }, (_, i) => maxY * i / 4)
  const step   = Math.max(1, Math.ceil(allX.length / 10))
  const xTicks = allX.filter((_, i) => i % step === 0)

  const svgPts = rawPts.map(p => ({
    sx: xScale(p.x), sy: yScale(p.y), xVal: p.x, yVal: p.y,
  }))
  const pathD    = svgPts.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.sx.toFixed(1)},${p.sy.toFixed(1)}`).join(' ')
  const totalLen = svgPts.reduce((acc, p, i) =>
    i === 0 ? 0 : acc + Math.hypot(p.sx - svgPts[i - 1].sx, p.sy - svgPts[i - 1].sy), 0)

  const floorY  = (PAD.top + innerH).toFixed(1)
  const areaD   = `${pathD} L${svgPts[svgPts.length - 1].sx.toFixed(1)},${floorY} L${svgPts[0].sx.toFixed(1)},${floorY} Z`
  const TT_W = 110, TT_H = 44

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ maxHeight: 300 }}
      onMouseLeave={() => setTooltip(null)}>

      {/* Grid + Y axis labels */}
      {yTicks.map((t, i) => (
        <g key={i}>
          <line x1={PAD.left} x2={W - PAD.right} y1={yScale(t)} y2={yScale(t)}
            stroke="white" strokeOpacity={0.06} />
          <text x={PAD.left - 6} y={yScale(t) + 4} textAnchor="end" fontSize={10}
            fill="rgba(255,255,255,0.35)">
            {t % 1 === 0 ? t : t.toFixed(1)}
          </text>
        </g>
      ))}
      {/* X axis labels */}
      {xTicks.map((t, i) => (
        <text key={i} x={xScale(t)} y={H - PAD.bottom + 16} textAnchor="middle"
          fontSize={10} fill="rgba(255,255,255,0.35)">{t}</text>
      ))}

      {/* Area fill */}
      <path d={areaD} fill={LINE_COLOR}
        fillOpacity={animated ? 0.08 : 0}
        style={{ transition: 'fill-opacity 0.6s ease-out' }} />

      {/* Line */}
      <path d={pathD} fill="none" stroke={LINE_COLOR} strokeWidth={2.5}
        strokeLinecap="round" strokeLinejoin="round"
        strokeDasharray={totalLen} strokeDashoffset={animated ? 0 : totalLen}
        style={{ transition: 'stroke-dashoffset 1.0s ease-out' }} />

      {/* Data points */}
      {svgPts.map((p, i) => {
        const isHov = tooltip?.xVal === p.xVal
        return (
          <circle key={i} cx={p.sx} cy={p.sy}
            r={isHov ? 5 : 2.5}
            fill={isHov ? 'white' : LINE_COLOR}
            stroke={isHov ? LINE_COLOR : 'none'} strokeWidth={isHov ? 2 : 0}
            opacity={animated ? 1 : 0}
            style={{ transition: 'opacity 0.3s 0.8s, r 0.1s', cursor: 'crosshair' }}
            onMouseEnter={() => setTooltip({ x: p.sx, y: p.sy, xVal: p.xVal, value: p.yVal })}
          />
        )
      })}

      {/* Crosshair */}
      {tooltip && (
        <line x1={xScale(tooltip.xVal)} x2={xScale(tooltip.xVal)}
          y1={PAD.top} y2={PAD.top + innerH}
          stroke="white" strokeOpacity={0.15} strokeWidth={1} strokeDasharray="4 3"
          style={{ pointerEvents: 'none' }} />
      )}

      {/* Tooltip */}
      {tooltip && (() => {
        const tx  = xScale(tooltip.xVal)
        const bx  = tx + TT_W + 12 > W - PAD.right ? tx - TT_W - 8 : tx + 8
        const by  = Math.max(PAD.top, Math.min(tooltip.y - TT_H / 2, PAD.top + innerH - TT_H))
        return (
          <g style={{ pointerEvents: 'none' }}>
            <rect x={bx} y={by} width={TT_W} height={TT_H} rx={6}
              fill="#1e1e2e" stroke="rgba(255,255,255,0.12)" strokeWidth={1} />
            <text x={bx + 10} y={by + 14} fontSize={10} fontWeight="600"
              fill="rgba(255,255,255,0.5)">{tooltip.xVal}</text>
            <text x={bx + TT_W - 8} y={by + 32} fontSize={12} fontWeight="700"
              fill={LINE_COLOR} textAnchor="end">
              {tooltip.value % 1 === 0 ? tooltip.value : tooltip.value.toFixed(2)}
            </text>
          </g>
        )
      })()}
    </svg>
  )
}

// ── Main component ─────────────────────────────────────────────────────────────

interface ActorCareerChartProps {
  actorId:      number
  actorName:    string
  firstFilmYear?: number
}

export default function ActorCareerChart({
  actorId,
  actorName,
  firstFilmYear = 1970,
}: ActorCareerChartProps) {
  const [yAxis,     setYAxis]     = useState('avg_rating')
  const [industry,  setIndustry]  = useState('All')
  const [yearFrom,  setYearFrom]  = useState(firstFilmYear)
  const [yearTo,    setYearTo]    = useState(2026)
  const [chartData, setChartData] = useState<ChartData | null>(null)
  const [loading,   setLoading]   = useState(false)
  const [error,     setError]     = useState<string | null>(null)
  const debounceRef = useRef<ReturnType<typeof setTimeout>>()

  const buildChart = useCallback(async (y: string, ind: string, yf: number, yt: number) => {
    if (yt <= yf || yf < 1950 || yt > 2026) return
    setLoading(true)
    setError(null)
    try {
      const data = await getChartData(
        FIXED_X, y, [actorId],
        ind === 'All' ? undefined : ind,
        yf, yt,
      )
      setChartData(data)
    } catch {
      setError('Failed to load chart.')
    } finally {
      setLoading(false)
    }
  }, [actorId])

  useEffect(() => {
    clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => {
      buildChart(yAxis, industry, yearFrom, yearTo)
    }, 350)
    return () => clearTimeout(debounceRef.current)
  }, [yAxis, industry, yearFrom, yearTo, buildChart])

  const yLabel  = Y_OPTIONS.find(o => o.value === yAxis)?.label ?? yAxis
  const yGroups = Y_OPTIONS.reduce<Record<string, typeof Y_OPTIONS>>((acc, o) => {
    if (!acc[o.group]) acc[o.group] = []
    acc[o.group].push(o)
    return acc
  }, {})

  return (
    <div data-section="career-chart" className="glass rounded-3xl p-6 sm:p-8">
      {/* Header */}
      <div className="mb-6">
        <h2 className="text-white font-bold text-xl tracking-tight">Career at a Glance</h2>
        <p className="text-white/40 text-sm mt-1">{actorName}'s year-by-year trajectory</p>
      </div>

      {/* Controls */}
      <div className="flex flex-wrap gap-4 items-end mb-6 pb-6 border-b border-white/[0.07]">
        {/* Metric */}
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

      {/* Chart area */}
      {chartData ? (
        <div key={`${yAxis}-${industry}-${yearFrom}-${yearTo}`}
          style={{ animation: 'careerChartFadeIn 0.35s ease-out both' }}>
          <style>{`
            @keyframes careerChartFadeIn {
              from { opacity: 0; transform: translateY(8px); }
              to   { opacity: 1; transform: translateY(0); }
            }
          `}</style>
          <div className="text-xs text-white/30 mb-4">
            Year vs {yLabel}{industry !== 'All' ? ` · ${industry}` : ''} · {yearFrom}–{yearTo}
          </div>
          <LineChart data={chartData} />
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
