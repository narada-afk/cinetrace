'use client'

import { useState, useEffect, useRef, useMemo } from 'react'
import { useRouter } from 'next/navigation'
import ActorAvatar from '@/components/ActorAvatar'
import {
  searchActors,
  getActorCollaborators,
  getActorLeadCollaborators,
  getActorDirectors,
  toActorSlug,
  type Actor,
} from '@/lib/api'

// ── Exported types ────────────────────────────────────────────────────────────

export interface NetworkNode {
  id: number | null
  name: string
  films: number
  kind: 'lead' | 'director' | 'supporting'
}

export interface NetworkCenter {
  id: number
  name: string
  gender?: 'M' | 'F' | null
}

// ── Layout constants ──────────────────────────────────────────────────────────

const SVG_W    = 1100
const SVG_H    = 400
const CX       = SVG_W / 2
const CY       = SVG_H / 2
const CENTER_R = 21

// Mobile inline canvas — square-ish, top 12 nodes, larger dots/fonts
const MOB_W   = 500
const MOB_H   = 500
const MOB_CX  = MOB_W / 2
const MOB_CY  = MOB_H / 2
const MOB_MAX = 12

// Expanded full-screen canvas
const EXP_W  = 1800
const EXP_H  = 900
const EXP_CX = EXP_W / 2
const EXP_CY = EXP_H / 2

const CENTER_COLOR = '#f5c518'

// Kind colours
const KIND_COLOR: Record<NetworkNode['kind'], string> = {
  lead:       '#f472b6',
  director:   '#22d3ee',
  supporting: '#e2e8f0',
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function sr(seed: number): number {
  const x = Math.sin(seed * 9301 + 49297) * 233280
  return x - Math.floor(x)
}

function scatterPos(i: number, kind: NetworkNode['kind']): { x: number; y: number } {
  const angle = sr(i * 37 + 11) * 2 * Math.PI
  const [rxMin, rxMax] = kind === 'lead'      ? [75,  360] : kind === 'director' ? [95,  390] : [110, 420]
  const [ryMin, ryMax] = kind === 'lead'      ? [45,  145] : kind === 'director' ? [55,  158] : [65,  172]
  const rx = rxMin + sr(i * 23 + 7)  * (rxMax - rxMin)
  const ry = ryMin + sr(i * 41 + 13) * (ryMax - ryMin)
  const jx = (sr(i * 53 + 17) - 0.5) * 45
  const jy = (sr(i * 61 + 19) - 0.5) * 28
  const x = Math.max(48, Math.min(SVG_W - 48, CX + rx * Math.cos(angle) + jx))
  const y = Math.max(24, Math.min(SVG_H - 24, CY + ry * Math.sin(angle) + jy))
  return { x, y }
}

function scatterPosExpanded(i: number, kind: NetworkNode['kind']): { x: number; y: number } {
  const PAD_X = 90, PAD_Y = 55
  // Minimum clear radius around the centre avatar
  const CLEAR_R = kind === 'director' ? 115 : 135

  // Uniformly distribute across the full canvas (fills all four corners)
  const x0 = PAD_X + sr(i * 37 + 11) * (EXP_W - 2 * PAD_X)
  const y0 = PAD_Y + sr(i * 41 + 13) * (EXP_H - 2 * PAD_Y)

  // If the point falls inside the centre clear-zone, push it outward
  const dx = x0 - EXP_CX
  const dy = y0 - EXP_CY
  const dist = Math.sqrt(dx * dx + dy * dy)
  if (dist < CLEAR_R) {
    const scale = CLEAR_R / Math.max(dist, 1)
    return {
      x: Math.max(PAD_X, Math.min(EXP_W - PAD_X, EXP_CX + dx * scale)),
      y: Math.max(PAD_Y, Math.min(EXP_H - PAD_Y, EXP_CY + dy * scale)),
    }
  }
  return { x: x0, y: y0 }
}

/** Scatter positions for the mobile 500×500 inline canvas (12 nodes max). */
function scatterPosMobile(i: number, kind: NetworkNode['kind']): { x: number; y: number } {
  const angle = sr(i * 37 + 11) * 2 * Math.PI
  // Wider radii so 12 nodes have breathing room across the 500×500 canvas
  const [rxMin, rxMax] = kind === 'lead' ? [80, 205] : kind === 'director' ? [88, 210] : [95, 215]
  const [ryMin, ryMax] = kind === 'lead' ? [75, 200] : kind === 'director' ? [82, 205] : [90, 210]
  const rx = rxMin + sr(i * 23 + 7)  * (rxMax - rxMin)
  const ry = ryMin + sr(i * 41 + 13) * (ryMax - ryMin)
  const jx = (sr(i * 53 + 17) - 0.5) * 18
  const jy = (sr(i * 61 + 19) - 0.5) * 18
  const x = Math.max(44, Math.min(MOB_W - 44, MOB_CX + rx * Math.cos(angle) + jx))
  const y = Math.max(36, Math.min(MOB_H - 36, MOB_CY + ry * Math.sin(angle) + jy))
  return { x, y }
}

function coreR(films: number, isHov: boolean, scale = 1): number {
  if (isHov) return 5.5 * scale
  return (1.4 + Math.min(films * 0.04, 1.2)) * scale
}

function glowR(films: number, scale = 1): number {
  return (9 + Math.min(films * 0.3, 8)) * scale
}

function initials(name: string) {
  return name.split(' ').map(w => w[0] ?? '').join('').slice(0, 2).toUpperCase()
}

// ── Toast ─────────────────────────────────────────────────────────────────────

function useToast() {
  const [toast, setToast] = useState<string | null>(null)
  function showToast(msg: string) {
    setToast(msg)
    setTimeout(() => setToast(null), 2200)
  }
  return { toast, showToast }
}

// ── ActorPicker ───────────────────────────────────────────────────────────────

function ActorPicker({
  onSelect,
  loading,
  defaultSuggestions = [],
  variant = 'compact',
}: {
  onSelect: (actor: Actor) => void
  loading: boolean
  defaultSuggestions?: Actor[]
  variant?: 'compact' | 'prominent'
}) {
  const [query, setQuery]         = useState('')
  const [results, setResults]     = useState<Actor[]>([])
  const [focused, setFocused]     = useState(false)
  const [searching, setSearching] = useState(false)
  const inputRef = useRef<HTMLInputElement>(null)
  const dropRef  = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!query.trim()) { setResults([]); return }
    setSearching(true)
    const tid = setTimeout(async () => {
      try { setResults((await searchActors(query)).slice(0, 7)) }
      catch { setResults([]) }
      finally { setSearching(false) }
    }, 220)
    return () => clearTimeout(tid)
  }, [query])

  useEffect(() => {
    const h = (e: MouseEvent) => {
      if (
        dropRef.current?.contains(e.target as Node) ||
        inputRef.current?.contains(e.target as Node)
      ) return
      setFocused(false)
    }
    document.addEventListener('mousedown', h)
    return () => document.removeEventListener('mousedown', h)
  }, [])

  const displayList  = query.trim() ? results : defaultSuggestions
  const showDropdown = focused && displayList.length > 0

  function handleSelect(actor: Actor) {
    setQuery(''); setResults([]); setFocused(false); onSelect(actor)
  }

  // ── Prominent (used in graph header + empty state) ────────────────────────
  if (variant === 'prominent') {
    return (
      <div className="relative w-full max-w-xs">
        <div className="flex items-center gap-2 rounded-2xl bg-white/[0.09] border border-white/[0.22] px-4 py-2.5 focus-within:border-white/40 focus-within:bg-white/[0.12] transition-all">
          <span className="text-white/50 text-sm">🔍</span>
          <input
            ref={inputRef}
            value={query}
            onChange={e => setQuery(e.target.value)}
            onFocus={() => setFocused(true)}
            placeholder={loading ? 'Loading…' : 'Switch actor…'}
            disabled={loading}
            className="bg-transparent text-white text-sm placeholder-white/40 outline-none flex-1 disabled:opacity-50"
          />
          {(searching || loading) && (
            <span className="text-white/30 text-xs animate-pulse">…</span>
          )}
        </div>
        {showDropdown && (
          <div
            ref={dropRef}
            className="absolute left-0 right-0 top-full mt-2 rounded-2xl overflow-hidden z-[100] shadow-2xl border border-white/[0.10]"
            style={{ background: '#1e1e2c' }}
          >
            {!query.trim() && (
              <p className="text-white/25 text-[10px] uppercase tracking-widest px-3 pt-2.5 pb-1">
                Popular actors
              </p>
            )}
            {displayList.map(a => (
              <button
                key={a.id}
                onMouseDown={e => e.preventDefault()}
                onClick={() => handleSelect(a)}
                className="w-full flex items-center gap-2.5 px-4 py-2.5 hover:bg-white/[0.08] transition-colors text-left border-b border-white/[0.05] last:border-0"
              >
                <ActorAvatar name={a.name} size={28} />
                <div className="min-w-0">
                  <p className="text-white text-sm font-medium truncate">{a.name}</p>
                  {a.industry && <p className="text-white/35 text-xs">{a.industry}</p>}
                </div>
              </button>
            ))}
          </div>
        )}
      </div>
    )
  }

  // ── Compact (used in empty state quick-picks) ─────────────────────────────
  return (
    <div className="relative">
      <div className="flex items-center gap-1.5 rounded-full bg-white/[0.07] border border-white/[0.12] px-3 py-1.5 focus-within:border-white/25 transition-all">
        <span className="text-white/30 text-xs">🔍</span>
        <input
          ref={inputRef}
          value={query}
          onChange={e => setQuery(e.target.value)}
          onFocus={() => setFocused(true)}
          placeholder={loading ? 'Loading…' : 'Switch actor…'}
          disabled={loading}
          className="bg-transparent text-white text-xs placeholder-white/30 outline-none w-32 disabled:opacity-50"
        />
        {(searching || loading) && (
          <span className="text-white/30 text-[10px] animate-pulse">…</span>
        )}
      </div>
      {showDropdown && (
        <div
          ref={dropRef}
          className="absolute right-0 top-full mt-1.5 w-56 rounded-2xl overflow-hidden z-[100] shadow-2xl border border-white/[0.10]"
          style={{ background: '#1e1e2c' }}
        >
          {!query.trim() && (
            <p className="text-white/25 text-[10px] uppercase tracking-widest px-3 pt-2.5 pb-1">
              Popular actors
            </p>
          )}
          {displayList.map(a => (
            <button
              key={a.id}
              onMouseDown={e => e.preventDefault()}
              onClick={() => handleSelect(a)}
              className="w-full flex items-center gap-2.5 px-3 py-2 hover:bg-white/[0.08] transition-colors text-left border-b border-white/[0.05] last:border-0"
            >
              <ActorAvatar name={a.name} size={24} />
              <div className="min-w-0">
                <p className="text-white text-xs font-medium truncate">{a.name}</p>
                {a.industry && <p className="text-white/35 text-[10px]">{a.industry}</p>}
              </div>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

// ── Shared constellation SVG ──────────────────────────────────────────────────

function ConstellationSVG({
  W, H, cx, cy,
  center, nodes, positions,
  bgStars,
  hovered, setHovered,
  centerImgError, setCenterImgError,
  onNodeClick, onCenterClick,
  idPrefix = '',
  fs = { name: 8.5, detail: 7, centerName: 10, centerSub: 7.5 },
  centerR,
  nodeScale = 1,
  svgTouchAction = 'pan-y',
}: {
  W: number; H: number; cx: number; cy: number
  center: NetworkCenter
  nodes: NetworkNode[]
  positions: { x: number; y: number }[]
  bgStars: { x: number; y: number; r: number; op: number }[]
  hovered: 'center' | number | null
  setHovered: (v: 'center' | number | null) => void
  centerImgError: boolean
  setCenterImgError: (v: boolean) => void
  onNodeClick: (node: NetworkNode) => void
  onCenterClick: () => void
  idPrefix?: string
  fs?: { name: number; detail: number; centerName: number; centerSub: number }
  centerR?: number
  /** Scale multiplier for node dot + glow radii (default 1; use 2 for mobile inline). */
  nodeScale?: number
  /** CSS touch-action for the SVG element (default 'pan-y'; use 'none' in fullscreen overlay). */
  svgTouchAction?: string
}) {
  const avatarSlug  = center.name.toLowerCase().replace(/[^a-z0-9]/g, '')
  const isHovCenter = hovered === 'center'
  const p = idPrefix  // shorthand
  const CR = centerR ?? CENTER_R

  return (
    <svg
      viewBox={`0 0 ${W} ${H}`}
      className="w-full h-full"
      style={{ display: 'block', touchAction: svgTouchAction as React.CSSProperties['touchAction'] }}
      onTouchStart={() => setHovered(null)}
    >
      <defs>
        <filter id={`${p}gp-galblur`} x="-50%" y="-50%" width="200%" height="200%">
          <feGaussianBlur stdDeviation="38"/>
        </filter>
        <linearGradient id={`${p}gp-gband`} x1="0.08" y1="0" x2="0.78" y2="1">
          <stop offset="0%"   stopColor="#6496ff" stopOpacity="0"/>
          <stop offset="25%"  stopColor="#90b4ff" stopOpacity="0.05"/>
          <stop offset="42%"  stopColor="#b8ccff" stopOpacity="0.12"/>
          <stop offset="56%"  stopColor="#c8d8ff" stopOpacity="0.09"/>
          <stop offset="75%"  stopColor="#90b4ff" stopOpacity="0.04"/>
          <stop offset="100%" stopColor="#6496ff" stopOpacity="0"/>
        </linearGradient>
        <radialGradient id={`${p}gp-gwarm`} cx="50%" cy="50%" r="50%">
          <stop offset="0%"   stopColor="#ff9040" stopOpacity="0.10"/>
          <stop offset="55%"  stopColor="#ff6820" stopOpacity="0.04"/>
          <stop offset="100%" stopColor="#ff4400" stopOpacity="0"/>
        </radialGradient>
        <radialGradient id={`${p}gp-gcool`} cx="50%" cy="50%" r="50%">
          <stop offset="0%"   stopColor="#7050f8" stopOpacity="0.09"/>
          <stop offset="55%"  stopColor="#5538d8" stopOpacity="0.03"/>
          <stop offset="100%" stopColor="#3820b0" stopOpacity="0"/>
        </radialGradient>
        <radialGradient id={`${p}gp-nebula`} cx="50%" cy="48%" r="50%">
          <stop offset="0%"   stopColor="#ef4444" stopOpacity="0.10"/>
          <stop offset="40%"  stopColor="#7c3aed" stopOpacity="0.04"/>
          <stop offset="100%" stopColor="#000000" stopOpacity="0"/>
        </radialGradient>
        <radialGradient id={`${p}gp-vignette`} cx="50%" cy="50%" r="50%">
          <stop offset="40%"  stopColor="#0a0a14" stopOpacity="0"/>
          <stop offset="78%"  stopColor="#0a0a14" stopOpacity="0.55"/>
          <stop offset="100%" stopColor="#07070f" stopOpacity="0.90"/>
        </radialGradient>
        <clipPath id={`${p}gp-centerclip`}>
          <circle cx={cx} cy={cy} r={CR - 2}/>
        </clipPath>
        <filter id={`${p}gp-nglow`} x="-80%" y="-80%" width="260%" height="260%">
          <feGaussianBlur in="SourceGraphic" stdDeviation="3.5" result="b"/>
          <feComposite in="SourceGraphic" in2="b" operator="over"/>
        </filter>
        <filter id={`${p}gp-nglow2`} x="-50%" y="-50%" width="200%" height="200%">
          <feGaussianBlur in="SourceGraphic" stdDeviation="1.5" result="b"/>
          <feComposite in="SourceGraphic" in2="b" operator="over"/>
        </filter>
        <filter id={`${p}gp-lineglow`} x="-200%" y="-200%" width="500%" height="500%">
          <feGaussianBlur in="SourceGraphic" stdDeviation="2.5" result="b"/>
          <feComposite in="SourceGraphic" in2="b" operator="over"/>
        </filter>
        <linearGradient id={`${p}gp-ring-grad`} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%"   stopColor="#ffcc44" stopOpacity="1"/>
          <stop offset="35%"  stopColor="#ff8800" stopOpacity="1"/>
          <stop offset="70%"  stopColor="#ffaa22" stopOpacity="1"/>
          <stop offset="100%" stopColor="#ffcc44" stopOpacity="1"/>
        </linearGradient>
        <filter id={`${p}gp-ringblur`} x="-20%" y="-20%" width="140%" height="140%">
          <feGaussianBlur stdDeviation="1.5"/>
        </filter>
        {nodes.map((node, i) => {
          const { x, y } = positions[i]
          const col = KIND_COLOR[node.kind]
          return (
            <linearGradient key={i} id={`${p}gp-lg${i}`}
              x1={cx} y1={cy} x2={x} y2={y} gradientUnits="userSpaceOnUse">
              <stop offset="0%"   stopColor={CENTER_COLOR} stopOpacity="0"/>
              <stop offset="100%" stopColor={col}           stopOpacity="0.15"/>
            </linearGradient>
          )
        })}
      </defs>

      {/* Galaxy atmosphere */}
      <rect x="0" y="0" width={W} height={H} fill={`url(#${p}gp-gband)`} filter={`url(#${p}gp-galblur)`}/>
      <rect x="0" y={H * 0.25} width={W * 0.55} height={H * 0.75} fill={`url(#${p}gp-gwarm)`} filter={`url(#${p}gp-galblur)`}/>
      <rect x={W * 0.45} y="0" width={W * 0.55} height={H * 0.75} fill={`url(#${p}gp-gcool)`} filter={`url(#${p}gp-galblur)`}/>

      {/* Background micro-stars (scale coords to current canvas) */}
      {bgStars.map((s, i) => (
        <circle key={i} cx={s.x * (W / SVG_W)} cy={s.y * (H / SVG_H)} r={s.r} fill="white" opacity={s.op}/>
      ))}

      {/* Nebula */}
      <rect x={cx - 180} y={cy - 130} width="360" height="260"
        fill={`url(#${p}gp-nebula)`} filter={`url(#${p}gp-galblur)`} opacity="0.9"/>

      {/* Connection lines */}
      {nodes.map((node, i) => {
        const { x, y } = positions[i]
        const isHov = hovered === i || hovered === 'center'
        return (
          <g key={i} style={{ pointerEvents: 'none' }}>
            {isHov && (
              <line x1={cx} y1={cy} x2={x} y2={y}
                stroke={KIND_COLOR[node.kind]} strokeWidth="1.2" strokeOpacity="0.35"
                filter={`url(#${p}gp-lineglow)`}/>
            )}
            <line x1={cx} y1={cy} x2={x} y2={y}
              stroke={`url(#${p}gp-lg${i})`}
              strokeWidth={isHov ? 0.8 : 0.5}
              style={{ transition: 'stroke-width 0.2s ease' }}/>
          </g>
        )
      })}

      {/* Scattered nodes */}
      {nodes.map((node, i) => {
        const { x, y } = positions[i]
        const col   = KIND_COLOR[node.kind]
        const isHov = hovered === i
        const cr    = coreR(node.films, isHov, nodeScale)
        const gr    = glowR(node.films, nodeScale)
        const floatAmp = 2.5 + sr(i * 7 + 1) * 3.5
        const floatDur = `${3.8 + sr(i * 11 + 3) * 2.8}s`
        const floatDel = `${sr(i * 13 + 7) * 2}s`
        return (
          <g key={i}
            style={{
              cursor: node.id ? 'pointer' : 'default',
              // @ts-ignore
              '--gp-amp': `-${floatAmp}px`,
              animation: `gp-float ${floatDur} ${floatDel} ease-in-out infinite`,
            }}
            onMouseEnter={() => setHovered(i)}
            onMouseLeave={() => setHovered(null)}
            onClick={() => onNodeClick(node)}
            onTouchStart={e => { e.stopPropagation(); setHovered(i) }}
          >
            <circle cx={x} cy={y} r={cr * 2.8} fill={col} opacity={isHov ? 0 : 0.12} filter={`url(#${p}gp-nglow)`}/>
            <circle cx={x} cy={y} r={gr * 1.8} fill={col} opacity={isHov ? 0.22 : 0}
              filter={`url(#${p}gp-nglow)`} style={{ transition: 'opacity 0.18s ease' }}/>
            <circle cx={x} cy={y} r={gr} fill={col} opacity={isHov ? 0.45 : 0}
              filter={`url(#${p}gp-nglow2)`} style={{ transition: 'opacity 0.18s ease' }}/>
            <circle cx={x} cy={y} r={cr} fill={isHov ? '#fff' : col} opacity={isHov ? 1 : 0.80}
              style={{ transition: 'r 0.18s ease, fill 0.18s ease' }}/>
            {/* Non-hover label only — hovered label is re-rendered after the vignette
                so the edge-darkening overlay never dims it */}
            {!isHov && (
              <text x={x} y={y - cr - 5} textAnchor="middle"
                fontSize={fs.name} fontWeight="400"
                fill="rgba(255,255,255,0.32)"
                style={{ userSelect: 'none' }}>
                {node.name.split(' ')[0]}
              </text>
            )}
          </g>
        )
      })}

      {/* Center sun */}
      <g style={{ cursor: 'pointer' }}
        onMouseEnter={() => setHovered('center')}
        onMouseLeave={() => setHovered(null)}
        onClick={onCenterClick}
      >
        <g>
          <animateTransform attributeName="transform" type="rotate"
            from={`0 ${cx} ${cy}`} to={`360 ${cx} ${cy}`}
            dur="7s" repeatCount="indefinite" calcMode="linear"/>
          <circle cx={cx} cy={cy} r={CR + 2}
            fill="none" stroke={`url(#${p}gp-ring-grad)`} strokeWidth="2.5"
            filter={`url(#${p}gp-ringblur)`} opacity="0.65">
            <animate attributeName="r"
              values={`${CR + 2};${CR + 2.7};${CR + 2}`}
              dur="3.5s" repeatCount="indefinite" calcMode="spline"
              keySplines="0.45 0 0.55 1;0.45 0 0.55 1"/>
          </circle>
        </g>
        <circle cx={cx} cy={cy} r={CR} fill="#0d0d15"/>
        {!centerImgError ? (
          <image
            href={`/avatars/${avatarSlug}.png`}
            x={cx - CR + 2} y={cy - CR + 2}
            width={(CR - 2) * 2} height={(CR - 2) * 2}
            clipPath={`url(#${p}gp-centerclip)`}
            preserveAspectRatio="xMidYMid slice"
            onError={() => setCenterImgError(true)}
          />
        ) : (
          <text x={cx} y={cy} textAnchor="middle" dominantBaseline="central"
            fontSize={CR * 0.48} fontWeight="700"
            fill={CENTER_COLOR} style={{ userSelect: 'none' }}>
            {initials(center.name)}
          </text>
        )}
        <text x={cx} y={cy + CR + 17} textAnchor="middle"
          fontSize={fs.centerName} fontWeight="600"
          fill={isHovCenter ? 'rgba(255,255,255,0.95)' : 'rgba(255,255,255,0.68)'}
          style={{ userSelect: 'none', transition: 'fill 0.22s ease' }}>
          {center.name.split(' ')[0]}
        </text>
        {isHovCenter && (
          <text x={cx} y={cy + CR + 29} textAnchor="middle"
            fontSize={fs.centerSub} fill="rgba(255,255,255,0.35)" style={{ userSelect: 'none' }}>
            tap to explore
          </text>
        )}
      </g>

      {/* Vignette */}
      <rect x="0" y="0" width={W} height={H}
        fill={`url(#${p}gp-vignette)`} style={{ pointerEvents: 'none' }}/>

      {/* Hovered node label — rendered AFTER the vignette so it is never dimmed
          by the edge-darkening overlay, regardless of how far from centre the node is */}
      {typeof hovered === 'number' && nodes[hovered] && (() => {
        const hn  = nodes[hovered]
        const hp  = positions[hovered]
        const hcr = coreR(hn.films, true, nodeScale)
        const col = KIND_COLOR[hn.kind]
        return (
          <g style={{ pointerEvents: 'none' }}>
            <text x={hp.x} y={hp.y - hcr - 5} textAnchor="middle"
              fontSize={fs.name * 1.9} fontWeight="700" fill="#ffffff"
              style={{ userSelect: 'none' }}>
              {hn.name}
            </text>
            <text x={hp.x} y={hp.y + hcr + 16} textAnchor="middle"
              fontSize={fs.detail * 1.5} fill={col} opacity="0.85"
              style={{ userSelect: 'none' }}>
              {hn.kind === 'director' ? 'Dir · ' : ''}{hn.films} {hn.films === 1 ? 'film' : 'films'}
            </text>
          </g>
        )
      })()}
    </svg>
  )
}

// ── Main component ────────────────────────────────────────────────────────────

export default function GraphPreview({
  networkData,
  suggestions = [],
}: {
  networkData: { center: NetworkCenter; nodes: NetworkNode[]; allNodes?: NetworkNode[] } | null
  suggestions?: Actor[]
}) {
  const router = useRouter()
  const [hovered, setHovered]                         = useState<'center' | number | null>(null)
  const [expandHovered, setExpandHovered]             = useState<'center' | number | null>(null)
  const [centerImgError, setCenterImgError]           = useState(false)
  // Seed state from SSR prop so the graph renders immediately on first paint
  // instead of waiting for the mount-time API calls to return.
  const [localCenter, setLocalCenter]                 = useState<NetworkCenter | null>(networkData?.center ?? null)
  const [localNodes, setLocalNodes]                   = useState<NetworkNode[]>(networkData?.nodes ?? [])
  const [localAllNodes, setLocalAllNodes]             = useState<NetworkNode[]>(networkData?.allNodes ?? networkData?.nodes ?? [])
  const [fetchingNetwork, setFetchingNetwork]         = useState(false)
  const [hasChosen, setHasChosen]                     = useState(false)
  const [graphContainerHovered, setGraphContainerHovered] = useState(false)
  const [isExpanded, setIsExpanded]                   = useState(false)
  // Mobile: 'graph' shows the constellation, 'list' shows a scrollable roster
  const [mobileTab, setMobileTab]                     = useState<'graph' | 'list'>('graph')
  // Fullscreen overlay pinch-to-zoom + single-finger pan
  // ─ We use refs for the live transform values and mutate the DOM directly so
  //   every touchmove frame skips React's reconciler entirely (no re-render jank).
  //   `isZoomed` is the only state — it flips at most twice per gesture and is
  //   only needed to show/hide the "Reset zoom" button.
  const [isZoomed, setIsZoomed]                       = useState(false)
  const expScaleRef       = useRef(1)
  const expOffsetRef      = useRef({ x: 0, y: 0 })
  const expContainerRef   = useRef<HTMLDivElement>(null)
  // Gesture tracking — prevents post-pan/pinch tap from firing node navigation
  const gestureMovedRef   = useRef(false)
  const pinchInitDistRef  = useRef<number | null>(null)
  const pinchInitScaleRef = useRef(1)
  const pinchInitOffsetRef = useRef({ x: 0, y: 0 })
  // Cached container centre so getBoundingClientRect isn't called per frame
  const containerCentreRef = useRef({ cx: 0, cy: 0 })
  const panStartRef       = useRef<{ x: number; y: number; ox: number; oy: number } | null>(null)
  const { toast, showToast } = useToast()

  useEffect(() => { setCenterImgError(false) }, [localCenter?.id])

  // ESC closes expanded view
  useEffect(() => {
    if (!isExpanded) return
    const h = (e: KeyboardEvent) => { if (e.key === 'Escape') setIsExpanded(false) }
    window.addEventListener('keydown', h)
    return () => window.removeEventListener('keydown', h)
  }, [isExpanded])

  useEffect(() => {
    if (networkData?.center && !hasChosen) {
      const { id, name, gender } = networkData.center
      handleActorSelect({ id, name, gender: gender ?? null } as Actor)
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const bgStars = useMemo(() => {
    const stars: { x: number; y: number; r: number; op: number }[] = []
    for (let i = 0; i < 220; i++) {
      stars.push({ x: sr(i*11+3)*SVG_W, y: sr(i*7+5)*SVG_H, r: sr(i*13+1)*0.85+0.1, op: sr(i*17+2)*0.35+0.04 })
    }
    for (let i = 0; i < 140; i++) {
      const t = sr(i*23+7)
      const spread = (sr(i*31+11)-0.5)*SVG_H*0.28
      const bx = t*(SVG_W*1.1)-SVG_W*0.05
      const by = (1-t)*(SVG_H*0.90)+spread+SVG_H*0.05
      stars.push({ x: Math.max(0,Math.min(SVG_W,bx)), y: Math.max(0,Math.min(SVG_H,by)), r: sr(i*19+3)*1.0+0.12, op: sr(i*29+7)*0.50+0.15 })
    }
    return stars
  }, [])

  async function handleActorSelect(actor: Actor) {
    setHasChosen(true)
    setFetchingNetwork(true)
    // Reset fullscreen zoom/pan whenever the centre actor changes
    expScaleRef.current  = 1
    expOffsetRef.current = { x: 0, y: 0 }
    setIsZoomed(false)
    if (expContainerRef.current) expContainerRef.current.style.transform = ''
    try {
      const [collaborators, leadCollabs, directors] = await Promise.all([
        getActorCollaborators(actor.id),
        getActorLeadCollaborators(actor.id).catch(() => []),
        getActorDirectors(actor.id).catch(()  => []),
      ])
      const leadNames  = new Set(leadCollabs.map(l => l.actor.toLowerCase().trim()))
      const dirNameSet = new Set(directors.slice(0, 8).map(d => d.director.toLowerCase().trim()))
      const nodes: NetworkNode[] = []
      directors.slice(0, 8).forEach(d => {
        nodes.push({ id: null, name: d.director, films: d.films, kind: 'director' })
      })
      const eligibleCollabs = collaborators.filter(c => !dirNameSet.has(c.actor.toLowerCase().trim()))

      // Filtered set for the compact inline view (~50 nodes)
      const TARGET = 50
      let threshold = 1
      for (let t = 1; t <= (eligibleCollabs[0]?.films ?? 1); t++) {
        const count = eligibleCollabs.filter(c => c.films >= t).length
        if (count <= TARGET) { threshold = t; break }
      }
      for (const c of eligibleCollabs) {
        if (c.films < threshold) break
        nodes.push({
          id:    c.actor_id || null,
          name:  c.actor,
          films: c.films,
          kind:  leadNames.has(c.actor.toLowerCase().trim()) ? 'lead' : 'supporting',
        })
      }

      // Full set — every collaborator, used in the expanded full-screen view
      const allNodes: NetworkNode[] = [
        ...directors.slice(0, 8).map(d => ({ id: null, name: d.director, films: d.films, kind: 'director' as const })),
        ...eligibleCollabs.map(c => ({
          id:    c.actor_id || null,
          name:  c.actor,
          films: c.films,
          kind:  leadNames.has(c.actor.toLowerCase().trim()) ? 'lead' as const : 'supporting' as const,
        })),
      ]

      setLocalCenter({ id: actor.id, name: actor.name, gender: actor.gender ?? null })
      setLocalNodes(nodes)
      setLocalAllNodes(allNodes)
    } catch {
      showToast('Failed to load network')
    } finally {
      setFetchingNetwork(false)
    }
  }

  async function handleShare() {
    if (!localCenter) return
    const url = typeof window !== 'undefined'
      ? `${window.location.origin}/?actor=${localCenter.id}`
      : `/?actor=${localCenter.id}`
    const shareData = { title: `${localCenter.name}'s Cinema Network`, text: `Explore ${localCenter.name}'s connections`, url }
    try {
      if (navigator.share && navigator.canShare?.(shareData)) await navigator.share(shareData)
      else { await navigator.clipboard.writeText(url); showToast('Link copied!') }
    } catch {
      try { await navigator.clipboard.writeText(url); showToast('Link copied!') } catch { /* noop */ }
    }
  }

  const center = localCenter
  const nodes  = localNodes

  // Mobile: top MOB_MAX nodes only (enough for a readable 500×500 canvas)
  const mobileNodes = useMemo(() => nodes.slice(0, MOB_MAX), [nodes])

  const positions = useMemo(
    () => nodes.map((n, i) => scatterPos(i, n.kind)),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [nodes.map(n => `${n.name}:${n.kind}`).join(',')]
  )
  const mobilePositions = useMemo(
    () => mobileNodes.map((n, i) => scatterPosMobile(i, n.kind)),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [mobileNodes.map(n => `${n.name}:${n.kind}`).join(',')]
  )
  const expandedPositions = useMemo(
    () => localAllNodes.map((n, i) => scatterPosExpanded(i, n.kind)),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [localAllNodes.map(n => `${n.name}:${n.kind}`).join(',')]
  )

  const leadCount     = nodes.filter(n => n.kind === 'lead').length
  const directorCount = nodes.filter(n => n.kind === 'director').length
  const supportCount  = nodes.filter(n => n.kind === 'supporting').length
  const hasGraph      = hasChosen && !!center && nodes.length > 0

  // ── Direct-DOM transform helpers (no React re-render per frame) ─────────────

  /** Apply scale + offset directly to the DOM node; only flips `isZoomed` state. */
  function applyExpTransform(scale: number, x: number, y: number) {
    expScaleRef.current  = scale
    expOffsetRef.current = { x, y }
    if (expContainerRef.current) {
      expContainerRef.current.style.transform = `translate(${x}px, ${y}px) scale(${scale})`
    }
    const nowZoomed = scale !== 1 || x !== 0 || y !== 0
    // Only trigger a re-render when the boolean flips (≤ 2 renders per gesture)
    setIsZoomed(prev => prev !== nowZoomed ? nowZoomed : prev)
  }

  function resetExpTransform() { applyExpTransform(1, 0, 0) }

  function handleNodeClick(node: NetworkNode) {
    // Suppress navigation if the finger just finished a pan/zoom gesture
    if (gestureMovedRef.current) { gestureMovedRef.current = false; return }
    if (node.id !== null) router.push(`/actors/${toActorSlug(node.name)}`)
  }
  function handleCenterClick() {
    if (gestureMovedRef.current) { gestureMovedRef.current = false; return }
    if (center) router.push(`/actors/${toActorSlug(center.name)}`)
  }

  // ── Pinch-to-zoom + single-finger pan for the fullscreen overlay ─────────────

  function getTouchDist(touches: React.TouchList) {
    const dx = touches[0].clientX - touches[1].clientX
    const dy = touches[0].clientY - touches[1].clientY
    return Math.sqrt(dx * dx + dy * dy)
  }

  function handleExpTouchStart(e: React.TouchEvent) {
    // Cache container centre once per gesture so we don't layout-thrash per frame
    const rect = (e.currentTarget as HTMLDivElement).getBoundingClientRect()
    containerCentreRef.current = { cx: rect.left + rect.width / 2, cy: rect.top + rect.height / 2 }
    gestureMovedRef.current = false

    if (e.touches.length === 2) {
      pinchInitDistRef.current   = getTouchDist(e.touches)
      pinchInitScaleRef.current  = expScaleRef.current
      pinchInitOffsetRef.current = { ...expOffsetRef.current }
      panStartRef.current = null
    } else if (e.touches.length === 1) {
      panStartRef.current = {
        x: e.touches[0].clientX, y: e.touches[0].clientY,
        ox: expOffsetRef.current.x, oy: expOffsetRef.current.y,
      }
      pinchInitDistRef.current = null
    }
  }

  function handleExpTouchMove(e: React.TouchEvent) {
    if (e.touches.length === 2 && pinchInitDistRef.current !== null) {
      e.preventDefault()
      const ratio    = getTouchDist(e.touches) / pinchInitDistRef.current
      // Cap at 3× (was 5×); clamp minimum to 0.8 so the graph never inverts
      const newScale = Math.max(0.8, Math.min(3, pinchInitScaleRef.current * ratio))

      // Zoom toward the pinch midpoint so the gesture feels anchored
      const mx = (e.touches[0].clientX + e.touches[1].clientX) / 2
      const my = (e.touches[0].clientY + e.touches[1].clientY) / 2
      const { cx, cy } = containerCentreRef.current
      const s0 = pinchInitScaleRef.current
      // Derived from the requirement that the content point under the midpoint
      // stays at the same viewport position as scale changes s0 → newScale:
      //   tx_new = (mx - cx) * (1 - newScale/s0) + tx0 * (newScale/s0)
      const r  = newScale / s0
      const tx = (mx - cx) * (1 - r) + pinchInitOffsetRef.current.x * r
      const ty = (my - cy) * (1 - r) + pinchInitOffsetRef.current.y * r

      applyExpTransform(newScale, tx, ty)
      gestureMovedRef.current = true

    } else if (e.touches.length === 1 && panStartRef.current) {
      e.preventDefault()
      const dx = e.touches[0].clientX - panStartRef.current.x
      const dy = e.touches[0].clientY - panStartRef.current.y
      // Only flag as a "gesture" once the finger has moved meaningfully
      if (Math.abs(dx) > 4 || Math.abs(dy) > 4) gestureMovedRef.current = true
      applyExpTransform(expScaleRef.current, panStartRef.current.ox + dx, panStartRef.current.oy + dy)
    }
  }

  function handleExpTouchEnd(e: React.TouchEvent) {
    if (e.touches.length < 2) pinchInitDistRef.current = null
    if (e.touches.length === 0) panStartRef.current = null
    // Keep gestureMovedRef true briefly so the synthetic onClick that fires
    // immediately after touchend is still suppressed
    if (gestureMovedRef.current) {
      setTimeout(() => { gestureMovedRef.current = false }, 300)
    }
  }

  const legendRow = (
    <p className="text-white/30 text-xs mt-0.5 flex items-center gap-2">
      {leadCount > 0 && <span className="flex items-center gap-1"><span style={{ color: KIND_COLOR.lead }} className="text-[9px]">●</span><span>{leadCount} leads</span></span>}
      {directorCount > 0 && <span className="flex items-center gap-1"><span style={{ color: KIND_COLOR.director }} className="text-[9px]">●</span><span>{directorCount} directors</span></span>}
      {supportCount > 0 && <span className="flex items-center gap-1"><span className="text-white/40 text-[9px]">●</span><span>{supportCount} supporting</span></span>}
    </p>
  )

  return (
    <div className="rounded-3xl border border-white/[0.08] overflow-visible" style={{ background: '#0d0d15' }}>

      {/* ── Header ── */}
      <div className="px-4 sm:px-6 pt-5 sm:pt-6 pb-3">
        {/* Row 1: name + stats (share button inline on mobile) */}
        <div className="flex items-start justify-between gap-2">
          <div className="min-w-0">
            <h2 className="text-base font-bold text-white flex items-center gap-2">
              <span style={{ color: CENTER_COLOR, opacity: 0.8 }}>✦</span>
              {hasChosen && center ? center.name : 'Discover Connections'}
            </h2>
            {hasGraph ? legendRow : (
              <p className="text-white/30 text-xs mt-0.5">Tap stars to explore connections</p>
            )}
          </div>
          {/* Desktop: share + picker inline with title */}
          <div className="hidden lg:flex items-center gap-2 flex-shrink-0 pt-1">
            {hasChosen && center && (
              <button onClick={handleShare}
                className="text-xs font-semibold px-3 py-1.5 rounded-full bg-white/[0.07] border border-white/[0.12] text-white/50 hover:text-white/80 hover:border-white/25 transition-all"
                aria-label="Share network">
                🔗
              </button>
            )}
            {hasChosen && (
              <ActorPicker
                onSelect={handleActorSelect}
                loading={fetchingNetwork}
                defaultSuggestions={suggestions}
                variant="prominent"
              />
            )}
          </div>
          {/* Mobile: share button only in this row */}
          {hasChosen && center && (
            <button onClick={handleShare}
              className="lg:hidden flex-shrink-0 text-xs font-semibold px-3 py-1.5 rounded-full bg-white/[0.07] border border-white/[0.12] text-white/50 transition-all mt-0.5"
              aria-label="Share network">
              🔗
            </button>
          )}
        </div>
        {/* Row 2 (mobile only): full-width actor picker */}
        {hasChosen && (
          <div className="flex lg:hidden mt-2.5">
            <div className="flex-1">
              <ActorPicker
                onSelect={handleActorSelect}
                loading={fetchingNetwork}
                defaultSuggestions={suggestions}
                variant="prominent"
              />
            </div>
          </div>
        )}
      </div>

      {/* ── Mobile tab toggle: Constellation / All Collaborators ── */}
      {hasGraph && (
        <div className="flex lg:hidden items-center gap-2 px-6 pb-3">
          <button
            onClick={() => setMobileTab('graph')}
            className="text-xs px-4 py-1.5 rounded-full transition-all font-medium"
            style={{
              background: mobileTab === 'graph' ? 'rgba(255,255,255,0.13)' : 'rgba(255,255,255,0.04)',
              border: `1px solid ${mobileTab === 'graph' ? 'rgba(255,255,255,0.28)' : 'rgba(255,255,255,0.10)'}`,
              color: mobileTab === 'graph' ? '#fff' : 'rgba(255,255,255,0.40)',
            }}
          >
            ✦ Constellation
          </button>
          <button
            onClick={() => setMobileTab('list')}
            className="text-xs px-4 py-1.5 rounded-full transition-all font-medium"
            style={{
              background: mobileTab === 'list' ? 'rgba(255,255,255,0.13)' : 'rgba(255,255,255,0.04)',
              border: `1px solid ${mobileTab === 'list' ? 'rgba(255,255,255,0.28)' : 'rgba(255,255,255,0.10)'}`,
              color: mobileTab === 'list' ? '#fff' : 'rgba(255,255,255,0.40)',
            }}
          >
            ☰ All Collaborators
          </button>
        </div>
      )}

      {/* ── Empty state ── */}
      {!hasChosen && (
        <div className="relative rounded-b-3xl overflow-hidden" style={{ minHeight: 300 }}>
          <svg viewBox={`0 0 ${SVG_W} ${SVG_H}`} className="absolute inset-0 w-full h-full"
            style={{ display: 'block', opacity: 0.5 }} aria-hidden>
            {bgStars.map((s, i) => <circle key={i} cx={s.x} cy={s.y} r={s.r} fill="white" opacity={s.op}/>)}
          </svg>
          <div className="relative z-10 flex flex-col items-center justify-center px-8 py-12 gap-5">
            <p className="text-white/40 text-[11px] uppercase tracking-[0.2em]">Choose an actor to explore</p>
            <ActorPicker onSelect={handleActorSelect} loading={fetchingNetwork} defaultSuggestions={suggestions} variant="prominent"/>
            {suggestions.length > 0 && (
              <div className="flex flex-wrap gap-2 justify-center">
                {suggestions.slice(0, 4).map(a => (
                  <button key={a.id} onClick={() => handleActorSelect(a)}
                    className="text-xs px-3 py-1.5 rounded-full border border-white/[0.12] text-white/50 hover:text-white hover:border-white/25 bg-white/[0.04] hover:bg-white/[0.08] transition-all">
                    {a.name}
                  </button>
                ))}
              </div>
            )}
          </div>
        </div>
      )}

      {/* ── Constellation + list area ── */}
      {hasChosen && (
        <div
          className="pb-5 relative"
          onMouseEnter={() => setGraphContainerHovered(true)}
          onMouseLeave={() => setGraphContainerHovered(false)}
        >
          {fetchingNetwork && (
            <div className="absolute inset-0 flex items-center justify-center z-10 rounded-2xl"
              style={{ background: 'rgba(13,13,21,0.75)' }}>
              <div className="flex gap-1.5">
                {[0,1,2,3,4].map(i => (
                  <div key={i} className="w-2 h-2 rounded-full bg-white/40 animate-bounce"
                    style={{ animationDelay: `${i * 0.1}s` }}/>
                ))}
              </div>
            </div>
          )}

          {!center || nodes.length === 0 ? (
            <div className="w-full flex items-center justify-center" style={{ minHeight: 200 }}>
              <p className="text-white/20 text-sm">No collaboration data available yet</p>
            </div>
          ) : (
            <>
              {/* ─ Desktop constellation — full 1100×400, hidden on mobile ─ */}
              <div className="hidden lg:block">
                <ConstellationSVG
                  W={SVG_W} H={SVG_H} cx={CX} cy={CY}
                  center={center} nodes={nodes} positions={positions}
                  bgStars={bgStars}
                  hovered={hovered} setHovered={setHovered}
                  centerImgError={centerImgError} setCenterImgError={setCenterImgError}
                  onNodeClick={handleNodeClick} onCenterClick={handleCenterClick}
                  idPrefix="inline-"
                />
              </div>

              {/* ─ Mobile constellation — 500×500, top 20 nodes, larger dots ─ */}
              {/* Shown on mobile only, and only when the Constellation tab is active */}
              <div className={mobileTab === 'graph' ? 'block lg:hidden' : 'hidden'}
                style={{ aspectRatio: '1 / 1' }}>
                <ConstellationSVG
                  W={MOB_W} H={MOB_H} cx={MOB_CX} cy={MOB_CY}
                  center={center} nodes={mobileNodes} positions={mobilePositions}
                  bgStars={bgStars}
                  hovered={hovered} setHovered={setHovered}
                  centerImgError={centerImgError} setCenterImgError={setCenterImgError}
                  onNodeClick={handleNodeClick} onCenterClick={handleCenterClick}
                  idPrefix="mob-"
                  fs={{ name: 11, detail: 8.5, centerName: 14, centerSub: 10 }}
                  centerR={28}
                  nodeScale={2}
                />
              </div>

              {/* ─ Mobile list view — shown only when the All Collaborators tab is active ─ */}
              {mobileTab === 'list' && (
                <div className="block lg:hidden px-4 pb-2" style={{ maxHeight: 380, overflowY: 'auto' }}>
                  {localAllNodes.map((node, i) => (
                    <button
                      key={i}
                      onClick={() => node.id ? router.push(`/actors/${toActorSlug(node.name)}`) : undefined}
                      disabled={!node.id}
                      className="w-full flex items-center justify-between py-2.5 border-b border-white/[0.06] last:border-0 text-left disabled:opacity-60"
                    >
                      <div className="flex items-center gap-2.5 min-w-0">
                        <span style={{ color: KIND_COLOR[node.kind], fontSize: 7, flexShrink: 0 }}>●</span>
                        <span className="text-white/85 text-sm font-medium truncate">{node.name}</span>
                        {node.kind === 'director' && (
                          <span className="text-[9px] px-1.5 py-0.5 rounded-full flex-shrink-0"
                            style={{ background: 'rgba(34,211,238,0.12)', color: '#22d3ee' }}>
                            Dir
                          </span>
                        )}
                        {node.kind === 'lead' && (
                          <span className="text-[9px] px-1.5 py-0.5 rounded-full flex-shrink-0"
                            style={{ background: 'rgba(244,114,182,0.12)', color: '#f472b6' }}>
                            Lead
                          </span>
                        )}
                      </div>
                      <span className="text-white/35 text-xs flex-shrink-0 ml-3">
                        {node.films} {node.films === 1 ? 'film' : 'films'}
                      </span>
                    </button>
                  ))}
                </div>
              )}
            </>
          )}

          {/* ─ Desktop expand button — hover-gated, hidden on mobile ─ */}
          {hasGraph && graphContainerHovered && !isExpanded && (
            <button
              onClick={() => setIsExpanded(true)}
              className="hidden lg:flex absolute bottom-4 right-4 z-20 items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-semibold transition-all"
              style={{
                background: 'rgba(255,255,255,0.09)',
                border: '1px solid rgba(255,255,255,0.20)',
                color: 'rgba(255,255,255,0.70)',
                backdropFilter: 'blur(8px)',
              }}
              onMouseEnter={e => { const b = e.currentTarget as HTMLButtonElement; b.style.background = 'rgba(255,255,255,0.16)'; b.style.color = '#fff' }}
              onMouseLeave={e => { const b = e.currentTarget as HTMLButtonElement; b.style.background = 'rgba(255,255,255,0.09)'; b.style.color = 'rgba(255,255,255,0.70)' }}
            >
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
                <path d="M8 3H5a2 2 0 0 0-2 2v3m18 0V5a2 2 0 0 0-2-2h-3m0 18h3a2 2 0 0 0 2-2v-3M3 16v3a2 2 0 0 0 2 2h3"/>
              </svg>
              See full network · {localAllNodes.length}
            </button>
          )}

          {/* ─ Mobile expand button — always visible, descriptive label ─ */}
          {hasGraph && !isExpanded && (
            <div className="flex lg:hidden justify-center pt-1 pb-2 px-4">
              <button
                onClick={() => setIsExpanded(true)}
                className="flex items-center justify-center gap-2 w-full px-5 py-2.5 rounded-full text-sm font-semibold transition-all"
                style={{
                  background: 'rgba(255,255,255,0.07)',
                  border: '1px solid rgba(255,255,255,0.18)',
                  color: 'rgba(255,255,255,0.65)',
                }}
              >
                <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
                  <path d="M8 3H5a2 2 0 0 0-2 2v3m18 0V5a2 2 0 0 0-2-2h-3m0 18h3a2 2 0 0 0 2-2v-3M3 16v3a2 2 0 0 0 2 2h3"/>
                </svg>
                See all {localAllNodes.length} collaborators
              </button>
            </div>
          )}
        </div>
      )}

      {/* ── Full-screen expanded overlay ── */}
      {isExpanded && hasGraph && center && (
        <div className="fixed inset-0 z-[1000] flex flex-col" style={{ background: '#07070f' }}>

          {/* Overlay header */}
          <div className="flex items-center justify-between px-4 sm:px-6 py-4 flex-shrink-0"
            style={{ borderBottom: '1px solid rgba(255,255,255,0.07)' }}>
            <div>
              <h2 className="text-base font-bold text-white flex items-center gap-2">
                <span style={{ color: CENTER_COLOR, opacity: 0.8 }}>✦</span>
                {center.name}&rsquo;s Connections
              </h2>
              <p className="text-white/30 text-xs mt-0.5 flex items-center gap-2">
                {localAllNodes.filter(n => n.kind === 'lead').length > 0 && (
                  <span className="flex items-center gap-1">
                    <span style={{ color: KIND_COLOR.lead }} className="text-[9px]">●</span>
                    <span>{localAllNodes.filter(n => n.kind === 'lead').length} leads</span>
                  </span>
                )}
                {localAllNodes.filter(n => n.kind === 'director').length > 0 && (
                  <span className="flex items-center gap-1">
                    <span style={{ color: KIND_COLOR.director }} className="text-[9px]">●</span>
                    <span>{localAllNodes.filter(n => n.kind === 'director').length} directors</span>
                  </span>
                )}
                {localAllNodes.filter(n => n.kind === 'supporting').length > 0 && (
                  <span className="flex items-center gap-1">
                    <span className="text-white/40 text-[9px]">●</span>
                    <span>{localAllNodes.filter(n => n.kind === 'supporting').length} supporting</span>
                  </span>
                )}
                <span className="text-white/20">· {localAllNodes.length} total</span>
              </p>
            </div>
            <div className="flex items-center gap-2 sm:gap-3">
              <div className="hidden lg:block">
                <ActorPicker
                  onSelect={(actor) => { handleActorSelect(actor) }}
                  loading={fetchingNetwork}
                  defaultSuggestions={suggestions}
                  variant="prominent"
                />
              </div>
              {/* Reset zoom — only shown when zoomed/panned */}
              {isZoomed && (
                <button
                  onClick={resetExpTransform}
                  className="text-xs px-3 py-1.5 rounded-full font-medium transition-all flex-shrink-0"
                  style={{ background: 'rgba(255,255,255,0.07)', border: '1px solid rgba(255,255,255,0.15)', color: 'rgba(255,255,255,0.55)' }}
                >
                  Reset zoom
                </button>
              )}
              <button
                onClick={() => { setIsExpanded(false); resetExpTransform() }}
                className="flex items-center gap-1.5 px-3 sm:px-4 py-2 rounded-full text-sm font-semibold transition-all flex-shrink-0"
                style={{ background: 'rgba(255,255,255,0.07)', border: '1px solid rgba(255,255,255,0.15)', color: 'rgba(255,255,255,0.60)' }}
                onMouseEnter={e => { const b = e.currentTarget as HTMLButtonElement; b.style.background = 'rgba(255,255,255,0.13)'; b.style.color = '#fff' }}
                onMouseLeave={e => { const b = e.currentTarget as HTMLButtonElement; b.style.background = 'rgba(255,255,255,0.07)'; b.style.color = 'rgba(255,255,255,0.60)' }}
              >
                <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
                  <path d="M8 3v3a2 2 0 0 1-2 2H3m18 0h-3a2 2 0 0 1-2-2V3m0 18v-3a2 2 0 0 1 2-2h3M3 16h3a2 2 0 0 1 2 2v3"/>
                </svg>
                Close
              </button>
            </div>
          </div>

          {/* Full-screen SVG — pinch to zoom, drag to pan on mobile */}
          <div
            className="flex-1 overflow-hidden"
            style={{ touchAction: 'none' }}
            onTouchStart={handleExpTouchStart}
            onTouchMove={handleExpTouchMove}
            onTouchEnd={handleExpTouchEnd}
            // Block click/tap events that fire immediately after a pan or pinch gesture.
            // gestureMovedRef is cleared 300 ms after touchend so normal taps still work.
            onClickCapture={e => {
              if (gestureMovedRef.current) { e.stopPropagation(); gestureMovedRef.current = false }
            }}
          >
            {/* ref lets us mutate transform directly without re-rendering */}
            <div
              ref={expContainerRef}
              style={{
                width: '100%',
                height: '100%',
                transformOrigin: 'center center',
                willChange: 'transform',
              }}
            >
              <ConstellationSVG
                W={EXP_W} H={EXP_H} cx={EXP_CX} cy={EXP_CY}
                center={center} nodes={localAllNodes} positions={expandedPositions}
                bgStars={bgStars}
                hovered={expandHovered} setHovered={setExpandHovered}
                centerImgError={centerImgError} setCenterImgError={setCenterImgError}
                onNodeClick={handleNodeClick} onCenterClick={handleCenterClick}
                idPrefix="exp-"
                fs={{ name: 11, detail: 9, centerName: 13, centerSub: 9.5 }}
                centerR={42}
                svgTouchAction="none"
              />
            </div>
          </div>

          {/* Footer hint — mobile shows pinch/pan hint, desktop shows ESC hint */}
          <p className="lg:hidden text-center text-white/15 text-[10px] pb-3 tracking-widest flex-shrink-0">
            Pinch to zoom · drag to pan
          </p>
          <p className="hidden lg:block text-center text-white/15 text-[10px] pb-3 tracking-widest flex-shrink-0">
            Press ESC to close
          </p>
        </div>
      )}

      {/* Toast */}
      {toast && (
        <div className="fixed bottom-6 left-1/2 -translate-x-1/2 z-50 px-4 py-2 rounded-full text-xs font-semibold bg-white text-[#0a0a0f] shadow-lg shadow-black/40"
          style={{ animation: 'gp-fadeup 0.2s ease' }}>
          {toast}
        </div>
      )}

      <style>{`
        @keyframes gp-fadeup {
          from { opacity: 0; transform: translate(-50%, 8px); }
          to   { opacity: 1; transform: translate(-50%, 0); }
        }
        @keyframes gp-float {
          0%, 100% { transform: translateY(0px); }
          50%       { transform: translateY(var(--gp-amp, -3px)); }
        }
      `}</style>
    </div>
  )
}
