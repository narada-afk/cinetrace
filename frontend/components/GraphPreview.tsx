'use client'

import { useState, useEffect, useRef, useMemo } from 'react'
import { useRouter } from 'next/navigation'
import { searchActors, getActorCollaborators, getActors, type Actor } from '@/lib/api'

// ── Exported types (consumed by page.tsx) ────────────────────────────────────

export interface NetworkNode {
  id: number | null   // null → no click navigation
  name: string
  films: number       // shared films with center actor
}

export interface NetworkCenter {
  id: number
  name: string
  gender?: 'M' | 'F' | null
}

// ── Layout constants ──────────────────────────────────────────────────────────

const SVG_W       = 1100
const SVG_H       = 380
const CX          = SVG_W / 2      // 550 — center of wide canvas
const CY          = SVG_H / 2 - 5  // 185
const RING_R      = 155            // base radius of surrounding ring
const CENTER_R    = 40             // center node radius

// Distinct but muted node colours — visible over galaxy, not garish
const COLORS = [
  '#a78bfa', '#60a5fa', '#34d399', '#fbbf24',
  '#f472b6', '#22d3ee', '#fb923c', '#a3e635',
]
const CENTER_COLOR = '#f5c518'   // golden — sun / bright star

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Deterministic seeded random (0–1) — consistent across renders */
function sr(seed: number): number {
  const x = Math.sin(seed * 9301 + 49297) * 233280
  return x - Math.floor(x)
}

/** Node position with slight angular + radial jitter for organic feel */
function polarPos(i: number, total: number) {
  const base   = (2 * Math.PI * i / total) - Math.PI / 2
  const jitter = (sr(i * 7 + 13) - 0.5) * 0.26          // ±~7° jitter
  const angle  = base + jitter
  const r      = RING_R + (sr(i * 3 + 1) - 0.5) * 24    // ±12px radial variance
  return { x: CX + r * Math.cos(angle), y: CY + r * Math.sin(angle) }
}

/** Depth factor 0.62–1.0 — simulates distance from viewer */
function depth(i: number): number {
  return 0.62 + sr(i * 5 + 29) * 0.38
}

/** Node radius scaled by film count (14–24px) then nudged by depth */
function nodeR(films: number, d = 1): number {
  const base = 14 + Math.min(films * 0.85, 10)
  return Math.round(base * (0.78 + d * 0.22))   // depth ±22% size variance
}

/** Up to 2 capital initials */
function initials(name: string) {
  return name.split(' ').map(w => w[0] ?? '').join('').slice(0, 2).toUpperCase()
}

/** Gender-aware pronoun */
function pronoun(gender?: 'M' | 'F' | null) {
  return gender === 'F' ? 'she' : 'he'
}

/** URL-safe actor slug */
function toSlug(name: string) {
  return name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '')
}

// ── Toast helper ──────────────────────────────────────────────────────────────

function useToast() {
  const [toast, setToast] = useState<string | null>(null)
  function showToast(msg: string) {
    setToast(msg)
    setTimeout(() => setToast(null), 2200)
  }
  return { toast, showToast }
}

// ── Actor picker dropdown ─────────────────────────────────────────────────────

function ActorPicker({
  onSelect,
  loading,
  defaultSuggestions = [],
}: {
  onSelect: (actor: Actor) => void
  loading: boolean
  defaultSuggestions?: Actor[]
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
              <div
                className="w-6 h-6 rounded-full flex items-center justify-center text-[9px] font-bold flex-shrink-0"
                style={{ background: '#ef444433', color: '#ef4444' }}
              >
                {initials(a.name)}
              </div>
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

// ── Main component ────────────────────────────────────────────────────────────

export default function GraphPreview({
  networkData,
  suggestions = [],
}: {
  networkData: { center: NetworkCenter; nodes: NetworkNode[] } | null
  suggestions?: Actor[]
}) {
  const router = useRouter()
  const [hovered, setHovered]             = useState<'center' | number | null>(null)
  const [centerImgError, setCenterImgError] = useState(false)
  const [localCenter, setLocalCenter]     = useState<NetworkCenter | null>(networkData?.center ?? null)
  const [localNodes,  setLocalNodes]      = useState<NetworkNode[]>(networkData?.nodes ?? [])
  const [fetchingNetwork, setFetchingNetwork] = useState(false)
  const { toast, showToast } = useToast()

  // Reset avatar error when center actor changes
  useEffect(() => { setCenterImgError(false) }, [localCenter?.id])

  // Precompute dense star field — scattered + band-concentrated (seeded)
  const bgStars = useMemo(() => {
    const stars: { x: number; y: number; r: number; op: number }[] = []

    // 200 scattered stars across the whole SVG
    for (let i = 0; i < 200; i++) {
      stars.push({
        x:  sr(i * 11 + 3)  * SVG_W,
        y:  sr(i * 7  + 5)  * SVG_H,
        r:  sr(i * 13 + 1)  * 1.1 + 0.15,
        op: sr(i * 17 + 2)  * 0.40 + 0.05,
      })
    }

    // 130 band-concentrated stars along the diagonal (milky way core density)
    for (let i = 0; i < 130; i++) {
      const t      = sr(i * 23 + 7)                        // 0→1 along band axis
      const spread = (sr(i * 31 + 11) - 0.5) * SVG_H * 0.28 // perpendicular scatter
      const bx     = t * (SVG_W * 1.1) - SVG_W * 0.05
      const by     = (1 - t) * (SVG_H * 0.90) + spread + SVG_H * 0.05
      stars.push({
        x:  Math.max(0, Math.min(SVG_W, bx)),
        y:  Math.max(0, Math.min(SVG_H, by)),
        r:  sr(i * 19 + 3)  * 1.3 + 0.20,
        op: sr(i * 29 + 7)  * 0.55 + 0.18,  // brighter in the band
      })
    }

    return stars
  }, [])

  async function handleActorSelect(actor: Actor) {
    setFetchingNetwork(true)
    try {
      const [collaborators, actors] = await Promise.all([
        getActorCollaborators(actor.id),
        getActors(true),
      ])
      const nameToId    = new Map(actors.map(a => [a.name.toLowerCase().trim(), a.id]))
      const centerActor = actors.find(a => a.id === actor.id)
      setLocalCenter({ id: actor.id, name: actor.name, gender: centerActor?.gender ?? actor.gender ?? null })
      setLocalNodes(
        collaborators.slice(0, 8).map(c => ({
          id:    nameToId.get(c.actor.toLowerCase().trim()) ?? null,
          name:  c.actor,
          films: c.films,
        }))
      )
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
    const shareData = {
      title: `${localCenter.name}'s Cinema Network`,
      text:  `Explore ${localCenter.name}'s collaboration network on South Cinema Analytics`,
      url,
    }
    try {
      if (navigator.share && navigator.canShare?.(shareData)) await navigator.share(shareData)
      else { await navigator.clipboard.writeText(url); showToast('Link copied!') }
    } catch {
      try { await navigator.clipboard.writeText(url); showToast('Link copied!') } catch { /* noop */ }
    }
  }

  const center   = localCenter
  const nodes    = localNodes.slice(0, 8)
  const title    = center ? `${center.name}'s Network` : 'Cinema Network'
  const subtitle = center
    ? `Actors ${pronoun(center.gender)} has collaborated with across industries`
    : 'Explore collaboration networks'

  // Precompute all node positions once per render (stable with same nodes)
  const positions = useMemo(
    () => nodes.map((_, i) => polarPos(i, nodes.length)),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [nodes.map(n => n.name).join(',')]
  )

  return (
    <div
      className="rounded-3xl border border-white/[0.08] overflow-visible"
      style={{ background: '#0d0d15' }}
    >
      {/* ── Header ── */}
      <div className="px-6 pt-6 pb-3 flex items-start justify-between gap-4">
        <div className="min-w-0">
          <p className="text-[10px] font-semibold uppercase tracking-[0.15em] text-white/25 mb-1">
            Showing network for
          </p>
          <h2 className="text-base font-bold text-white flex items-center gap-2">
            <span style={{ color: CENTER_COLOR, opacity: 0.8 }}>✦</span> {title}
          </h2>
          <p className="text-white/30 text-xs mt-0.5">{subtitle}</p>
        </div>
        <div className="flex items-center gap-2 flex-shrink-0 pt-1">
          {center && (
            <button
              onClick={handleShare}
              className="text-xs font-semibold px-3 py-1.5 rounded-full bg-white/[0.07] border border-white/[0.12] text-white/50 hover:text-white/80 hover:border-white/25 transition-all"
              aria-label="Share network"
            >
              🔗
            </button>
          )}
          <ActorPicker
            onSelect={handleActorSelect}
            loading={fetchingNetwork}
            defaultSuggestions={suggestions}
          />
        </div>
      </div>

      {/* ── Galaxy graph ── */}
      <div className="pb-5 relative">

        {/* Loading overlay */}
        {fetchingNetwork && (
          <div
            className="absolute inset-0 flex items-center justify-center z-10 rounded-2xl"
            style={{ background: 'rgba(13,13,21,0.75)' }}
          >
            <div className="flex gap-1.5">
              {[0,1,2,3,4].map(i => (
                <div
                  key={i}
                  className="w-2 h-2 rounded-full bg-white/40 animate-bounce"
                  style={{ animationDelay: `${i * 0.1}s` }}
                />
              ))}
            </div>
          </div>
        )}

        {!center || nodes.length === 0 ? (
          <div
            className="w-full flex items-center justify-center"
            style={{ minHeight: 200 }}
          >
            <p className="text-white/20 text-sm">No collaboration data available yet</p>
          </div>
        ) : (

          <svg
            viewBox={`0 0 ${SVG_W} ${SVG_H}`}
            className="w-full"
            style={{ display: 'block' }}
          >
            <defs>
              {/* ── Galaxy atmosphere inside the card ─────────────────── */}

              {/* Heavy blur for galaxy band layers */}
              {/* Heavy blur — makes galaxy feel distant, not painted-on */}
              <filter id="gp-galblur" x="-50%" y="-50%" width="200%" height="200%">
                <feGaussianBlur stdDeviation="38"/>
              </filter>

              {/* Diagonal blue-white core band — asymmetric, off-centre sweep */}
              <linearGradient id="gp-gband" x1="0.08" y1="0" x2="0.78" y2="1">
                <stop offset="0%"   stopColor="#6496ff" stopOpacity="0"/>
                <stop offset="25%"  stopColor="#90b4ff" stopOpacity="0.05"/>
                <stop offset="42%"  stopColor="#b8ccff" stopOpacity="0.12"/>
                <stop offset="56%"  stopColor="#c8d8ff" stopOpacity="0.09"/>
                <stop offset="75%"  stopColor="#90b4ff" stopOpacity="0.04"/>
                <stop offset="100%" stopColor="#6496ff" stopOpacity="0"/>
              </linearGradient>

              {/* Warm orange/gold cloud — lower-left, atmospheric only */}
              <radialGradient id="gp-gwarm" cx="50%" cy="50%" r="50%">
                <stop offset="0%"   stopColor="#ff9040" stopOpacity="0.10"/>
                <stop offset="55%"  stopColor="#ff6820" stopOpacity="0.04"/>
                <stop offset="100%" stopColor="#ff4400" stopOpacity="0"/>
              </radialGradient>

              {/* Cool purple haze — upper-right, atmospheric only */}
              <radialGradient id="gp-gcool" cx="50%" cy="50%" r="50%">
                <stop offset="0%"   stopColor="#7050f8" stopOpacity="0.09"/>
                <stop offset="55%"  stopColor="#5538d8" stopOpacity="0.03"/>
                <stop offset="100%" stopColor="#3820b0" stopOpacity="0"/>
              </radialGradient>

              {/* Circular clip for center avatar */}
              <clipPath id="gp-centerclip">
                <circle cx={CX} cy={CY} r={CENTER_R - 2}/>
              </clipPath>

              {/* ── Standard node/center glow filters ──────────────────── */}

              {/* Glow filter for center star */}
              <filter id="gp-cglow" x="-80%" y="-80%" width="260%" height="260%">
                <feGaussianBlur in="SourceGraphic" stdDeviation="10" result="b"/>
                <feComposite in="SourceGraphic" in2="b" operator="over"/>
              </filter>

              {/* Glow filter for surrounding nodes — tight halo, not fog */}
              <filter id="gp-nglow" x="-50%" y="-50%" width="200%" height="200%">
                <feGaussianBlur in="SourceGraphic" stdDeviation="3" result="b"/>
                <feComposite in="SourceGraphic" in2="b" operator="over"/>
              </filter>

              {/* Sharp inner halo for close-in glow ring */}
              <filter id="gp-nglow2" x="-40%" y="-40%" width="180%" height="180%">
                <feGaussianBlur in="SourceGraphic" stdDeviation="1.5" result="b"/>
                <feComposite in="SourceGraphic" in2="b" operator="over"/>
              </filter>

              {/* Vignette — strong edge darkening, centres the eye */}
              <radialGradient id="gp-vignette" cx="50%" cy="50%" r="50%">
                <stop offset="40%" stopColor="#0a0a14" stopOpacity="0"/>
                <stop offset="78%" stopColor="#0a0a14" stopOpacity="0.55"/>
                <stop offset="100%" stopColor="#07070f" stopOpacity="0.90"/>
              </radialGradient>

              {/* Nebula — soft radial atmosphere behind center */}
              <radialGradient id="gp-nebula" cx="50%" cy="48%" r="50%">
                <stop offset="0%"   stopColor="#ef4444" stopOpacity="0.12"/>
                <stop offset="40%"  stopColor="#7c3aed" stopOpacity="0.05"/>
                <stop offset="100%" stopColor="#000000" stopOpacity="0"/>
              </radialGradient>

              {/* Line glow filter — thin luminous halo on lit lines */}
              <filter id="gp-lineglow" x="-200%" y="-200%" width="500%" height="500%">
                <feGaussianBlur in="SourceGraphic" stdDeviation="2.5" result="b"/>
                <feComposite in="SourceGraphic" in2="b" operator="over"/>
              </filter>

              {/* Diffraction spike blur — soft ray dissolve */}
              <filter id="gp-spike" x="-300%" y="-300%" width="700%" height="700%">
                <feGaussianBlur in="SourceGraphic" stdDeviation="1.0"/>
              </filter>

              {/* Per-line gradient: golden centre → node colour */}
              {nodes.map((_, i) => {
                const p = positions[i]
                const c = COLORS[i % COLORS.length]
                return (
                  <linearGradient
                    key={i}
                    id={`gp-lg${i}`}
                    x1={CX} y1={CY}
                    x2={p?.x ?? 0} y2={p?.y ?? 0}
                    gradientUnits="userSpaceOnUse"
                  >
                    <stop offset="0%"   stopColor={CENTER_COLOR} stopOpacity="0.25"/>
                    <stop offset="100%" stopColor={c}            stopOpacity="0.75"/>
                  </linearGradient>
                )
              })}
            </defs>

            {/* ── Galaxy atmosphere — deepest layer, behind everything ── */}

            {/* Diagonal core band spans full wide canvas */}
            <rect
              x={-80} y={-80} width={SVG_W + 160} height={SVG_H + 160}
              fill="url(#gp-gband)"
              filter="url(#gp-galblur)"
              style={{ mixBlendMode: 'screen' }}
            />
            {/* Warm orange cloud — shifted lower-left, away from center */}
            <ellipse
              cx={SVG_W * 0.12} cy={SVG_H * 0.88}
              rx={300} ry={150}
              fill="url(#gp-gwarm)"
              filter="url(#gp-galblur)"
              style={{ mixBlendMode: 'screen' }}
            />
            {/* Cool purple haze — shifted upper-right, away from center */}
            <ellipse
              cx={SVG_W * 0.88} cy={SVG_H * 0.12}
              rx={280} ry={140}
              fill="url(#gp-gcool)"
              filter="url(#gp-galblur)"
              style={{ mixBlendMode: 'screen' }}
            />

            {/* ── Background micro-stars ── */}
            {bgStars.map((s, i) => (
              <circle key={i} cx={s.x} cy={s.y} r={s.r} fill="white" opacity={s.op}/>
            ))}

            {/* ── Nebula glow behind center ── */}
            <ellipse cx={CX} cy={CY} rx={155} ry={125} fill="url(#gp-nebula)"/>

            {/* ── Constellation lines ── */}
            {nodes.map((node, i) => {
              const p    = positions[i]
              const d    = depth(i)
              const lit  = hovered === i || hovered === 'center'
              const fade = hovered !== null && !lit
              if (!p) return null
              return (
                <line
                  key={i}
                  x1={CX} y1={CY}
                  x2={p.x} y2={p.y}
                  stroke={`url(#gp-lg${i})`}
                  strokeWidth={lit ? 1.8 : 0.8 + d * 0.55}
                  opacity={fade ? 0.07 : lit ? 1 : 0.44 + d * 0.24}
                  filter="url(#gp-lineglow)"
                  style={{ transition: 'opacity 0.22s ease, stroke-width 0.22s ease' }}
                />
              )
            })}

            {/* ── Collaborator stars ── */}
            {nodes.map((node, i) => {
              const p       = positions[i]
              const color   = COLORS[i % COLORS.length]
              const d       = depth(i)
              // Star size: glow cloud r, bright-core r
              const glowR   = nodeR(node.films, d)           // 10–24px glow radius
              const coreR   = Math.max(2.2, glowR * 0.32)   // tight bright centre
              const isHov   = hovered === i
              const fade    = hovered !== null && hovered !== i && hovered !== 'center'
              const baseOp  = 0.48 + d * 0.52
              const floatDur = `${3.0 + i * 0.32}s`
              const floatAmp = d > 0.78 ? '-4px' : '-2px'

              if (!p) return null

              return (
                <g
                  key={i}
                  style={{
                    cursor: node.id ? 'pointer' : 'default',
                    opacity: fade ? 0.12 : baseOp,
                    ['--gp-amp' as string]: floatAmp,
                    animation: `gp-float ${floatDur} ease-in-out ${i * 0.42}s infinite`,
                    transition: 'opacity 0.22s ease',
                  }}
                  onMouseEnter={() => setHovered(i)}
                  onMouseLeave={() => setHovered(null)}
                  onClick={() => node.id && router.push(`/actors/${toSlug(node.name)}`)}
                >
                  {/* Wide colour glow cloud */}
                  <circle
                    cx={p.x} cy={p.y}
                    r={glowR * 2.0}
                    fill={color}
                    opacity={isHov ? 0.22 * d : 0.10 * d}
                    filter="url(#gp-nglow)"
                    style={{ transition: 'opacity 0.22s ease' }}
                  />
                  {/* Tight colour halo */}
                  <circle
                    cx={p.x} cy={p.y}
                    r={glowR}
                    fill={color}
                    opacity={isHov ? 0.45 * d : 0.20 * d}
                    filter="url(#gp-nglow2)"
                    style={{ transition: 'opacity 0.22s ease' }}
                  />
                  {/* Diffraction spikes — 4 main + 4 diagonal rays */}
                  {(() => {
                    const spikeMain = coreR * (isHov ? 7.5 : 5.0) * (0.7 + d * 0.3)
                    const spikeDiag = spikeMain * 0.55
                    const rot       = sr(i * 11 + 7) * 28
                    const spikeOp   = isHov ? 0.55 : 0.22 * d
                    const diagOp    = isHov ? 0.35 : 0.14 * d
                    const toRad     = (deg: number) => (deg + rot) * Math.PI / 180
                    const mainAngles = [0, 90, 180, 270]
                    const diagAngles = [45, 135, 225, 315]
                    return (
                      <g filter="url(#gp-spike)">
                        {mainAngles.map(a => (
                          <line key={a}
                            x1={p.x} y1={p.y}
                            x2={p.x + spikeMain * Math.cos(toRad(a))}
                            y2={p.y + spikeMain * Math.sin(toRad(a))}
                            stroke="white" strokeWidth="0.9" opacity={spikeOp}
                          />
                        ))}
                        {diagAngles.map(a => (
                          <line key={a}
                            x1={p.x} y1={p.y}
                            x2={p.x + spikeDiag * Math.cos(toRad(a))}
                            y2={p.y + spikeDiag * Math.sin(toRad(a))}
                            stroke="white" strokeWidth="0.6" opacity={diagOp}
                          />
                        ))}
                      </g>
                    )
                  })()}

                  {/* Bright white core dot */}
                  <circle
                    cx={p.x} cy={p.y}
                    r={isHov ? coreR * 1.4 : coreR * 1.1}
                    fill="white"
                    opacity={isHov ? 1 : 0.72 + d * 0.26}
                    style={{ transition: 'r 0.22s ease, opacity 0.22s ease' }}
                  />
                  {/* Name — subtle star label, brightens on hover */}
                  <text
                    x={p.x} y={p.y + glowR + 12}
                    textAnchor="middle" fontSize="8"
                    fill={isHov ? 'rgba(255,255,255,0.65)' : `rgba(255,255,255,${(0.14 + d * 0.14).toFixed(2)})`}
                    style={{ userSelect: 'none', transition: 'fill 0.22s ease' }}
                  >
                    {node.name.split(' ')[0]}
                  </text>
                  {/* Film count on hover */}
                  {isHov && (
                    <text
                      x={p.x} y={p.y + glowR + 23}
                      textAnchor="middle" fontSize="7"
                      fill="rgba(255,255,255,0.45)"
                      style={{ userSelect: 'none' }}
                    >
                      {node.films} {node.films === 1 ? 'film' : 'films'}
                    </text>
                  )}
                </g>
              )
            })}

            {/* ── Center star — main actor as a golden sun ── */}
            {(() => {
              const avatarSlug = center.name.toLowerCase().replace(/\s+/g, '')
              const isHovCenter = hovered === 'center'
              return (
                <g
                  style={{ cursor: 'pointer' }}
                  onMouseEnter={() => setHovered('center')}
                  onMouseLeave={() => setHovered(null)}
                  onClick={() => router.push(`/actors/${toSlug(center.name)}`)}
                >
                  {/* Outermost pulsing corona — golden */}
                  <circle cx={CX} cy={CY} r={CENTER_R} fill={CENTER_COLOR} opacity="0">
                    <animate attributeName="r"
                      values={`${CENTER_R + 2};${CENTER_R + 44};${CENTER_R + 2}`}
                      dur="3.6s" repeatCount="indefinite"/>
                    <animate attributeName="opacity"
                      values="0.22;0;0.22" dur="3.6s" repeatCount="indefinite"/>
                  </circle>

                  {/* Second pulse ring — offset */}
                  <circle cx={CX} cy={CY} r={CENTER_R} fill={CENTER_COLOR} opacity="0">
                    <animate attributeName="r"
                      values={`${CENTER_R};${CENTER_R + 26};${CENTER_R}`}
                      dur="3.6s" begin="0.8s" repeatCount="indefinite"/>
                    <animate attributeName="opacity"
                      values="0.28;0;0.28" dur="3.6s" begin="0.8s" repeatCount="indefinite"/>
                  </circle>

                  {/* Wide golden glow cloud */}
                  <circle cx={CX} cy={CY} r={CENTER_R + 18}
                    fill={CENTER_COLOR}
                    opacity={isHovCenter ? 0.38 : 0.24}
                    filter="url(#gp-cglow)"
                    style={{ transition: 'opacity 0.22s ease' }}
                  />

                  {/* Golden border ring around avatar */}
                  <circle cx={CX} cy={CY} r={CENTER_R}
                    fill="#1a1025"
                    stroke={CENTER_COLOR}
                    strokeWidth={isHovCenter ? 2.5 : 1.8}
                    strokeOpacity={isHovCenter ? 1 : 0.85}
                    style={{ transition: 'all 0.22s ease' }}
                  />

                  {/* Avatar image — clipped to circle */}
                  {!centerImgError ? (
                    <image
                      href={`/avatars/${avatarSlug}.png`}
                      x={CX - CENTER_R + 2} y={CY - CENTER_R + 2}
                      width={(CENTER_R - 2) * 2} height={(CENTER_R - 2) * 2}
                      clipPath="url(#gp-centerclip)"
                      preserveAspectRatio="xMidYMid slice"
                      onError={() => setCenterImgError(true)}
                    />
                  ) : (
                    /* Fallback: golden initials when no avatar */
                    <text x={CX} y={CY}
                      textAnchor="middle" dominantBaseline="central"
                      fontSize={CENTER_R * 0.48} fontWeight="700"
                      fill={CENTER_COLOR}
                      style={{ userSelect: 'none' }}
                    >
                      {initials(center.name)}
                    </text>
                  )}

                  {/* Name below */}
                  <text x={CX} y={CY + CENTER_R + 17}
                    textAnchor="middle" fontSize="10" fontWeight="600"
                    fill={isHovCenter ? 'rgba(255,255,255,0.95)' : 'rgba(255,255,255,0.68)'}
                    style={{ userSelect: 'none', transition: 'fill 0.22s ease' }}
                  >
                    {center.name.split(' ')[0]}
                  </text>
                  {isHovCenter && (
                    <text x={CX} y={CY + CENTER_R + 29}
                      textAnchor="middle" fontSize="7.5"
                      fill="rgba(255,255,255,0.35)"
                      style={{ userSelect: 'none' }}
                    >
                      tap to explore
                    </text>
                  )}
                </g>
              )
            })()}

            {/* ── Vignette — darkens edges, draws eye inward ── */}
            <rect
              x="0" y="0" width={SVG_W} height={SVG_H}
              fill="url(#gp-vignette)"
              style={{ pointerEvents: 'none' }}
            />
          </svg>
        )}

        {center && nodes.length > 0 && (
          <p className="text-center text-[10px] text-white/15 mt-3 tracking-wide">
            Lines represent shared films · click any node to explore
          </p>
        )}
      </div>

      {/* Toast */}
      {toast && (
        <div
          className="fixed bottom-6 left-1/2 -translate-x-1/2 z-50 px-4 py-2 rounded-full text-xs font-semibold bg-white text-[#0a0a0f] shadow-lg shadow-black/40"
          style={{ animation: 'gp-fadeup 0.2s ease' }}
        >
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
