'use client'

import { useState, useEffect, useRef, useMemo } from 'react'
import { useRouter } from 'next/navigation'
import {
  searchActors,
  getActorCollaborators,
  getActorLeadCollaborators,
  getActorDirectors,
  getActors,
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

const CENTER_COLOR = '#f5c518'

// Kind colours
const KIND_COLOR: Record<NetworkNode['kind'], string> = {
  lead:       '#f472b6',   // rose — lead actress
  director:   '#22d3ee',   // cyan — director
  supporting: '#e2e8f0',   // soft white — supporting
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Deterministic seeded random (0–1) */
function sr(seed: number): number {
  const x = Math.sin(seed * 9301 + 49297) * 233280
  return x - Math.floor(x)
}

/**
 * Fully random scatter position — no circular pattern.
 * kind nudges the base radius range but jitter dominates.
 */
function scatterPos(i: number, kind: NetworkNode['kind']): { x: number; y: number } {
  // Fully random angle — no even division by total nodes
  const angle = sr(i * 37 + 11) * 2 * Math.PI

  // Base radius range per kind (leads slightly closer, supporting further out)
  const [rMin, rMax] = kind === 'lead'
    ? [58, 140]
    : kind === 'director'
    ? [75, 155]
    : [90, 180]

  const r = rMin + sr(i * 23 + 7) * (rMax - rMin)

  // Additional random jitter so nodes at similar r don't form a faint ring
  const jx = (sr(i * 53 + 17) - 0.5) * 28
  const jy = (sr(i * 61 + 19) - 0.5) * 22

  const x = Math.max(40, Math.min(SVG_W - 40, CX + r * Math.cos(angle) + jx))
  const y = Math.max(22, Math.min(SVG_H - 22, CY + r * Math.sin(angle) + jy))
  return { x, y }
}

/** Visual size of a node's bright core */
function coreR(films: number, isHov: boolean): number {
  if (isHov) return 3.2
  return 1.4 + Math.min(films * 0.04, 1.2)
}

/** Glow cloud radius (for hover bloom) */
function glowR(films: number): number {
  return 9 + Math.min(films * 0.3, 8)
}

function initials(name: string) {
  return name.split(' ').map(w => w[0] ?? '').join('').slice(0, 2).toUpperCase()
}

function pronoun(gender?: 'M' | 'F' | null) {
  return gender === 'F' ? 'she' : 'he'
}

function toSlug(name: string) {
  return name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '')
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

  if (variant === 'prominent') {
    return (
      <div className="relative w-full max-w-sm">
        <div className="flex items-center gap-2 rounded-2xl bg-white/[0.07] border border-white/[0.15] px-4 py-3 focus-within:border-white/30 focus-within:bg-white/[0.09] transition-all">
          <span className="text-white/40 text-sm">🔍</span>
          <input
            ref={inputRef}
            value={query}
            onChange={e => setQuery(e.target.value)}
            onFocus={() => setFocused(true)}
            placeholder="Search an actor (Rajinikanth, Vijay…)"
            disabled={loading}
            className="bg-transparent text-white text-sm placeholder-white/35 outline-none flex-1 disabled:opacity-50"
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
            {displayList.map(a => (
              <button
                key={a.id}
                onMouseDown={e => e.preventDefault()}
                onClick={() => handleSelect(a)}
                className="w-full flex items-center gap-2.5 px-4 py-2.5 hover:bg-white/[0.08] transition-colors text-left border-b border-white/[0.05] last:border-0"
              >
                <div
                  className="w-7 h-7 rounded-full flex items-center justify-center text-[10px] font-bold flex-shrink-0"
                  style={{ background: '#ef444433', color: '#ef4444' }}
                >
                  {initials(a.name)}
                </div>
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
  const [hovered, setHovered]               = useState<'center' | number | null>(null)
  const [centerImgError, setCenterImgError] = useState(false)
  const [localCenter, setLocalCenter]       = useState<NetworkCenter | null>(null)
  const [localNodes,  setLocalNodes]        = useState<NetworkNode[]>([])
  const [fetchingNetwork, setFetchingNetwork] = useState(false)
  const [hasChosen, setHasChosen]           = useState(false)
  const { toast, showToast } = useToast()

  useEffect(() => { setCenterImgError(false) }, [localCenter?.id])

  // Auto-load from server-provided networkData on first mount
  useEffect(() => {
    if (networkData?.center && !hasChosen) {
      const { id, name, gender } = networkData.center
      handleActorSelect({ id, name, gender: gender ?? null } as Actor)
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // Background star field — scattered + milky-way band (seeded, stable)
  const bgStars = useMemo(() => {
    const stars: { x: number; y: number; r: number; op: number }[] = []
    for (let i = 0; i < 220; i++) {
      stars.push({
        x:  sr(i * 11 + 3) * SVG_W,
        y:  sr(i * 7  + 5) * SVG_H,
        r:  sr(i * 13 + 1) * 0.85 + 0.1,   // smaller: max ~0.95px
        op: sr(i * 17 + 2) * 0.35 + 0.04,
      })
    }
    // Milky way band density
    for (let i = 0; i < 140; i++) {
      const t      = sr(i * 23 + 7)
      const spread = (sr(i * 31 + 11) - 0.5) * SVG_H * 0.28
      const bx     = t * (SVG_W * 1.1) - SVG_W * 0.05
      const by     = (1 - t) * (SVG_H * 0.90) + spread + SVG_H * 0.05
      stars.push({
        x:  Math.max(0, Math.min(SVG_W, bx)),
        y:  Math.max(0, Math.min(SVG_H, by)),
        r:  sr(i * 19 + 3) * 1.0 + 0.12,   // band stars slightly larger but still small
        op: sr(i * 29 + 7) * 0.50 + 0.15,
      })
    }
    return stars
  }, [])

  async function handleActorSelect(actor: Actor) {
    setHasChosen(true)
    setFetchingNetwork(true)
    try {
      // Fetch all three in parallel
      const [collaborators, leadCollabs, directors] = await Promise.all([
        getActorCollaborators(actor.id),
        getActorLeadCollaborators(actor.id),
        getActorDirectors(actor.id),
      ])

      const leadNames = new Set(leadCollabs.map(l => l.actor.toLowerCase().trim()))
      const dirNameSet = new Set(directors.slice(0, 7).map(d => d.director.toLowerCase().trim()))

      const nodes: NetworkNode[] = []

      // Directors first (up to 7)
      directors.slice(0, 7).forEach(d => {
        nodes.push({ id: null, name: d.director, films: d.films, kind: 'director' })
      })

      // Top collaborators, categorised, skip if already added as director
      let added = 0
      for (const c of collaborators) {
        if (added >= 30) break
        const nameLow = c.actor.toLowerCase().trim()
        if (dirNameSet.has(nameLow)) continue   // already in directors layer
        nodes.push({
          id:    c.actor_id || null,
          name:  c.actor,
          films: c.films,
          kind:  leadNames.has(nameLow) ? 'lead' : 'supporting',
        })
        added++
      }

      const centerActor = { id: actor.id, name: actor.name, gender: actor.gender ?? null }
      setLocalCenter(centerActor)
      setLocalNodes(nodes)
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

  const center    = localCenter
  const nodes     = localNodes   // no slice — show all

  // Stable positions (recompute only when node list changes)
  const positions = useMemo(
    () => nodes.map((n, i) => scatterPos(i, n.kind)),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [nodes.map(n => `${n.name}:${n.kind}`).join(',')]
  )

  // Legend counts
  const leadCount      = nodes.filter(n => n.kind === 'lead').length
  const directorCount  = nodes.filter(n => n.kind === 'director').length
  const supportCount   = nodes.filter(n => n.kind === 'supporting').length

  return (
    <div
      className="rounded-3xl border border-white/[0.08] overflow-visible"
      style={{ background: '#0d0d15' }}
    >
      {/* ── Header ── */}
      <div className="px-6 pt-6 pb-3 flex items-start justify-between gap-4">
        <div className="min-w-0">
          <h2 className="text-base font-bold text-white flex items-center gap-2">
            <span style={{ color: CENTER_COLOR, opacity: 0.8 }}>✦</span>
            {hasChosen && center ? center.name : 'Discover Connections'}
          </h2>
          {hasChosen && center && nodes.length > 0 ? (
            <p className="text-white/30 text-xs mt-0.5 flex items-center gap-2">
              {leadCount > 0 && (
                <span className="flex items-center gap-1">
                  <span style={{ color: KIND_COLOR.lead }} className="text-[9px]">●</span>
                  <span>{leadCount} leads</span>
                </span>
              )}
              {directorCount > 0 && (
                <span className="flex items-center gap-1">
                  <span style={{ color: KIND_COLOR.director }} className="text-[9px]">●</span>
                  <span>{directorCount} directors</span>
                </span>
              )}
              {supportCount > 0 && (
                <span className="flex items-center gap-1">
                  <span className="text-white/40 text-[9px]">●</span>
                  <span>{supportCount} supporting</span>
                </span>
              )}
            </p>
          ) : (
            <p className="text-white/30 text-xs mt-0.5">Tap stars to explore connections</p>
          )}
        </div>
        <div className="flex items-center gap-2 flex-shrink-0 pt-1">
          {hasChosen && center && (
            <button
              onClick={handleShare}
              className="text-xs font-semibold px-3 py-1.5 rounded-full bg-white/[0.07] border border-white/[0.12] text-white/50 hover:text-white/80 hover:border-white/25 transition-all"
              aria-label="Share network"
            >
              🔗
            </button>
          )}
          {hasChosen && (
            <ActorPicker
              onSelect={handleActorSelect}
              loading={fetchingNetwork}
              defaultSuggestions={suggestions}
            />
          )}
        </div>
      </div>

      {/* ── Empty state ── */}
      {!hasChosen && (
        <div className="relative rounded-b-3xl overflow-hidden" style={{ minHeight: 300 }}>
          <svg
            viewBox={`0 0 ${SVG_W} ${SVG_H}`}
            className="absolute inset-0 w-full h-full"
            style={{ display: 'block', opacity: 0.5 }}
            aria-hidden
          >
            {bgStars.map((s, i) => (
              <circle key={i} cx={s.x} cy={s.y} r={s.r} fill="white" opacity={s.op}/>
            ))}
          </svg>
          <div className="relative z-10 flex flex-col items-center justify-center px-8 py-12 gap-5">
            <p className="text-white/40 text-[11px] uppercase tracking-[0.2em]">
              Choose an actor to explore
            </p>
            <ActorPicker
              onSelect={handleActorSelect}
              loading={fetchingNetwork}
              defaultSuggestions={suggestions}
              variant="prominent"
            />
            {suggestions.length > 0 && (
              <div className="flex flex-wrap gap-2 justify-center">
                {suggestions.slice(0, 4).map(a => (
                  <button
                    key={a.id}
                    onClick={() => handleActorSelect(a)}
                    className="text-xs px-3 py-1.5 rounded-full border border-white/[0.12] text-white/50 hover:text-white hover:border-white/25 bg-white/[0.04] hover:bg-white/[0.08] transition-all"
                  >
                    {a.name}
                  </button>
                ))}
              </div>
            )}
            <p className="text-white/20 text-[10px] tracking-wide">
              Tap stars to explore connections
            </p>
          </div>
        </div>
      )}

      {/* ── Constellation ── */}
      {hasChosen && (
        <div className="pb-5 relative">
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
            <div className="w-full flex items-center justify-center" style={{ minHeight: 200 }}>
              <p className="text-white/20 text-sm">No collaboration data available yet</p>
            </div>
          ) : (
            <svg
              viewBox={`0 0 ${SVG_W} ${SVG_H}`}
              className="w-full"
              style={{ display: 'block', touchAction: 'pan-y' }}
              onTouchStart={() => setHovered(null)}
            >
              <defs>
                {/* Galaxy atmosphere */}
                <filter id="gp-galblur" x="-50%" y="-50%" width="200%" height="200%">
                  <feGaussianBlur stdDeviation="38"/>
                </filter>
                <linearGradient id="gp-gband" x1="0.08" y1="0" x2="0.78" y2="1">
                  <stop offset="0%"   stopColor="#6496ff" stopOpacity="0"/>
                  <stop offset="25%"  stopColor="#90b4ff" stopOpacity="0.05"/>
                  <stop offset="42%"  stopColor="#b8ccff" stopOpacity="0.12"/>
                  <stop offset="56%"  stopColor="#c8d8ff" stopOpacity="0.09"/>
                  <stop offset="75%"  stopColor="#90b4ff" stopOpacity="0.04"/>
                  <stop offset="100%" stopColor="#6496ff" stopOpacity="0"/>
                </linearGradient>
                <radialGradient id="gp-gwarm" cx="50%" cy="50%" r="50%">
                  <stop offset="0%"   stopColor="#ff9040" stopOpacity="0.10"/>
                  <stop offset="55%"  stopColor="#ff6820" stopOpacity="0.04"/>
                  <stop offset="100%" stopColor="#ff4400" stopOpacity="0"/>
                </radialGradient>
                <radialGradient id="gp-gcool" cx="50%" cy="50%" r="50%">
                  <stop offset="0%"   stopColor="#7050f8" stopOpacity="0.09"/>
                  <stop offset="55%"  stopColor="#5538d8" stopOpacity="0.03"/>
                  <stop offset="100%" stopColor="#3820b0" stopOpacity="0"/>
                </radialGradient>

                {/* Nebula */}
                <radialGradient id="gp-nebula" cx="50%" cy="48%" r="50%">
                  <stop offset="0%"   stopColor="#ef4444" stopOpacity="0.10"/>
                  <stop offset="40%"  stopColor="#7c3aed" stopOpacity="0.04"/>
                  <stop offset="100%" stopColor="#000000" stopOpacity="0"/>
                </radialGradient>

                {/* Vignette */}
                <radialGradient id="gp-vignette" cx="50%" cy="50%" r="50%">
                  <stop offset="40%" stopColor="#0a0a14" stopOpacity="0"/>
                  <stop offset="78%" stopColor="#0a0a14" stopOpacity="0.55"/>
                  <stop offset="100%" stopColor="#07070f" stopOpacity="0.90"/>
                </radialGradient>

                {/* Center avatar clip */}
                <clipPath id="gp-centerclip">
                  <circle cx={CX} cy={CY} r={CENTER_R - 2}/>
                </clipPath>

                {/* Sun glow */}
                <filter id="gp-cglow" x="-80%" y="-80%" width="260%" height="260%">
                  <feGaussianBlur in="SourceGraphic" stdDeviation="10" result="b"/>
                  <feComposite in="SourceGraphic" in2="b" operator="over"/>
                </filter>

                {/* Node glows */}
                <filter id="gp-nglow" x="-80%" y="-80%" width="260%" height="260%">
                  <feGaussianBlur in="SourceGraphic" stdDeviation="3.5" result="b"/>
                  <feComposite in="SourceGraphic" in2="b" operator="over"/>
                </filter>
                <filter id="gp-nglow2" x="-50%" y="-50%" width="200%" height="200%">
                  <feGaussianBlur in="SourceGraphic" stdDeviation="1.5" result="b"/>
                  <feComposite in="SourceGraphic" in2="b" operator="over"/>
                </filter>

                {/* Connection line glow */}
                <filter id="gp-lineglow" x="-200%" y="-200%" width="500%" height="500%">
                  <feGaussianBlur in="SourceGraphic" stdDeviation="2.5" result="b"/>
                  <feComposite in="SourceGraphic" in2="b" operator="over"/>
                </filter>

                {/* Sun rotating ring */}
                <linearGradient id="gp-ring-grad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%"   stopColor="#ffcc44" stopOpacity="1"/>
                  <stop offset="35%"  stopColor="#ff8800" stopOpacity="1"/>
                  <stop offset="70%"  stopColor="#ffaa22" stopOpacity="1"/>
                  <stop offset="100%" stopColor="#ffcc44" stopOpacity="1"/>
                </linearGradient>
                <filter id="gp-ringblur" x="-20%" y="-20%" width="140%" height="140%">
                  <feGaussianBlur stdDeviation="1.5"/>
                </filter>

                {/* Per-node gradient lines */}
                {nodes.map((node, i) => {
                  const { x, y } = positions[i]
                  const col = KIND_COLOR[node.kind]
                  return (
                    <linearGradient
                      key={i}
                      id={`gp-lg${i}`}
                      x1={CX} y1={CY} x2={x} y2={y}
                      gradientUnits="userSpaceOnUse"
                    >
                      <stop offset="0%"   stopColor={CENTER_COLOR} stopOpacity="0"/>
                      <stop offset="100%" stopColor={col}           stopOpacity="0.15"/>
                    </linearGradient>
                  )
                })}
              </defs>

              {/* ── Galaxy atmosphere ── */}
              <rect x="0" y="0" width={SVG_W} height={SVG_H}
                fill="url(#gp-gband)" filter="url(#gp-galblur)"/>
              <rect x="0" y={SVG_H * 0.25} width={SVG_W * 0.55} height={SVG_H * 0.75}
                fill="url(#gp-gwarm)" filter="url(#gp-galblur)"/>
              <rect x={SVG_W * 0.45} y="0" width={SVG_W * 0.55} height={SVG_H * 0.75}
                fill="url(#gp-gcool)" filter="url(#gp-galblur)"/>

              {/* ── Background micro-stars ── */}
              {bgStars.map((s, i) => (
                <circle key={i} cx={s.x} cy={s.y} r={s.r} fill="white" opacity={s.op}/>
              ))}

              {/* ── Nebula behind center ── */}
              <rect x={CX - 180} y={CY - 130} width="360" height="260"
                fill="url(#gp-nebula)" filter="url(#gp-galblur)" opacity="0.9"/>

              {/* ── Connection lines (below nodes) ── */}
              {nodes.map((node, i) => {
                const { x, y } = positions[i]
                const isHov = hovered === i || hovered === 'center'
                return (
                  <g key={i} style={{ pointerEvents: 'none' }}>
                    {isHov && (
                      <line x1={CX} y1={CY} x2={x} y2={y}
                        stroke={KIND_COLOR[node.kind]}
                        strokeWidth="1.2"
                        strokeOpacity="0.35"
                        filter="url(#gp-lineglow)"/>
                    )}
                    <line x1={CX} y1={CY} x2={x} y2={y}
                      stroke={`url(#gp-lg${i})`}
                      strokeWidth={isHov ? 0.8 : 0.5}
                      style={{ transition: 'stroke-width 0.2s ease' }}/>
                  </g>
                )
              })}

              {/* ── Scattered nodes ── */}
              {nodes.map((node, i) => {
                const { x, y } = positions[i]
                const col      = KIND_COLOR[node.kind]
                const isHov    = hovered === i
                const cr       = coreR(node.films, isHov)
                const gr       = glowR(node.films)
                const floatAmp = 2.5 + sr(i * 7 + 1) * 3.5
                const floatDur = `${3.8 + sr(i * 11 + 3) * 2.8}s`
                const floatDel = `${sr(i * 13 + 7) * 2}s`

                return (
                  <g
                    key={i}
                    style={{
                      cursor: node.id ? 'pointer' : 'default',
                      // @ts-ignore
                      '--gp-amp': `-${floatAmp}px`,
                      animation: `gp-float ${floatDur} ${floatDel} ease-in-out infinite`,
                    }}
                    onMouseEnter={() => setHovered(i)}
                    onMouseLeave={() => setHovered(null)}
                    onClick={() => node.id !== null && router.push(`/actors/${toSlug(node.name)}`)}
                    onTouchStart={e => { e.stopPropagation(); setHovered(i) }}
                  >
                    {/* Resting soft halo */}
                    <circle cx={x} cy={y} r={cr * 2.8} fill={col}
                      opacity={isHov ? 0 : 0.12} filter="url(#gp-nglow)"/>

                    {/* Hover bloom */}
                    <circle cx={x} cy={y} r={gr * 1.8} fill={col}
                      opacity={isHov ? 0.22 : 0}
                      filter="url(#gp-nglow)"
                      style={{ transition: 'opacity 0.18s ease' }}/>
                    <circle cx={x} cy={y} r={gr} fill={col}
                      opacity={isHov ? 0.45 : 0}
                      filter="url(#gp-nglow2)"
                      style={{ transition: 'opacity 0.18s ease' }}/>

                    {/* Core dot */}
                    <circle cx={x} cy={y} r={cr} fill={isHov ? '#fff' : col}
                      opacity={isHov ? 1 : 0.80}
                      style={{ transition: 'r 0.18s ease, fill 0.18s ease' }}/>

                    {/* Name */}
                    <text
                      x={x} y={y - cr - 5}
                      textAnchor="middle" fontSize="8.5" fontWeight={isHov ? '600' : '400'}
                      fill={isHov ? 'rgba(255,255,255,0.95)' : 'rgba(255,255,255,0.32)'}
                      style={{ userSelect: 'none', transition: 'fill 0.18s ease' }}
                    >
                      {node.name.split(' ')[0]}
                    </text>

                    {/* Hover detail */}
                    {isHov && (
                      <text x={x} y={y + cr + 13}
                        textAnchor="middle" fontSize="7"
                        fill={col} opacity="0.75"
                        style={{ userSelect: 'none' }}
                      >
                        {node.kind === 'director' ? 'Dir · ' : ''}{node.films} {node.films === 1 ? 'film' : 'films'}
                      </text>
                    )}
                  </g>
                )
              })}

              {/* ── Center sun ── */}
              {(() => {
                const avatarSlug  = center.name.toLowerCase().replace(/\s+/g, '')
                const isHovCenter = hovered === 'center'
                return (
                  <g
                    style={{ cursor: 'pointer' }}
                    onMouseEnter={() => setHovered('center')}
                    onMouseLeave={() => setHovered(null)}
                    onClick={() => router.push(`/actors/${toSlug(center.name)}`)}
                  >
                    {/* Rotating fire ring */}
                    <g>
                      <animateTransform attributeName="transform" type="rotate"
                        from={`0 ${CX} ${CY}`} to={`360 ${CX} ${CY}`}
                        dur="7s" repeatCount="indefinite" calcMode="linear"/>
                      <circle cx={CX} cy={CY} r={CENTER_R + 2}
                        fill="none"
                        stroke="url(#gp-ring-grad)"
                        strokeWidth="2.5"
                        filter="url(#gp-ringblur)"
                        opacity="0.65"
                      >
                        <animate attributeName="r"
                          values={`${CENTER_R + 2};${CENTER_R + 2.7};${CENTER_R + 2}`}
                          dur="3.5s" repeatCount="indefinite" calcMode="spline"
                          keySplines="0.45 0 0.55 1;0.45 0 0.55 1"/>
                      </circle>
                    </g>

                    {/* Avatar backing */}
                    <circle cx={CX} cy={CY} r={CENTER_R} fill="#0d0d15"/>

                    {/* Avatar image */}
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
                      <text x={CX} y={CY}
                        textAnchor="middle" dominantBaseline="central"
                        fontSize={CENTER_R * 0.48} fontWeight="700"
                        fill={CENTER_COLOR} style={{ userSelect: 'none' }}
                      >
                        {initials(center.name)}
                      </text>
                    )}

                    {/* Name label */}
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

              {/* Vignette */}
              <rect x="0" y="0" width={SVG_W} height={SVG_H}
                fill="url(#gp-vignette)" style={{ pointerEvents: 'none' }}/>
            </svg>
          )}

          {center && nodes.length > 0 && (
            <p className="text-center text-[10px] text-white/20 mt-3 tracking-wide select-none">
              Tap stars to explore connections
            </p>
          )}
        </div>
      )}

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
