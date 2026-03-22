'use client'

import { useState } from 'react'
import { useRouter } from 'next/navigation'

// ── Toast helper (inline; no extra dep) ──────────────────────────────────────

function useToast() {
  const [toast, setToast] = useState<string | null>(null)
  function showToast(msg: string) {
    setToast(msg)
    setTimeout(() => setToast(null), 2200)
  }
  return { toast, showToast }
}

// ── Exported types (consumed by page.tsx) ────────────────────────────────────

export interface NetworkNode {
  id: number | null  // null → no click navigation
  name: string
  films: number      // shared films with center actor
}

export interface NetworkCenter {
  id: number
  name: string
  gender?: 'M' | 'F' | null
}

// ── Layout constants ──────────────────────────────────────────────────────────

const SVG_W = 480
const SVG_H = 340
const CX = SVG_W / 2        // 240 — horizontal center
const CY = SVG_H / 2 - 10  // 160 — vertical center (slightly above mid)
const RING_R = 128           // radius of surrounding node ring
const CENTER_R = 34          // center node circle radius (~13% larger for prominence)
const NODE_R = 19            // surrounding node circle radius

// Colour palette — matches existing graph visual style
const COLORS = [
  '#8b5cf6', '#3b82f6', '#10b981', '#f59e0b',
  '#ec4899', '#06b6d4', '#ef4444', '#a3e635',
]
const CENTER_COLOR = '#ef4444'

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Position of surrounding node i out of total on a circle */
function polarPos(i: number, total: number) {
  const angle = (2 * Math.PI * i / total) - Math.PI / 2  // start from top
  return {
    x: CX + RING_R * Math.cos(angle),
    y: CY + RING_R * Math.sin(angle),
  }
}

/** Up to 2 capital initials from a name */
function initials(name: string) {
  return name.split(' ').map(w => w[0] ?? '').join('').slice(0, 2).toUpperCase()
}

/** Gender-aware pronoun for subtitle */
function pronoun(gender?: 'M' | 'F' | null) {
  return gender === 'F' ? 'she' : 'he'
}

// ── Component ─────────────────────────────────────────────────────────────────

export default function GraphPreview({
  networkData,
}: {
  networkData: { center: NetworkCenter; nodes: NetworkNode[] } | null
}) {
  const router = useRouter()
  const [hovered, setHovered] = useState<'center' | number | null>(null)
  const { toast, showToast } = useToast()

  /** Scroll to and focus the hero search input */
  function scrollToHeroSearch() {
    const input = document.getElementById('hero-search-input') as HTMLInputElement | null
    if (input) {
      input.scrollIntoView({ behavior: 'smooth', block: 'center' })
      setTimeout(() => input.focus(), 500)
    }
  }

  /** Share this network by URL: /?actor={center.id} */
  async function handleShare() {
    if (!center) return
    const url = typeof window !== 'undefined'
      ? `${window.location.origin}/?actor=${center.id}`
      : `/?actor=${center.id}`
    const shareData = {
      title: `${center.name}'s Cinema Network`,
      text:  `Explore ${center.name}'s collaboration network on South Cinema Analytics`,
      url,
    }
    try {
      if (navigator.share && navigator.canShare?.(shareData)) {
        await navigator.share(shareData)
      } else {
        await navigator.clipboard.writeText(url)
        showToast('Link copied!')
      }
    } catch {
      try {
        await navigator.clipboard.writeText(url)
        showToast('Link copied!')
      } catch { /* nothing we can do */ }
    }
  }

  const center   = networkData?.center
  const nodes    = (networkData?.nodes ?? []).slice(0, 8)
  const title    = center ? `${center.name}'s Network` : 'Cinema Network'
  const subtitle = center
    ? `Actors ${pronoun(center.gender)} has collaborated with across industries`
    : 'Explore collaboration networks'

  return (
    <div
      className="rounded-3xl border border-white/[0.08] overflow-hidden"
      style={{ background: '#0d0d15' }}
    >
      {/* ── Header ── */}
      <div className="px-6 pt-6 pb-3 flex items-center justify-between">
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-[0.15em] text-white/25 mb-1">
            Showing network for
          </p>
          <h2 className="text-base font-bold text-white flex items-center gap-2">
            🌐 {title}
          </h2>
          <p className="text-white/30 text-xs mt-0.5">{subtitle}</p>
        </div>
        <div className="flex items-center gap-2 flex-shrink-0">
          {/* Share this network */}
          {center && (
            <button
              onClick={handleShare}
              className="text-xs font-semibold px-3 py-1.5 rounded-full bg-white/[0.07] border border-white/[0.12] text-white/50 hover:text-white/80 hover:border-white/25 transition-all"
              aria-label="Share network"
            >
              🔗
            </button>
          )}
          <button
            onClick={scrollToHeroSearch}
            className="text-xs font-semibold px-4 py-1.5 rounded-full bg-white/[0.07] border border-white/[0.12] text-white/60 hover:text-white hover:border-white/25 transition-all"
          >
            Explore your favourite actor →
          </button>
        </div>
      </div>

      {/* ── Graph or fallback ── */}
      <div className="px-4 pb-5">
        {!center || nodes.length === 0 ? (

          // No data — clean fallback message
          <div
            className="w-full rounded-2xl flex items-center justify-center"
            style={{ background: '#08080f', minHeight: 200 }}
          >
            <p className="text-white/20 text-sm">No collaboration data available yet</p>
          </div>

        ) : (

          // Star graph — center + surrounding collaborators
          // Only center → collaborator edges; NO cross-edges between collaborators
          <svg
            viewBox={`0 0 ${SVG_W} ${SVG_H}`}
            className="w-full rounded-2xl"
            style={{ background: '#08080f', display: 'block', maxHeight: 320 }}
          >
            {/* ── Edges: center → each collaborator only ── */}
            {nodes.map((node, i) => {
              const { x, y } = polarPos(i, nodes.length)
              const lit   = hovered === i || hovered === 'center'
              const faded = hovered !== null && !lit
              return (
                <line
                  key={i}
                  x1={CX} y1={CY}
                  x2={x}  y2={y}
                  stroke={
                    lit   ? 'rgba(255,255,255,0.38)' :
                    faded ? 'rgba(255,255,255,0.03)' :
                            'rgba(255,255,255,0.10)'
                  }
                  strokeWidth={lit ? 2 : 1}
                  style={{ transition: 'stroke 0.15s ease, stroke-width 0.15s ease' }}
                />
              )
            })}

            {/* ── Surrounding nodes (collaborators) ── */}
            {nodes.map((node, i) => {
              const { x, y } = polarPos(i, nodes.length)
              const color = COLORS[i % COLORS.length]
              const isHov = hovered === i
              const faded = hovered !== null && hovered !== i && hovered !== 'center'

              return (
                <g
                  key={i}
                  style={{
                    cursor: node.id ? 'pointer' : 'default',
                    opacity: faded ? 0.22 : 1,
                    transition: 'opacity 0.15s ease',
                  }}
                  onMouseEnter={() => setHovered(i)}
                  onMouseLeave={() => setHovered(null)}
                  onClick={() => node.id && router.push(`/actors/${node.id}`)}
                >
                  {/* Glow ring on hover */}
                  {isHov && (
                    <circle cx={x} cy={y} r={NODE_R + 11} fill={color} opacity={0.14} />
                  )}

                  {/* Node circle */}
                  <circle
                    cx={x} cy={y} r={NODE_R}
                    fill={isHov ? color : color + '2e'}
                    stroke={color}
                    strokeWidth={isHov ? 2 : 1.5}
                    strokeOpacity={isHov ? 1 : 0.55}
                    style={{ transition: 'fill 0.15s ease, stroke-opacity 0.15s ease' }}
                  />

                  {/* Initials */}
                  <text
                    x={x} y={y}
                    textAnchor="middle" dominantBaseline="central"
                    fontSize={9} fontWeight="700"
                    fill={isHov ? '#fff' : color}
                    style={{ userSelect: 'none', transition: 'fill 0.15s ease' }}
                  >
                    {initials(node.name)}
                  </text>

                  {/* Name label — always below node */}
                  <text
                    x={x} y={y + NODE_R + 13}
                    textAnchor="middle" fontSize="9"
                    fill={isHov ? 'rgba(255,255,255,0.80)' : 'rgba(255,255,255,0.30)'}
                    style={{ userSelect: 'none', transition: 'fill 0.15s ease' }}
                  >
                    {node.name.split(' ')[0]}
                  </text>

                  {/* Film count — two overlapping layers cross-fade; no layout shift */}
                  {/* Layer 1: bare number, visible at rest, fades out on hover */}
                  <text
                    x={x} y={y + NODE_R + 24}
                    textAnchor="middle" fontSize="7.5"
                    fill="rgba(255,255,255,0.18)"
                    style={{ userSelect: 'none', transition: 'opacity 0.18s ease', opacity: isHov ? 0 : 1 }}
                  >
                    {node.films}
                  </text>
                  {/* Layer 2: "N films", invisible at rest, fades in on hover */}
                  <text
                    x={x} y={y + NODE_R + 24}
                    textAnchor="middle" fontSize="7.5"
                    fill="rgba(255,255,255,0.50)"
                    style={{ userSelect: 'none', transition: 'opacity 0.18s ease', opacity: isHov ? 1 : 0 }}
                  >
                    {node.films} films
                  </text>
                </g>
              )
            })}

            {/* ── Center node (the selected actor) ── */}
            <g
              style={{ cursor: 'pointer' }}
              onMouseEnter={() => setHovered('center')}
              onMouseLeave={() => setHovered(null)}
              onClick={() => router.push(`/actors/${center.id}`)}
            >
              {/* Glow ring */}
              {hovered === 'center' && (
                <circle cx={CX} cy={CY} r={CENTER_R + 12} fill={CENTER_COLOR} opacity={0.15} />
              )}

              {/* Node circle */}
              <circle
                cx={CX} cy={CY} r={CENTER_R}
                fill={hovered === 'center' ? CENTER_COLOR : CENTER_COLOR + '33'}
                stroke={CENTER_COLOR}
                strokeWidth={hovered === 'center' ? 2 : 1.5}
                strokeOpacity={hovered === 'center' ? 1 : 0.70}
                style={{ transition: 'fill 0.15s ease' }}
              />

              {/* Initials */}
              <text
                x={CX} y={CY}
                textAnchor="middle" dominantBaseline="central"
                fontSize={Math.round(CENTER_R * 0.52)} fontWeight="700"
                fill={hovered === 'center' ? '#fff' : CENTER_COLOR}
                style={{ userSelect: 'none', transition: 'fill 0.15s ease' }}
              >
                {initials(center.name)}
              </text>

              {/* Name label */}
              <text
                x={CX} y={CY + CENTER_R + 13}
                textAnchor="middle" fontSize="9.5"
                fill={hovered === 'center' ? 'rgba(255,255,255,0.80)' : 'rgba(255,255,255,0.50)'}
                style={{ userSelect: 'none', transition: 'fill 0.15s ease' }}
              >
                {center.name.split(' ')[0]}
              </text>
            </g>
          </svg>

        )}

        {/* Legend — only shown when graph is visible */}
        {center && nodes.length > 0 && (
          <p className="text-center text-[10px] text-white/15 mt-3 tracking-wide">
            Lines represent shared films
          </p>
        )}
      </div>

      {/* Toast notification */}
      {toast && (
        <div
          className="fixed bottom-6 left-1/2 -translate-x-1/2 z-50
                     px-4 py-2 rounded-full text-xs font-semibold
                     bg-white text-[#0a0a0f] shadow-lg shadow-black/40"
          style={{ animation: 'fadeInUpGraph 0.2s ease' }}
        >
          {toast}
        </div>
      )}
      <style>{`
        @keyframes fadeInUpGraph {
          from { opacity: 0; transform: translate(-50%, 8px); }
          to   { opacity: 1; transform: translate(-50%, 0); }
        }
      `}</style>
    </div>
  )
}
