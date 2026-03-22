'use client'

/**
 * ConnectionResult — Progressive center-focused path animation.
 *
 * Reveals Actor → Movie → Actor → … one node at a time.
 * The current node is always centered in the viewport; previous nodes
 * shift left. Uses only CSS transitions + a setTimeout chain — no libs.
 *
 * Layout (px, fixed widths make translateX maths exact):
 *   Actor node  : ACTOR_W  = 80 px
 *   Movie node  : MOVIE_W  = 120 px
 *   Connector   : CONN_W   = 44 px (between every pair of nodes)
 *   Track height: 116 px   (fits 52px avatar + 8px gap + 14px name, scaled)
 *
 * Sequence timing:
 *   STEP_MS = 520 ms between advances; total ≤ 4 s for a 6-degree path.
 *
 * Controls (shown after animation completes):
 *   ↩ Replay  — resets step to 0, re-runs the reveal animation
 *   🔗 Share  — Web Share API with clipboard fallback + "Link copied!" toast
 *               URL: /connect?from={actor1_id}&to={actor2_id}
 */

import { useState, useEffect, useRef } from 'react'
import { useRouter } from 'next/navigation'
import ActorAvatar from '@/components/ActorAvatar'
import type { ConnectionPath } from '@/lib/api'

// ── Sizing ─────────────────────────────────────────────────────────────────────

const ACTOR_W = 80
const MOVIE_W = 120
const CONN_W  = 44
const STEP_MS = 520

// ── Item model ─────────────────────────────────────────────────────────────────

type Item =
  | { kind: 'actor'; id: number; name: string }
  | { kind: 'movie'; id: number; title: string }

function buildItems(result: ConnectionPath): Item[] {
  const out: Item[] = []
  result.path.forEach((actor, i) => {
    out.push({ kind: 'actor', id: actor.id, name: actor.name })
    if (i < result.connections.length) {
      out.push({
        kind:  'movie',
        id:    result.connections[i].movie_id,
        title: result.connections[i].movie_title,
      })
    }
  })
  return out
}

function itemWidth(item: Item) {
  return item.kind === 'actor' ? ACTOR_W : MOVIE_W
}

// ── Actor node ─────────────────────────────────────────────────────────────────

function ActorNode({
  item, active, past, onClick,
}: {
  item:    Extract<Item, { kind: 'actor' }>
  active:  boolean
  past:    boolean
  onClick: () => void
}) {
  return (
    <div
      onClick={active || past ? onClick : undefined}
      style={{
        width:         ACTOR_W,
        flexShrink:    0,
        display:       'flex',
        flexDirection: 'column',
        alignItems:    'center',
        gap:           8,
        opacity:       active ? 1 : past ? 0.38 : 0,
        transform:     active ? 'scale(1.08)' : 'scale(1)',
        transition:    'opacity 0.38s ease, transform 0.38s cubic-bezier(0.34,1.3,0.64,1)',
        cursor:        active || past ? 'pointer' : 'default',
        pointerEvents: active || past ? 'auto' : 'none',
      }}
    >
      <ActorAvatar name={item.name} size={52} />
      <p style={{
        color:      active ? 'rgba(255,255,255,0.90)' : 'rgba(255,255,255,0.55)',
        fontSize:   11,
        fontWeight: 600,
        textAlign:  'center',
        lineHeight: 1.3,
        maxWidth:   ACTOR_W,
        margin:     0,
        transition: 'color 0.38s ease',
      }}>
        {/* First name only keeps labels compact */}
        {item.name.split(' ')[0]}
      </p>
    </div>
  )
}

// ── Movie node ─────────────────────────────────────────────────────────────────

function MovieNode({
  item, active, past,
}: {
  item:   Extract<Item, { kind: 'movie' }>
  active: boolean
  past:   boolean
}) {
  return (
    <div style={{
      width:         MOVIE_W,
      flexShrink:    0,
      display:       'flex',
      alignItems:    'center',
      justifyContent: 'center',
      opacity:       active ? 1 : past ? 0.38 : 0,
      transform:     active ? 'scale(1.05)' : 'scale(1)',
      transition:    'opacity 0.38s ease, transform 0.38s ease',
      pointerEvents: 'none',
    }}>
      <div style={{
        borderRadius: 12,
        padding:      '8px 12px',
        background:   active ? 'rgba(255,255,255,0.09)' : 'rgba(255,255,255,0.04)',
        border:       `1px solid ${active ? 'rgba(255,255,255,0.22)' : 'rgba(255,255,255,0.07)'}`,
        transition:   'background 0.38s ease, border-color 0.38s ease',
        maxWidth:     MOVIE_W - 8,
        textAlign:    'center',
      }}>
        <p style={{
          color:      'rgba(255,255,255,0.75)',
          fontSize:   10,
          fontWeight: 500,
          lineHeight: 1.45,
          margin:     0,
        }}>
          {item.title}
        </p>
      </div>
    </div>
  )
}

// ── Connector line ─────────────────────────────────────────────────────────────

function Connector({ shown }: { shown: boolean }) {
  return (
    <div style={{ width: CONN_W, flexShrink: 0, display: 'flex', alignItems: 'center' }}>
      <div style={{
        height:     1,
        background: 'rgba(255,255,255,0.18)',
        width:      shown ? '100%' : '0%',
        // Slight delay: line draws after the incoming node starts appearing
        transition: shown ? 'width 0.28s ease 0.12s' : 'none',
      }} />
    </div>
  )
}

// ── Main ───────────────────────────────────────────────────────────────────────

export default function ConnectionResult({ result }: { result: ConnectionPath }) {
  const router       = useRouter()
  const containerRef = useRef<HTMLDivElement>(null)
  const [step,       setStep]       = useState(0)
  const [containerW, setContainerW] = useState(600) // sensible default; corrected after mount
  const [toast,      setToast]      = useState<string | null>(null)

  const items   = buildItems(result)
  const maxStep = items.length - 1
  const done    = step >= maxStep

  // ── Measure real container width once after mount ──
  useEffect(() => {
    if (containerRef.current) setContainerW(containerRef.current.clientWidth)
  }, [])

  // ── Auto-advance: one step every STEP_MS ms ──
  useEffect(() => {
    if (step >= maxStep) return
    const tid = setTimeout(() => setStep(s => s + 1), STEP_MS)
    return () => clearTimeout(tid)
  }, [step, maxStep])

  // ── translateX so the active item is horizontally centred ──
  // leftEdge = sum of (itemWidth + CONN_W) for items 0 … step-1
  function calcTranslate(atStep: number): number {
    let leftEdge = 0
    for (let i = 0; i < atStep; i++) {
      leftEdge += itemWidth(items[i]) + CONN_W
    }
    const curW = atStep < items.length ? itemWidth(items[atStep]) : ACTOR_W
    return containerW / 2 - leftEdge - curW / 2
  }

  // ── Replay: reset to beginning ──
  function handleReplay() {
    setStep(0)
  }

  // ── Share: Web Share API with clipboard fallback ──
  async function handleShare() {
    const actor1Id = result.path[0]?.id
    const actor2Id = result.path.at(-1)?.id
    const url = typeof window !== 'undefined'
      ? `${window.location.origin}/connect?from=${actor1Id}&to=${actor2Id}`
      : `/connect?from=${actor1Id}&to=${actor2Id}`

    const shareData = {
      title: `${result.path[0]?.name} → ${result.path.at(-1)?.name}`,
      text:  `Connected in ${result.depth} step${result.depth !== 1 ? 's' : ''} on South Cinema Analytics`,
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
      // User cancelled share or clipboard failed gracefully
      try {
        await navigator.clipboard.writeText(url)
        showToast('Link copied!')
      } catch {
        // Nothing we can do
      }
    }
  }

  function showToast(msg: string) {
    setToast(msg)
    setTimeout(() => setToast(null), 2200)
  }

  // ── No-path fallback ──
  if (!result.found) {
    return (
      <div className="text-center py-8">
        <p className="text-3xl mb-3">🔍</p>
        <p className="text-white/60 text-sm">No connection found within 6 degrees.</p>
      </div>
    )
  }

  const translateX = calcTranslate(step)

  return (
    <div className="mt-6">

      {/* ── Depth label ── */}
      <p className="text-center text-white/40 text-xs uppercase tracking-widest mb-6">
        Connected in{' '}
        <span className="text-white font-bold">{result.depth}</span>{' '}
        step{result.depth !== 1 ? 's' : ''}
      </p>

      {/* ── Track ── */}
      {/* overflow: hidden masks nodes outside the viewport; height is fixed so
          the scaled active node doesn't cause layout reflow */}
      <div
        ref={containerRef}
        className="relative overflow-hidden"
        style={{ height: 116 }}
      >
        <div
          className="absolute inset-y-0 flex items-center"
          style={{
            transform:  `translateX(${translateX}px)`,
            transition: 'transform 0.50s cubic-bezier(0.4, 0, 0.2, 1)',
            willChange: 'transform',
          }}
        >
          {items.map((item, i) => (
            <div key={i} style={{ display: 'flex', alignItems: 'center' }}>
              {/* Connector appears before every node except the first */}
              {i > 0 && <Connector shown={i <= step} />}

              {item.kind === 'actor' ? (
                <ActorNode
                  item={item}
                  active={i === step}
                  past={i < step}
                  onClick={() => router.push(`/actors/${item.id}`)}
                />
              ) : (
                <MovieNode
                  item={item}
                  active={i === step}
                  past={i < step}
                />
              )}
            </div>
          ))}
        </div>
      </div>

      {/* ── Progress dots ── */}
      {/* Actors get larger dots; movies get smaller. Click a past dot to replay from there. */}
      <div className="flex justify-center items-center gap-2 mt-5">
        {items.map((item, i) => (
          <button
            key={i}
            aria-label={item.kind === 'actor' ? item.name : item.title}
            onClick={() => i < step ? setStep(i) : undefined}
            style={{
              width:        item.kind === 'actor' ? 7 : 5,
              height:       item.kind === 'actor' ? 7 : 5,
              borderRadius: '50%',
              border:       'none',
              padding:      0,
              flexShrink:   0,
              background:
                i === step ? 'rgba(255,255,255,0.90)' :
                i <  step  ? 'rgba(255,255,255,0.40)' :
                              'rgba(255,255,255,0.12)',
              transition: 'background 0.3s ease',
              cursor:     i < step ? 'pointer' : 'default',
            }}
          />
        ))}
      </div>

      {/* ── Replay + Share buttons — appear after animation completes ── */}
      <div
        className="flex justify-center gap-3 mt-6"
        style={{
          opacity:    done ? 1 : 0,
          transform:  done ? 'translateY(0)' : 'translateY(6px)',
          transition: 'opacity 0.4s ease 0.1s, transform 0.4s ease 0.1s',
          pointerEvents: done ? 'auto' : 'none',
        }}
      >
        <button
          onClick={handleReplay}
          className="flex items-center gap-1.5 px-4 py-2 rounded-full text-xs font-semibold transition-all
                     bg-white/[0.07] border border-white/[0.12] text-white/55
                     hover:bg-white/[0.12] hover:text-white/80 hover:border-white/25"
        >
          ↩ Replay
        </button>

        <button
          onClick={handleShare}
          className="flex items-center gap-1.5 px-4 py-2 rounded-full text-xs font-semibold transition-all
                     bg-white/[0.07] border border-white/[0.12] text-white/55
                     hover:bg-white/[0.12] hover:text-white/80 hover:border-white/25"
        >
          🔗 Share
        </button>
      </div>

      {/* ── Toast notification ── */}
      {toast && (
        <div
          className="fixed bottom-6 left-1/2 -translate-x-1/2 z-50
                     px-4 py-2 rounded-full text-xs font-semibold
                     bg-white text-[#0a0a0f] shadow-lg shadow-black/40"
          style={{
            animation: 'fadeInUp 0.2s ease',
          }}
        >
          {toast}
        </div>
      )}

      <style>{`
        @keyframes fadeInUp {
          from { opacity: 0; transform: translate(-50%, 8px); }
          to   { opacity: 1; transform: translate(-50%, 0); }
        }
      `}</style>

    </div>
  )
}
