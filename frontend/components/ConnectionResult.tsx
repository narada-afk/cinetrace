'use client'

/**
 * ConnectionResult — Progressive center-focused path animation.
 *
 * Reveals Actor → Movie → Actor → … one node at a time.
 * The current node is always centered in the viewport; previous nodes
 * shift left. Uses only CSS transitions + keyframes — no libs.
 *
 * Layout (px):
 *   Actor node  : ACTOR_W  = 80 px
 *   Movie node  : MOVIE_W  = 120 px
 *   Connector   : CONN_W   = 44 px
 *   Track height: 116 px
 *
 * Timing (Part 4 — rhythmic, not mechanical):
 *   Actor step: 500 ms  — more dramatic, deserves a beat
 *   Movie step: 350 ms  — quick connector, keeps momentum
 *
 * Interactions:
 *   Part 1 — Entry: container fades in + track scales 0.97 → 1; first step
 *             delayed 250 ms so the user sees the actor before it moves.
 *   Part 2 — Pause: any hover/mousedown/touch pauses; auto-resumes 1.8 s
 *             after last interaction (resumeTick triggers effect re-run).
 *   Part 3 — Payoff: last actor gets a scale-bump + expanding glow ring;
 *             "Connected in N steps" text fades in below.
 *   Part 5 — Edge fade: CSS mask gradient on track (left + right).
 *   Part 6 — Replay: resets step + re-triggers entry animation.
 *   Part 7 — Share: Web Share API → clipboard fallback → toast.
 *   Part 8 — Post-done: past actor nodes stay clickable + get hover scale.
 */

import { useState, useEffect, useRef } from 'react'
import { useRouter } from 'next/navigation'
import ActorAvatar from '@/components/ActorAvatar'
import type { ConnectionPath } from '@/lib/api'

// ── Sizing ─────────────────────────────────────────────────────────────────────

const ACTOR_W       = 80
const MOVIE_W       = 72    // poster card width
const CONN_W        = 36
const ACTOR_STEP_MS = 500
const MOVIE_STEP_MS = 350

// ── Item model ─────────────────────────────────────────────────────────────────

type Item =
  | { kind: 'actor'; id: number; name: string }
  | { kind: 'movie'; id: number; title: string; posterUrl: string | null; tmdbId: number | null }

function buildItems(result: ConnectionPath): Item[] {
  const out: Item[] = []
  result.path.forEach((actor, i) => {
    out.push({ kind: 'actor', id: actor.id, name: actor.name })
    if (i < result.connections.length) {
      const c = result.connections[i]
      out.push({
        kind:      'movie',
        id:        c.movie_id,
        title:     c.movie_title,
        posterUrl: c.poster_url,
        tmdbId:    c.tmdb_id,
      })
    }
  })
  return out
}

function itemWidth(item: Item) {
  return item.kind === 'actor' ? ACTOR_W : MOVIE_W
}

// ── Actor node (Parts 3 + 8: payoff animation + hover-scale after done) ────────

function ActorNode({
  item, active, past, payoff, done, onClick,
}: {
  item:    Extract<Item, { kind: 'actor' }>
  active:  boolean
  past:    boolean
  /** True only for the final actor once animation completes */
  payoff:  boolean
  /** True once all steps have been revealed */
  done:    boolean
  onClick: () => void
}) {
  const [isHov, setIsHov] = useState(false)
  const canInteract = active || past

  return (
    <div
      onClick={canInteract ? onClick : undefined}
      onMouseEnter={() => { if (canInteract) setIsHov(true) }}
      onMouseLeave={() => setIsHov(false)}
      style={{
        width:         ACTOR_W,
        flexShrink:    0,
        display:       'flex',
        flexDirection: 'column',
        alignItems:    'center',
        gap:           8,
        position:      'relative',
        // Part 8: past actors brighten on hover once done
        opacity:       active ? 1 : past ? (done && isHov ? 0.62 : 0.38) : 0,
        // Part 3: hand off transform to CSS animation when payoff fires
        transform:     payoff                    ? undefined
                     : active                   ? 'scale(1.08)'
                     : (done && isHov && past)  ? 'scale(1.04)'
                     :                            'scale(1)',
        animation:     payoff ? 'crPayoffScale 0.4s ease-out forwards' : undefined,
        transition:    payoff
          ? 'opacity 0.38s ease'
          : 'opacity 0.38s ease, transform 0.38s cubic-bezier(0.34,1.3,0.64,1)',
        cursor:        canInteract ? 'pointer' : 'default',
        pointerEvents: canInteract ? 'auto'    : 'none',
      }}
    >
      {/* Avatar wrapper — needed for absolutely-positioned glow ring */}
      <div style={{ position: 'relative', flexShrink: 0 }}>
        <ActorAvatar name={item.name} size={52} />

        {/* Part 3: expanding glow ring on final step */}
        {payoff && (
          <div
            aria-hidden="true"
            style={{
              position:     'absolute',
              inset:        -10,
              borderRadius: '50%',
              border:       '1.5px solid rgba(255,255,255,0.45)',
              animation:    'crPayoffGlow 0.55s ease-out forwards',
              pointerEvents: 'none',
            }}
          />
        )}
      </div>

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
        {item.name.split(' ')[0]}
      </p>
    </div>
  )
}

// ── Movie node — poster card, clicks to TMDB ──────────────────────────────────

function MovieNode({
  item, active, past,
}: {
  item:   Extract<Item, { kind: 'movie' }>
  active: boolean
  past:   boolean
}) {
  const POSTER_W = MOVIE_W        // 72
  const POSTER_H = Math.round(MOVIE_W * 1.5)  // 108 — 2:3 aspect

  function openTmdb() {
    if (item.tmdbId) {
      window.open(`https://www.themoviedb.org/movie/${item.tmdbId}`, '_blank', 'noopener')
    }
  }

  const clickable = (active || past) && !!item.tmdbId

  return (
    <div
      onClick={clickable ? openTmdb : undefined}
      title={clickable ? `Open "${item.title}" on TMDB` : item.title}
      style={{
        width:          POSTER_W,
        flexShrink:     0,
        display:        'flex',
        flexDirection:  'column',
        alignItems:     'center',
        gap:            6,
        opacity:        active ? 1 : past ? 0.45 : 0,
        transform:      active ? 'scale(1.06)' : 'scale(1)',
        transition:     'opacity 0.38s ease, transform 0.38s ease',
        cursor:         clickable ? 'pointer' : 'default',
        pointerEvents:  (active || past) ? 'auto' : 'none',
      }}
    >
      {/* Poster */}
      <div style={{
        width:        POSTER_W,
        height:       POSTER_H,
        borderRadius: 8,
        overflow:     'hidden',
        background:   'rgba(255,255,255,0.06)',
        border:       `1px solid ${active ? 'rgba(255,255,255,0.22)' : 'rgba(255,255,255,0.08)'}`,
        flexShrink:   0,
        position:     'relative',
        transition:   'border-color 0.38s ease, box-shadow 0.2s ease',
        boxShadow:    active ? '0 4px 18px rgba(0,0,0,0.55)' : 'none',
      }}>
        {item.posterUrl ? (
          // eslint-disable-next-line @next/next/no-img-element
          <img
            src={item.posterUrl}
            alt={item.title}
            style={{ width: '100%', height: '100%', objectFit: 'cover', display: 'block' }}
          />
        ) : (
          <div style={{
            width: '100%', height: '100%',
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}>
            <span style={{ fontSize: 9, color: 'rgba(255,255,255,0.35)', textAlign: 'center', padding: '0 4px', lineHeight: 1.4 }}>
              {item.title}
            </span>
          </div>
        )}
        {/* TMDB hover overlay */}
        {clickable && (
          <div style={{
            position:       'absolute', inset: 0,
            background:     'rgba(0,0,0,0)',
            display:        'flex', alignItems: 'center', justifyContent: 'center',
            transition:     'background 0.2s ease',
          }}
          className="movie-node-overlay"
          />
        )}
      </div>

      {/* Title label */}
      <p style={{
        fontSize:   9,
        fontWeight: 500,
        color:      active ? 'rgba(255,255,255,0.70)' : 'rgba(255,255,255,0.35)',
        textAlign:  'center',
        lineHeight: 1.35,
        margin:     0,
        maxWidth:   POSTER_W,
        overflow:   'hidden',
        display:    '-webkit-box',
        WebkitLineClamp: 2,
        WebkitBoxOrient: 'vertical',
        transition: 'color 0.38s ease',
      }}>
        {item.title}
      </p>
    </div>
  )
}

// ── Connector line (unchanged) ─────────────────────────────────────────────────

function Connector({ shown }: { shown: boolean }) {
  return (
    <div style={{ width: CONN_W, flexShrink: 0, display: 'flex', alignItems: 'center' }}>
      <div style={{
        height:     1,
        background: 'rgba(255,255,255,0.18)',
        width:      shown ? '100%' : '0%',
        transition: shown ? 'width 0.28s ease 0.12s' : 'none',
      }} />
    </div>
  )
}

// ── Main ───────────────────────────────────────────────────────────────────────

export default function ConnectionResult({ result }: { result: ConnectionPath }) {
  const router       = useRouter()
  const containerRef = useRef<HTMLDivElement>(null)

  // ── State
  const [step,       setStep]       = useState(0)
  const [containerW, setContainerW] = useState(600)
  const [toast,      setToast]      = useState<string | null>(null)
  /** Part 1: gates entry animation — steps don't start until this is true */
  const [isReady,    setIsReady]    = useState(false)
  /** Part 2: increments on resume to re-trigger the step-advance effect */
  const [resumeTick, setResumeTick] = useState(0)
  /** Part 3: fires shortly after last step to trigger payoff animation */
  const [showPayoff, setShowPayoff] = useState(false)

  // ── Refs
  const isPausedRef    = useRef(false)
  const resumeTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const items   = buildItems(result)
  const maxStep = items.length - 1
  const done    = step >= maxStep

  // ── Measure container width once after mount ──────────────────────────────
  useEffect(() => {
    if (containerRef.current) setContainerW(containerRef.current.clientWidth)
  }, [])

  // ── Part 1: Entry gate — 250ms delay before first step ───────────────────
  // Container fades in over 300ms (opacity transition on wrapper).
  // The 250ms delay means the user sees the first actor at rest before it moves.
  useEffect(() => {
    const tid = setTimeout(() => setIsReady(true), 250)
    return () => clearTimeout(tid)
  }, [])

  // ── Part 3: Payoff — fires 80ms after the last step is reached ───────────
  useEffect(() => {
    if (!done || !isReady) return
    const tid = setTimeout(() => setShowPayoff(true), 80)
    return () => clearTimeout(tid)
  }, [done, isReady])

  // ── Parts 2 + 4: Step advancement — variable timing, respects pause ───────
  // Depends on resumeTick so it re-runs when the user stops interacting.
  useEffect(() => {
    if (!isReady || step >= maxStep || isPausedRef.current) return
    // Part 4: actor steps linger longer; movie labels move quickly
    const delay = items[step]?.kind === 'actor' ? ACTOR_STEP_MS : MOVIE_STEP_MS
    const tid = setTimeout(() => {
      // Re-check inside callback — user may have started hovering mid-wait
      if (!isPausedRef.current) setStep(s => s + 1)
    }, delay)
    return () => clearTimeout(tid)
  }, [step, maxStep, isReady, resumeTick])

  // ── Part 2: Pause / resume helpers ───────────────────────────────────────

  /** Pause immediately; cancel any pending resume timer */
  function pause() {
    isPausedRef.current = true
    if (resumeTimerRef.current) clearTimeout(resumeTimerRef.current)
  }

  /**
   * Schedule auto-resume after 1.8 s of inactivity.
   * Increments resumeTick → re-triggers step-advance effect.
   */
  function scheduleResume() {
    if (resumeTimerRef.current) clearTimeout(resumeTimerRef.current)
    resumeTimerRef.current = setTimeout(() => {
      isPausedRef.current = false
      setResumeTick(t => t + 1)
    }, 1800)
  }

  // ── translateX — active item centred while animating; full chain centred when done ──
  function calcTranslate(atStep: number): number {
    // When all steps revealed, center the whole chain
    if (atStep >= maxStep) {
      const totalW = items.reduce((sum, item, i) =>
        sum + itemWidth(item) + (i < items.length - 1 ? CONN_W : 0), 0)
      return Math.max(0, (containerW - totalW) / 2)
    }
    // Otherwise track the active node
    let leftEdge = 0
    for (let i = 0; i < atStep; i++) {
      leftEdge += itemWidth(items[i]) + CONN_W
    }
    const curW = itemWidth(items[atStep])
    return containerW / 2 - leftEdge - curW / 2
  }

  // ── Part 6: Replay ───────────────────────────────────────────────────────
  function handleReplay() {
    // Clear pause state immediately so replay starts without waiting for inactivity
    isPausedRef.current = false
    if (resumeTimerRef.current) clearTimeout(resumeTimerRef.current)
    setStep(0)
    setShowPayoff(false)
    // Re-trigger entry animation: briefly drop isReady → true after 80 ms
    setIsReady(false)
    setTimeout(() => setIsReady(true), 80)
  }

  // ── Part 7: Share ─────────────────────────────────────────────────────────
  async function handleShare() {
    const actor1Id = result.path[0]?.id
    const actor2Id = result.path.at(-1)?.id
    const url = typeof window !== 'undefined'
      ? `${window.location.origin}/connect?from=${actor1Id}&to=${actor2Id}`
      : `/connect?from=${actor1Id}&to=${actor2Id}`

    const shareData = {
      title: `${result.path[0]?.name} → ${result.path.at(-1)?.name}`,
      text:  `Connected in ${result.depth} step${result.depth !== 1 ? 's' : ''} on CineTrace`,
      url,
    }

    try {
      if (navigator.share && navigator.canShare?.(shareData)) {
        await navigator.share(shareData)
      } else {
        await navigator.clipboard.writeText(url)
        showToastMsg('Link copied!')
      }
    } catch {
      try {
        await navigator.clipboard.writeText(url)
        showToastMsg('Link copied!')
      } catch { /* nothing we can do */ }
    }
  }

  function showToastMsg(msg: string) {
    setToast(msg)
    setTimeout(() => setToast(null), 2200)
  }

  // ── No-path fallback ──────────────────────────────────────────────────────
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
    <>
      {/* ── Part 1: Wrapper fades in (opacity 0→1 over 300ms) on isReady ── */}
      <div
        className="mt-6"
        style={{
          opacity:    isReady ? 1 : 0,
          transition: 'opacity 0.30s ease',
        }}
        // Part 2: hover/interaction pauses; leaving schedules auto-resume
        onMouseEnter={pause}
        onMouseLeave={scheduleResume}
        onMouseDown={() => { pause(); scheduleResume() }}
        onTouchStart={() => { pause(); scheduleResume() }}
        onTouchEnd={scheduleResume}
      >

        {/* ── Depth label (top context) ── */}
        <p className="text-center text-white/40 text-xs uppercase tracking-widest mb-6">
          Only{' '}
          <span className="text-white font-bold">{result.depth}</span>{' '}
          step{result.depth !== 1 ? 's' : ''} apart 😳
        </p>

        {/* ── Part 1 + 5: Track — entry scale bump + edge fade mask ── */}
        <div
          style={{
            // Part 1: subtle scale-up from 0.97 → 1.0 on entry
            transform:       isReady ? 'scale(1)' : 'scale(0.97)',
            transition:      'transform 0.35s cubic-bezier(0.34, 1.3, 0.64, 1)',
            // Part 5: soft gradient fade at left + right edges
            maskImage:       'linear-gradient(to right, transparent 0%, black 10%, black 90%, transparent 100%)',
            WebkitMaskImage: 'linear-gradient(to right, transparent 0%, black 10%, black 90%, transparent 100%)',
          }}
        >
          <div
            ref={containerRef}
            className="relative overflow-hidden"
            style={{ height: 148 }}
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
                  {i > 0 && <Connector shown={i <= step} />}

                  {item.kind === 'actor' ? (
                    <ActorNode
                      item={item}
                      active={i === step}
                      past={i < step}
                      // Part 3: payoff only on the very last actor, after done
                      payoff={showPayoff && i === maxStep}
                      done={done}
                      onClick={() => router.push(`/actors/${item.name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '')}`)}
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
        </div>

        {/* ── Progress dots (Part 8: stay interactive post-done) ── */}
        <div className="flex justify-center items-center gap-2 mt-5">
          {items.map((item, i) => (
            <button
              key={i}
              aria-label={item.kind === 'actor' ? item.name : item.title}
              onClick={() => {
                // Allow jumping to any past step; pause then schedule resume
                if (i !== step) {
                  pause()
                  setStep(i)
                  scheduleResume()
                }
              }}
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
                cursor:     i <= step ? 'pointer' : 'default',
              }}
            />
          ))}
        </div>

        {/* Part 3 placeholder — depth label shown at top; no duplicate here */}

        {/* ── Parts 6 + 7: Replay + Share — appear after animation completes ── */}
        <div
          className="flex justify-center gap-3 mt-5"
          style={{
            opacity:       done ? 1 : 0,
            transform:     done ? 'translateY(0)' : 'translateY(6px)',
            transition:    'opacity 0.40s ease 0.25s, transform 0.40s ease 0.25s',
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

      </div>

      {/* ── Toast (fixed — outside main flow) ── */}
      {toast && (
        <div
          className="fixed bottom-6 left-1/2 -translate-x-1/2 z-50
                     px-4 py-2 rounded-full text-xs font-semibold
                     bg-white text-[#0a0a0f] shadow-lg shadow-black/40"
          style={{ animation: 'crFadeInUp 0.2s ease' }}
        >
          {toast}
        </div>
      )}

      {/* ── CSS keyframes (cr- prefix avoids collisions with other components) ── */}
      <style>{`
        /* Movie poster hover — reveal a subtle overlay */
        .movie-node-overlay:hover { background: rgba(0,0,0,0.28) !important; }
        /* Part 3: last actor scale bump — 1.08 → 1.12 → 1.0 */
        @keyframes crPayoffScale {
          0%   { transform: scale(1.08); }
          45%  { transform: scale(1.12); }
          100% { transform: scale(1.0);  }
        }
        /* Part 3: glow ring expands outward and fades */
        @keyframes crPayoffGlow {
          0%   { opacity: 0; transform: scale(0.85); }
          30%  { opacity: 1; }
          100% { opacity: 0; transform: scale(1.5);  }
        }
        /* Toast slide-up */
        @keyframes crFadeInUp {
          from { opacity: 0; transform: translate(-50%, 8px); }
          to   { opacity: 1; transform: translate(-50%, 0);   }
        }
      `}</style>
    </>
  )
}
