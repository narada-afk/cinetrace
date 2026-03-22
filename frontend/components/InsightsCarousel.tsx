'use client'

/**
 * InsightsCarousel — auto-scrolling carousel for Cinema Insight cards.
 *
 * Behaviour
 * - Slow, continuous left→right scroll using requestAnimationFrame
 * - Pauses on hover or focus (user interaction)
 * - Pauses when tab is hidden (visibilitychange)
 * - Pauses when scrolled off-screen (IntersectionObserver)
 * - Resumes automatically when all pause conditions clear
 * - Manual scroll (trackpad / touch) works at any time — RAF picks up from
 *   wherever the user scrolled to
 * - Infinite loop: cards are tripled so the seam is never visible; scrollLeft
 *   resets silently when it reaches 1/3 of the total track width
 * - Edge fade: CSS mask gradient on left + right for a Netflix-style look
 *
 * Implementation notes
 * - No animation library — pure RAF + scrollLeft manipulation
 * - Two refs (hoverPausedRef, viewPausedRef) avoid re-renders on pause toggles
 * - scrollbarWidth: none hides the scrollbar cross-browser
 */

import { useEffect, useRef } from 'react'
import InsightCard, { type InsightCardData } from '@/components/InsightCard'

// Width of each card in px — ~10 % wider than a 3-col grid cell at 1 200 px
const CARD_W = 360

// Scroll speed in px / ms  →  ~50 px/s  →  slow, comfortable for reading
const SPEED = 0.05

export default function InsightsCarousel({
  cards,
}: {
  cards: InsightCardData[]
}) {
  const scrollRef     = useRef<HTMLDivElement>(null)
  // Paused by hover or keyboard focus
  const hoverPausedRef = useRef(false)
  // Paused because tab is hidden or element is off-screen
  const viewPausedRef  = useRef(false)
  // Track latest intersection state for use inside the visibilitychange handler
  const isIntersectingRef = useRef(true)

  // Triple the set — the loop resets after 1/3 of total scroll width
  const items = cards.length > 0 ? [...cards, ...cards, ...cards] : cards

  useEffect(() => {
    const el = scrollRef.current
    if (!el || items.length === 0) return

    // ── RAF loop ──────────────────────────────────────────────────────────────
    let rafId: number
    let prev: DOMHighResTimeStamp | null = null

    function tick(now: DOMHighResTimeStamp) {
      const dt = prev != null ? now - prev : 0
      prev = now

      if (!hoverPausedRef.current && !viewPausedRef.current && el) {
        el.scrollLeft += SPEED * dt

        // Seamless reset: jump back one "set" when we cross the 1/3 boundary
        const setWidth = el.scrollWidth / 3
        if (el.scrollLeft >= setWidth) {
          el.scrollLeft -= setWidth
        }
      }

      rafId = requestAnimationFrame(tick)
    }

    rafId = requestAnimationFrame(tick)

    // ── IntersectionObserver — pause when not in viewport ──────────────────
    const observer = new IntersectionObserver(
      ([entry]) => {
        isIntersectingRef.current = entry.isIntersecting
        viewPausedRef.current = document.hidden || !entry.isIntersecting
      },
      { threshold: 0.1 },
    )
    observer.observe(el)

    // ── visibilitychange — pause when tab is hidden ────────────────────────
    function onVisibility() {
      viewPausedRef.current = document.hidden || !isIntersectingRef.current
    }
    document.addEventListener('visibilitychange', onVisibility)

    return () => {
      cancelAnimationFrame(rafId)
      observer.disconnect()
      document.removeEventListener('visibilitychange', onVisibility)
    }
  }, []) // intentionally run once — items are stable server-component props

  if (cards.length === 0) return null

  return (
    <>
      {/* Minimal inline keyframe — hides webkit scrollbar without globals.css change */}
      <style>{`
        .insights-scroll::-webkit-scrollbar { display: none; }
      `}</style>

      {/* Outer wrapper carries the edge-fade mask */}
      <div
        style={{
          // Fade 7% on each edge — wide enough to hint at more cards
          maskImage:       'linear-gradient(to right, transparent 0%, black 7%, black 93%, transparent 100%)',
          WebkitMaskImage: 'linear-gradient(to right, transparent 0%, black 7%, black 93%, transparent 100%)',
        }}
      >
        <div
          ref={scrollRef}
          className="insights-scroll overflow-x-auto"
          style={{ scrollbarWidth: 'none', msOverflowStyle: 'none' }}
          aria-live="off"
          onMouseEnter={() => { hoverPausedRef.current = true  }}
          onMouseLeave={() => { hoverPausedRef.current = false }}
          onFocus={()     => { hoverPausedRef.current = true  }}
          onBlur={()      => { hoverPausedRef.current = false }}
        >
          <div className="flex gap-4 pb-1" style={{ width: 'max-content' }}>
            {items.map((card, i) => (
              <div key={i} style={{ width: CARD_W, flexShrink: 0 }}>
                <InsightCard {...card} />
              </div>
            ))}
          </div>
        </div>
      </div>
    </>
  )
}
