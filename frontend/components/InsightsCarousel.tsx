'use client'

/**
 * InsightsCarousel — auto-scrolling carousel with Netflix-style nav buttons.
 *
 * Behaviour
 * - Slow, continuous left→right auto-scroll using requestAnimationFrame
 * - Pauses on hover / focus / tab-hidden / off-screen (same as before)
 * - Left / right chevron buttons jump 3 cards at a time with smooth scroll
 * - Buttons fade in when the carousel is hovered; always visible at low opacity
 * - Clicking a button pauses auto-scroll for 800 ms so RAF doesn't fight the
 *   browser's smooth-scroll animation
 * - Infinite loop: cards are tripled; left-wrap guard prevents going past 0
 */

import { useEffect, useRef } from 'react'
import InsightCard, { type InsightCardData } from '@/components/InsightCard'

const CARD_W   = 380
const GAP      = 16   // gap-4
const SPEED    = 0.22 // px/ms → ~220 px/s ≈ one card every 1.5 s — visibly drifting
const JUMP     = 2    // cards per button click

// Responsive card width:
//   mobile  — the carousel breaks out of page padding (handled by -mx-6 in page.tsx),
//             so 100vw is the true container. We subtract 56px: 24px left-indent
//             (pl-6 on the flex row) + 16px gap + 16px right-peek for scroll hint.
//   desktop — fixed 380 px (page padding is intact so no break-out)
// clamp(min, preferred, max) ensures it never shrinks below 260 px or grows past 380 px
const CARD_WIDTH_CSS = 'clamp(260px, calc(100vw - 56px), 380px)'

export default function InsightsCarousel({ cards }: { cards: InsightCardData[] }) {
  const scrollRef          = useRef<HTMLDivElement>(null)
  const hoverPausedRef     = useRef(false)
  const viewPausedRef      = useRef(false)
  const isIntersectingRef  = useRef(true)
  const manualScrollingRef = useRef(false)   // prevents loop-reset during smooth scroll
  const touchActiveRef     = useRef(false)   // prevents loop-reset while finger is on screen
  // Touch-direction detection — only pause RAF for horizontal swipes, not vertical page scrolls
  const touchStartXRef     = useRef(0)
  const touchStartYRef     = useRef(0)
  const touchDirectionRef  = useRef<'h' | 'v' | null>(null)

  const items = cards.length > 0 ? [...cards, ...cards, ...cards] : cards

  useEffect(() => {
    const el = scrollRef.current
    if (!el || items.length === 0) return

    const setWidth = el.scrollWidth / 3
    el.scrollLeft = Math.random() * setWidth

    let rafId: number
    let prev: DOMHighResTimeStamp | null = null

    function tick(now: DOMHighResTimeStamp) {
      const dt = prev != null ? now - prev : 0
      prev = now

      if (!hoverPausedRef.current && !viewPausedRef.current && el) {
        el.scrollLeft += SPEED * dt
        // Only reset the loop seam when neither a smooth-scroll nor a touch
        // gesture is in progress (avoids fighting the browser's native scroll)
        if (!manualScrollingRef.current && !touchActiveRef.current && el.scrollLeft >= setWidth) {
          el.scrollLeft -= setWidth
        }
      }

      rafId = requestAnimationFrame(tick)
    }

    rafId = requestAnimationFrame(tick)

    const observer = new IntersectionObserver(
      ([entry]) => {
        isIntersectingRef.current = entry.isIntersecting
        viewPausedRef.current = document.hidden || !entry.isIntersecting
      },
      // Fire as soon as any pixel is visible so the scroll is already
      // drifting by the time the user finishes scrolling to this section
      { threshold: 0.01 },
    )
    observer.observe(el)

    function onVisibility() {
      viewPausedRef.current = document.hidden || !isIntersectingRef.current
    }
    document.addEventListener('visibilitychange', onVisibility)

    return () => {
      cancelAnimationFrame(rafId)
      observer.disconnect()
      document.removeEventListener('visibilitychange', onVisibility)
    }
  }, [])

  // ── Nav button handlers ───────────────────────────────────────────────────────

  function scrollBy(direction: 1 | -1) {
    const el = scrollRef.current
    if (!el) return
    const jump = JUMP * (CARD_W + GAP)

    // Left guard: if scrollLeft would go negative, jump to the equivalent
    // position in the second copy of the set before animating back
    if (direction === -1 && el.scrollLeft < jump) {
      el.scrollLeft += el.scrollWidth / 3
    }

    manualScrollingRef.current = true
    hoverPausedRef.current     = true
    el.scrollBy({ left: direction * jump, behavior: 'smooth' })

    // Resume auto-scroll after smooth animation completes
    setTimeout(() => {
      hoverPausedRef.current     = false
      manualScrollingRef.current = false
    }, 600)
  }

  if (cards.length === 0) return null

  // Shared button style
  const btnBase: React.CSSProperties = {
    background:     'rgba(10,10,20,0.82)',
    border:         '1px solid rgba(255,255,255,0.13)',
    backdropFilter: 'blur(10px)',
    boxShadow:      '0 4px 20px rgba(0,0,0,0.55)',
  }

  return (
    <>
      <style>{`.insights-scroll::-webkit-scrollbar { display: none; }`}</style>

      {/* Outer wrapper: relative so buttons can be positioned on the edges */}
      <div className="relative group">

        {/* ── Left chevron ─────────────────────────────────────────────────── */}
        <button
          onClick={() => scrollBy(-1)}
          aria-label="Previous cards"
          className="
            hidden sm:flex
            absolute left-0 top-1/2 -translate-y-1/2 -translate-x-1/2
            z-20 w-11 h-11 rounded-full items-center justify-center
            opacity-0 group-hover:opacity-100
            hover:scale-110 active:scale-95
            transition-all duration-200
          "
          style={btnBase}
        >
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none"
            stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="15,18 9,12 15,6" />
          </svg>
        </button>

        {/* ── Right chevron ────────────────────────────────────────────────── */}
        <button
          onClick={() => scrollBy(1)}
          aria-label="Next cards"
          className="
            hidden sm:flex
            absolute right-0 top-1/2 -translate-y-1/2 translate-x-1/2
            z-20 w-11 h-11 rounded-full items-center justify-center
            opacity-0 group-hover:opacity-100
            hover:scale-110 active:scale-95
            transition-all duration-200
          "
          style={btnBase}
        >
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none"
            stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="9,18 15,12 9,6" />
          </svg>
        </button>

        {/* ── Carousel with edge-fade mask ─────────────────────────────────── */}
        <div
          style={{
            maskImage:       'linear-gradient(to right, transparent 0%, black 7%, black 93%, transparent 100%)',
            WebkitMaskImage: 'linear-gradient(to right, transparent 0%, black 7%, black 93%, transparent 100%)',
          }}
        >
          <div
            ref={scrollRef}
            className="insights-scroll overflow-x-auto"
            style={{
              scrollbarWidth: 'none',
              msOverflowStyle: 'none',
              // Snap to card boundaries after each swipe on mobile.
              // Does not affect desktop — RAF keeps the scroll moving continuously
              // so the browser never sees a "resting" state to snap from.
              // 'proximity' is less aggressive than 'mandatory' — it won't
              // fight the RAF's small incremental scrollLeft changes on iPad Safari
              scrollSnapType: 'x proximity',
            }}
            aria-live="off"
            onMouseEnter={() => { hoverPausedRef.current = true  }}
            onMouseLeave={() => { hoverPausedRef.current = false }}
            onFocus={()     => { hoverPausedRef.current = true  }}
            onBlur={()      => { hoverPausedRef.current = false }}
            // ── Touch handlers ─────────────────────────────────────────────
            // Only pause the RAF for HORIZONTAL swipes (carousel interaction).
            // Vertical page scrolls must NOT pause it — on iPad the finger often
            // starts inside the carousel area while the user is scrolling the page.
            onTouchStart={e => {
              touchStartXRef.current    = e.touches[0].clientX
              touchStartYRef.current    = e.touches[0].clientY
              touchDirectionRef.current = null
              touchActiveRef.current    = true
            }}
            onTouchMove={e => {
              if (touchDirectionRef.current !== null) return
              const dx = Math.abs(e.touches[0].clientX - touchStartXRef.current)
              const dy = Math.abs(e.touches[0].clientY - touchStartYRef.current)
              if (dx < 4 && dy < 4) return  // haven't moved enough yet
              touchDirectionRef.current = dx >= dy ? 'h' : 'v'
              if (touchDirectionRef.current === 'h') {
                // Horizontal swipe — pause RAF so it doesn't fight the gesture
                hoverPausedRef.current = true
              }
            }}
            onTouchEnd={() => {
              setTimeout(() => {
                hoverPausedRef.current = false
                touchActiveRef.current = false
                touchDirectionRef.current = null
              }, 800)
            }}
            onTouchCancel={() => {
              setTimeout(() => {
                hoverPausedRef.current = false
                touchActiveRef.current = false
                touchDirectionRef.current = null
              }, 300)
            }}
          >
            {/* pl-6 on mobile aligns first card with section heading; sm:pl-0 resets on desktop */}
            <div className="flex gap-4 pb-1 pl-6 sm:pl-0" style={{ width: 'max-content' }}>
              {items.map((card, i) => (
                <div key={i} style={{ width: CARD_WIDTH_CSS, flexShrink: 0, scrollSnapAlign: 'start' }}>
                  <InsightCard {...card} />
                </div>
              ))}
            </div>
          </div>
        </div>

      </div>
    </>
  )
}
