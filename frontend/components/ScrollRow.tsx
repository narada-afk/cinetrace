'use client'

import { useRef, useState, useEffect, useCallback } from 'react'

interface ScrollRowProps {
  children: React.ReactNode
  className?: string
}

export default function ScrollRow({ children, className = '' }: ScrollRowProps) {
  const ref        = useRef<HTMLDivElement>(null)
  const [canLeft,  setCanLeft]  = useState(false)
  const [canRight, setCanRight] = useState(false)

  const update = useCallback(() => {
    const el = ref.current
    if (!el) return
    setCanLeft(el.scrollLeft > 4)
    setCanRight(el.scrollLeft < el.scrollWidth - el.clientWidth - 4)
  }, [])

  useEffect(() => {
    const el = ref.current
    if (!el) return
    update()
    el.addEventListener('scroll', update, { passive: true })
    const ro = new ResizeObserver(update)
    ro.observe(el)
    return () => { el.removeEventListener('scroll', update); ro.disconnect() }
  }, [update])

  function scroll(dir: 'left' | 'right') {
    const el = ref.current
    if (!el) return
    el.scrollBy({ left: dir === 'left' ? -260 : 260, behavior: 'smooth' })
  }

  return (
    <div className="relative">
      {/* Left arrow */}
      {canLeft && (
        <button
          onClick={() => scroll('left')}
          aria-label="Scroll left"
          className="absolute left-0 top-1/2 -translate-y-1/2 z-10 -translate-x-1
            w-8 h-8 rounded-full flex items-center justify-center
            bg-[#1a1a2e] border border-white/10 text-white/60
            hover:text-white hover:border-white/25 hover:bg-[#22223a]
            transition-all duration-150 shadow-lg"
          style={{ top: 'calc(50% - 14px)' }}  /* align to avatar centre, not label */
        >
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
            <path d="M9 11L5 7L9 3" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
        </button>
      )}

      {/* Right arrow */}
      {canRight && (
        <button
          onClick={() => scroll('right')}
          aria-label="Scroll right"
          className="absolute right-0 top-1/2 -translate-y-1/2 z-10 translate-x-1
            w-8 h-8 rounded-full flex items-center justify-center
            bg-[#1a1a2e] border border-white/10 text-white/60
            hover:text-white hover:border-white/25 hover:bg-[#22223a]
            transition-all duration-150 shadow-lg"
          style={{ top: 'calc(50% - 14px)' }}
        >
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
            <path d="M5 3L9 7L5 11" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
        </button>
      )}

      {/* Fade edges */}
      {canLeft && (
        <div className="absolute left-0 inset-y-0 w-10 pointer-events-none z-[5]"
          style={{ background: 'linear-gradient(to right, #0a0a0f 0%, transparent 100%)' }} />
      )}
      {canRight && (
        <div className="absolute right-0 inset-y-0 w-10 pointer-events-none z-[5]"
          style={{ background: 'linear-gradient(to left, #0a0a0f 0%, transparent 100%)' }} />
      )}

      {/* Scrollable content */}
      <style>{`.scroll-row-inner::-webkit-scrollbar { display: none; }`}</style>
      <div
        ref={ref}
        className={`scroll-row-inner overflow-x-auto ${className}`}
        style={{ scrollbarWidth: 'none', msOverflowStyle: 'none' }}
      >
        {children}
      </div>
    </div>
  )
}
