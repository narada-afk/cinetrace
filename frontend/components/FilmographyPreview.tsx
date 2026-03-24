'use client'

import React, { useEffect, useRef } from 'react'
import Image from 'next/image'
import type { ActorMovie } from '@/lib/api'

// Same speed as InsightsCarousel — 0.05 px/ms ≈ 50 px/s
const SPEED = 0.05

interface FilmographyPreviewProps {
  movies: ActorMovie[]
  totalCount: number
}

export default function FilmographyPreview({ movies, totalCount }: FilmographyPreviewProps) {
  const sorted = [...movies]
    .filter(m => m.release_year != null && m.release_year > 0)
    .sort((a, b) => b.release_year - a.release_year)
  if (sorted.length === 0) return null

  const scrollRef      = useRef<HTMLDivElement>(null)
  const hoverPausedRef = useRef(false)
  const viewPausedRef  = useRef(false)
  const isIntersecting = useRef(true)

  useEffect(() => {
    const el = scrollRef.current
    if (!el) return

    // ── RAF loop — identical pattern to InsightsCarousel ──
    let rafId: number
    let prev: DOMHighResTimeStamp | null = null

    function tick(now: DOMHighResTimeStamp) {
      const dt = prev != null ? now - prev : 0
      prev = now

      if (!hoverPausedRef.current && !viewPausedRef.current && el) {
        el.scrollLeft += SPEED * dt

        const setWidth = el.scrollWidth / 3
        if (el.scrollLeft >= setWidth) {
          el.scrollLeft -= setWidth
        }
      }

      rafId = requestAnimationFrame(tick)
    }

    rafId = requestAnimationFrame(tick)

    // ── IntersectionObserver ──────────────────────────────
    const observer = new IntersectionObserver(
      ([entry]) => {
        isIntersecting.current = entry.isIntersecting
        viewPausedRef.current  = document.hidden || !entry.isIntersecting
      },
      { threshold: 0.1 },
    )
    observer.observe(el)

    // ── Visibility change ─────────────────────────────────
    function onVisibility() {
      viewPausedRef.current = document.hidden || !isIntersecting.current
    }
    document.addEventListener('visibilitychange', onVisibility)

    return () => {
      cancelAnimationFrame(rafId)
      observer.disconnect()
      document.removeEventListener('visibilitychange', onVisibility)
    }
  }, [])

  return (
    <div className="flex flex-col gap-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-bold text-white/80">🎬 Filmography</h2>
        {totalCount > 0 && (
          <a href="#full-filmography" className="text-xs text-white/35 hover:text-white/60 transition-colors">
            View all {totalCount} films ↓
          </a>
        )}
      </div>

      <div
        style={{
          maskImage:       'linear-gradient(to right, transparent 0%, black 6%, black 94%, transparent 100%)',
          WebkitMaskImage: 'linear-gradient(to right, transparent 0%, black 6%, black 94%, transparent 100%)',
        }}
      >
        <style>{`.fp-strip::-webkit-scrollbar { display: none; }`}</style>
        <div
          ref={scrollRef}
          className="fp-strip overflow-x-auto"
          aria-live="off"
          style={{ scrollbarWidth: 'none', msOverflowStyle: 'none' }}
          onMouseEnter={() => { hoverPausedRef.current = true  }}
          onMouseLeave={() => { hoverPausedRef.current = false }}
          onFocus={()     => { hoverPausedRef.current = true  }}
          onBlur={()      => { hoverPausedRef.current = false }}
        >
          <div className="flex gap-3 pb-2" style={{ width: 'max-content' }}>
            {[0, 1, 2].map(set => (
              <React.Fragment key={set}>
                {sorted.map((movie, i) => (
                  <FilmCard key={`${set}-${movie.title}-${i}`} movie={movie} />
                ))}
                {/* Loop seam — subtle dot divider */}
                <div className="flex-shrink-0 flex items-center justify-center" style={{ width: 32 }}>
                  <div className="flex flex-col gap-1.5">
                    <div className="w-1 h-1 rounded-full bg-white/10" />
                    <div className="w-1 h-1 rounded-full bg-white/10" />
                    <div className="w-1 h-1 rounded-full bg-white/10" />
                  </div>
                </div>
              </React.Fragment>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}

function FilmCard({ movie }: { movie: ActorMovie }) {
  const rating =
    movie.vote_average != null && movie.vote_average > 0
      ? movie.vote_average.toFixed(1)
      : null
  const yearLabel = movie.release_year > 0 ? movie.release_year : 'TBA'

  return (
    <div
      className="flex-shrink-0 group hover:scale-[1.05] transition-transform duration-200 cursor-default"
      style={{ width: 100 }}
    >
      <div
        className="relative rounded-xl overflow-hidden bg-[#1a1a24] shadow-sm group-hover:shadow-lg transition-shadow duration-200"
        style={{ aspectRatio: '2/3' }}
      >
        {movie.poster_url ? (
          <Image src={movie.poster_url} alt={movie.title} fill sizes="100px" className="object-cover" />
        ) : (
          <div className="w-full h-full flex items-end justify-center pb-3 px-2">
            <p className="text-white/20 text-[10px] text-center leading-snug line-clamp-3">{movie.title}</p>
          </div>
        )}

        <div className="absolute bottom-0 inset-x-0 bg-gradient-to-t from-black/80 to-transparent pt-6 pb-1.5 px-2">
          <p className="text-white/60 text-[10px] font-medium">{yearLabel}</p>
        </div>

        {rating && (
          <div className="absolute top-1.5 right-1.5 bg-black/70 backdrop-blur-sm rounded-full px-1.5 py-0.5 text-[9px] font-bold text-yellow-400">
            ★ {rating}
          </div>
        )}
      </div>

      <p className="text-white/50 text-[10px] leading-snug line-clamp-2 mt-1.5 px-0.5">{movie.title}</p>
    </div>
  )
}
