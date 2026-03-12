'use client'
/**
 * FilmGrid — actor filmography grid for the comparison page.
 *
 * Features:
 *  • Actor color identity (header + accent bar + rating badge + top-rated glow)
 *  • Films sorted by rating descending
 *  • Posters link to TMDB in a new tab
 *  • Hover: poster scales 1→1.05, overlay shows rating / year / director
 *  • Top Rated film gets accent-colored glow + ⭐ badge
 *  • Staggered entry animation on scroll (IntersectionObserver)
 *  • Badges: ⭐ Top Rated · 🔥 Popular · 🆕 Latest
 */

import { useRef, useEffect, useState } from 'react'
import Image from 'next/image'
import MissingData from '@/components/MissingData'
import type { ActorMovie } from '@/lib/api'

// ── Badge ─────────────────────────────────────────────────────────────────────

function FilmBadge({ emoji, label }: { emoji: string; label: string }) {
  return (
    <span className="absolute top-1.5 left-1.5 z-10 flex items-center gap-0.5
      bg-black/75 backdrop-blur-sm rounded-full px-1.5 py-0.5
      text-[9px] font-bold text-white pointer-events-none">
      {emoji} {label}
    </span>
  )
}

// ── Props ─────────────────────────────────────────────────────────────────────

interface FilmGridProps {
  actorName:         string
  movies:            ActorMovie[]
  accentColor:       string        // '#f59e0b' (amber) | '#06b6d4' (cyan)
  highlightedRatedId: string | null
  highlightedPopId:  string | null
  latestId:          string | null
}

// ── Component ─────────────────────────────────────────────────────────────────

export default function FilmGrid({
  actorName,
  movies,
  accentColor,
  highlightedRatedId,
  highlightedPopId,
  latestId,
}: FilmGridProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [visible,    setVisible]    = useState(false)
  const [hoveredKey, setHoveredKey] = useState<string | null>(null)

  // Scroll-triggered entry animation — fires once
  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    const observer = new IntersectionObserver(
      ([entry]) => { if (entry.isIntersecting) { setVisible(true); observer.disconnect() } },
      { threshold: 0.1 },
    )
    observer.observe(el)
    return () => observer.disconnect()
  }, [])

  if (!movies.length) return <MissingData type="cast" />

  // Display order: highest rated first
  const sorted = [...movies].sort((a, b) => (b.vote_average ?? 0) - (a.vote_average ?? 0))

  return (
    <div ref={containerRef} className="flex flex-col gap-4">

      {/* ── Actor header with color identity ──────────────────────── */}
      <div className="flex flex-col gap-1.5">
        <span
          className="text-xs font-bold uppercase tracking-[0.18em]"
          style={{ color: accentColor }}
        >
          {actorName}
        </span>
        {/* Accent underline */}
        <div
          className="h-[2px] w-10 rounded-full"
          style={{ background: accentColor, opacity: 0.65 }}
        />
      </div>

      {/* ── Poster grid ───────────────────────────────────────────── */}
      <div className="grid grid-cols-3 gap-3">
        {sorted.map((movie, i) => {
          const key        = `${movie.title}-${movie.release_year}`
          const uid        = `${key}-${i}`
          const isTopRated = key === highlightedRatedId
          const isMostPop  = key === highlightedPopId
          const isLatest   = key === latestId
          const hasRating  = (movie.vote_average ?? 0) > 0
          const isVintage  = movie.release_year > 0 && movie.release_year < 1980
          const tmdbUrl    = movie.tmdb_id
            ? `https://www.themoviedb.org/movie/${movie.tmdb_id}`
            : null
          const isHovered  = hoveredKey === uid
          const stagger    = i * 55   // ms between each poster

          const posterInner = (
            // Aspect-ratio box + rounded corners + overflow clip
            <div
              className="relative aspect-[2/3] rounded-xl overflow-hidden bg-[#1a1a24]"
              style={{
                // Top-rated glow in actor's colour
                boxShadow: isTopRated
                  ? `0 0 0 1.5px ${accentColor}55, 0 0 18px ${accentColor}40`
                  : 'none',
              }}
            >
              {/* Poster image */}
              {movie.poster_url ? (
                <Image
                  src={movie.poster_url}
                  alt={movie.title}
                  fill
                  sizes="(max-width: 768px) 33vw, 15vw"
                  className="object-cover"
                />
              ) : isVintage ? (
                <MissingData type="poster_old" title={movie.title} />
              ) : (
                <MissingData type="poster" title={movie.title} />
              )}

              {/* Rating badge — accent-coloured */}
              {hasRating && (
                <div
                  className="absolute top-1.5 right-1.5 z-10 rounded-full px-1.5 py-0.5
                    backdrop-blur-sm text-[10px] font-bold pointer-events-none"
                  style={{ background: 'rgba(0,0,0,0.75)', color: accentColor }}
                >
                  ★ {movie.vote_average!.toFixed(1)}
                </div>
              )}

              {/* Context badges (top-left) */}
              {isTopRated                           && <FilmBadge emoji="⭐" label="Top Rated" />}
              {!isTopRated && isMostPop             && <FilmBadge emoji="🔥" label="Popular"   />}
              {!isTopRated && !isMostPop && isLatest && <FilmBadge emoji="🆕" label="Latest"    />}

              {/* Hover info overlay */}
              <div
                className="absolute inset-0 z-20 flex flex-col justify-end p-2 pointer-events-none"
                style={{
                  background:  'linear-gradient(to top, rgba(0,0,0,0.85) 0%, rgba(0,0,0,0.35) 55%, transparent 100%)',
                  opacity:     isHovered ? 1 : 0,
                  transition:  'opacity 150ms ease',
                }}
              >
                <div className="flex flex-col gap-0.5">
                  {hasRating && (
                    <span className="text-[10px] font-bold" style={{ color: accentColor }}>
                      ★ {movie.vote_average!.toFixed(1)}
                    </span>
                  )}
                  {movie.release_year > 0 && (
                    <span className="text-[10px] text-white/75">📅 {movie.release_year}</span>
                  )}
                  {movie.director && (
                    <span className="text-[10px] text-white/75 truncate">🎬 {movie.director}</span>
                  )}
                </div>
              </div>
            </div>
          )

          return (
            // ── Entry animation wrapper ──────────────────────────────
            <div
              key={uid}
              style={{
                opacity:    visible ? 1 : 0,
                transform:  visible ? 'scale(1)' : 'scale(0.95)',
                transition: `opacity 300ms ease-out ${stagger}ms, transform 300ms ease-out ${stagger}ms`,
              }}
            >
              {/* ── Hover + scale + TMDB link wrapper ───────────────── */}
              <div
                className="flex flex-col gap-1.5"
                onMouseEnter={() => setHoveredKey(uid)}
                onMouseLeave={() => setHoveredKey(null)}
                style={{
                  transform:  isHovered ? 'scale(1.05)' : 'scale(1)',
                  transition: 'transform 200ms ease',
                  cursor:     tmdbUrl ? 'pointer' : 'default',
                }}
              >
                {/* Poster — wrapped in <a> when TMDB id is available */}
                {tmdbUrl ? (
                  <a
                    href={tmdbUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="block"
                    tabIndex={0}
                    aria-label={`Open ${movie.title} on TMDB`}
                    // Prevent hover scale from being double-counted on the <a> itself
                    onClick={(e) => e.stopPropagation()}
                  >
                    {posterInner}
                  </a>
                ) : (
                  posterInner
                )}

                {/* Title + year below poster */}
                <div className="flex flex-col gap-0.5 px-0.5">
                  <span className="text-xs font-medium text-white/80 leading-snug line-clamp-2">
                    {movie.title}
                  </span>
                  <span className="text-[10px] text-white/35">
                    {movie.release_year > 0 ? movie.release_year : 'Coming Soon'}
                  </span>
                </div>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
