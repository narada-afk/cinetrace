'use client'

import { useState, useRef } from 'react'
import Image from 'next/image'
import ScrollRow from './ScrollRow'
import type { DirectorCollab, ActorMovie } from '@/lib/api'

interface DirectorsSectionProps {
  directors: DirectorCollab[]
  movies: ActorMovie[]
}

export default function DirectorsSection({ directors, movies }: DirectorsSectionProps) {
  const [selected, setSelected] = useState<string | null>(null)
  const cache = useRef<Record<string, ActorMovie[]>>({})

  const topDirs = directors.slice(0, 20)

  function handleChip(director: string) {
    if (selected === director) {
      setSelected(null)
      return
    }
    // Build cache on first click
    if (!cache.current[director]) {
      cache.current[director] = movies
        .filter(m => m.director === director && m.release_year > 0)
        .sort((a, b) => (b.release_year ?? 0) - (a.release_year ?? 0))
    }
    setSelected(director)
  }

  const directorMovies = selected ? (cache.current[selected] ?? []) : []
  const isOpen = selected !== null

  return (
    <div className="flex flex-col gap-4">
      <h2 className="text-lg font-bold text-white/80">🎬 Directors Worked With</h2>

      {/* Chips */}
      <div className="flex flex-wrap gap-2">
        {topDirs.map(d => {
          const active = selected === d.director
          return (
            <button
              key={d.director}
              onClick={() => handleChip(d.director)}
              className={`
                inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-sm
                border transition-all duration-200 cursor-pointer
                ${active
                  ? 'border-violet-500/60 text-white/90 bg-violet-500/15'
                  : 'border-white/[0.08] text-white/60 hover:text-white/80 hover:border-white/20'
                }
              `}
              style={{ background: active ? undefined : '#13131a' }}
            >
              {d.director}
              <span className={`text-xs font-medium ml-0.5 ${active ? 'text-violet-300/70' : 'text-white/25'}`}>
                {d.films}
              </span>
            </button>
          )
        })}
      </div>

      {/* Expandable panel */}
      <div
        style={{
          display: 'grid',
          gridTemplateRows: isOpen ? '1fr' : '0fr',
          transition: 'grid-template-rows 280ms cubic-bezier(0.4, 0, 0.2, 1)',
        }}
      >
        <div style={{ overflow: 'hidden' }}>
          <div
            style={{
              opacity: isOpen ? 1 : 0,
              transform: isOpen ? 'translateY(0)' : 'translateY(-6px)',
              transition: 'opacity 220ms ease, transform 220ms ease',
            }}
          >
            {/* Panel */}
            <div
              className="rounded-2xl border border-white/[0.07] overflow-hidden mt-1"
              style={{ background: '#0d0d15' }}
            >
              {/* Header */}
              {selected && (
                <div className="px-5 pt-4 pb-3 border-b border-white/[0.05] flex items-center justify-between">
                  <span className="text-xs font-semibold text-white/40 uppercase tracking-widest">
                    {selected}
                  </span>
                  <span className="text-xs text-white/25">
                    {directorMovies.length} {directorMovies.length === 1 ? 'film' : 'films'}
                  </span>
                </div>
              )}

              {/* Movie scroll */}
              <div className="px-4 py-4">
                {directorMovies.length === 0 ? (
                  <p className="text-white/25 text-sm py-2 px-1">No films found</p>
                ) : (
                  <ScrollRow>
                    <div className="flex gap-3 pb-1 px-1" style={{ width: 'max-content' }}>
                      {directorMovies.map(movie => (
                        <MovieCard key={`${movie.title}-${movie.release_year}`} movie={movie} />
                      ))}
                    </div>
                  </ScrollRow>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

function MovieCard({ movie }: { movie: ActorMovie }) {
  const rating = movie.vote_average && movie.vote_average > 0
    ? movie.vote_average.toFixed(1)
    : null

  return (
    <div
      className="flex-shrink-0 group cursor-default"
      style={{ width: 90 }}
    >
      <div
        className="relative rounded-xl overflow-hidden bg-white/[0.04] shadow-sm group-hover:shadow-lg transition-all duration-200 group-hover:scale-[1.04]"
        style={{ aspectRatio: '2/3' }}
      >
        {movie.poster_url ? (
          <Image
            src={movie.poster_url}
            alt={movie.title}
            fill
            sizes="90px"
            className="object-cover"
          />
        ) : (
          <div className="w-full h-full flex items-end justify-center pb-3 px-2">
            <p className="text-white/20 text-[9px] text-center leading-snug line-clamp-3">
              {movie.title}
            </p>
          </div>
        )}

        {/* Year badge */}
        <div className="absolute bottom-0 inset-x-0 bg-gradient-to-t from-black/80 to-transparent pt-5 pb-1.5 px-2">
          <p className="text-white/55 text-[10px] font-medium">{movie.release_year}</p>
        </div>

        {/* Rating badge */}
        {rating && (
          <div className="absolute top-1.5 right-1.5 bg-black/70 backdrop-blur-sm rounded-full px-1.5 py-0.5 text-[9px] font-bold text-yellow-400">
            ★ {rating}
          </div>
        )}
      </div>

      <p className="text-white/45 text-[10px] leading-snug line-clamp-2 mt-1.5 px-0.5">
        {movie.title}
      </p>
    </div>
  )
}
