'use client'

import { useState } from 'react'
import Image from 'next/image'
import type { ActorMovie } from '@/lib/api'
import MissingData from './MissingData'

const INITIAL_SHOW = 12

export default function FullFilmography({ movies }: { movies: ActorMovie[] }) {
  const [showAll, setShowAll] = useState(false)
  const displayed = showAll ? movies : movies.slice(0, INITIAL_SHOW)

  if (movies.length === 0) return null

  return (
    <div id="full-filmography" className="flex flex-col gap-4 scroll-mt-8">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-bold text-white/80">📽 All Films</h2>
        <span className="text-white/30 text-xs">{movies.length} films</span>
      </div>

      {/* Grid */}
      <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-5 lg:grid-cols-6 gap-4">
        {displayed.map((movie, i) => (
          <MovieCard key={`${movie.title}-${i}`} movie={movie} />
        ))}
      </div>

      {/* Expand / collapse toggle */}
      {movies.length > INITIAL_SHOW && (
        <div className="flex justify-center mt-2">
          <button
            onClick={() => setShowAll(v => !v)}
            className="px-6 py-2.5 rounded-full text-xs font-semibold border border-white/[0.12] text-white/50 hover:text-white/80 hover:border-white/25 transition-all"
            style={{ background: '#13131a' }}
          >
            {showAll ? 'Show less ↑' : `Show all ${movies.length} films ↓`}
          </button>
        </div>
      )}
    </div>
  )
}

function MovieCard({ movie }: { movie: ActorMovie }) {
  const hasRating = movie.vote_average != null && movie.vote_average > 0
  const rating    = hasRating ? movie.vote_average!.toFixed(1) : null
  const isVintage = movie.release_year > 0 && movie.release_year < 1980
  const yearLabel = movie.release_year > 0 ? movie.release_year : null

  return (
    <div className="group flex flex-col gap-2 hover:scale-[1.03] transition-transform duration-200 cursor-default">
      {/* Poster */}
      <div className="relative aspect-[2/3] rounded-xl overflow-hidden bg-[#1a1a24]">
        {movie.poster_url ? (
          <Image
            src={movie.poster_url}
            alt={movie.title}
            fill
            sizes="(max-width: 640px) 50vw, (max-width: 768px) 33vw, (max-width: 1024px) 20vw, 17vw"
            className="object-cover"
          />
        ) : isVintage ? (
          <MissingData type="poster_old" title={movie.title} />
        ) : (
          <MissingData type="poster" title={movie.title} />
        )}

        {rating && (
          <div className="absolute top-2 right-2 bg-black/70 backdrop-blur-sm rounded-full px-2 py-0.5 text-xs font-semibold text-yellow-400">
            ★ {rating}
          </div>
        )}
      </div>

      {/* Title + year */}
      <div className="flex flex-col gap-0.5 px-0.5">
        <span className="text-sm font-medium text-white/90 leading-snug line-clamp-2">
          {movie.title}
        </span>
        {yearLabel ? (
          <span className="text-xs text-white/40">{yearLabel}</span>
        ) : (
          <span className="text-xs text-white/30 italic">Coming soon</span>
        )}
      </div>
    </div>
  )
}
