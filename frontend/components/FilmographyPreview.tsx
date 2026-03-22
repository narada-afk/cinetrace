import Image from 'next/image'
import type { ActorMovie } from '@/lib/api'

const PREVIEW_COUNT = 12

interface FilmographyPreviewProps {
  movies: ActorMovie[]
  totalCount: number
}

export default function FilmographyPreview({ movies, totalCount }: FilmographyPreviewProps) {
  const preview = movies.slice(0, PREVIEW_COUNT)
  if (preview.length === 0) return null

  return (
    <div className="flex flex-col gap-4">
      {/* Section header */}
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-bold text-white/80">🎬 Filmography</h2>
        {totalCount > PREVIEW_COUNT && (
          <a
            href="#full-filmography"
            className="text-xs text-white/35 hover:text-white/60 transition-colors"
          >
            View all {totalCount} films ↓
          </a>
        )}
      </div>

      {/* Horizontal scroll strip */}
      <>
        <style>{`.fp-strip::-webkit-scrollbar { display: none; }`}</style>
        <div
          className="fp-strip overflow-x-auto"
          style={{ scrollbarWidth: 'none', msOverflowStyle: 'none' }}
        >
          <div className="flex gap-3 pb-2" style={{ width: 'max-content' }}>
            {preview.map((movie, i) => (
              <FilmCard key={`${movie.title}-${i}`} movie={movie} />
            ))}

            {/* View more stub */}
            {totalCount > PREVIEW_COUNT && (
              <a
                href="#full-filmography"
                className="flex-shrink-0 flex flex-col items-center justify-center rounded-xl border border-white/[0.08] text-white/30 hover:text-white/60 hover:border-white/20 transition-all gap-1"
                style={{ width: 100, aspectRatio: '2/3', background: '#0d0d15' }}
              >
                <span className="text-2xl">+</span>
                <span className="text-xs font-medium">{totalCount - PREVIEW_COUNT} more</span>
              </a>
            )}
          </div>
        </div>
      </>
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
      className="flex-shrink-0 group hover:scale-[1.04] transition-transform duration-200 cursor-default"
      style={{ width: 100 }}
    >
      {/* Poster */}
      <div
        className="relative rounded-xl overflow-hidden bg-[#1a1a24]"
        style={{ aspectRatio: '2/3' }}
      >
        {movie.poster_url ? (
          <Image
            src={movie.poster_url}
            alt={movie.title}
            fill
            sizes="100px"
            className="object-cover"
          />
        ) : (
          <div className="w-full h-full flex items-end justify-center pb-3 px-2">
            <p className="text-white/20 text-[10px] text-center leading-snug line-clamp-3">
              {movie.title}
            </p>
          </div>
        )}

        {/* Year overlay at bottom */}
        <div className="absolute bottom-0 inset-x-0 bg-gradient-to-t from-black/80 to-transparent pt-6 pb-1.5 px-2">
          <p className="text-white/60 text-[10px] font-medium">{yearLabel}</p>
        </div>

        {/* Rating badge */}
        {rating && (
          <div className="absolute top-1.5 right-1.5 bg-black/70 backdrop-blur-sm rounded-full px-1.5 py-0.5 text-[9px] font-bold text-yellow-400">
            ★ {rating}
          </div>
        )}
      </div>

      {/* Title below */}
      <p className="text-white/50 text-[10px] leading-snug line-clamp-2 mt-1.5 px-0.5">
        {movie.title}
      </p>
    </div>
  )
}
