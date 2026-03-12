import Image from 'next/image'
import type { ActorMovie } from '@/lib/api'
import MissingData from '@/components/MissingData'

interface FilmographyGridProps {
  movies: ActorMovie[]
}

export default function FilmographyGrid({ movies }: FilmographyGridProps) {
  if (!movies.length) {
    return <MissingData type="cast" />
  }

  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-5 gap-4">
      {movies.map((movie, i) => (
        <MovieCard key={`${movie.title}-${i}`} movie={movie} />
      ))}
    </div>
  )
}

function MovieCard({ movie }: { movie: ActorMovie }) {
  const hasRating =
    movie.vote_average !== null &&
    movie.vote_average !== undefined &&
    movie.vote_average > 0

  const rating = hasRating ? movie.vote_average!.toFixed(1) : null

  // Pre-1980 films get a sepia/vintage poster placeholder
  // release_year is typed as number; 0 means "unknown/TBA"
  const isVintage = movie.release_year > 0 && movie.release_year < 1980

  // Year display — show placeholder when year is 0 (unknown/announced)
  const yearLabel = movie.release_year > 0 ? movie.release_year : null

  return (
    <div
      className="
        group flex flex-col gap-2
        hover:scale-[1.03] transition-transform duration-200
        cursor-default
      "
    >
      {/* ── Poster ──────────────────────────────────────────── */}
      <div className="relative aspect-[2/3] rounded-xl overflow-hidden bg-[#1a1a24]">
        {movie.poster_url ? (
          <Image
            src={movie.poster_url}
            alt={movie.title}
            fill
            sizes="(max-width: 640px) 50vw, (max-width: 768px) 33vw, 20vw"
            className="object-cover"
          />
        ) : isVintage ? (
          <MissingData type="poster_old" title={movie.title} />
        ) : (
          <MissingData type="poster" title={movie.title} />
        )}

        {/* Rating badge — only when a real rating exists */}
        {rating && (
          <div className="absolute top-2 right-2 bg-black/70 backdrop-blur-sm rounded-full px-2 py-0.5 text-xs font-semibold text-yellow-400">
            ★ {rating}
          </div>
        )}
      </div>

      {/* ── Title + year + rating ────────────────────────────── */}
      <div className="flex flex-col gap-0.5 px-0.5">
        <span className="text-sm font-medium text-white/90 leading-snug line-clamp-2">
          {movie.title}
        </span>
        {yearLabel ? (
          <span className="text-xs text-white/40">{yearLabel}</span>
        ) : (
          <span className="text-xs text-white/40">
            Coming Soon
            <span className="block text-[10px] text-white/20 italic">
              Release date still under wraps.
            </span>
          </span>
        )}
        {!hasRating && <MissingData type="rating" />}
      </div>
    </div>
  )
}
