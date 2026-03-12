import ActorAvatar from './ActorAvatar'
import type { ActorProfile } from '@/lib/api'

interface ActorHeroProps {
  actor: ActorProfile
  collaboratorCount: number
  directorCount: number
}

export default function ActorHero({
  actor,
  collaboratorCount,
  directorCount,
}: ActorHeroProps) {
  const yearRange =
    actor.first_film_year && actor.last_film_year
      ? `${actor.first_film_year} – ${actor.last_film_year}`
      : null

  return (
    <div className="relative w-full overflow-hidden rounded-2xl">
      {/* Layered gradient background */}
      <div className="absolute inset-0 bg-gradient-to-br from-[#1a1a2e] via-[#16213e] to-[#0a0a0f]" />
      <div className="absolute inset-0 bg-gradient-to-t from-[#0a0a0f]/80 via-transparent to-transparent" />

      <div className="relative z-10 px-8 py-10 flex flex-col sm:flex-row items-center sm:items-end gap-6">
        {/* Avatar — large */}
        <div className="flex-shrink-0 ring-4 ring-white/10 rounded-full">
          <ActorAvatar name={actor.name} size={96} />
        </div>

        {/* Text info */}
        <div className="flex flex-col gap-2 text-center sm:text-left">
          {/* Industry + year badge row */}
          <div className="flex items-center gap-2 justify-center sm:justify-start">
            <span className="text-xs font-semibold uppercase tracking-widest text-white/50 px-3 py-1 rounded-full glass">
              {actor.industry}
            </span>
            {yearRange && (
              <span className="text-xs text-white/30">{yearRange}</span>
            )}
          </div>

          {/* Name */}
          <h1 className="text-4xl sm:text-5xl font-bold text-white tracking-tight leading-none">
            {actor.name}
          </h1>

          {/* Quick stats line */}
          <div className="flex items-center gap-2 text-sm justify-center sm:justify-start flex-wrap mt-1">
            <HeroStat value={actor.film_count} label="films" />
            <span className="text-white/20">•</span>
            <HeroStat value={collaboratorCount} label="co-actors" />
            <span className="text-white/20">•</span>
            <HeroStat value={directorCount} label="directors" />
          </div>
        </div>
      </div>
    </div>
  )
}

function HeroStat({ value, label }: { value: number; label: string }) {
  return (
    <span>
      <strong className="text-white font-semibold">{value}</strong>
      <span className="text-white/40"> {label}</span>
    </span>
  )
}
