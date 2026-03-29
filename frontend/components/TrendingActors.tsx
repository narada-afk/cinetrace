'use client'

import Link from 'next/link'
import ActorAvatar from './ActorAvatar'
import { toActorSlug } from '@/lib/api'

export interface TrendingActor {
  id: number
  name: string
  avatarSlug?: string
  movies?: number
}

interface TrendingActorsProps {
  actors: TrendingActor[]
  /** Section heading — defaults to "Trending Actors" */
  title?: string
}

export default function TrendingActors({ actors, title = 'Trending Actors' }: TrendingActorsProps) {
  const valid = actors.filter((a) => !!a.name)
  if (!valid.length) return null

  return (
    <section className="w-full max-w-[1200px] mx-auto px-6 mt-10">
      <h2 className="text-xs font-semibold uppercase tracking-widest text-white/40 mb-4">
        {title}
      </h2>
      <div className="flex items-start gap-6 overflow-x-auto pb-2 scrollbar-hide">
        {valid.map((actor) => (
          <Link
            key={actor.id}
            href={`/actors/${toActorSlug(actor.name)}`}
            className="flex flex-col items-center gap-2 flex-shrink-0 group"
          >
            <div className="ring-2 ring-white/10 group-hover:ring-white/30 rounded-full transition-all duration-200">
              <ActorAvatar name={actor.name} avatarSlug={actor.avatarSlug} size={56} />
            </div>
            <span className="text-xs text-white/60 group-hover:text-white/90 transition-colors text-center max-w-[72px] leading-snug">
              {actor.name}
            </span>
          </Link>
        ))}
      </div>
    </section>
  )
}
