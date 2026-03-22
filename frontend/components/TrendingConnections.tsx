import Link from 'next/link'
import ActorAvatar from './ActorAvatar'

export interface CollabPairData {
  actor1: string
  actor2: string
  films: number
  href: string
}

export default function TrendingConnections({ pairs }: { pairs: CollabPairData[] }) {
  if (!pairs.length) return null

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
      {pairs.map((pair, i) => (
        <Link
          key={i}
          href={pair.href}
          className="
            flex items-center gap-4 px-5 py-4 rounded-2xl
            border border-white/[0.07] bg-white/[0.03]
            hover:bg-white/[0.07] hover:border-white/[0.13]
            transition-all duration-200 group
          "
        >
          {/* Overlapping avatars */}
          <div className="flex items-center flex-shrink-0">
            <div className="relative z-10 ring-2 ring-[#0a0a0f] rounded-full">
              <ActorAvatar name={pair.actor1} size={44} />
            </div>
            <div className="-ml-3 ring-2 ring-[#0a0a0f] rounded-full">
              <ActorAvatar name={pair.actor2} size={44} />
            </div>
          </div>

          {/* Names + count */}
          <div className="flex-1 min-w-0">
            <p className="text-white text-sm font-semibold truncate">
              {pair.actor1} <span className="text-white/30">+</span> {pair.actor2}
            </p>
            <p className="text-white/40 text-xs mt-0.5">{pair.films} films together</p>
          </div>

          {/* Arrow */}
          <span className="text-white/20 group-hover:text-white/55 transition-colors flex-shrink-0">
            →
          </span>
        </Link>
      ))}
    </div>
  )
}
