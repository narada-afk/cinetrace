'use client'

import Link from 'next/link'
import ActorAvatar from './ActorAvatar'

export interface InsightCardData {
  emoji: string
  label: string
  headline: string
  stat: string | number
  subtext?: string
  actors?: Array<{ name: string; avatarSlug?: string }>
  gradient: 'red' | 'purple' | 'orange' | 'blue' | 'green'
  href?: string
}

const GRADIENTS: Record<InsightCardData['gradient'], string> = {
  red: 'from-red-700/80 to-red-900/60',
  purple: 'from-purple-700/80 to-purple-900/60',
  orange: 'from-orange-600/80 to-orange-900/60',
  blue: 'from-blue-700/80 to-blue-900/60',
  green: 'from-green-700/80 to-green-900/60',
}

const BORDER_COLORS: Record<InsightCardData['gradient'], string> = {
  red: 'border-red-500/20',
  purple: 'border-purple-500/20',
  orange: 'border-orange-500/20',
  blue: 'border-blue-500/20',
  green: 'border-green-500/20',
}

export default function InsightCard({
  emoji,
  label,
  headline,
  stat,
  subtext,
  actors,
  gradient,
  href = '#',
}: InsightCardData) {
  return (
    <Link href={href} className="block">
      <div
        className={`
          relative rounded-2xl p-6 flex flex-col gap-3 overflow-hidden h-full
          bg-gradient-to-br ${GRADIENTS[gradient]}
          border ${BORDER_COLORS[gradient]}
          backdrop-blur-sm
          hover:scale-[1.02] hover:shadow-xl transition-all duration-200
          cursor-pointer
        `}
      >
        {/* Background texture */}
        <div className="absolute inset-0 bg-[#0a0a0f]/30 pointer-events-none" />

        <div className="relative z-10 flex flex-col gap-3 h-full">
          {/* Label row */}
          <div className="flex items-center gap-2">
            <span className="text-xl">{emoji}</span>
            <span className="text-xs font-semibold uppercase tracking-widest text-white/50">
              {label}
            </span>
          </div>

          {/* BIG STAT NUMBER — primary focus */}
          <div className="text-4xl font-bold text-white leading-none">
            {stat}
          </div>

          {/* Supporting text */}
          <p className="text-sm text-white/80 leading-snug">
            {headline}
          </p>

          {/* Subtext */}
          {subtext && (
            <p className="text-xs text-white/40">{subtext}</p>
          )}

          {/* Avatars row */}
          {actors && actors.length > 0 && (
            <div className="flex items-center gap-2 mt-auto pt-2">
              <div className="flex -space-x-2">
                {actors.map((actor) => (
                  <ActorAvatar
                    key={actor.name}
                    name={actor.name}
                    avatarSlug={actor.avatarSlug}
                    size={32}
                  />
                ))}
              </div>
              <span className="text-xs text-white/40 ml-1">
                {actors.map((a) => a.name).join(' + ')}
              </span>
            </div>
          )}
        </div>
      </div>
    </Link>
  )
}
