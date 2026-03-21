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

const CARD_BG: Record<InsightCardData['gradient'], string> = {
  red:    '#130507',
  purple: '#0b0613',
  orange: '#130904',
  blue:   '#030f19',
  green:  '#031308',
}

const ACCENT: Record<InsightCardData['gradient'], string> = {
  red:    '#f87171',
  purple: '#c084fc',
  orange: '#fb923c',
  blue:   '#60a5fa',
  green:  '#4ade80',
}

const GLOW: Record<InsightCardData['gradient'], string> = {
  red:    'rgba(239,68,68,0.18)',
  purple: 'rgba(168,85,247,0.18)',
  orange: 'rgba(249,115,22,0.18)',
  blue:   'rgba(59,130,246,0.18)',
  green:  'rgba(34,197,94,0.18)',
}

export default function InsightCard({
  emoji,
  label,
  headline,
  stat,
  subtext,
  actors = [],
  gradient,
  href = '#',
}: InsightCardData) {
  const accentColor = ACCENT[gradient]
  const bgColor     = CARD_BG[gradient]
  const glowColor   = GLOW[gradient]

  const singleActor = actors.length === 1
  const multiActor  = actors.length >= 2

  return (
    <Link href={href} className="block h-full">
      <div
        className="relative rounded-2xl overflow-hidden h-[168px] flex cursor-pointer
                   hover:scale-[1.02] hover:brightness-110 transition-all duration-200
                   border border-white/5"
        style={{ background: bgColor }}
      >
        {/* Left radial glow — colour bleed from the accent */}
        <div
          className="absolute inset-0 pointer-events-none"
          style={{
            background: `radial-gradient(ellipse 90% 90% at 0% 50%, ${glowColor}, transparent 70%)`,
          }}
        />

        {/* ── LEFT: text content ───────────────────────────── */}
        <div className="relative z-10 flex flex-col justify-between p-5 flex-1 min-w-0 pr-2">

          {/* Label */}
          <span
            className="text-[10px] font-bold uppercase tracking-widest"
            style={{ color: accentColor }}
          >
            {emoji}&nbsp;&nbsp;{label}
          </span>

          {/* Big stat — primary visual focus */}
          <div className="text-[2.75rem] font-black text-white leading-none tracking-tight">
            {stat}
          </div>

          {/* Headline + subtext */}
          <div className="min-w-0">
            <p className="text-[11px] text-white/60 leading-snug line-clamp-2">
              {headline}
            </p>
            {subtext && (
              <p className="text-[10px] mt-0.5" style={{ color: accentColor + '99' }}>
                {subtext}
              </p>
            )}
          </div>
        </div>

        {/* ── RIGHT: actor portrait(s) ─────────────────────── */}
        {actors.length > 0 && (
          <div className="relative flex-shrink-0 flex items-center self-stretch">

            {/* Single actor — large portrait bleeding off bottom-right */}
            {singleActor && (
              <div className="relative self-end mb-[-20px] mr-[-16px]">
                {/* Glow halo behind avatar */}
                <div
                  className="absolute inset-[-8px] rounded-full blur-2xl"
                  style={{ background: glowColor }}
                />
                <div className="relative ring-2 ring-white/10 rounded-full">
                  <ActorAvatar
                    name={actors[0].name}
                    avatarSlug={actors[0].avatarSlug}
                    size={130}
                  />
                </div>
              </div>
            )}

            {/* Two actors — overlapping circles, football-card style */}
            {multiActor && (
              <div className="flex items-center pr-5">
                {actors.slice(0, 2).map((actor, i) => (
                  <div
                    key={actor.name}
                    className="relative rounded-full"
                    style={{
                      marginLeft: i === 0 ? 0 : -30,
                      zIndex: actors.length - i,
                    }}
                  >
                    {/* Glow behind each avatar */}
                    <div
                      className="absolute inset-0 rounded-full blur-lg"
                      style={{ background: glowColor, opacity: 0.6 }}
                    />
                    <div
                      className="relative rounded-full ring-2"
                      style={{ boxShadow: '-4px 0 14px rgba(0,0,0,0.7)', ringColor: 'rgba(0,0,0,0.5)' }}
                    >
                      <ActorAvatar
                        name={actor.name}
                        avatarSlug={actor.avatarSlug}
                        size={88}
                      />
                    </div>
                  </div>
                ))}
              </div>
            )}

          </div>
        )}
      </div>
    </Link>
  )
}
