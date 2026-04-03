'use client'

import { useState } from 'react'
import Link from 'next/link'
import Image from 'next/image'
import ActorAvatar from './ActorAvatar'

export interface InsightCardData {
  emoji: string
  label: string
  headline: string
  stat: string | number
  subtext?: string
  actors?: Array<{ name: string; avatarSlug?: string }>
  gradient: 'red' | 'purple' | 'orange' | 'blue' | 'green' | 'amber'
  href?: string
}

const CARD_BG: Record<InsightCardData['gradient'], string> = {
  red:    '#130507',
  purple: '#0b0613',
  orange: '#130904',
  blue:   '#030f19',
  green:  '#031308',
  amber:  '#130b02',
}

const ACCENT: Record<InsightCardData['gradient'], string> = {
  red:    '#f87171',
  purple: '#c084fc',
  orange: '#fb923c',
  blue:   '#60a5fa',
  green:  '#4ade80',
  amber:  '#fbbf24',
}

const GLOW: Record<InsightCardData['gradient'], string> = {
  red:    'rgba(239,68,68,0.18)',
  purple: 'rgba(168,85,247,0.18)',
  orange: 'rgba(249,115,22,0.18)',
  blue:   'rgba(59,130,246,0.18)',
  green:  'rgba(34,197,94,0.18)',
  amber:  'rgba(251,191,36,0.18)',
}

// Stronger glow for hover state — same hue, higher opacity
const HOVER_SHADOW: Record<InsightCardData['gradient'], string> = {
  red:    '0 0 36px rgba(239,68,68,0.45), 0 8px 28px rgba(0,0,0,0.5)',
  purple: '0 0 36px rgba(168,85,247,0.45), 0 8px 28px rgba(0,0,0,0.5)',
  orange: '0 0 36px rgba(249,115,22,0.45), 0 8px 28px rgba(0,0,0,0.5)',
  blue:   '0 0 36px rgba(59,130,246,0.45), 0 8px 28px rgba(0,0,0,0.5)',
  green:  '0 0 36px rgba(34,197,94,0.45),  0 8px 28px rgba(0,0,0,0.5)',
  amber:  '0 0 36px rgba(251,191,36,0.45), 0 8px 28px rgba(0,0,0,0.5)',
}

/**
 * Split a stat string into a dominant numeral + optional unit label.
 *
 * Examples
 *   "14 films together"  →  { main: "14",        unit: "films together" }
 *   "4 industries"       →  { main: "4",          unit: "industries"     }
 *   "2005–2010"          →  { main: "2005–2010",  unit: null             }
 *   42                   →  { main: "42",          unit: null             }
 */
function splitStat(stat: string | number): { main: string; unit: string | null } {
  const s = String(stat)
  // Year-range strings (e.g. "2005–2010") — keep together as the main value
  if (/^\d{4}[–\-]\d{4}$/.test(s)) return { main: s, unit: null }
  // Plain number — no unit
  if (/^\d+$/.test(s)) return { main: s, unit: null }
  // "14 films together" → main="14", unit="films together"
  const spaceIdx = s.indexOf(' ')
  if (spaceIdx !== -1) return { main: s.slice(0, spaceIdx), unit: s.slice(spaceIdx + 1) }
  return { main: s, unit: null }
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
  const hoverShadow = HOVER_SHADOW[gradient]

  const [copied, setCopied] = useState(false)

  const { main: statMain, unit: statUnit } = splitStat(stat)

  function handleShare(e: React.MouseEvent) {
    e.preventDefault()
    e.stopPropagation()
    const url = typeof window !== 'undefined' ? `${window.location.origin}${href}` : href
    navigator.clipboard.writeText(url).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 1600)
    }).catch(() => {})
  }

  const singleActor = actors.length === 1
  const multiActor  = actors.length >= 2

  return (
    <Link href={href} className="block h-full">
      <div
        className="group relative rounded-2xl overflow-hidden h-[220px] flex cursor-pointer
                   hover:scale-[1.02] hover:brightness-110 transition-all duration-200
                   border border-white/5"
        style={{ background: bgColor }}
        onMouseEnter={e => { (e.currentTarget as HTMLDivElement).style.boxShadow = hoverShadow }}
        onMouseLeave={e => { (e.currentTarget as HTMLDivElement).style.boxShadow = 'none' }}
      >

        {/* Share button — appears on hover */}
        <button
          onClick={handleShare}
          className="absolute top-3 right-3 z-20 w-7 h-7 rounded-full flex items-center justify-center
                     opacity-0 group-hover:opacity-100 transition-opacity duration-200"
          style={{ background: 'rgba(255,255,255,0.10)' }}
          title={copied ? 'Copied!' : 'Share'}
          aria-label="Share"
        >
          {copied ? (
            <span className="text-[10px] text-green-400 font-bold">✓</span>
          ) : (
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"
              style={{ color: 'rgba(255,255,255,0.5)' }}>
              <circle cx="18" cy="5" r="3"/><circle cx="6" cy="12" r="3"/><circle cx="18" cy="19" r="3"/>
              <line x1="8.59" y1="13.51" x2="15.42" y2="17.49"/>
              <line x1="15.41" y1="6.51" x2="8.59" y2="10.49"/>
            </svg>
          )}
        </button>

        {/* Left accent glow — colour bleed from gradient */}
        <div
          className="absolute inset-0 pointer-events-none rounded-2xl"
          style={{
            background: `radial-gradient(ellipse 90% 110% at 0% 50%, ${glowColor}, transparent 70%)`,
          }}
        />


        {/* ── LEFT: text content ──────────────────────────────── */}
        <div
          className="relative z-10 flex flex-col justify-between p-6 pr-4 flex-1 min-w-0"
          style={{ maxWidth: '60%' }}
        >

          {/* Row 1 — Header / hook: dimmed white tag */}
          <span
            className="text-[10px] font-bold uppercase tracking-widest leading-none"
            style={{ color: 'rgba(255,255,255,0.7)' }}
          >
            {label}
          </span>

          {/* Row 2 — Stat: dominant numeral + accent unit */}
          <div>
            <div className="text-[3rem] font-black text-white leading-none tracking-tight">
              {statMain}
            </div>
            {statUnit && (
              <div
                className="text-[12px] font-semibold mt-[5px] leading-none tracking-wide"
                style={{ color: accentColor, opacity: 0.8 }}
              >
                {statUnit}
              </div>
            )}
          </div>

          {/* Row 3 — One-liner insight: bright, wraps to 2 lines */}
          <p className="text-[11px] text-white/85 leading-snug line-clamp-2 min-w-0">
            {headline}
          </p>

        </div>

        {/* ── RIGHT: actor portrait — absolutely positioned so the card bg is seamless */}
        {actors.length > 0 && (
          <div className="absolute bottom-0 right-0 pointer-events-none z-[2]">

            {/* Left-to-right fade — blends portrait into the continuous card background */}
            <div
              className="absolute inset-y-0 left-0 w-24 z-10"
              style={{ background: `linear-gradient(to right, ${bgColor} 0%, transparent 100%)` }}
            />

            {/* Single actor */}
            {singleActor && (
              <div className="relative" style={{ opacity: 0.65 }}>
                <div
                  className="absolute inset-0 blur-2xl scale-75"
                  style={{ background: glowColor }}
                />
                {actors[0].avatarSlug ? (
                  <Image
                    src={`/avatars/${actors[0].avatarSlug}.png`}
                    alt={actors[0].name}
                    width={158}
                    height={158}
                    className="relative object-cover object-top"
                    style={{
                      maskImage:       'radial-gradient(ellipse 70% 90% at 60% 55%, rgba(0,0,0,0.9) 35%, rgba(0,0,0,0) 100%)',
                      WebkitMaskImage: 'radial-gradient(ellipse 70% 90% at 60% 55%, rgba(0,0,0,0.9) 35%, rgba(0,0,0,0) 100%)',
                      filter:          'drop-shadow(0 8px 24px rgba(0,0,0,0.6))',
                    }}
                    onError={e => { (e.currentTarget as HTMLImageElement).style.display = 'none' }}
                  />
                ) : (
                  <ActorAvatar name={actors[0].name} size={146} />
                )}
              </div>
            )}

            {/* Two actors — overlapping */}
            {multiActor && (
              <div className="relative flex items-end" style={{ opacity: 0.65 }}>
                {actors.slice(0, 2).map((actor, i) => (
                  <div
                    key={actor.name}
                    className="relative"
                    style={{ marginLeft: i === 0 ? 0 : -26, zIndex: i === 0 ? 2 : 1 }}
                  >
                    <div
                      className="absolute inset-0 blur-xl scale-75"
                      style={{ background: glowColor, opacity: 0.45 }}
                    />
                    {actor.avatarSlug ? (
                      <Image
                        src={`/avatars/${actor.avatarSlug}.png`}
                        alt={actor.name}
                        width={108}
                        height={108}
                        className="relative object-cover object-top"
                        style={{
                          maskImage:       'radial-gradient(ellipse 70% 90% at 60% 55%, rgba(0,0,0,0.85) 35%, rgba(0,0,0,0) 100%)',
                          WebkitMaskImage: 'radial-gradient(ellipse 70% 90% at 60% 55%, rgba(0,0,0,0.85) 35%, rgba(0,0,0,0) 100%)',
                          filter:          'drop-shadow(0 6px 16px rgba(0,0,0,0.65))',
                        }}
                        onError={e => { (e.currentTarget as HTMLImageElement).style.display = 'none' }}
                      />
                    ) : (
                      <ActorAvatar name={actor.name} size={96} />
                    )}
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
