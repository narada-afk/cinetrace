'use client'

import { useState } from 'react'
import Link from 'next/link'
import Image from 'next/image'

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

// Vivid single-tone card backgrounds
const CARD_BG: Record<InsightCardData['gradient'], string> = {
  red:    '#9b1c1c',
  purple: '#6b21a8',
  orange: '#9a3412',
  blue:   '#1e3a8a',
  green:  '#14532d',
  amber:  '#92400e',
}

// Layered background: base + radial highlight top-left + darker edge bottom-right
// Creates the appearance of interior light source on a vivid field
const CARD_GRADIENT: Record<InsightCardData['gradient'], string> = {
  red:    'radial-gradient(ellipse 80% 60% at 15% 20%, rgba(255,100,100,0.25) 0%, transparent 55%), radial-gradient(ellipse 60% 80% at 90% 90%, rgba(80,0,0,0.45) 0%, transparent 60%), #9b1c1c',
  purple: 'radial-gradient(ellipse 80% 60% at 15% 20%, rgba(210,140,255,0.25) 0%, transparent 55%), radial-gradient(ellipse 60% 80% at 90% 90%, rgba(40,0,80,0.45) 0%, transparent 60%), #6b21a8',
  orange: 'radial-gradient(ellipse 80% 60% at 15% 20%, rgba(255,160,80,0.25) 0%, transparent 55%),  radial-gradient(ellipse 60% 80% at 90% 90%, rgba(80,20,0,0.45) 0%, transparent 60%),  #9a3412',
  blue:   'radial-gradient(ellipse 80% 60% at 15% 20%, rgba(100,160,255,0.25) 0%, transparent 55%), radial-gradient(ellipse 60% 80% at 90% 90%, rgba(0,10,60,0.45) 0%, transparent 60%),  #1e3a8a',
  green:  'radial-gradient(ellipse 80% 60% at 15% 20%, rgba(80,220,120,0.22) 0%, transparent 55%),  radial-gradient(ellipse 60% 80% at 90% 90%, rgba(0,40,10,0.45) 0%, transparent 60%),  #14532d',
  amber:  'radial-gradient(ellipse 80% 60% at 15% 20%, rgba(255,210,80,0.25) 0%, transparent 55%),  radial-gradient(ellipse 60% 80% at 90% 90%, rgba(60,20,0,0.45) 0%, transparent 60%),  #92400e',
}

// Light accent tints — readable on bright backgrounds
const ACCENT: Record<InsightCardData['gradient'], string> = {
  red:    '#fca5a5',
  purple: '#d8b4fe',
  orange: '#fdba74',
  blue:   '#93c5fd',
  green:  '#86efac',
  amber:  '#fcd34d',
}

// Stat text-shadow glow — soft halo matching the accent
const STAT_GLOW: Record<InsightCardData['gradient'], string> = {
  red:    '0 0 24px rgba(252,165,165,0.55)',
  purple: '0 0 24px rgba(216,180,254,0.55)',
  orange: '0 0 24px rgba(253,186,116,0.55)',
  blue:   '0 0 24px rgba(147,197,253,0.55)',
  green:  '0 0 24px rgba(134,239,172,0.55)',
  amber:  '0 0 24px rgba(252,211,77,0.55)',
}

// Resting box-shadow — subtle depth before hover
const CARD_SHADOW: Record<InsightCardData['gradient'], string> = {
  red:    '0 4px 20px rgba(155,28,28,0.4)',
  purple: '0 4px 20px rgba(107,33,168,0.4)',
  orange: '0 4px 20px rgba(154,52,18,0.4)',
  blue:   '0 4px 20px rgba(30,58,138,0.4)',
  green:  '0 4px 20px rgba(20,83,45,0.4)',
  amber:  '0 4px 20px rgba(146,64,14,0.4)',
}

// Hover shadow — stronger lifted glow
const HOVER_SHADOW: Record<InsightCardData['gradient'], string> = {
  red:    '0 -2px 0 rgba(252,165,165,0.15), 0 0 48px rgba(220,38,38,0.65),  0 12px 32px rgba(0,0,0,0.5)',
  purple: '0 -2px 0 rgba(216,180,254,0.15), 0 0 48px rgba(147,51,234,0.65), 0 12px 32px rgba(0,0,0,0.5)',
  orange: '0 -2px 0 rgba(253,186,116,0.15), 0 0 48px rgba(234,88,12,0.65),  0 12px 32px rgba(0,0,0,0.5)',
  blue:   '0 -2px 0 rgba(147,197,253,0.15), 0 0 48px rgba(37,99,235,0.65),  0 12px 32px rgba(0,0,0,0.5)',
  green:  '0 -2px 0 rgba(134,239,172,0.15), 0 0 48px rgba(22,163,74,0.65),  0 12px 32px rgba(0,0,0,0.5)',
  amber:  '0 -2px 0 rgba(252,211,77,0.15),  0 0 48px rgba(217,119,6,0.65),  0 12px 32px rgba(0,0,0,0.5)',
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
  if (/^\d{4}[–\-]\d{4}$/.test(s)) return { main: s, unit: null }
  if (/^\d+$/.test(s)) return { main: s, unit: null }
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
  const accentColor  = ACCENT[gradient]
  const bgColor      = CARD_BG[gradient]
  const bgGradient   = CARD_GRADIENT[gradient]
  const statGlow     = STAT_GLOW[gradient]
  const cardShadow   = CARD_SHADOW[gradient]
  const hoverShadow  = HOVER_SHADOW[gradient]

  const [copied, setCopied] = useState(false)
  const [hovered, setHovered] = useState(false)

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
        className="group relative rounded-2xl h-[220px] flex cursor-pointer
                   border border-white/8 overflow-hidden"
        style={{
          background:  bgGradient,
          boxShadow:   hovered ? hoverShadow : cardShadow,
          transform:   hovered ? 'translateY(-3px) scale(1.015)' : 'translateY(0) scale(1)',
          transition:  'transform 220ms ease, box-shadow 220ms ease',
        }}
        onMouseEnter={() => setHovered(true)}
        onMouseLeave={() => setHovered(false)}
      >

        {/* Top-edge accent line — thin colour flash, adds premium feel */}
        <div
          className="absolute top-0 left-0 right-0 h-[1.5px] z-20 rounded-t-2xl"
          style={{ background: `linear-gradient(to right, transparent 5%, ${accentColor}55 40%, ${accentColor}33 70%, transparent 95%)` }}
        />

        {/* Share button — appears on hover */}
        <button
          onClick={handleShare}
          className="absolute top-3 right-3 z-20 w-7 h-7 rounded-full flex items-center justify-center
                     opacity-0 group-hover:opacity-100 transition-opacity duration-200"
          style={{ background: 'rgba(255,255,255,0.12)', backdropFilter: 'blur(4px)' }}
          title={copied ? 'Copied!' : 'Share'}
          aria-label="Share"
        >
          {copied ? (
            <span className="text-[10px] text-green-400 font-bold">✓</span>
          ) : (
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"
              style={{ color: 'rgba(255,255,255,0.6)' }}>
              <circle cx="18" cy="5" r="3"/><circle cx="6" cy="12" r="3"/><circle cx="18" cy="19" r="3"/>
              <line x1="8.59" y1="13.51" x2="15.42" y2="17.49"/>
              <line x1="15.41" y1="6.51" x2="8.59" y2="10.49"/>
            </svg>
          )}
        </button>

        {/* ── LEFT: text content ──────────────────────────────── */}
        <div
          className="relative z-10 flex flex-col justify-between p-6 pr-4 flex-1 min-w-0"
          style={{ maxWidth: '60%' }}
        >

          {/* Row 1 — label tag */}
          <span
            className="text-[10px] font-bold uppercase tracking-widest leading-none"
            style={{ color: 'rgba(255,255,255,0.75)' }}
          >
            {label}
          </span>

          {/* Row 2 — Stat: dominant numeral with glow + accent unit */}
          <div>
            <div
              className="text-[3rem] font-black leading-none tracking-tight"
              style={{
                color:      '#ffffff',
                textShadow: statGlow,
              }}
            >
              {statMain}
            </div>
            {statUnit && (
              <div
                className="text-[12px] font-semibold mt-[5px] leading-none tracking-wide"
                style={{ color: accentColor }}
              >
                {statUnit}
              </div>
            )}
          </div>

          {/* Row 3 — One-liner headline */}
          <p className="text-[11px] text-white/85 leading-snug line-clamp-2 min-w-0">
            {headline}
          </p>

        </div>

        {/* ── RIGHT: actor portrait ────────────────────────────── */}
        {actors.length > 0 && (
          <div className={`pointer-events-none z-[2] ${multiActor ? 'absolute bottom-0 right-0 flex items-end' : 'absolute bottom-0 right-0'}`}>

            {/* Left-edge fade — blends portrait into the text column (single actor only) */}
            {singleActor && (
              <div
                className="absolute inset-y-0 left-0 w-24 z-10"
                style={{ background: `linear-gradient(to right, ${bgColor} 0%, ${bgColor}00 100%)` }}
              />
            )}

            {/* Single actor — slight zoom on hover for cinematic presence */}
            {singleActor && actors[0].avatarSlug && (
              <Image
                src={`/avatars/${actors[0].avatarSlug}.png`}
                alt={actors[0].name}
                width={220}
                height={220}
                className="object-cover object-top"
                style={{
                  transform:  hovered ? 'scale(1.07)' : 'scale(1.0)',
                  transition: 'transform 280ms ease',
                  transformOrigin: 'center bottom',
                }}
                onError={e => { (e.currentTarget as HTMLImageElement).style.display = 'none' }}
              />
            )}

            {/* Two actors — circular avatar chips, overlapping */}
            {multiActor && (
              <div
                className="flex items-end pb-5 pr-5"
                style={{ gap: 0 }}
              >
                {actors.slice(0, 2).map((actor, i) => (
                  <div
                    key={actor.name}
                    className="relative flex-shrink-0 rounded-full overflow-hidden"
                    style={{
                      width: 72, height: 72,
                      marginLeft: i === 0 ? 0 : -20,
                      zIndex: i === 0 ? 2 : 1,
                      border: `2.5px solid ${bgColor}`,
                      boxShadow: '0 2px 12px rgba(0,0,0,0.5)',
                      transform: hovered ? 'scale(1.06)' : 'scale(1)',
                      transition: 'transform 280ms ease',
                    }}
                  >
                    {actor.avatarSlug ? (
                      <Image
                        src={`/avatars/${actor.avatarSlug}.png`}
                        alt={actor.name}
                        width={72}
                        height={72}
                        className="object-cover object-top w-full h-full"
                        onError={e => { (e.currentTarget as HTMLImageElement).style.display = 'none' }}
                      />
                    ) : (
                      /* Initials fallback */
                      <div
                        className="w-full h-full flex items-center justify-center text-[15px] font-bold text-white/80"
                        style={{ background: 'rgba(255,255,255,0.12)' }}
                      >
                        {actor.name.split(' ').map(w => w[0]).join('').slice(0, 2).toUpperCase()}
                      </div>
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
