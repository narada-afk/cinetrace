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

// Bright, vivid single-tone card backgrounds — uniform across the whole card
const CARD_BG: Record<InsightCardData['gradient'], string> = {
  red:    '#9b1c1c',
  purple: '#6b21a8',
  orange: '#9a3412',
  blue:   '#1e3a8a',
  green:  '#14532d',
  amber:  '#92400e',
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

// Hover shadow — lifted glow matching the card hue
const HOVER_SHADOW: Record<InsightCardData['gradient'], string> = {
  red:    '0 0 40px rgba(220,38,38,0.6),  0 8px 28px rgba(0,0,0,0.4)',
  purple: '0 0 40px rgba(147,51,234,0.6), 0 8px 28px rgba(0,0,0,0.4)',
  orange: '0 0 40px rgba(234,88,12,0.6),  0 8px 28px rgba(0,0,0,0.4)',
  blue:   '0 0 40px rgba(37,99,235,0.6),  0 8px 28px rgba(0,0,0,0.4)',
  green:  '0 0 40px rgba(22,163,74,0.6),  0 8px 28px rgba(0,0,0,0.4)',
  amber:  '0 0 40px rgba(217,119,6,0.6),  0 8px 28px rgba(0,0,0,0.4)',
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

        {/* ── RIGHT: actor portrait — floats on the uniform card bg */}
        {actors.length > 0 && (
          <div className="absolute bottom-0 right-0 pointer-events-none z-[2]">

            {/* Left-edge fade — blends portrait into the text column */}
            <div
              className="absolute inset-y-0 left-0 w-24 z-10"
              style={{ background: `linear-gradient(to right, ${bgColor} 0%, transparent 100%)` }}
            />

            {/* Single actor — full image, no mask */}
            {singleActor && actors[0].avatarSlug && (
              <Image
                src={`/avatars/${actors[0].avatarSlug}.png`}
                alt={actors[0].name}
                width={220}
                height={220}
                className="object-cover object-top"
                onError={e => { (e.currentTarget as HTMLImageElement).style.display = 'none' }}
              />
            )}

            {/* Two actors — overlapping, full images */}
            {multiActor && (
              <div className="relative flex items-center">
                {actors.slice(0, 2).map((actor, i) => (
                  actor.avatarSlug ? (
                    <div
                      key={actor.name}
                      className="relative"
                      style={{ marginLeft: i === 0 ? 0 : -28, zIndex: i === 0 ? 2 : 1 }}
                    >
                      <Image
                        src={`/avatars/${actor.avatarSlug}.png`}
                        alt={actor.name}
                        width={150}
                        height={150}
                        className="object-cover object-top"
                        onError={e => { (e.currentTarget as HTMLImageElement).style.display = 'none' }}
                      />
                    </div>
                  ) : null
                ))}
              </div>
            )}

          </div>
        )}

      </div>
    </Link>
  )
}
