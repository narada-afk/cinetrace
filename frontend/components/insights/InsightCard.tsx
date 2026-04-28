'use client'

/**
 * components/insights/InsightCard.tsx
 * ─────────────────────────────────────
 * StatsMuse-inspired redesign.
 *
 * Design language:
 *  • Flat solid background — one strong colour per insight type
 *  • Stat-first: giant number dominates the left column
 *  • Minimal decoration (1 px accent line, subtle shadow only)
 *  • Actor portrait bleeds right edge with left-colour fade
 *  • CINETRACE branding tiny bottom-right
 *  • Hover: translateY(-2px) + deeper shadow — nothing more
 *
 * Props are unchanged from the previous version — no data-layer edits needed.
 */

import { useState } from 'react'
import Image from 'next/image'
import Link from 'next/link'
import { shareInsight } from '@/lib/shareInsight'

// ── Types (unchanged) ─────────────────────────────────────────────────────────

export type InsightType =
  | 'cross_industry'
  | 'collab_shock'
  | 'hidden_dominance'
  | 'career_peak'
  | 'network_power'
  | 'director_loyalty'
  | string

export interface InsightCardProps {
  type:               InsightType
  title:              string
  value:              string | number
  label:              string
  footer?:            string
  imageUrl?:          string
  actorName?:         string
  secondaryImageUrl?: string
  extraMeta?:         unknown
  href?:              string
}

// ── Theme — flat solid backgrounds, lighter accent for type labels ─────────────

interface CardTheme {
  bg:     string   // solid background
  accent: string   // lighter tint — used for label, unit, accent line
}

const THEMES: Record<string, CardTheme> = {
  cross_industry:   { bg: '#0B5D3D', accent: '#6ee7b7' },
  collab_shock:     { bg: '#7A2208', accent: '#fca47c' },
  hidden_dominance: { bg: '#5A189A', accent: '#d8b4fe' },
  career_peak:      { bg: '#8A6A00', accent: '#fde68a' },
  network_power:    { bg: '#005B96', accent: '#93c5fd' },
  director_loyalty: { bg: '#006D67', accent: '#5eead4' },
}

// Legacy / alias fallbacks — keep same legacy keys working
THEMES.collaboration       = THEMES.collab_shock
THEMES.director            = THEMES.director_loyalty
THEMES.supporting          = THEMES.hidden_dominance
THEMES.director_box_office = THEMES.career_peak

const FALLBACK_THEME: CardTheme = { bg: '#1E293B', accent: '#94a3b8' }

function getTheme(type: string): CardTheme {
  return THEMES[type] ?? FALLBACK_THEME
}

// ── Stat parser ───────────────────────────────────────────────────────────────

function splitStat(v: string | number): { main: string; unit: string | null } {
  const s = String(v).trim()
  if (/^\d{4}[–\-]\d{4}$/.test(s)) return { main: s, unit: null }   // "2005–2010"
  if (/^[\d,]+$/.test(s))           return { main: s, unit: null }   // "42"
  const idx = s.indexOf(' ')
  if (idx > 0) return { main: s.slice(0, idx), unit: s.slice(idx + 1) }
  return { main: s, unit: null }
}

// ── Dynamic font size so short stats are huge and long strings still fit ──────

function statFontSize(s: string): string {
  if (s.length <= 3)  return 'clamp(3.25rem, 13vw, 4.5rem)'   // "5", "394"
  if (s.length <= 6)  return 'clamp(2.4rem,  10vw, 3.5rem)'   // "1,200"
  return                     'clamp(1.75rem,  7vw, 2.6rem)'    // "2021–2025"
}

// ── Component ─────────────────────────────────────────────────────────────────

export default function InsightCard({
  type,
  title,
  value,
  label,
  footer,
  imageUrl,
  actorName,
  secondaryImageUrl,
  href = '#',
}: InsightCardProps) {
  const [hovered,    setHovered]    = useState(false)
  const [shareState, setShareState] = useState<'idle' | 'copied'>('idle')

  const theme = getTheme(type)
  const { main: statMain, unit: statUnit } = splitStat(value)

  const hasDuo    = !!imageUrl && !!secondaryImageUrl
  const hasSingle = !!imageUrl && !hasDuo
  const hasImage  = hasSingle || hasDuo

  async function handleShare(e: React.MouseEvent) {
    e.preventDefault()
    e.stopPropagation()
    const r = await shareInsight({ href, actorName, statValue: value })
    if (r.ok) {
      setShareState('copied')
      setTimeout(() => setShareState('idle'), 1800)
    }
  }

  return (
    <Link href={href} className="block h-full" tabIndex={-1}>
      <article
        aria-label={`${title}: ${value}${label ? ` — ${label}` : ''}`}
        className="group relative rounded-2xl overflow-hidden cursor-pointer h-[220px] sm:h-[250px]"
        style={{
          background: theme.bg,
          boxShadow: hovered
            ? '0 10px 36px rgba(0,0,0,0.50), 0 2px 8px rgba(0,0,0,0.30)'
            : '0 2px 14px rgba(0,0,0,0.38)',
          transform:  hovered ? 'translateY(-2px)' : 'translateY(0)',
          transition: 'transform 200ms ease, box-shadow 200ms ease',
        }}
        onMouseEnter={() => setHovered(true)}
        onMouseLeave={() => setHovered(false)}
      >

        {/* ── Top 1 px accent line ────────────────────────────────────────── */}
        <div
          className="absolute top-0 inset-x-0 h-px z-20 pointer-events-none rounded-t-2xl"
          style={{
            background: `linear-gradient(to right,
              transparent 4%,
              ${theme.accent}90 30%,
              ${theme.accent}90 70%,
              transparent 96%)`,
          }}
        />

        {/* ── Single actor portrait — bleeds to right edge ────────────────── */}
        {hasSingle && (
          <div
            className="absolute top-0 right-0 bottom-0 z-[2] pointer-events-none"
            style={{ width: '40%' }}
          >
            <div className="relative w-full h-full">
              {/* Left-edge colour fade so text never fights the portrait */}
              <div
                className="absolute inset-y-0 left-0 w-20 z-10"
                style={{
                  background: `linear-gradient(to right, ${theme.bg} 0%, transparent 100%)`,
                }}
              />
              {/* Bottom vignette */}
              <div
                className="absolute bottom-0 inset-x-0 h-16 z-10"
                style={{
                  background: `linear-gradient(to top, ${theme.bg}bb 0%, transparent 100%)`,
                }}
              />
              <Image
                src={imageUrl!}
                alt={actorName ?? 'Actor portrait'}
                fill
                sizes="(max-width: 640px) 130px, 155px"
                className="object-cover object-top"
                style={{
                  transform:  hovered ? 'scale(1.04)' : 'scale(1.0)',
                  transition: 'transform 400ms ease',
                }}
                onError={e => { (e.currentTarget as HTMLImageElement).style.display = 'none' }}
              />
            </div>
          </div>
        )}

        {/* ── Duo portraits — overlapping circles, bottom-right ────────────── */}
        {hasDuo && (
          <div className="absolute bottom-0 right-0 z-[2] pointer-events-none flex items-end pb-3 pr-3">
            {[
              { src: imageUrl!,          name: actorName ?? '', z: 2 },
              { src: secondaryImageUrl!, name: '',              z: 1 },
            ].map((a, i) => (
              <div
                key={i}
                className="relative rounded-full overflow-hidden flex-shrink-0"
                style={{
                  width:      92,
                  height:     92,
                  marginLeft: i === 0 ? 0 : -24,
                  zIndex:     a.z,
                  border:     '2.5px solid rgba(0,0,0,0.45)',
                  boxShadow:  '0 4px 18px rgba(0,0,0,0.60)',
                  transform:  hovered ? 'scale(1.06)' : 'scale(1)',
                  transition: `transform ${320 + i * 40}ms ease`,
                }}
              >
                <Image
                  src={a.src}
                  alt={a.name || 'Actor portrait'}
                  fill
                  sizes="92px"
                  className="object-cover object-top"
                  onError={e => { (e.currentTarget as HTMLImageElement).style.display = 'none' }}
                />
              </div>
            ))}
          </div>
        )}

        {/* ── Text content ─────────────────────────────────────────────────── */}
        <div
          className="relative z-10 flex flex-col h-full p-5 sm:p-6"
          style={{ maxWidth: hasImage ? '62%' : '100%' }}
        >

          {/* Top label — "NO LANGUAGE BARRIERS" */}
          <p
            className="text-[10px] md:text-xs font-semibold uppercase leading-none"
            style={{
              letterSpacing: '0.25em',
              color: `${theme.accent}cc`,
            }}
          >
            {title}
          </p>

          {/* Stat block — hero of the card */}
          <div className="mt-3 flex-1 min-h-0">

            {/* Giant number */}
            <div
              className="font-black leading-none tracking-tight text-white"
              style={{ fontSize: statFontSize(statMain) }}
            >
              {statMain}
            </div>

            {/* Metric label ("industries", "films together") */}
            {statUnit && (
              <p
                className="text-base md:text-lg font-semibold mt-1 leading-none"
                style={{ color: `${theme.accent}f2` }}
              >
                {statUnit}
              </p>
            )}

            {/* No blurb — footer phrase below carries the context */}
          </div>

          {/* Footer row — cinematic phrase + CINETRACE brand */}
          <div className="mt-auto pt-2 flex items-end justify-between gap-2">
            {footer ? (
              <p
                className="text-sm font-semibold leading-tight"
                style={{ color: 'rgba(255,255,255,0.92)' }}
              >
                {footer}
              </p>
            ) : (
              <span />
            )}
            <span
              className="text-[9px] uppercase tracking-[0.22em] flex-shrink-0"
              style={{ color: 'rgba(255,255,255,0.35)', opacity: 0.6 }}
            >
              CINETRACE
            </span>
          </div>
        </div>

        {/* ── Share button — top-right, appears on hover ──────────────────── */}
        <button
          onClick={handleShare}
          className="
            absolute top-3.5 right-3.5 z-30
            w-7 h-7 rounded-full flex items-center justify-center
            opacity-0 group-hover:opacity-100 focus:opacity-100
            transition-opacity duration-200
            focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-white/30
          "
          style={{
            background:     'rgba(0,0,0,0.28)',
            backdropFilter: 'blur(6px)',
            border:         '1px solid rgba(255,255,255,0.18)',
          }}
          aria-label={shareState === 'copied' ? 'Copied!' : 'Share this insight'}
          title={shareState === 'copied' ? 'Copied!' : 'Share'}
        >
          {shareState === 'copied' ? (
            <svg width="11" height="11" viewBox="0 0 24 24" fill="none"
              stroke="#4ade80" strokeWidth="3" strokeLinecap="round">
              <polyline points="20 6 9 17 4 12"/>
            </svg>
          ) : (
            <svg width="11" height="11" viewBox="0 0 24 24" fill="none"
              stroke="rgba(255,255,255,0.75)" strokeWidth="2.5">
              <circle cx="18" cy="5" r="3"/>
              <circle cx="6" cy="12" r="3"/>
              <circle cx="18" cy="19" r="3"/>
              <line x1="8.59" y1="13.51" x2="15.42" y2="17.49"/>
              <line x1="15.41" y1="6.51" x2="8.59" y2="10.49"/>
            </svg>
          )}
        </button>

      </article>
    </Link>
  )
}
