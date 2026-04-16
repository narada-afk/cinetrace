'use client'

import { useEffect, useRef, useState } from 'react'
import Image from 'next/image'
import Link from 'next/link'
import ActorAvatar from './ActorAvatar'
import DirectorsSection from './DirectorsSection'
import ScrollRow from './ScrollRow'
import { toActorSlug } from '@/lib/api'
import type { Collaborator, DirectorCollab, Actor, ActorMovie, Blockbuster } from '@/lib/api'

interface CollaborationsSectionProps {
  collaborators: Collaborator[]
  leadCollaborators: Collaborator[]
  directors: DirectorCollab[]
  blockbusters: Blockbuster[]
  movies: ActorMovie[]
  allActors: Actor[]
  allFemaleActors: Actor[]
  actorIdMap: Record<string, number>
  actorGender?: 'M' | 'F' | null
}

export default function CollaborationsSection({
  collaborators,
  leadCollaborators,
  directors,
  blockbusters,
  movies,
  allActors,
  allFemaleActors,
  actorIdMap,
  actorGender,
}: CollaborationsSectionProps) {
  // Build gender maps
  const femaleNames = new Set(allFemaleActors.map(a => a.name.toLowerCase()))

  const genderMap: Record<string, 'M' | 'F'> = {}
  for (const a of allActors) {
    if (a.name && a.gender) {
      genderMap[a.name.toLowerCase()] = a.gender
    }
  }

  // Primary-tier names — used to filter male co-stars (excludes Brahmanandam etc.)
  const primaryNames = new Set(
    allActors
      .filter(a => a.actor_tier === 'primary')
      .map(a => a.name.toLowerCase())
  )

  // Known-tier names — actors with any assigned tier ('primary' | 'network').
  // Lead actresses in South Indian films are typically tagged 'network' (not 'primary')
  // because 'primary' skews toward top male stars. Using knownTierNames for the
  // female co-star filter ensures lead heroines appear while excluding extras (null tier).
  const knownTierNames = new Set(
    allActors
      .filter(a => a.actor_tier !== null && a.actor_tier !== undefined)
      .map(a => a.name.toLowerCase())
  )

  // For a male actor → show female lead co-stars ("Lead Actresses")
  // For a female actor → show male lead co-stars ("Lead Actors")
  const leadLabel = actorGender === 'F' ? '🎬 Lead Actors' : '✨ Lead Actresses'

  // Use ALL collaborators for the actresses section — NOT leadCollaborators.
  // TMDB marks heroines as role_type='supporting' (billed after the male lead),
  // so leadCollaborators (primary-role for both) misses most lead actresses.
  const actresses = actorGender === 'F'
    // actress page: show known primary male actors
    ? collaborators.filter(c => {
        const low = c.actor.toLowerCase()
        return genderMap[low] === 'M' && primaryNames.has(low)
      })
    // actor page: show female actors who have a known tier (lead or prominent supporting)
    // knownTierNames (primary | network) catches lead actresses; null-tier extras are excluded
    : collaborators.filter(c => {
        const low = c.actor.toLowerCase()
        return femaleNames.has(low) && knownTierNames.has(low)
      })

  // Build latest year each director worked with this actor (from movies already fetched)
  const dirLatestYear: Record<string, number> = {}
  for (const m of movies) {
    if (m.director && m.release_year > 0) {
      if (!dirLatestYear[m.director] || m.release_year > dirLatestYear[m.director]) {
        dirLatestYear[m.director] = m.release_year
      }
    }
  }

  // Two-tier sort:
  //   Tier 1 (3+ films): ranked by film count DESC, then recency as tiebreaker
  //   Tier 2 (1–2 films): ranked by most recent collaboration first
  const topDirs = [...directors]
    .sort((a, b) => {
      const aHigh = a.films >= 3
      const bHigh = b.films >= 3
      if (aHigh !== bHigh) return aHigh ? -1 : 1          // tier 1 always above tier 2
      if (aHigh) {
        if (b.films !== a.films) return b.films - a.films  // more films first within tier 1
        return (dirLatestYear[b.director] ?? 0) - (dirLatestYear[a.director] ?? 0)
      }
      return (dirLatestYear[b.director] ?? 0) - (dirLatestYear[a.director] ?? 0) // recency within tier 2
    })
    .slice(0, 20)

  const hasActresses   = actresses.length > 0
  const hasDirs        = topDirs.length > 0
  const hasBlockbusters = blockbusters.length > 0

  if (!hasActresses && !hasDirs && !hasBlockbusters) return null

  return (
    <div className="flex flex-col gap-10">

      {/* ── Lead Actresses / Lead Actors ────────────────── */}
      {hasActresses && (
        <div className="flex flex-col gap-4">
          <h2 className="text-lg font-bold text-white/80">{leadLabel}</h2>
          <ScrollRow>
            <div className="flex gap-5 pb-2 px-1" style={{ width: 'max-content' }}>
              {actresses.map(c => {
                const actorId = actorIdMap[c.actor]
                const inner = (
                  <div className="flex flex-col items-center gap-2 flex-shrink-0 group">
                    <div className={`ring-2 rounded-full transition-all ${
                      actorId
                        ? 'ring-white/10 group-hover:ring-pink-400/40 cursor-pointer'
                        : 'ring-white/[0.05]'
                    }`}>
                      <ActorAvatar name={c.actor} size={64} />
                    </div>
                    <p className={`text-xs font-medium text-center w-20 truncate transition-colors ${
                      actorId
                        ? 'text-white/55 group-hover:text-white/80'
                        : 'text-white/35'
                    }`}>
                      {c.actor.split(' ').slice(0, 2).join(' ')}
                    </p>
                    <p className="text-white/25 text-[10px] -mt-1">{c.films} {c.films === 1 ? 'film' : 'films'}</p>
                  </div>
                )
                return actorId ? (
                  <Link key={c.actor} href={`/actors/${toActorSlug(c.actor)}`}>{inner}</Link>
                ) : (
                  <div key={c.actor}>{inner}</div>
                )
              })}
            </div>
          </ScrollRow>
        </div>
      )}

      {/* ── Directors ───────────────────────────────────── */}
      {hasDirs && (
        <DirectorsSection directors={topDirs} movies={movies} />
      )}

      {/* ── Blockbusters ─────────────────────────────────── */}
      {hasBlockbusters && <BlockbustersList blockbusters={blockbusters} />}

    </div>
  )
}

// ── Blockbusters sub-component (needs animation state) ────────────────────────

const MICRO_COPY = [
  'Career peak performance',
  'Record-breaking opener',
  'Fan-favorite blockbuster',
  'Major box office success',
  'Powerful commercial hit',
  'Solid crowd-pleaser',
  'Profitable theatrical run',
  'Strong commercial outing',
  'Dependable box office draw',
  'Impressive theatrical run',
]

function formatCrore(val: number) {
  if (val >= 1000) return `₹${(val / 1000).toFixed(2)}K Cr`
  return `₹${Math.round(val)} Cr`
}

function BlockbustersList({ blockbusters }: { blockbusters: Blockbuster[] }) {
  const [barsReady, setBarsReady] = useState(false)
  const containerRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const el = containerRef.current
    if (!el) return

    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          const t = setTimeout(() => setBarsReady(true), 80)
          return () => clearTimeout(t)
        } else {
          setBarsReady(false)
        }
      },
      { threshold: 0.15 }
    )

    observer.observe(el)
    return () => observer.disconnect()
  }, [])

  const maxCrore = blockbusters[0]?.box_office_crore ?? 1

  return (
    <div ref={containerRef} className="flex flex-col gap-4">
      <style>{`
        @keyframes bb-shimmer {
          0%   { left: -40%; }
          100% { left: 120%; }
        }
        .bb-bar::after {
          content: "";
          position: absolute;
          top: 0;
          left: -40%;
          width: 35%;
          height: 100%;
          background: linear-gradient(90deg, transparent, rgba(255,255,255,0.28), transparent);
          animation: bb-shimmer 2.5s ease-in-out infinite;
        }
      `}</style>
      <h2 className="text-lg font-bold text-white/80">💰 Blockbusters</h2>
      <div className="flex flex-col gap-2">
        {blockbusters.map((b, i) => {
          const pct       = (b.box_office_crore / maxCrore) * 100
          const isFirst   = i === 0
          const isTop3    = i < 3

          // Size tiers
          const posterW   = isFirst ? 52 : isTop3 ? 42 : 36
          const posterH   = isFirst ? 78 : isTop3 ? 63 : 54
          const py        = isFirst ? 'py-4' : isTop3 ? 'py-3' : 'py-2.5'

          const rankColor =
            i === 0 ? 'text-yellow-400'
            : i === 1 ? 'text-slate-300/80'
            : i === 2 ? 'text-amber-600/80'
            : 'text-white/18'

          const bgColor   = isFirst ? 'rgba(207,175,107,0.07)' : 'rgba(255,255,255,0.02)'
          const bgHover   = isFirst ? 'rgba(207,175,107,0.11)' : 'rgba(255,255,255,0.045)'
          const border    = isFirst ? '1px solid rgba(207,175,107,0.22)' : '1px solid rgba(255,255,255,0.05)'
          const glow      = isFirst ? '0 0 28px rgba(245,217,139,0.10)' : 'none'
          const barGlow   = isFirst ? '0 0 14px rgba(245,217,139,0.35)' : '0 0 10px rgba(245,217,139,0.2)'

          return (
            <div
              key={b.title}
              className={`rounded-xl px-4 ${py} transition-all duration-200 cursor-default`}
              style={{ background: bgColor, border, boxShadow: glow }}
              onMouseEnter={e => {
                const el = e.currentTarget as HTMLDivElement
                el.style.transform = 'scale(1.01)'
                el.style.background = bgHover
              }}
              onMouseLeave={e => {
                const el = e.currentTarget as HTMLDivElement
                el.style.transform = 'scale(1)'
                el.style.background = bgColor
              }}
            >
              <div className="flex items-center gap-4">
                {/* Rank */}
                <span
                  className={`flex-shrink-0 font-bold w-6 text-center tabular-nums ${rankColor}`}
                  style={{ fontSize: isFirst ? 14 : isTop3 ? 12 : 11 }}
                >
                  #{i + 1}
                </span>

                {/* Poster */}
                <div
                  className="flex-shrink-0 rounded-lg overflow-hidden bg-white/[0.04] shadow-md"
                  style={{ width: posterW, height: posterH }}
                >
                  {b.poster_url ? (
                    <Image
                      src={b.poster_url}
                      alt={b.title}
                      width={posterW}
                      height={posterH}
                      className="object-cover w-full h-full"
                    />
                  ) : (
                    <div className="w-full h-full" />
                  )}
                </div>

                {/* Content */}
                <div className="flex-1 min-w-0 flex flex-col gap-1.5">
                  {/* Top row: title + amount */}
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      {isFirst && (
                        <p className="text-[10px] font-semibold uppercase tracking-widest mb-1" style={{ color: '#F5D98B', opacity: 0.8 }}>
                          🏆 Highest Grossing
                        </p>
                      )}
                      <p className={`font-semibold truncate leading-snug ${
                        isFirst ? 'text-white text-[15px]'
                        : isTop3 ? 'text-white/85 text-sm'
                        : 'text-white/70 text-sm'
                      }`}>
                        {b.title}
                      </p>
                      <p className={`mt-0.5 ${isFirst ? 'text-white/35 text-xs' : 'text-white/25 text-[11px]'}`}>
                        {b.release_year}
                      </p>
                    </div>
                    {/* Collection + ROI */}
                    <div className="flex-shrink-0 text-right">
                      <span className={`font-bold tabular-nums block ${
                        isFirst ? 'text-[#F5D98B] text-base'
                        : isTop3 ? 'text-[#CFAF6B]/90 text-sm'
                        : 'text-[#CFAF6B]/60 text-xs'
                      }`}>
                        {formatCrore(b.box_office_crore)}
                      </span>
                      {b.budget_crore && b.budget_crore > 0 && (
                        <span className="text-[10px] text-white/30 tabular-nums">
                          {(b.box_office_crore / b.budget_crore).toFixed(1)}x ROI
                        </span>
                      )}
                    </div>
                  </div>

                  {/* Budget row */}
                  {b.budget_crore && b.budget_crore > 0 && (
                    <div className="flex items-center gap-1.5">
                      <span className="text-[10px] text-white/25">Budget</span>
                      <span className="text-[10px] text-white/40 font-medium tabular-nums">{formatCrore(b.budget_crore)}</span>
                      <span className="text-[9px] text-white/15">({b.budget_source})</span>
                    </div>
                  )}

                  {/* Progress bar */}
                  <div className="h-[4px] rounded-full w-full" style={{ background: 'rgba(255,255,255,0.08)' }}>
                    <div
                      className="bb-bar h-full rounded-full relative overflow-hidden"
                      style={{
                        width: barsReady ? `${pct}%` : '0%',
                        background: 'linear-gradient(90deg, #CFAF6B, #F5D98B, #CFAF6B)',
                        boxShadow: barGlow,
                        transition: `width ${700 + i * 40}ms cubic-bezier(0.25, 0.46, 0.45, 0.94)`,
                      }}
                    />
                  </div>
                </div>
              </div>
            </div>
          )
        })}
      </div>
      <p className="text-white/20 text-[11px] text-right">
        Box office: TMDB · Budget: TMDB / Wikipedia
      </p>
    </div>
  )
}
