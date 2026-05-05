'use client'

import { useState } from 'react'
import Image from 'next/image'
import { type SharedFilm } from '@/lib/api'

// ── Role helpers ───────────────────────────────────────────────────────────────

function normaliseRole(role: string | null): string | null {
  if (!role) return null
  const l = role.toLowerCase()
  if (l === 'primary' || l === 'lead') return 'Lead'
  if (l === 'supporting') return 'Supporting'
  return role.charAt(0).toUpperCase() + role.slice(1)
}

function isVoiceRole(character: string | null): boolean {
  if (!character) return false
  const l = character.toLowerCase()
  return l.includes('voice') || l.includes('narrator')
}

function RolePill({
  actorName,
  character,
  role,
}: {
  actorName: string
  character: string | null
  role: string | null
}) {
  const firstName = actorName.split(' ')[0]

  if (isVoiceRole(character)) {
    return (
      <span className="inline-flex items-center gap-1 rounded-full px-2.5 py-0.5 text-[11px] font-medium bg-blue-500/15 text-blue-400">
        {firstName} · Voice
      </span>
    )
  }

  if (!character) {
    return (
      <span className="inline-flex items-center gap-1 rounded-full px-2.5 py-0.5 text-[11px] font-medium bg-white/[0.06] text-white/40">
        {firstName}
      </span>
    )
  }

  const displayRole = normaliseRole(role)
  const label = displayRole ? `${character} · ${displayRole}` : character
  const isLead = displayRole === 'Lead'

  return (
    <span
      className={`inline-flex items-center gap-1 rounded-full px-2.5 py-0.5 text-[11px] font-medium ${
        isLead ? 'bg-emerald-500/15 text-emerald-400' : 'bg-white/[0.06] text-white/50'
      }`}
    >
      {label}
    </span>
  )
}

// ── Main component ─────────────────────────────────────────────────────────────

export default function FilmsTogether({
  films,
  name1,
  name2,
}: {
  films: SharedFilm[]
  name1: string
  name2: string
}) {
  const [open, setOpen] = useState(false)

  if (!films.length) {
    return (
      <div className="glass rounded-2xl px-6 py-10 text-center">
        <p className="text-white/30 text-sm mb-2">No shared films found</p>
        <p className="text-white/15 text-xs">
          These actors haven't starred together in films in our database.
        </p>
      </div>
    )
  }

  return (
    <div>
      {/* ── Toggle button ── */}
      <button
        onClick={() => setOpen((prev) => !prev)}
        className="w-full glass rounded-2xl px-5 py-4 flex items-center justify-between gap-3 hover:bg-white/[0.07] transition-colors group"
        aria-expanded={open}
      >
        <div className="flex items-center gap-3">
          <span className="text-sm font-semibold text-white/80">
            {films.length} film{films.length !== 1 ? 's' : ''} together
          </span>
          {/* Film count pill */}
          <span
            className="text-[11px] px-2.5 py-0.5 rounded-full font-medium"
            style={{ background: 'rgba(139,92,246,0.15)', color: '#a78bfa' }}
          >
            {open ? 'Hide' : 'View all'}
          </span>
        </div>
        {/* Chevron */}
        <svg
          width="16"
          height="16"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          className="text-white/30 group-hover:text-white/50 transition-all flex-shrink-0"
          style={{
            transform: open ? 'rotate(180deg)' : 'rotate(0deg)',
            transition: 'transform 250ms ease',
          }}
        >
          <polyline points="6 9 12 15 18 9" />
        </svg>
      </button>

      {/* ── Film list (collapsible) ── */}
      {open && (
        <div className="flex flex-col gap-3 mt-3">
          {films.map((film, i) => {
            const hasRating = (film.vote_average ?? 0) > 0
            const rating = hasRating ? film.vote_average!.toFixed(1) : null

            return (
              <div
                key={`${film.title}-${i}`}
                className="glass rounded-2xl flex gap-4 overflow-hidden hover:bg-white/[0.06] transition-colors"
              >
                <div className="relative flex-shrink-0 w-16 aspect-[2/3] bg-[#1a1a24]">
                  {film.poster_url ? (
                    <Image
                      src={film.poster_url}
                      alt={film.title}
                      fill
                      sizes="64px"
                      className="object-cover"
                    />
                  ) : (
                    <div className="w-full h-full flex items-center justify-center text-white/10">
                      🎬
                    </div>
                  )}
                </div>

                <div className="flex flex-col gap-2 py-4 pr-4 flex-1 min-w-0">
                  <div className="flex items-baseline gap-2">
                    <span className="font-semibold text-white/90 text-sm leading-snug">
                      {film.title}
                    </span>
                    <span className="text-xs text-white/30 flex-shrink-0">
                      {film.release_year > 0 ? film.release_year : ''}
                    </span>
                    {rating && (
                      <span className="text-xs text-yellow-400 flex-shrink-0 ml-auto">
                        ★ {rating}
                      </span>
                    )}
                  </div>
                  {film.director && (
                    <p className="text-xs text-white/40">Dir. {film.director}</p>
                  )}
                  <div className="flex flex-wrap gap-2 mt-1">
                    <RolePill
                      actorName={name1}
                      character={film.actor1_character}
                      role={film.actor1_role}
                    />
                    <RolePill
                      actorName={name2}
                      character={film.actor2_character}
                      role={film.actor2_role}
                    />
                  </div>
                </div>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
