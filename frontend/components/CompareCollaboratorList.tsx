'use client'

import { useState, useRef, useCallback } from 'react'
import ActorAvatar from './ActorAvatar'
import type { Collaborator, SharedFilm } from '@/lib/api'

// Plain browser fetch — avoids next.revalidate which is server-only
async function fetchSharedFilms(mainActorId: number, collaboratorId: number): Promise<SharedFilm[]> {
  const apiUrl = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000'
  const res = await fetch(`${apiUrl}/actors/${mainActorId}/shared/${collaboratorId}`)
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res.json()
}

interface Props {
  actorName: string
  mainActorId: number
  collaborators: Collaborator[]
  accentColor: string
}

function ChevronIcon({ open }: { open: boolean }) {
  return (
    <svg
      width="14"
      height="14"
      viewBox="0 0 14 14"
      fill="none"
      style={{
        transition: 'transform 0.25s ease',
        transform: open ? 'rotate(180deg)' : 'rotate(0deg)',
        flexShrink: 0,
        opacity: 0.4,
      }}
    >
      <path
        d="M2.5 5L7 9.5L11.5 5"
        stroke="white"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  )
}

function FilmDropdown({
  films,
  loading,
  error,
  accentColor,
}: {
  films: SharedFilm[] | null
  loading: boolean
  error?: string
  accentColor: string
}) {
  const isEmpty  = !loading && films !== null && films.length === 0
  const maxHeight = loading ? 52
    : error       ? 44
    : isEmpty     ? 44
    : films       ? films.length * 52 + 16
    : 0

  return (
    <div style={{ overflow: 'hidden', maxHeight, transition: 'max-height 0.35s cubic-bezier(0.4,0,0.2,1)' }}>
      <div className="pt-1 pb-2 px-2 flex flex-col gap-1">

        {loading && (
          <div className="flex items-center gap-2 py-3 px-3">
            <div className="w-2.5 h-2.5 rounded-full animate-pulse" style={{ background: accentColor, opacity: 0.5 }} />
            <span className="text-xs text-white/30">Loading films…</span>
          </div>
        )}

        {!loading && error && (
          <p className="text-xs text-red-400/50 py-2.5 px-3">{error}</p>
        )}

        {!loading && !error && isEmpty && (
          <p className="text-xs text-white/25 py-2.5 px-3">No shared films found</p>
        )}

        {!loading && !error && films && films.map((film, i) => (
          <div
            key={`${film.title}-${i}`}
            className="flex items-center gap-3 rounded-lg px-3 py-2"
            style={{ background: 'rgba(255,255,255,0.03)' }}
          >
            <span className="text-[10px] font-mono flex-shrink-0" style={{ color: accentColor, opacity: 0.7 }}>
              {film.release_year > 0 ? film.release_year : '—'}
            </span>
            <span className="text-xs text-white/70 truncate">{film.title}</span>
            {(film.vote_average ?? 0) > 0 && (
              <span className="text-[10px] text-yellow-400/60 flex-shrink-0 ml-auto">
                ★ {film.vote_average!.toFixed(1)}
              </span>
            )}
          </div>
        ))}

      </div>
    </div>
  )
}

export default function CompareCollaboratorList({
  actorName,
  mainActorId,
  collaborators,
  accentColor,
}: Props) {
  const [expandedId, setExpandedId]   = useState<number | null>(null)
  const [filmsCache, setFilmsCache]   = useState<Record<number, SharedFilm[]>>({})
  const [loadingId, setLoadingId]     = useState<number | null>(null)
  const [errorMap, setErrorMap]       = useState<Record<number, string>>({})

  // Always-fresh refs — eliminates any stale-closure risk from useCallback deps
  const mainActorIdRef  = useRef(mainActorId)
  const filmsCacheRef   = useRef(filmsCache)
  const expandedIdRef   = useRef(expandedId)
  mainActorIdRef.current  = mainActorId
  filmsCacheRef.current   = filmsCache
  expandedIdRef.current   = expandedId

  const handleRowClick = useCallback(async (collaboratorId: number) => {
    // Collapse if already open
    if (expandedIdRef.current === collaboratorId) {
      setExpandedId(null)
      return
    }
    setExpandedId(collaboratorId)

    // Already cached — no fetch needed
    if (filmsCacheRef.current[collaboratorId] !== undefined) return

    const actorId = mainActorIdRef.current
    setLoadingId(collaboratorId)
    try {
      const films = await fetchSharedFilms(actorId, collaboratorId)
      films.sort((a, b) => b.release_year - a.release_year)
      setFilmsCache(prev => ({ ...prev, [collaboratorId]: films }))
    } catch (err) {
      console.error('[CollabList] fetchSharedFilms failed:', actorId, collaboratorId, err)
      setErrorMap(prev => ({ ...prev, [collaboratorId]: 'Failed to load films' }))
      setFilmsCache(prev => ({ ...prev, [collaboratorId]: [] }))
    } finally {
      setLoadingId(null)
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []) // stable — reads latest values via refs

  if (!collaborators.length) {
    return (
      <div className="flex flex-col gap-3">
        <p className="text-[10px] font-bold uppercase tracking-[0.2em] text-white/30 mb-1">
          {actorName}
        </p>
        <p className="text-sm text-white/25 py-4">No collaborator data available</p>
      </div>
    )
  }

  return (
    <div className="flex flex-col gap-3">
      {/* Section label */}
      <p className="text-[10px] font-bold uppercase tracking-[0.2em] text-white/30 mb-1">
        {actorName}
      </p>

      <div className="flex flex-col gap-2">
        {collaborators.map((c, i) => {
          const isOpen = expandedId === c.actor_id
          const isLoading = loadingId === c.actor_id
          const cachedFilms = filmsCache[c.actor_id] ?? null

          return (
            <div
              key={c.actor_id}
              className="glass rounded-xl overflow-hidden transition-all"
              style={{
                border: isOpen
                  ? `1px solid ${accentColor}33`
                  : '1px solid transparent',
              }}
            >
              {/* Row */}
              <button
                className="w-full px-4 py-3 flex items-center gap-3 hover:bg-white/[0.06] transition-colors group text-left"
                onClick={() => c.actor_id && handleRowClick(c.actor_id)}
                aria-expanded={isOpen}
                disabled={!c.actor_id}
              >
                {/* Rank */}
                <span className="text-xs text-white/20 font-mono w-4 flex-shrink-0">
                  {i + 1}
                </span>
                <ActorAvatar name={c.actor} size={36} />
                <div className="flex flex-col gap-0.5 flex-1 min-w-0">
                  <span className="text-sm font-medium text-white/80 truncate">{c.actor}</span>
                  <span className="text-xs" style={{ color: accentColor + 'aa' }}>
                    {c.films} film{c.films !== 1 ? 's' : ''} together
                  </span>
                </div>
                {/* Pill bar */}
                <div className="w-12 h-1.5 bg-white/[0.06] rounded-full overflow-hidden flex-shrink-0">
                  <div
                    className="h-full rounded-full"
                    style={{
                      width: `${Math.min(100, (c.films / (collaborators[0]?.films || 1)) * 100)}%`,
                      background: accentColor,
                      opacity: 0.55,
                    }}
                  />
                </div>
                <ChevronIcon open={isOpen} />
              </button>

              {/* Film dropdown */}
              {isOpen && (
                <FilmDropdown
                  films={isLoading ? null : cachedFilms}
                  loading={isLoading}
                  error={errorMap[c.actor_id]}
                  accentColor={accentColor}
                />
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}
