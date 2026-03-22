import Link from 'next/link'
import ActorAvatar from './ActorAvatar'
import type { Collaborator, DirectorCollab, Actor } from '@/lib/api'

interface CollaborationsSectionProps {
  collaborators: Collaborator[]
  directors: DirectorCollab[]
  allActors: Actor[]
  actorIdMap: Record<string, number>
}

export default function CollaborationsSection({
  collaborators,
  directors,
  allActors,
  actorIdMap,
}: CollaborationsSectionProps) {
  // Build name → gender map from allActors
  const genderMap: Record<string, 'M' | 'F'> = {}
  for (const a of allActors) {
    if (a.name && a.gender) {
      genderMap[a.name.toLowerCase()] = a.gender
    }
  }

  // Lead actresses: collaborators where gender === 'F'
  const actresses = collaborators
    .filter(c => genderMap[c.actor.toLowerCase()] === 'F')
    .slice(0, 8)

  // Top directors
  const topDirs = directors.slice(0, 12)

  // Top co-stars for bar chart (top 8)
  const topCoStars = collaborators.slice(0, 8)
  const maxFilms   = topCoStars[0]?.films ?? 1

  const hasActresses = actresses.length > 0
  const hasDirs      = topDirs.length > 0
  const hasCoStars   = topCoStars.length > 0

  if (!hasActresses && !hasDirs && !hasCoStars) return null

  return (
    <div className="flex flex-col gap-10">

      {/* ── Lead Actresses ──────────────────────────────── */}
      {hasActresses && (
        <div className="flex flex-col gap-4">
          <h2 className="text-lg font-bold text-white/80">✨ Lead Actresses</h2>
          <>
            <style>{`.act-strip::-webkit-scrollbar { display: none; }`}</style>
            <div
              className="act-strip overflow-x-auto"
              style={{ scrollbarWidth: 'none', msOverflowStyle: 'none' }}
            >
              <div className="flex gap-5 pb-2" style={{ width: 'max-content' }}>
                {actresses.map(c => {
                  const actorId = actorIdMap[c.actor]
                  const inner = (
                    <div className="flex flex-col items-center gap-2 flex-shrink-0 group">
                      <div className="ring-2 ring-white/10 group-hover:ring-pink-400/40 rounded-full transition-all">
                        <ActorAvatar name={c.actor} size={64} />
                      </div>
                      <p className="text-white/55 text-xs font-medium text-center group-hover:text-white/80 transition-colors w-16 truncate">
                        {c.actor.split(' ')[0]}
                      </p>
                      <p className="text-white/25 text-[10px] -mt-1">{c.films} films</p>
                    </div>
                  )
                  return actorId ? (
                    <Link key={c.actor} href={`/actors/${actorId}`}>{inner}</Link>
                  ) : (
                    <div key={c.actor}>{inner}</div>
                  )
                })}
              </div>
            </div>
          </>
        </div>
      )}

      {/* ── Directors ───────────────────────────────────── */}
      {hasDirs && (
        <div className="flex flex-col gap-4">
          <h2 className="text-lg font-bold text-white/80">🎬 Directors Worked With</h2>
          <div className="flex flex-wrap gap-2">
            {topDirs.map(d => (
              <span
                key={d.director}
                className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-sm border border-white/[0.08] text-white/60 hover:text-white/80 hover:border-white/20 transition-all cursor-default"
                style={{ background: '#13131a' }}
              >
                {d.director}
                <span className="text-white/25 text-xs font-medium ml-0.5">{d.films}</span>
              </span>
            ))}
          </div>
        </div>
      )}

      {/* ── Top Co-Stars bar chart ───────────────────────── */}
      {hasCoStars && (
        <div className="flex flex-col gap-4">
          <h2 className="text-lg font-bold text-white/80">🔥 Top Co-Stars</h2>
          <div
            className="rounded-2xl p-5 border border-white/[0.07]"
            style={{ background: '#0d0d15' }}
          >
            <div className="flex flex-col gap-3.5">
              {topCoStars.map(c => {
                const pct     = (c.films / maxFilms) * 100
                const actorId = actorIdMap[c.actor]

                return (
                  <div key={c.actor} className="flex items-center gap-3">
                    {/* Name */}
                    {actorId ? (
                      <Link
                        href={`/actors/${actorId}`}
                        className="text-white/60 hover:text-white/90 text-xs font-medium transition-colors flex-shrink-0 w-28 truncate"
                      >
                        {c.actor}
                      </Link>
                    ) : (
                      <span className="text-white/60 text-xs font-medium flex-shrink-0 w-28 truncate">
                        {c.actor}
                      </span>
                    )}

                    {/* Bar */}
                    <div className="flex-1 h-1.5 rounded-full bg-white/[0.07] overflow-hidden">
                      <div
                        className="h-full rounded-full bg-gradient-to-r from-violet-500/70 to-blue-500/70"
                        style={{ width: `${pct}%` }}
                      />
                    </div>

                    {/* Count */}
                    <span className="text-white/35 text-xs flex-shrink-0 w-14 text-right">
                      {c.films} films
                    </span>
                  </div>
                )
              })}
            </div>
          </div>
        </div>
      )}

    </div>
  )
}
