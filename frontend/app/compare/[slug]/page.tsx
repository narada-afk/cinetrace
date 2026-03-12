import Link from 'next/link'
import Image from 'next/image'
import { notFound } from 'next/navigation'

import Header from '@/components/Header'
import ActorAvatar from '@/components/ActorAvatar'
import MissingData from '@/components/MissingData'
import ShareButton from '@/components/ShareButton'
import VerdictCard from '@/components/VerdictCard'
import { calcYearsActive, calcAvgRating } from '@/lib/metrics'
import {
  searchActors,
  getActor,
  getActorMovies,
  getActorCollaborators,
  getActorDirectors,
  getSharedFilms,
  type ActorProfile,
  type ActorMovie,
  type Collaborator,
  type DirectorCollab,
  type SharedFilm,
} from '@/lib/api'

// ── Types ──────────────────────────────────────────────────────────────────────

interface ActorData {
  profile: ActorProfile
  movies: ActorMovie[]
  collaborators: Collaborator[]
  directors: DirectorCollab[]
}

interface PageProps {
  params: { slug: string }
}

// ── Data helpers ───────────────────────────────────────────────────────────────

function parseSlug(slug: string): [string, string] | null {
  const parts = slug.split('-vs-')
  if (parts.length !== 2) return null
  if (/^\d+$/.test(parts[0]) && /^\d+$/.test(parts[1])) return [parts[0], parts[1]]
  return [parts[0].replace(/-/g, ' '), parts[1].replace(/-/g, ' ')]
}

function toTitleCase(s: string): string {
  return s.replace(/\b\w/g, (c) => c.toUpperCase())
}

/** Pick the backdrop from the most popular film that has one. */
function getBestBackdrop(movies: ActorMovie[]): string | null {
  return (
    [...movies]
      .filter((m) => m.backdrop_url)
      .sort((a, b) => (b.popularity ?? 0) - (a.popularity ?? 0))[0]
      ?.backdrop_url ?? null
  )
}

/** Sort by popularity descending, take top N. */
function topMovies(movies: ActorMovie[], n: number): ActorMovie[] {
  return [...movies].sort((a, b) => (b.popularity ?? 0) - (a.popularity ?? 0)).slice(0, n)
}

/** Highest vote_average, must have rating > 0. */
function highestRated(movies: ActorMovie[]): ActorMovie | null {
  const rated = movies.filter((m) => (m.vote_average ?? 0) > 0)
  if (!rated.length) return null
  return rated.reduce((best, m) => ((m.vote_average ?? 0) > (best.vote_average ?? 0) ? m : best))
}

/** Latest release by year. */
function latestFilm(movies: ActorMovie[]): ActorMovie | null {
  const released = movies.filter((m) => m.release_year > 0)
  if (!released.length) return null
  return released.reduce((a, b) => (b.release_year > a.release_year ? b : a))
}

/** Find actors who appear in BOTH collaborators lists. */
function findSharedCollaborators(
  c1: Collaborator[],
  c2: Collaborator[],
  name1: string,
  name2: string,
  limit = 5,
): { name: string; films1: number; films2: number }[] {
  const map2 = new Map(c2.map((c) => [c.actor.toLowerCase(), c.films]))
  return c1
    .filter((c) => {
      const l = c.actor.toLowerCase()
      return l !== name2.toLowerCase() && l !== name1.toLowerCase() && map2.has(l)
    })
    .map((c) => ({ name: c.actor, films1: c.films, films2: map2.get(c.actor.toLowerCase())! }))
    .sort((a, b) => b.films1 + b.films2 - (a.films1 + a.films2))
    .slice(0, limit)
}

/** Count films per release year (1975–2026 window). */
function buildTimeline(movies: ActorMovie[]): Map<number, number> {
  const map = new Map<number, number>()
  for (const m of movies) {
    const y = m.release_year
    if (y >= 1975 && y <= 2026) map.set(y, (map.get(y) ?? 0) + 1)
  }
  return map
}

/** Auto-generate 2–3 insight strings from actor data. */
function generateInsights(d1: ActorData, d2: ActorData): string[] {
  const insights: string[] = []

  // Director richness
  const dirs1 = d1.directors.length
  const dirs2 = d2.directors.length
  if (Math.abs(dirs1 - dirs2) >= 3) {
    const [more, less] = dirs1 > dirs2 ? [d1, d2] : [d2, d1]
    insights.push(
      `${more.profile.name} has explored ${Math.max(dirs1, dirs2)} different directors — ${Math.abs(dirs1 - dirs2)} more than ${less.profile.name}, suggesting broader creative range.`,
    )
  }

  // Highest rated films
  const top1 = highestRated(d1.movies)
  const top2 = highestRated(d2.movies)
  if (top1 && top2) {
    const r1 = top1.vote_average!.toFixed(1)
    const r2 = top2.vote_average!.toFixed(1)
    insights.push(
      `${d1.profile.name}'s highest-rated film is "${top1.title}" (${r1}/10), while ${d2.profile.name}'s is "${top2.title}" (${r2}/10).`,
    )
  }

  // Collaborator network
  const c1 = d1.collaborators.length
  const c2 = d2.collaborators.length
  if (Math.abs(c1 - c2) >= 15) {
    const more = c1 > c2 ? d1 : d2
    insights.push(
      `${more.profile.name} has appeared on screen with ${Math.max(c1, c2)} different co-stars — one of the widest networks in South Indian cinema.`,
    )
  }

  // Peak year
  const peak = (movies: ActorMovie[]) => {
    const byYear = buildTimeline(movies)
    if (!byYear.size) return null
    return [...byYear.entries()].reduce((a, b) => (b[1] > a[1] ? b : a))
  }
  const p1 = peak(d1.movies)
  const p2 = peak(d2.movies)
  if (p1 && p2 && p1[0] !== p2[0]) {
    insights.push(
      `${d1.profile.name} was most prolific in ${p1[0]} (${p1[1]} film${p1[1] > 1 ? 's' : ''}), while ${d2.profile.name} peaked in ${p2[0]} (${p2[1]} film${p2[1] > 1 ? 's' : ''}).`,
    )
  }

  return insights.slice(0, 3)
}

/** Generate a short rivalry narrative paragraph. */
function generateRivalryStory(d1: ActorData, d2: ActorData): string {
  const { profile: p1, directors: dirs1, collaborators: c1 } = d1
  const { profile: p2, directors: dirs2, collaborators: c2 } = d2

  const parts: string[] = []

  // Entry into the industry
  if (p1.first_film_year && p2.first_film_year) {
    const diff = Math.abs(p1.first_film_year - p2.first_film_year)
    if (diff === 0) {
      parts.push(`${p1.name} and ${p2.name} both made their on-screen debuts in ${p1.first_film_year}, launching parallel careers that have unfolded across the same era.`)
    } else {
      const [earlier, later] = p1.first_film_year < p2.first_film_year ? [p1, p2] : [p2, p1]
      parts.push(
        `${earlier.name} entered the industry ${diff > 1 ? `${diff} years` : 'a year'} before ${later.name}, building a head start in filmography that ${later.name} has been steadily closing.`,
      )
    }
  }

  // Films leader
  if (p1.film_count !== p2.film_count) {
    const [more, less] = p1.film_count > p2.film_count ? [p1, p2] : [p2, p1]
    const gap = Math.abs(p1.film_count - p2.film_count)
    parts.push(
      `${more.name} edges ahead with ${Math.max(p1.film_count, p2.film_count)} total productions — ${gap} more than ${less.name}'s ${Math.min(p1.film_count, p2.film_count)}.`,
    )
  }

  // Director diversity
  if (dirs1.length !== dirs2.length) {
    const [more, less] = dirs1.length > dirs2.length ? [p1, p2] : [p2, p1]
    parts.push(
      `In terms of creative collaborations, ${more.name} has worked with more directors, reflecting a broader stylistic range than ${less.name}.`,
    )
  }

  // Collaborator network
  if (c1.length !== c2.length) {
    const wider = c1.length > c2.length ? p1 : p2
    parts.push(`${wider.name} has built the wider on-screen network, sharing the frame with more co-stars across their career.`)
  }

  return parts.length ? parts.join(' ') : `${p1.name} and ${p2.name} represent two distinct forces in South Indian cinema, each bringing their own style and filmography to this rivalry.`
}

async function fetchActorData(nameOrId: string): Promise<ActorData | null> {
  try {
    let id: number | string
    if (/^\d+$/.test(nameOrId)) {
      id = parseInt(nameOrId, 10)
    } else {
      const results = await searchActors(nameOrId)
      if (!results.length) return null
      id = results[0].id
    }
    const [profile, movies, collaborators, directors] = await Promise.all([
      getActor(id).catch(() => null),
      getActorMovies(id).catch(() => [] as ActorMovie[]),
      getActorCollaborators(id).catch(() => [] as Collaborator[]),
      getActorDirectors(id).catch(() => [] as DirectorCollab[]),
    ])
    if (!profile) return null
    return { profile, movies, collaborators, directors }
  } catch {
    return null
  }
}

// ── Section label ─────────────────────────────────────────────────────────────

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <p className="text-[10px] font-bold text-white/25 uppercase tracking-[0.2em] mb-4">
      {children}
    </p>
  )
}

// ── TASK 1: Hero Banner ────────────────────────────────────────────────────────

function HeroBanner({ data1, data2 }: { data1: ActorData; data2: ActorData }) {
  const { profile: p1 } = data1
  const { profile: p2 } = data2
  const backdrop1 = getBestBackdrop(data1.movies)
  const backdrop2 = getBestBackdrop(data2.movies)

  return (
    <div className="relative w-full rounded-3xl overflow-hidden" style={{ minHeight: '260px' }}>
      {/* Two half-panels side by side */}
      <div className="flex h-full" style={{ minHeight: '260px' }}>

        {/* ── Left: Actor 1 ── */}
        <div className="relative flex-1 overflow-hidden">
          {backdrop1 ? (
            <Image
              src={backdrop1}
              alt={p1.name}
              fill
              sizes="50vw"
              className="object-cover object-center"
              priority
            />
          ) : (
            <div className="absolute inset-0 bg-gradient-to-br from-amber-950/60 via-amber-900/20 to-transparent" />
          )}
          {/* Dark overlay — stronger on left edge, fades toward centre */}
          <div className="absolute inset-0 bg-gradient-to-r from-[#0a0a0f] via-[#0a0a0f]/75 to-[#0a0a0f]/40" />

          {/* Actor info */}
          <div className="relative z-10 flex flex-col gap-2 p-6 pb-8 h-full justify-end">
            <span
              className="self-start text-[11px] font-bold uppercase tracking-widest px-2.5 py-0.5 rounded-full"
              style={{ background: 'rgba(245,158,11,0.15)', color: '#f59e0b' }}
            >
              {p1.industry}
            </span>
            <h2 className="text-2xl sm:text-3xl font-bold text-white leading-tight drop-shadow-lg">
              {p1.name}
            </h2>
            {(p1.first_film_year || p1.last_film_year) && (
              <p className="text-sm text-white/45">
                {p1.first_film_year ?? '?'} – {p1.last_film_year ?? 'Present'}
              </p>
            )}
          </div>
        </div>

        {/* ── Right: Actor 2 ── */}
        <div className="relative flex-1 overflow-hidden">
          {backdrop2 ? (
            <Image
              src={backdrop2}
              alt={p2.name}
              fill
              sizes="50vw"
              className="object-cover object-center"
              priority
            />
          ) : (
            <div className="absolute inset-0 bg-gradient-to-bl from-cyan-950/60 via-cyan-900/20 to-transparent" />
          )}
          {/* Dark overlay — stronger on right edge, fades toward centre */}
          <div className="absolute inset-0 bg-gradient-to-l from-[#0a0a0f] via-[#0a0a0f]/75 to-[#0a0a0f]/40" />

          {/* Actor info */}
          <div className="relative z-10 flex flex-col items-end gap-2 p-6 pb-8 h-full justify-end">
            <span
              className="self-end text-[11px] font-bold uppercase tracking-widest px-2.5 py-0.5 rounded-full"
              style={{ background: 'rgba(6,182,212,0.15)', color: '#06b6d4' }}
            >
              {p2.industry}
            </span>
            <h2 className="text-2xl sm:text-3xl font-bold text-white leading-tight text-right drop-shadow-lg">
              {p2.name}
            </h2>
            {(p2.first_film_year || p2.last_film_year) && (
              <p className="text-sm text-white/45">
                {p2.first_film_year ?? '?'} – {p2.last_film_year ?? 'Present'}
              </p>
            )}
          </div>
        </div>
      </div>

      {/* ── VS badge — absolute centre ── */}
      <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 z-20">
        <div
          className="w-14 h-14 rounded-full flex items-center justify-center border border-white/10 backdrop-blur-sm shadow-2xl"
          style={{ background: '#0a0a0f' }}
        >
          <span className="text-white/50 font-black text-sm tracking-wide">VS</span>
        </div>
      </div>
    </div>
  )
}

// ── TASK 2: Verdict Card ───────────────────────────────────────────────────────

// ── TASK 3: Shared Collaborators ──────────────────────────────────────────────

function SharedCollaboratorsSection({
  shared,
  name1,
  name2,
}: {
  shared: { name: string; films1: number; films2: number }[]
  name1: string
  name2: string
}) {
  if (!shared.length) {
    return (
      <div className="glass rounded-2xl px-6 py-8 text-center text-white/25 text-sm">
        No shared collaborators found — these two stars travel in different circles.
      </div>
    )
  }

  return (
    <div className="flex flex-col gap-3">
      {shared.map((c) => (
        <div
          key={c.name}
          className="glass rounded-2xl px-5 py-4 flex items-center gap-4 hover:bg-white/[0.06] transition-colors"
        >
          <ActorAvatar name={c.name} size={44} />
          <div className="flex-1 min-w-0">
            <p className="text-sm font-semibold text-white/85 truncate">{c.name}</p>
            <div className="flex gap-3 mt-1 flex-wrap">
              <span
                className="text-[11px] px-2 py-0.5 rounded-full"
                style={{ background: 'rgba(245,158,11,0.12)', color: '#f59e0b' }}
              >
                {c.films1} film{c.films1 !== 1 ? 's' : ''} with {name1.split(' ')[0]}
              </span>
              <span
                className="text-[11px] px-2 py-0.5 rounded-full"
                style={{ background: 'rgba(6,182,212,0.12)', color: '#06b6d4' }}
              >
                {c.films2} film{c.films2 !== 1 ? 's' : ''} with {name2.split(' ')[0]}
              </span>
            </div>
          </div>
          <span className="text-white/20 text-sm font-bold flex-shrink-0">
            {c.films1 + c.films2}
          </span>
        </div>
      ))}
    </div>
  )
}

// ── TASK 4: Did You Know ──────────────────────────────────────────────────────

function DidYouKnow({ insights }: { insights: string[] }) {
  if (!insights.length) return null

  const EMOJIS = ['💡', '🎬', '⭐']

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
      {insights.map((text, i) => (
        <div
          key={i}
          className="glass rounded-2xl p-5 flex flex-col gap-3"
          style={{
            background:
              i === 0
                ? 'rgba(245,158,11,0.05)'
                : i === 1
                ? 'rgba(255,255,255,0.03)'
                : 'rgba(6,182,212,0.05)',
          }}
        >
          <span className="text-xl">{EMOJIS[i]}</span>
          <p className="text-sm text-white/65 leading-relaxed">{text}</p>
        </div>
      ))}
    </div>
  )
}

// ── TASK 5: Career Timeline ───────────────────────────────────────────────────

function CareerTimeline({
  movies1,
  movies2,
  name1,
  name2,
}: {
  movies1: ActorMovie[]
  movies2: ActorMovie[]
  name1: string
  name2: string
}) {
  const c1 = buildTimeline(movies1)
  const c2 = buildTimeline(movies2)
  const allYears = [...new Set([...c1.keys(), ...c2.keys()])].sort()

  if (allYears.length < 3) return null

  const minYear = allYears[0]
  const maxYear = allYears[allYears.length - 1]
  const yearSpan = maxYear - minYear || 1

  const maxCount = Math.max(...allYears.flatMap((y) => [c1.get(y) ?? 0, c2.get(y) ?? 0]))
  if (maxCount === 0) return null

  const W = 600
  const H = 150
  const PAD = { t: 10, r: 16, b: 28, l: 24 }
  const cW = W - PAD.l - PAD.r
  const cH = H - PAD.t - PAD.b

  const toX = (y: number) => PAD.l + ((y - minYear) / yearSpan) * cW
  const toY = (c: number) => PAD.t + cH - (c / maxCount) * cH

  const pts1 = allYears.map((y) => `${toX(y)},${toY(c1.get(y) ?? 0)}`).join(' ')
  const pts2 = allYears.map((y) => `${toX(y)},${toY(c2.get(y) ?? 0)}`).join(' ')

  // X labels: every 5 years, starting from the nearest multiple of 5
  const firstLabel = Math.ceil(minYear / 5) * 5
  const xLabels: number[] = []
  for (let y = firstLabel; y <= maxYear; y += 5) xLabels.push(y)

  // Y labels: 0, mid, max
  const yLabels = [0, Math.ceil(maxCount / 2), maxCount]

  return (
    <div className="glass rounded-3xl p-6">
      {/* Legend */}
      <div className="flex gap-6 mb-4">
        <div className="flex items-center gap-2">
          <div className="w-5 h-0.5 rounded-full" style={{ background: '#f59e0b' }} />
          <span className="text-xs text-white/45">{name1}</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-5 h-0.5 rounded-full" style={{ background: '#06b6d4' }} />
          <span className="text-xs text-white/45">{name2}</span>
        </div>
      </div>

      <svg
        viewBox={`0 0 ${W} ${H}`}
        className="w-full h-auto"
        aria-label={`Career timeline: ${name1} vs ${name2}`}
      >
        {/* Horizontal grid */}
        {yLabels.map((v) => (
          <line
            key={v}
            x1={PAD.l}
            y1={toY(v)}
            x2={W - PAD.r}
            y2={toY(v)}
            stroke="rgba(255,255,255,0.05)"
            strokeWidth="1"
          />
        ))}

        {/* Y-axis labels */}
        {yLabels.map((v) => (
          <text
            key={v}
            x={PAD.l - 3}
            y={toY(v) + 4}
            textAnchor="end"
            fill="rgba(255,255,255,0.2)"
            fontSize="8"
          >
            {v}
          </text>
        ))}

        {/* X-axis labels */}
        {xLabels.map((y) => (
          <text
            key={y}
            x={toX(y)}
            y={H - 4}
            textAnchor="middle"
            fill="rgba(255,255,255,0.2)"
            fontSize="8"
          >
            {y}
          </text>
        ))}

        {/* Actor 1 area shadow (subtle) */}
        <polyline
          points={pts1}
          fill="none"
          stroke="#f59e0b"
          strokeWidth="2.5"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeOpacity="0.75"
        />
        {allYears
          .filter((y) => (c1.get(y) ?? 0) > 0)
          .map((y) => (
            <circle
              key={y}
              cx={toX(y)}
              cy={toY(c1.get(y)!)}
              r="3.5"
              fill="#f59e0b"
              fillOpacity="0.9"
            />
          ))}

        {/* Actor 2 line */}
        <polyline
          points={pts2}
          fill="none"
          stroke="#06b6d4"
          strokeWidth="2.5"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeOpacity="0.75"
        />
        {allYears
          .filter((y) => (c2.get(y) ?? 0) > 0)
          .map((y) => (
            <circle
              key={y}
              cx={toX(y)}
              cy={toY(c2.get(y)!)}
              r="3.5"
              fill="#06b6d4"
              fillOpacity="0.9"
            />
          ))}
      </svg>
    </div>
  )
}

// ── TASK 6: Improved Collaborators ────────────────────────────────────────────

function CollaboratorList({
  actorName,
  collaborators,
  accentColor,
}: {
  actorName: string
  collaborators: Collaborator[]
  accentColor: string
}) {
  if (!collaborators.length) return <MissingData type="cast" />

  return (
    <div className="flex flex-col gap-3">
      <SectionLabel>{actorName}</SectionLabel>
      <div className="flex flex-col gap-2">
        {collaborators.map((c, i) => (
          <div
            key={c.actor}
            className="glass rounded-xl px-4 py-3 flex items-center gap-3 hover:bg-white/[0.06] transition-colors group"
          >
            {/* Rank */}
            <span className="text-xs text-white/20 font-mono w-4 flex-shrink-0">{i + 1}</span>
            <ActorAvatar name={c.actor} size={36} />
            <div className="flex flex-col gap-0.5 flex-1 min-w-0">
              <span className="text-sm font-medium text-white/80 truncate">{c.actor}</span>
              <span className="text-xs" style={{ color: accentColor + 'aa' }}>
                {c.films} film{c.films !== 1 ? 's' : ''} together
              </span>
            </div>
            {/* Pill bar */}
            <div className="w-16 h-1.5 bg-white/[0.06] rounded-full overflow-hidden flex-shrink-0">
              <div
                className="h-full rounded-full"
                style={{
                  width: `${Math.min(100, (c.films / (collaborators[0]?.films || 1)) * 100)}%`,
                  background: accentColor,
                  opacity: 0.6,
                }}
              />
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

// ── TASK 7: Filmography Grid with badges ──────────────────────────────────────

function FilmBadge({ label, emoji }: { label: string; emoji: string }) {
  return (
    <span className="absolute top-1.5 left-1.5 flex items-center gap-0.5 bg-black/75 backdrop-blur-sm rounded-full px-1.5 py-0.5 text-[9px] font-bold text-white">
      {emoji} {label}
    </span>
  )
}

function FilmGrid({
  actorName,
  movies,
  highlightedRatedId,
  highlightedPopId,
  latestId,
}: {
  actorName: string
  movies: ActorMovie[]
  highlightedRatedId: string | null
  highlightedPopId: string | null
  latestId: string | null
}) {
  if (!movies.length) return <MissingData type="cast" />

  return (
    <div className="flex flex-col gap-3">
      <SectionLabel>{actorName}</SectionLabel>
      <div className="grid grid-cols-3 sm:grid-cols-3 gap-3">
        {movies.map((movie, i) => {
          const key = `${movie.title}-${movie.release_year}`
          const isTopRated = key === highlightedRatedId
          const isMostPop = key === highlightedPopId
          const isLatest = key === latestId
          const hasRating = (movie.vote_average ?? 0) > 0
          const isVintage = movie.release_year > 0 && movie.release_year < 1980

          return (
            <div
              key={`${key}-${i}`}
              className="flex flex-col gap-1.5 hover:scale-[1.03] transition-transform duration-200 cursor-default"
            >
              <div className="relative aspect-[2/3] rounded-xl overflow-hidden bg-[#1a1a24]">
                {movie.poster_url ? (
                  <Image
                    src={movie.poster_url}
                    alt={movie.title}
                    fill
                    sizes="(max-width: 768px) 33vw, 15vw"
                    className="object-cover"
                  />
                ) : isVintage ? (
                  <MissingData type="poster_old" title={movie.title} />
                ) : (
                  <MissingData type="poster" title={movie.title} />
                )}

                {/* Rating badge */}
                {hasRating && (
                  <div className="absolute top-1.5 right-1.5 bg-black/70 backdrop-blur-sm rounded-full px-1.5 py-0.5 text-[10px] font-semibold text-yellow-400">
                    ★ {movie.vote_average!.toFixed(1)}
                  </div>
                )}

                {/* Special badges */}
                {isTopRated && <FilmBadge emoji="⭐" label="Top Rated" />}
                {!isTopRated && isMostPop && <FilmBadge emoji="🔥" label="Popular" />}
                {!isTopRated && !isMostPop && isLatest && <FilmBadge emoji="🆕" label="Latest" />}
              </div>

              <div className="flex flex-col gap-0.5 px-0.5">
                <span className="text-xs font-medium text-white/80 leading-snug line-clamp-2">
                  {movie.title}
                </span>
                <span className="text-[10px] text-white/35">
                  {movie.release_year > 0 ? movie.release_year : 'Coming Soon'}
                </span>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ── Task 8: Rivalry Story ─────────────────────────────────────────────────────

function RivalryStory({ story }: { story: string }) {
  return (
    <div
      className="rounded-3xl px-7 py-6"
      style={{
        background:
          'linear-gradient(135deg, rgba(245,158,11,0.07) 0%, rgba(6,182,212,0.07) 100%)',
        border: '1px solid rgba(255,255,255,0.06)',
      }}
    >
      <p className="text-[10px] font-bold uppercase tracking-[0.2em] text-white/30 mb-3">
        The Rivalry
      </p>
      <p className="text-white/65 text-sm leading-relaxed">{story}</p>
    </div>
  )
}

// ── Films Together (enhanced) ─────────────────────────────────────────────────

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

function FilmsTogether({ films, name1, name2 }: { films: SharedFilm[]; name1: string; name2: string }) {
  if (!films.length) {
    return (
      <div className="glass rounded-2xl px-6 py-10 text-center">
        <p className="text-white/30 text-sm mb-2">No shared films found</p>
        <p className="text-white/15 text-xs">These actors haven't starred together in films in our database.</p>
      </div>
    )
  }

  return (
    <div className="flex flex-col gap-3">
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
                <div className="w-full h-full flex items-center justify-center text-white/10">🎬</div>
              )}
            </div>

            <div className="flex flex-col gap-2 py-4 pr-4 flex-1 min-w-0">
              <div className="flex items-baseline gap-2">
                <span className="font-semibold text-white/90 text-sm leading-snug">{film.title}</span>
                <span className="text-xs text-white/30 flex-shrink-0">
                  {film.release_year > 0 ? film.release_year : ''}
                </span>
                {rating && (
                  <span className="text-xs text-yellow-400 flex-shrink-0 ml-auto">★ {rating}</span>
                )}
              </div>
              {film.director && <p className="text-xs text-white/40">Dir. {film.director}</p>}
              <div className="flex flex-wrap gap-2 mt-1">
                <RolePill actorName={name1} character={film.actor1_character} role={film.actor1_role} />
                <RolePill actorName={name2} character={film.actor2_character} role={film.actor2_role} />
              </div>
            </div>
          </div>
        )
      })}
    </div>
  )
}

// ── Metadata ───────────────────────────────────────────────────────────────────

export async function generateMetadata({ params }: PageProps) {
  const names = parseSlug(params.slug)
  if (!names) return { title: 'Compare · South Cinema Analytics' }
  const [n1, n2] = names.map(toTitleCase)
  return {
    title: `${n1} vs ${n2} · South Cinema Analytics`,
    description: `Cinematic head-to-head comparison of ${n1} and ${n2} — films, collaborators, directors, timeline, and shared story.`,
  }
}

// ── Page ───────────────────────────────────────────────────────────────────────

export default async function ComparePage({ params }: PageProps) {
  const names = parseSlug(params.slug)
  if (!names) notFound()

  const [data1, data2] = await Promise.all([fetchActorData(names[0]), fetchActorData(names[1])])
  if (!data1 || !data2) notFound()

  const sharedFilms = await getSharedFilms(data1.profile.id, data2.profile.id).catch(
    () => [] as SharedFilm[],
  )

  // Pre-computed data for various sections
  const topFilms1 = topMovies(data1.movies, 6)
  const topFilms2 = topMovies(data2.movies, 6)

  const allFilms1Keys = topFilms1.map((m) => `${m.title}-${m.release_year}`)
  const allFilms2Keys = topFilms2.map((m) => `${m.title}-${m.release_year}`)

  const topRated1 = highestRated(topFilms1)
  const topRated2 = highestRated(topFilms2)
  const latest1 = latestFilm(topFilms1)
  const latest2 = latestFilm(topFilms2)
  const mostPop1 = topFilms1[0] // already sorted by popularity
  const mostPop2 = topFilms2[0]

  const sharedCollabs = findSharedCollaborators(
    data1.collaborators,
    data2.collaborators,
    data1.profile.name,
    data2.profile.name,
  )

  const insights = generateInsights(data1, data2)
  const rivalryStory = generateRivalryStory(data1, data2)

  const p1 = data1.profile
  const p2 = data2.profile

  // Pre-compute the 5 verdict metrics so Share Card stays in sync
  const shareYrs1 = calcYearsActive(p1)
  const shareYrs2 = calcYearsActive(p2)
  const shareRat1 = calcAvgRating(data1.movies)
  const shareRat2 = calcAvgRating(data2.movies)

  const metrics5 = [
    { v1: p1.film_count,                  v2: p2.film_count },
    { v1: shareYrs1,                      v2: shareYrs2 },
    { v1: shareRat1,                      v2: shareRat2 },
    { v1: data1.directors.length,         v2: data2.directors.length },
    { v1: data1.collaborators.length,     v2: data2.collaborators.length },
  ]
  const wins1 = metrics5.filter((m) => m.v1 > m.v2).length
  const wins2 = metrics5.filter((m) => m.v2 > m.v1).length
  const winner = wins1 > wins2 ? p1.name : wins2 > wins1 ? p2.name : null
  const winnerLeads = Math.max(wins1, wins2)

  return (
    <div className="min-h-screen bg-[#0a0a0f]">
      <Header />

      <main className="max-w-[1000px] mx-auto px-4 sm:px-6 pb-28 flex flex-col gap-10">

        {/* Back link */}
        <div className="pt-3">
          <Link href="/compare" className="text-sm text-white/30 hover:text-white/60 transition-colors">
            ← Back to Compare
          </Link>
        </div>

        {/* ── TASK 1: Hero Banner ──────────────────────────────── */}
        <HeroBanner data1={data1} data2={data2} />

        {/* ── TASK 2: Verdict Card ─────────────────────────────── */}
        <section>
          <SectionLabel>🏆 Verdict</SectionLabel>
          <VerdictCard data1={data1} data2={data2} />
        </section>

        {/* ── TASK 4: Did You Know ─────────────────────────────── */}
        {insights.length > 0 && (
          <section>
            <SectionLabel>💡 Did You Know?</SectionLabel>
            <DidYouKnow insights={insights} />
          </section>
        )}

        {/* ── TASK 8: Rivalry Story ─────────────────────────────── */}
        <RivalryStory story={rivalryStory} />

        {/* ── TASK 5: Career Timeline ──────────────────────────── */}
        <section>
          <SectionLabel>📈 Films Per Year</SectionLabel>
          <CareerTimeline
            movies1={data1.movies}
            movies2={data2.movies}
            name1={p1.name}
            name2={p2.name}
          />
        </section>

        {/* ── Films Together ───────────────────────────────────── */}
        <section>
          <div className="flex items-baseline gap-3 mb-4">
            <SectionLabel>🎬 Films Together</SectionLabel>
            <span className="text-sm text-white/30 -mt-4 ml-1">
              {sharedFilms.length} film{sharedFilms.length !== 1 ? 's' : ''}
            </span>
          </div>
          <FilmsTogether films={sharedFilms} name1={p1.name} name2={p2.name} />
        </section>

        {/* ── TASK 3: Shared Collaborators ─────────────────────── */}
        <section>
          <SectionLabel>🤝 Shared Collaborators</SectionLabel>
          <p className="text-xs text-white/30 mb-4">
            Actors who have worked with both {p1.name.split(' ')[0]} and {p2.name.split(' ')[0]}
          </p>
          <SharedCollaboratorsSection shared={sharedCollabs} name1={p1.name} name2={p2.name} />
        </section>

        {/* ── TASK 6: Top Collaborators ────────────────────────── */}
        <section>
          <SectionLabel>🔥 Top Collaborators</SectionLabel>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
            <CollaboratorList
              actorName={p1.name}
              collaborators={data1.collaborators.slice(0, 8)}
              accentColor="#f59e0b"
            />
            <CollaboratorList
              actorName={p2.name}
              collaborators={data2.collaborators.slice(0, 8)}
              accentColor="#06b6d4"
            />
          </div>
        </section>

        {/* ── TASK 7: Filmography with badges ─────────────────── */}
        <section>
          <SectionLabel>🎥 Top Films</SectionLabel>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
            <FilmGrid
              actorName={p1.name}
              movies={topFilms1}
              highlightedRatedId={topRated1 ? `${topRated1.title}-${topRated1.release_year}` : null}
              highlightedPopId={mostPop1 ? `${mostPop1.title}-${mostPop1.release_year}` : null}
              latestId={latest1 ? `${latest1.title}-${latest1.release_year}` : null}
            />
            <FilmGrid
              actorName={p2.name}
              movies={topFilms2}
              highlightedRatedId={topRated2 ? `${topRated2.title}-${topRated2.release_year}` : null}
              highlightedPopId={mostPop2 ? `${mostPop2.title}-${mostPop2.release_year}` : null}
              latestId={latest2 ? `${latest2.title}-${latest2.release_year}` : null}
            />
          </div>
        </section>

        {/* ── TASK 9: Share Card Generator ─────────────────────── */}
        <section>
          <SectionLabel>📸 Share</SectionLabel>
          {/* Visual share card — designed for screenshots */}
          <div
            id="share-card"
            className="rounded-3xl p-8 mb-6 relative overflow-hidden"
            style={{
              background: 'linear-gradient(135deg, #0a0a0f 0%, #13131a 100%)',
              border: '1px solid rgba(255,255,255,0.06)',
            }}
          >
            {/* Accent lines */}
            <div
              className="absolute top-0 left-0 h-0.5 w-1/2"
              style={{ background: 'linear-gradient(to right, #f59e0b, transparent)' }}
            />
            <div
              className="absolute top-0 right-0 h-0.5 w-1/2"
              style={{ background: 'linear-gradient(to left, #06b6d4, transparent)' }}
            />

            {/* Names */}
            <div className="flex items-center justify-center gap-4 mb-8">
              <h2 className="text-xl sm:text-2xl font-bold text-white text-right flex-1 truncate">
                {p1.name}
              </h2>
              <span className="text-white/20 font-black text-sm px-3 py-1.5 rounded-full glass flex-shrink-0">
                VS
              </span>
              <h2 className="text-xl sm:text-2xl font-bold text-white text-left flex-1 truncate">
                {p2.name}
              </h2>
            </div>

            {/* Stats grid */}
            <div className="grid grid-cols-5 gap-2 mb-6">
              {[
                { label: 'Films',            v1: p1.film_count,              v2: p2.film_count,              d1: String(p1.film_count),         d2: String(p2.film_count) },
                { label: 'Years Active',     v1: shareYrs1,                  v2: shareYrs2,                  d1: String(shareYrs1),             d2: String(shareYrs2) },
                { label: 'Avg Rating',       v1: shareRat1,                  v2: shareRat2,                  d1: shareRat1.toFixed(1),          d2: shareRat2.toFixed(1) },
                { label: 'Unique Directors', v1: data1.directors.length,     v2: data2.directors.length,     d1: String(data1.directors.length), d2: String(data2.directors.length) },
                { label: 'Co-Stars',         v1: data1.collaborators.length, v2: data2.collaborators.length, d1: String(data1.collaborators.length), d2: String(data2.collaborators.length) },
              ].map(({ label, v1, v2, d1, d2 }) => {
                const lead = v1 > v2 ? 1 : v2 > v1 ? 2 : 0
                return (
                  <div key={label} className="glass rounded-2xl p-3 text-center">
                    <p className="text-[8px] text-white/30 uppercase tracking-widest mb-2 leading-tight">{label}</p>
                    <div className="flex items-center justify-center gap-1">
                      <span
                        className="text-base font-bold tabular-nums"
                        style={{ color: lead === 1 ? '#f59e0b' : 'rgba(255,255,255,0.5)' }}
                      >
                        {d1}
                      </span>
                      <span className="text-white/15 text-[10px]">—</span>
                      <span
                        className="text-base font-bold tabular-nums"
                        style={{ color: lead === 2 ? '#06b6d4' : 'rgba(255,255,255,0.5)' }}
                      >
                        {d2}
                      </span>
                    </div>
                  </div>
                )
              })}
            </div>

            {/* Verdict line */}
            {winner ? (
              <p className="text-center text-sm font-semibold"
                style={{ color: winner === p1.name ? '#f59e0b' : '#06b6d4' }}>
                🏆 {winner} leads in {winnerLeads} of 5 metrics
              </p>
            ) : (
              <p className="text-center text-sm text-white/40">All square — perfectly matched</p>
            )}

            {/* Branding */}
            <p className="text-center text-[11px] text-white/20 mt-4">southcinemaanalytics.com</p>
          </div>

          {/* Generate button */}
          <div className="flex flex-col items-center gap-3">
            <ShareButton
              name1={p1.name}
              name2={p2.name}
              industry1={p1.industry}
              industry2={p2.industry}
              films1={p1.film_count}
              films2={p2.film_count}
              yearsActive1={shareYrs1}
              yearsActive2={shareYrs2}
              avgRating1={shareRat1}
              avgRating2={shareRat2}
              uniqueDirs1={data1.directors.length}
              uniqueDirs2={data2.directors.length}
              coStars1={data1.collaborators.length}
              coStars2={data2.collaborators.length}
              winner={winner}
              winnerLeads={winnerLeads}
            />
            <p className="text-xs text-white/20">
              Downloads a 1200×630 PNG — ready for social media
            </p>
          </div>
        </section>

      </main>
    </div>
  )
}
