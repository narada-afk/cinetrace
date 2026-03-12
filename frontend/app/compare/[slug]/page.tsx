import Link from 'next/link'
import Image from 'next/image'
import { notFound } from 'next/navigation'

import Header from '@/components/Header'
import ActorAvatar from '@/components/ActorAvatar'
import MissingData from '@/components/MissingData'
import CompareSummary from '@/components/CompareSummary'
import {
  searchActors,
  getActor,
  getActorMovies,
  getActorCollaborators,
  getActorDirectors,
  type ActorProfile,
  type ActorMovie,
  type Collaborator,
  type DirectorCollab,
} from '@/lib/api'

// ── Types ─────────────────────────────────────────────────────────────────────

interface ActorData {
  profile: ActorProfile
  movies: ActorMovie[]
  collaborators: Collaborator[]
  directors: DirectorCollab[]
}

interface PageProps {
  params: { slug: string }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/**
 * "rajinikanth-vs-kamal-haasan" → ["rajinikanth", "kamal haasan"]
 * Splits on the literal "-vs-" token; hyphens within names become spaces.
 */
function parseSlug(slug: string): [string, string] | null {
  const parts = slug.split('-vs-')
  if (parts.length !== 2) return null
  return [
    parts[0].replace(/-/g, ' '),
    parts[1].replace(/-/g, ' '),
  ]
}

/** Title-case every word in a string. */
function toTitleCase(s: string): string {
  return s.replace(/\b\w/g, (c) => c.toUpperCase())
}

/** Sort by popularity descending and take the top N. */
function topMovies(movies: ActorMovie[], n: number): ActorMovie[] {
  return [...movies]
    .sort((a, b) => (b.popularity ?? 0) - (a.popularity ?? 0))
    .slice(0, n)
}

async function fetchActorData(name: string): Promise<ActorData | null> {
  try {
    const results = await searchActors(name)
    if (!results.length) return null

    const id = results[0].id
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

// ── Sub-components ────────────────────────────────────────────────────────────

function ActorPanel({ data }: { data: ActorData }) {
  const { profile, collaborators, directors } = data

  const stats = [
    { label: 'Films',      value: profile.film_count },
    { label: 'Co-Stars',   value: collaborators.length },
    { label: 'Directors',  value: directors.length },
  ]

  return (
    <div className="glass rounded-2xl p-6 flex flex-col items-center gap-5">
      <ActorAvatar name={profile.name} size={96} />

      <div className="flex flex-col items-center gap-2 text-center">
        <h2 className="text-2xl font-bold text-white leading-tight">
          {profile.name}
        </h2>
        <span className="glass rounded-full px-3 py-1 text-xs text-white/50 uppercase tracking-wide">
          {profile.industry}
        </span>
        {(profile.first_film_year || profile.last_film_year) && (
          <p className="text-xs text-white/25">
            {profile.first_film_year ?? '?'}
            {' – '}
            {profile.last_film_year ?? 'Present'}
          </p>
        )}
      </div>

      {/* Stat trio */}
      <div className="grid grid-cols-3 gap-2 w-full">
        {stats.map(({ label, value }) => (
          <div
            key={label}
            className="flex flex-col items-center gap-0.5 bg-white/[0.04] rounded-xl py-4"
          >
            <span className="text-xl font-bold text-white">
              {value.toLocaleString()}
            </span>
            <span className="text-[10px] text-white/35 uppercase tracking-wide">
              {label}
            </span>
          </div>
        ))}
      </div>
    </div>
  )
}

// ── "Who leads?" comparison table ─────────────────────────────────────────────

function StatTable({ data1, data2 }: { data1: ActorData; data2: ActorData }) {
  const rows = [
    {
      label: 'Films',
      v1: data1.profile.film_count,
      v2: data2.profile.film_count,
    },
    {
      label: 'Collaborators',
      v1: data1.collaborators.length,
      v2: data2.collaborators.length,
    },
    {
      label: 'Directors',
      v1: data1.directors.length,
      v2: data2.directors.length,
    },
  ]

  return (
    <div className="glass rounded-2xl overflow-hidden">
      {/* Column headers */}
      <div className="grid grid-cols-3 px-6 py-3 border-b border-white/5">
        <div />
        <p className="text-center text-sm font-medium text-white/40 truncate px-2">
          {data1.profile.name}
        </p>
        <p className="text-center text-sm font-medium text-white/40 truncate px-2">
          {data2.profile.name}
        </p>
      </div>

      {rows.map(({ label, v1, v2 }) => {
        const lead = v1 > v2 ? 1 : v2 > v1 ? 2 : 0
        return (
          <div
            key={label}
            className="grid grid-cols-3 items-center px-6 py-5 border-b border-white/5 last:border-0"
          >
            <span className="text-sm text-white/40">{label}</span>
            <span
              className={`text-center text-2xl font-bold tabular-nums ${
                lead === 1 ? 'text-emerald-400' : 'text-white/50'
              }`}
            >
              {v1.toLocaleString()}
            </span>
            <span
              className={`text-center text-2xl font-bold tabular-nums ${
                lead === 2 ? 'text-emerald-400' : 'text-white/50'
              }`}
            >
              {v2.toLocaleString()}
            </span>
          </div>
        )
      })}
    </div>
  )
}

// ── Top collaborators column ───────────────────────────────────────────────────

function CollaboratorList({
  actorName,
  collaborators,
}: {
  actorName: string
  collaborators: Collaborator[]
}) {
  if (!collaborators.length) return <MissingData type="cast" />

  return (
    <div className="flex flex-col gap-3">
      <h3 className="text-xs font-semibold text-white/40 uppercase tracking-wide">
        {actorName}
      </h3>
      <div className="flex flex-col gap-2">
        {collaborators.map((c) => (
          <div
            key={c.actor}
            className="glass rounded-xl px-4 py-3 flex items-center gap-3 hover:bg-white/[0.06] transition-colors"
          >
            <ActorAvatar name={c.actor} size={36} />
            <div className="flex flex-col gap-0.5 min-w-0">
              <span className="text-sm font-medium text-white/80 truncate">
                {c.actor}
              </span>
              <span className="text-xs text-white/30">
                {c.films} film{c.films !== 1 ? 's' : ''} together
              </span>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

// ── Mini movie card (filmography preview) ─────────────────────────────────────

function MiniMovieCard({ movie }: { movie: ActorMovie }) {
  const hasRating =
    movie.vote_average !== null &&
    movie.vote_average !== undefined &&
    movie.vote_average > 0

  const rating = hasRating ? movie.vote_average!.toFixed(1) : null
  const isVintage = movie.release_year > 0 && movie.release_year < 1980

  return (
    <div className="flex flex-col gap-1.5 hover:scale-[1.03] transition-transform duration-200 cursor-default">
      {/* Poster */}
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

        {rating && (
          <div className="absolute top-1.5 right-1.5 bg-black/70 backdrop-blur-sm rounded-full px-1.5 py-0.5 text-[10px] font-semibold text-yellow-400">
            ★ {rating}
          </div>
        )}
      </div>

      {/* Meta */}
      <div className="flex flex-col gap-0.5 px-0.5">
        <span className="text-xs font-medium text-white/80 leading-snug line-clamp-2">
          {movie.title}
        </span>
        <span className="text-[10px] text-white/35">
          {movie.release_year > 0 ? movie.release_year : 'Coming Soon'}
        </span>
        {!hasRating && <MissingData type="rating" />}
      </div>
    </div>
  )
}

function FilmPreview({
  actorName,
  movies,
}: {
  actorName: string
  movies: ActorMovie[]
}) {
  if (!movies.length) return <MissingData type="cast" />

  return (
    <div className="flex flex-col gap-3">
      <h3 className="text-xs font-semibold text-white/40 uppercase tracking-wide">
        {actorName}
      </h3>
      <div className="grid grid-cols-3 gap-3">
        {movies.map((movie, i) => (
          <MiniMovieCard key={`${movie.title}-${i}`} movie={movie} />
        ))}
      </div>
    </div>
  )
}

// ── Metadata ──────────────────────────────────────────────────────────────────

export async function generateMetadata({ params }: PageProps) {
  const names = parseSlug(params.slug)
  if (!names) return { title: 'Compare · South Cinema Analytics' }
  const [n1, n2] = names.map(toTitleCase)
  return {
    title: `${n1} vs ${n2} · South Cinema Analytics`,
    description: `Side-by-side comparison of ${n1} and ${n2} — films, collaborators, directors.`,
  }
}

// ── Page ──────────────────────────────────────────────────────────────────────

export default async function ComparePage({ params }: PageProps) {
  const names = parseSlug(params.slug)
  if (!names) notFound()

  const [data1, data2] = await Promise.all([
    fetchActorData(names[0]),
    fetchActorData(names[1]),
  ])

  if (!data1 || !data2) notFound()

  const films1 = topMovies(data1.movies, 6)
  const films2 = topMovies(data2.movies, 6)

  return (
    <div className="min-h-screen bg-[#0a0a0f]">
      <Header />

      <main className="max-w-[1200px] mx-auto px-6 pb-24 flex flex-col gap-12">

        {/* ── Back link + Title ──────────────────────────────────── */}
        <div className="flex flex-col gap-4 pt-2">
          <Link
            href="/"
            className="text-sm text-white/30 hover:text-white/60 transition-colors w-fit"
          >
            ← Back to Insights
          </Link>

          <div className="flex flex-col gap-1">
            <h1 className="text-3xl md:text-4xl font-bold text-white leading-tight">
              {data1.profile.name}{' '}
              <span className="text-white/25">vs</span>{' '}
              {data2.profile.name}
            </h1>
            <p className="text-sm text-white/30">Side-by-side comparison</p>
          </div>
        </div>

        {/* ── Summary Card (screenshot-friendly) ────────────────── */}
        <CompareSummary
          actorA={{
            name:               data1.profile.name,
            filmCount:          data1.profile.film_count,
            collaboratorCount:  data1.collaborators.length,
            directorCount:      data1.directors.length,
          }}
          actorB={{
            name:               data2.profile.name,
            filmCount:          data2.profile.film_count,
            collaboratorCount:  data2.collaborators.length,
            directorCount:      data2.directors.length,
          }}
        />

        {/* ── Actor Panels ───────────────────────────────────────── */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-10">
          <ActorPanel data={data1} />
          <ActorPanel data={data2} />
        </div>

        {/* ── Who leads? ─────────────────────────────────────────── */}
        <section className="flex flex-col gap-4">
          <h2 className="text-lg font-bold text-white/80">Who leads?</h2>
          <StatTable data1={data1} data2={data2} />
        </section>

        {/* ── Top Collaborators ──────────────────────────────────── */}
        <section className="flex flex-col gap-4">
          <h2 className="text-lg font-bold text-white/80">Top Collaborators</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-10">
            <CollaboratorList
              actorName={data1.profile.name}
              collaborators={data1.collaborators.slice(0, 5)}
            />
            <CollaboratorList
              actorName={data2.profile.name}
              collaborators={data2.collaborators.slice(0, 5)}
            />
          </div>
        </section>

        {/* ── Filmography Preview ────────────────────────────────── */}
        <section className="flex flex-col gap-4">
          <h2 className="text-lg font-bold text-white/80">Filmography</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-10">
            <FilmPreview actorName={data1.profile.name} movies={films1} />
            <FilmPreview actorName={data2.profile.name} movies={films2} />
          </div>
        </section>

      </main>
    </div>
  )
}
