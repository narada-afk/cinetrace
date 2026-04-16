import { notFound, redirect } from 'next/navigation'
import TrackEvent from '@/components/TrackEvent'
import Header from '@/components/Header'
import ActorHero from '@/components/ActorHero'
import FilmographyPreview from '@/components/FilmographyPreview'
import CollaborationsSection from '@/components/CollaborationsSection'
import CompareSection from '@/components/CompareSection'
import ActorConnections from '@/components/ActorConnections'
import ActorInsightsCarousel from '@/components/ActorInsightsCarousel'
import FullFilmography from '@/components/FullFilmography'
import {
  getActor,
  getActorMovies,
  getActorCollaborators,
  getActorLeadCollaborators,
  getActorDirectors,
  getActorBlockbusters,
  getActors,
  searchActors,
} from '@/lib/api'

// NOTE: Do NOT add `force-dynamic` here — it disables the Data Cache and causes
// the backend to be hit on every single request. Accessing `params`/`searchParams`
// already makes this page dynamic; the fetch cache still works without force-dynamic.

interface PageProps {
  params: { slug: string }
  searchParams: { compare?: string }
}

export default async function ActorPage({ params, searchParams }: PageProps) {
  const slug = params.slug

  // Resolve slug → numeric actor ID
  // Supports both legacy numeric IDs and name slugs (e.g. "venkatesh", "jr-ntr")
  let id: number | string = slug
  if (!/^\d+$/.test(slug)) {
    const nameFromSlug = slug.replace(/-/g, ' ')
    const searchResults = await searchActors(nameFromSlug).catch(() => [])
    if (!searchResults.length) notFound()
    id = searchResults[0].id
  }

  // ?compare=<id> — redirect to the dedicated compare page
  // e.g. /actors/kamalhaasan?compare=2  →  /compare/kamalhaasan-vs-rajinikanth
  if (searchParams.compare) {
    const compareId = searchParams.compare
    const [actor, compareActor] = await Promise.all([
      getActor(id).catch(() => null),
      getActor(compareId).catch(() => null),
    ])
    if (actor && compareActor) {
      const slug1 = actor.name.toLowerCase().replace(/\s+/g, '')
      const slug2 = compareActor.name.toLowerCase().replace(/\s+/g, '')
      redirect(`/compare/${slug1}-vs-${slug2}`)
    }
    // If either actor doesn't exist, fall through to normal page rendering
  }

  // Fetch all data in parallel — individual failures are caught gracefully.
  // We fetch the full actor list once and derive the female subset client-side,
  // saving one redundant backend call (the /actors?gender=F endpoint).
  const [actor, movies, collaborators, leadCollaborators, directors, blockbusters, allActors] = await Promise.all([
    getActor(id).catch(() => null),
    getActorMovies(id).catch(() => []),
    getActorCollaborators(id).catch(() => []),
    getActorLeadCollaborators(id).catch(() => []),
    getActorDirectors(id).catch(() => []),
    getActorBlockbusters(id).catch(() => []),
    getActors().catch(() => []),
  ])
  // Derive female actors from the already-fetched full list
  const allFemaleActors = allActors.filter(a => a.gender === 'F')

  if (!actor) notFound()

  const numericId = Number(id)

  // Build name → id map from full actors list for collaborator linking
  const actorIdMap: Record<string, number> = {}
  for (const a of allActors) {
    if (a.name) actorIdMap[a.name] = a.id
  }

  // gender isn't on the single-actor endpoint — look it up from allActors list
  const actorGender = allActors.find(a => a.id === numericId)?.gender ?? null

  // Compare suggestions: primary-tier only, same gender, excluding self
  // actor_tier === 'primary' filters out supporting/character actors (Nassar, Prakash Raj etc.)
  const suggestions = allActors
    .filter(a => a.id !== numericId)
    .filter(a => a.actor_tier === 'primary')
    .filter(a => !actorGender || a.gender === actorGender)
    .slice(0, 8)

  // Earliest film for hero "First film" line — exclude TBA (year 0 or null)
  const firstFilm = movies.length > 0
    ? [...movies]
        .filter(m => m.release_year && m.release_year > 0)
        .sort((a, b) => a.release_year - b.release_year)[0] ?? null
    : null

  return (
    <div className="min-h-screen bg-[#0a0a0f]">
      <TrackEvent event="actor_viewed" props={{ actor_id: actor.id, actor_name: actor.name, industry: actor.industry }} />
      <Header />

      <main className="max-w-[1200px] mx-auto px-6 pb-24 flex flex-col gap-14">

        {/* ── 1. Hero ───────────────────────────────────────────── */}
        <ActorHero
          actor={actor}
          collaboratorCount={collaborators.length}
          directorCount={directors.length}
          firstFilm={firstFilm ? { title: firstFilm.title, year: firstFilm.release_year } : null}
        />

        {/* ── 2. Filmography Preview (horizontal strip) — released films only ── */}
        {movies.length > 0 && (
          <FilmographyPreview
            movies={movies.filter(m => m.release_year && m.release_year > 0)}
            totalCount={movies.length}
          />
        )}

        {/* ── 3. Collaborations (actresses · directors · co-stars) ── */}
        {(collaborators.length > 0 || directors.length > 0) && (
          <CollaborationsSection
            collaborators={collaborators}
            leadCollaborators={leadCollaborators}
            directors={directors}
            blockbusters={blockbusters}
            movies={movies}
            allActors={allActors}
            allFemaleActors={allFemaleActors}
            actorIdMap={actorIdMap}
            actorGender={actorGender}
          />
        )}

        {/* ── 4. Compare ────────────────────────────────────────── */}
        <CompareSection
          currentActor={{ id: Number(id), name: actor.name }}
          suggestions={suggestions}
          actorGender={actorGender}
        />

        {/* ── 5. Connections (inline BFS finder) ────────────────── */}
        <ActorConnections
          actor={{ id: Number(id), name: actor.name, industry: actor.industry }}
        />

        {/* ── 6. Insights (synthetic cards) ─────────────────────── */}
        <ActorInsightsCarousel
          actor={actor}
          actorGender={actorGender}
          collaborators={collaborators}
          leadCollaborators={leadCollaborators}
          directors={directors}
          blockbusters={blockbusters}
          allFemaleActors={allFemaleActors}
          movies={movies}
        />

        {/* ── 7. Full Filmography (expandable grid) ─────────────── */}
        {movies.length > 0 && (
          <FullFilmography movies={movies} />
        )}

      </main>
    </div>
  )
}
