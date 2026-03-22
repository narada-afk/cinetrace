import { notFound } from 'next/navigation'
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
  getActorDirectors,
  getActors,
} from '@/lib/api'

export const dynamic = 'force-dynamic'

interface PageProps {
  params: { id: string }
}

export default async function ActorPage({ params }: PageProps) {
  const id = params.id

  // Fetch all data in parallel — individual failures are caught gracefully
  const [actor, movies, collaborators, directors, allActors] = await Promise.all([
    getActor(id).catch(() => null),
    getActorMovies(id).catch(() => []),
    getActorCollaborators(id).catch(() => []),
    getActorDirectors(id).catch(() => []),
    getActors().catch(() => []),
  ])

  console.log('[actor page] API response:', actor?.name ?? 'null',
    '| movies:', movies.length,
    '| collaborators:', collaborators.length,
    '| directors:', directors.length)

  if (!actor) notFound()

  // Build name → id map from full actors list for collaborator linking
  const actorIdMap: Record<string, number> = {}
  for (const a of allActors) {
    if (a.name) actorIdMap[a.name] = a.id
  }

  // Compare suggestions: other actors in the database, excluding current actor
  const suggestions = allActors
    .filter(a => a.id !== Number(id))
    .slice(0, 8)

  // Most frequent co-star name for hero sub-line
  const topCollaborator = collaborators[0]?.actor

  return (
    <div className="min-h-screen bg-[#0a0a0f]">
      <Header />

      <main className="max-w-[1200px] mx-auto px-6 pb-24 flex flex-col gap-14">

        {/* ── 1. Hero ───────────────────────────────────────────── */}
        <ActorHero
          actor={actor}
          collaboratorCount={collaborators.length}
          directorCount={directors.length}
          topCollaborator={topCollaborator}
        />

        {/* ── 2. Filmography Preview (horizontal strip) ─────────── */}
        {movies.length > 0 && (
          <FilmographyPreview movies={movies} totalCount={movies.length} />
        )}

        {/* ── 3. Collaborations (actresses · directors · co-stars) ── */}
        {(collaborators.length > 0 || directors.length > 0) && (
          <CollaborationsSection
            collaborators={collaborators}
            directors={directors}
            allActors={allActors}
            actorIdMap={actorIdMap}
          />
        )}

        {/* ── 4. Compare ────────────────────────────────────────── */}
        <CompareSection
          currentActor={{ id: Number(id), name: actor.name }}
          suggestions={suggestions}
        />

        {/* ── 5. Connections (inline BFS finder) ────────────────── */}
        <ActorConnections
          actor={{ id: Number(id), name: actor.name, industry: actor.industry }}
        />

        {/* ── 6. Insights (synthetic cards) ─────────────────────── */}
        <ActorInsightsCarousel
          actor={actor}
          collaborators={collaborators}
          directors={directors}
        />

        {/* ── 7. Full Filmography (expandable grid) ─────────────── */}
        {movies.length > 0 && (
          <FullFilmography movies={movies} />
        )}

      </main>
    </div>
  )
}
