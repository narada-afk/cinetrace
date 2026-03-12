import { notFound } from 'next/navigation'
import Header from '@/components/Header'
import ActorHero from '@/components/ActorHero'
import ActorStats from '@/components/ActorStats'
import CollaboratorGrid from '@/components/CollaboratorGrid'
import DirectorList from '@/components/DirectorList'
import FilmographyGrid from '@/components/FilmographyGrid'
import MissingData from '@/components/MissingData'
import {
  getActor,
  getActorMovies,
  getActorCollaborators,
  getActorDirectors,
  getActors,
} from '@/lib/api'

interface PageProps {
  params: { id: string }
}

// Section wrapper for consistent styling
function Section({
  title,
  children,
}: {
  title: string
  children: React.ReactNode
}) {
  return (
    <section className="flex flex-col gap-4">
      <h2 className="text-lg font-bold text-white/80">{title}</h2>
      {children}
    </section>
  )
}

export default async function ActorPage({ params }: PageProps) {
  const id = params.id

  // Fetch all data in parallel — individual failures are caught gracefully
  const [actor, movies, collaborators, directors, allActors] =
    await Promise.all([
      getActor(id).catch(() => null),
      getActorMovies(id).catch(() => []),
      getActorCollaborators(id).catch(() => []),
      getActorDirectors(id).catch(() => []),
      getActors().catch(() => []),
    ])

  if (!actor) notFound()

  // Build name → id map from full actors list for collaborator linking
  const actorIdMap: Record<string, number> = {}
  for (const a of allActors) {
    if (a.name) actorIdMap[a.name] = a.id
  }

  return (
    <div className="min-h-screen bg-[#0a0a0f]">
      <Header />

      <main className="max-w-[1200px] mx-auto px-6 pb-24 flex flex-col gap-10">
        {/* ── Hero ────────────────────────────────────────────── */}
        <ActorHero
          actor={actor}
          collaboratorCount={collaborators.length}
          directorCount={directors.length}
        />

        {/* ── Quick Stats ─────────────────────────────────────── */}
        <ActorStats
          filmCount={actor.film_count}
          collaboratorCount={collaborators.length}
          directorCount={directors.length}
          industry={actor.industry}
        />

        {/* ── Top Collaborators ───────────────────────────────── */}
        <Section title="🔥 Top Collaborators">
          <CollaboratorGrid
            collaborators={collaborators}
            actorIdMap={actorIdMap}
          />
        </Section>

        {/* ── Directors ───────────────────────────────────────── */}
        <Section title="🎬 Directors Worked With">
          {directors.length > 0 ? (
            <DirectorList directors={directors} />
          ) : (
            <MissingData type="director" />
          )}
        </Section>

        {/* ── Filmography ─────────────────────────────────────── */}
        <Section title="Filmography">
          <FilmographyGrid movies={movies} />
        </Section>
      </main>
    </div>
  )
}
