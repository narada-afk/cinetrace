/**
 * /actors/[slug]/chart
 *
 * Minimal page that renders ONLY the ActorCareerChart component.
 * Used exclusively by the screenshot bot — no header, no other sections,
 * so Next.js SSR + React hydration is fast even for actors with large datasets.
 *
 * Playwright target selector: [data-section="career-chart"] svg
 */

import { notFound } from 'next/navigation'
import dynamic from 'next/dynamic'
import { getActor, getActorMovies, searchActors } from '@/lib/api'

// Load chart client-side only so the minimal SSR page is fast to generate,
// and React hydration is instant (only one component to attach).
const ActorCareerChart = dynamic(() => import('@/components/ActorCareerChart'), { ssr: false })

interface PageProps {
  params: { slug: string }
}

export default async function ActorChartPage({ params }: PageProps) {
  const slug = params.slug

  // Resolve slug → numeric actor ID (same logic as the main actor page)
  let id: number | string = slug
  if (!/^\d+$/.test(slug)) {
    const nameFromSlug = slug.replace(/-/g, ' ')
    const searchResults = await searchActors(nameFromSlug).catch(() => [])
    if (!searchResults.length) notFound()
    id = searchResults[0].id
  }

  const [actor, movies] = await Promise.all([
    getActor(id).catch(() => null),
    getActorMovies(id).catch(() => []),
  ])

  if (!actor) notFound()

  const numericId = Number(id)
  const firstFilm = movies.length > 0
    ? [...movies]
        .filter(m => m.release_year && m.release_year > 0)
        .sort((a, b) => a.release_year - b.release_year)[0] ?? null
    : null

  return (
    <div className="min-h-screen bg-[#0a0a0f] p-6 sm:p-8">
      <ActorCareerChart
        actorId={numericId}
        actorName={actor.name}
        firstFilmYear={firstFilm?.release_year ?? 1970}
      />
    </div>
  )
}
