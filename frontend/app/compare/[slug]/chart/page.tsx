/**
 * /compare/[slug]/chart
 *
 * Minimal page that renders ONLY the CompareChartBuilder component.
 * Used exclusively by the screenshot bot — no header, no hero, no other sections.
 *
 * URL format:  /compare/rajinikanth-vs-kamal-haasan/chart?metric=film_count
 * Playwright target: [data-section="compare-chart"] svg
 *
 * Supported metric values (maps to CompareChartBuilder Y axis):
 *   film_count | avg_rating | avg_box_office | hit_rate |
 *   total_box_office | avg_popularity | unique_directors |
 *   unique_costars | total_collaborations
 */

import { notFound } from 'next/navigation'
import dynamic from 'next/dynamic'
import { getActor, searchActors } from '@/lib/api'

const CompareChartBuilder = dynamic(
  () => import('@/components/CompareChartBuilder'),
  { ssr: false }
)

const VALID_METRICS = [
  'avg_rating', 'hit_rate', 'film_count', 'total_box_office',
  'avg_box_office', 'avg_budget', 'avg_popularity',
  'unique_directors', 'unique_costars', 'total_collaborations',
  'director_collaborations',
]

interface PageProps {
  params:       { slug: string }
  searchParams: { metric?: string }
}

function parseVsSlug(slug: string): [string, string] | null {
  const idx = slug.indexOf('-vs-')
  if (idx < 1) return null
  const a = slug.slice(0, idx).replace(/-/g, ' ')
  const b = slug.slice(idx + 4).replace(/-/g, ' ')
  if (!a || !b) return null
  return [a, b]
}

async function resolveActor(nameOrSlug: string) {
  const results = await searchActors(nameOrSlug).catch(() => [])
  if (!results.length) return null
  return getActor(results[0].id).catch(() => null)
}

export default async function CompareChartPage({ params, searchParams }: PageProps) {
  const names = parseVsSlug(params.slug)
  if (!names) notFound()

  const [actor1, actor2] = await Promise.all([
    resolveActor(names[0]),
    resolveActor(names[1]),
  ])
  if (!actor1 || !actor2) notFound()

  const metric = VALID_METRICS.includes(searchParams.metric ?? '')
    ? searchParams.metric!
    : 'film_count'

  return (
    <div className="min-h-screen bg-[#0a0a0f] p-4 sm:p-6" data-section="compare-chart">
      <CompareChartBuilder
        actor1={{ id: actor1.id, name: actor1.name, industry: actor1.industry }}
        actor2={{ id: actor2.id, name: actor2.name, industry: actor2.industry }}
        initialMetric={metric}
      />
    </div>
  )
}
