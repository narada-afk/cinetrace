import type { ActorProfile, Collaborator, DirectorCollab } from '@/lib/api'
import type { InsightCardData } from './InsightCard'
import InsightsCarousel from './InsightsCarousel'

interface ActorInsightsCarouselProps {
  actor: ActorProfile
  collaborators: Collaborator[]
  directors: DirectorCollab[]
}

function slugify(name: string) {
  return name.toLowerCase().replace(/\s+/g, '')
}

export default function ActorInsightsCarousel({
  actor,
  collaborators,
  directors,
}: ActorInsightsCarouselProps) {
  const cards: InsightCardData[] = []

  // 1. Top collaborator pair
  const topCollab = collaborators[0]
  if (topCollab) {
    cards.push({
      emoji:    '🎬',
      label:    'Top Duo',
      headline: `${actor.name} and ${topCollab.actor} have shared the screen together more than any other pair`,
      stat:     topCollab.films,
      subtext:  `${topCollab.films} films together`,
      actors:   [
        { name: actor.name,      avatarSlug: slugify(actor.name) },
        { name: topCollab.actor, avatarSlug: slugify(topCollab.actor) },
      ],
      gradient: 'red',
      href:     `/actors/${actor.id}`,
    })
  }

  // 2. Career span
  if (actor.first_film_year && actor.last_film_year) {
    const span = actor.last_film_year - actor.first_film_year
    if (span > 0) {
      cards.push({
        emoji:    '⏳',
        label:    'Career Span',
        headline: `Active from ${actor.first_film_year} to ${actor.last_film_year} across ${actor.industry} cinema`,
        stat:     `${span}yr`,
        subtext:  `${actor.first_film_year} – ${actor.last_film_year}`,
        actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
        gradient: 'orange',
        href:     `/actors/${actor.id}`,
      })
    }
  }

  // 3. Top director partnership
  const topDir = directors[0]
  if (topDir && topDir.films >= 2) {
    cards.push({
      emoji:    '🎥',
      label:    'Director Bond',
      headline: `${topDir.director} is ${actor.name}'s most frequent director`,
      stat:     topDir.films,
      subtext:  `films directed by ${topDir.director}`,
      actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
      gradient: 'blue',
      href:     `/actors/${actor.id}`,
    })
  }

  // 4. Total films milestone
  if (actor.film_count >= 10) {
    cards.push({
      emoji:    '🏆',
      label:    'Filmography',
      headline: `One of the most prolific actors in ${actor.industry} cinema`,
      stat:     actor.film_count,
      subtext:  `films in the database`,
      actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
      gradient: 'green',
      href:     `/actors/${actor.id}`,
    })
  }

  // 5. Average runtime
  if (actor.avg_runtime && actor.avg_runtime > 0) {
    const hrs   = Math.floor(actor.avg_runtime / 60)
    const mins  = Math.round(actor.avg_runtime % 60)
    const label = hrs > 0 ? `${hrs}h ${mins}m` : `${mins}m`
    cards.push({
      emoji:    '⏱',
      label:    'Avg Runtime',
      headline: `${actor.name}'s films average ${label} each — legendary screen presence`,
      stat:     label,
      subtext:  `across ${actor.film_count} films`,
      actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
      gradient: 'purple',
      href:     `/actors/${actor.id}`,
    })
  }

  if (cards.length === 0) return null

  return (
    <div className="flex flex-col gap-4">
      <h2 className="text-lg font-bold text-white/80">✨ Insights</h2>
      <InsightsCarousel cards={cards} />
    </div>
  )
}
