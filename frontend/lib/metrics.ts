import type { ActorProfile, ActorMovie } from './api'

/** Years active = (last film year − first film year) + 1. */
export function calcYearsActive(profile: ActorProfile): number {
  const first = profile.first_film_year
  const last  = profile.last_film_year
  if (!first || !last) return 0
  return (last - first) + 1
}

/** Average vote_average across films that have a rating > 0, rounded to 1 dp. */
export function calcAvgRating(movies: ActorMovie[]): number {
  const rated = movies.filter((m) => (m.vote_average ?? 0) > 0)
  if (!rated.length) return 0
  const sum = rated.reduce((acc, m) => acc + (m.vote_average ?? 0), 0)
  return Math.round((sum / rated.length) * 10) / 10
}
