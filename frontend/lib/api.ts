const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000'

async function apiFetch<T>(path: string): Promise<T> {
  const res = await fetch(`${API_URL}${path}`, {
    next: { revalidate: 60 },
  })
  if (!res.ok) {
    throw new Error(`API error ${res.status}: ${path}`)
  }
  return res.json() as Promise<T>
}

// ── Types ────────────────────────────────────────────────────────────────────

export interface Actor {
  id: number
  name: string
  industry?: string
  debut_year?: number | null
}

export interface ActorProfile {
  id: number
  name: string
  industry: string
  film_count: number
  first_film_year: number | null
  last_film_year: number | null
  avg_runtime: number | null
}

export interface ActorMovie {
  title: string
  release_year: number
  director: string | null
  runtime: number | null
  production_company: string | null
  language: string | null
  tmdb_id: number | null
  poster_url: string | null
  backdrop_url: string | null
  vote_average: number | null
  popularity: number | null
}

export interface Collaborator {
  actor: string   // co-star name
  films: number
}

export interface DirectorCollab {
  director: string
  films: number
}

export interface TopCollaboration {
  actor_1: string
  actor_2: string
  films: number
}

export interface Insight {
  type: 'collaboration' | 'director' | 'supporting'
  headline: string
  value: number
  unit: string
  actors: string[]
  /** Database IDs for the actors[] array — same order. Use for building URLs
   *  without any name→slug conversion (avoids issues with special chars like dots). */
  actor_ids: number[]
}

export interface SharedFilm {
  title: string
  release_year: number
  director: string | null
  poster_url: string | null
  vote_average: number | null
  popularity: number | null
  actor1_character: string | null
  actor1_role: string | null
  actor2_character: string | null
  actor2_role: string | null
}

// ── Endpoints ────────────────────────────────────────────────────────────────

export async function getInsights(industry?: string): Promise<Insight[]> {
  const param =
    industry && industry !== 'all' && industry !== 'explore'
      ? `?industry=${encodeURIComponent(industry)}`
      : ''
  const data = await apiFetch<{ insights: Insight[] }>(`/analytics/insights${param}`)
  return data.insights
}

export async function getTopCollaborations(
  limit = 5
): Promise<TopCollaboration[]> {
  return apiFetch<TopCollaboration[]>(
    `/analytics/top-collaborations?limit=${limit}`
  )
}

export async function getActors(): Promise<Actor[]> {
  return apiFetch<Actor[]>('/actors')
}

export async function searchActors(q: string): Promise<Actor[]> {
  return apiFetch<Actor[]>(`/actors/search?q=${encodeURIComponent(q)}`)
}

export async function getActor(id: number | string): Promise<ActorProfile> {
  return apiFetch<ActorProfile>(`/actors/${id}`)
}

export async function getActorMovies(id: number | string): Promise<ActorMovie[]> {
  return apiFetch<ActorMovie[]>(`/actors/${id}/movies`)
}

export async function getActorCollaborators(id: number | string): Promise<Collaborator[]> {
  return apiFetch<Collaborator[]>(`/actors/${id}/collaborators`)
}

export async function getActorDirectors(id: number | string): Promise<DirectorCollab[]> {
  return apiFetch<DirectorCollab[]>(`/actors/${id}/directors`)
}

export async function getSharedFilms(
  actor1Id: number | string,
  actor2Id: number | string
): Promise<SharedFilm[]> {
  return apiFetch<SharedFilm[]>(`/actors/${actor1Id}/shared/${actor2Id}`)
}
