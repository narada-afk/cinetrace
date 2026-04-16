// Server-side (SSR/RSC): use Docker-internal hostname so containers can talk.
// Client-side (browser): use localhost which the browser can actually reach.
const API_URL =
  typeof window === 'undefined'
    ? (process.env.API_URL          ?? 'http://backend:8000')   // server
    : (process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000') // browser

async function apiFetch<T>(path: string, revalidate = 60): Promise<T> {
  const res = await fetch(`${API_URL}${path}`, {
    next: { revalidate },
  })
  if (!res.ok) {
    throw new Error(`API error ${res.status}: ${path}`)
  }
  return res.json() as Promise<T>
}

// ── URL helpers ──────────────────────────────────────────────────────────────

/** Convert an actor name to a URL-safe slug, e.g. "Jr. NTR" → "jr-ntr" */
export function toActorSlug(name: string): string {
  return name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '')
}

// ── Types ────────────────────────────────────────────────────────────────────

export interface Actor {
  id: number
  name: string
  industry?: string
  debut_year?: number | null
  gender?: 'M' | 'F' | null
  actor_tier?: 'primary' | 'network' | null
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
  box_office: number | null
}

export interface Collaborator {
  actor: string    // co-star name
  films: number
  actor_id: number // co-star's database ID (0 when unavailable)
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
  /** Legacy types: 'collaboration' | 'director' | 'supporting'
   *  WOW types: 'collab_shock' | 'hidden_dominance' | 'cross_industry'
   *             | 'career_peak' | 'network_power' | 'director_loyalty' */
  type: string
  /** Broad grouping for filtering/diversity: 'collaboration' | 'network' | 'career' | 'industry' */
  category?: string
  headline: string
  /** Numeric count for most types; string range (e.g. "2005–2010") for career_peak. */
  value: number | string
  unit: string
  actors: string[]
  /** Database IDs for the actors[] array — same order. Use for building URLs
   *  without any name→slug conversion (avoids issues with special chars like dots). */
  actor_ids: number[]
  /** WOW story sentence — the surprising context behind the stat. Undefined for legacy types. */
  subtext?: string
  /** Normalised score 0–1. Higher = more surprising / impressive. */
  confidence?: number
  /** Primary industry of the actor(s) — Tamil | Telugu | Malayalam | Kannada */
  industry?: string
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

export interface DirectorStat {
  name: string
  film_count: number
  industries: string | null  // comma-separated, e.g. "Telugu, Tamil"
}

export interface ProductionHouseStat {
  name: string
  film_count: number
  industries: string | null  // comma-separated
}

// ── Endpoints ────────────────────────────────────────────────────────────────

export async function getInsights(industry?: string): Promise<Insight[]> {
  const param =
    industry && industry !== 'all' && industry !== 'explore'
      ? `?industry=${encodeURIComponent(industry)}`
      : ''
  const data = await apiFetch<{ insights: Insight[] }>(`/analytics/insights${param}`, 10)
  return data.insights
}

export async function getTopCollaborations(
  limit = 5
): Promise<TopCollaboration[]> {
  return apiFetch<TopCollaboration[]>(
    `/analytics/top-collaborations?limit=${limit}`
  )
}

export async function getActors(primaryOnly = false, gender?: 'M' | 'F'): Promise<Actor[]> {
  const params = new URLSearchParams()
  if (primaryOnly) params.set('primary_only', 'true')
  if (gender)      params.set('gender', gender)
  const qs = params.toString()
  return apiFetch<Actor[]>(`/actors${qs ? `?${qs}` : ''}`)
}

export async function searchActors(q: string, leadOnly = false): Promise<Actor[]> {
  const params = `/actors/search?q=${encodeURIComponent(q)}${leadOnly ? '&lead_only=true' : ''}`
  return apiFetch<Actor[]>(params)
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

export async function getActorLeadCollaborators(id: number | string): Promise<Collaborator[]> {
  return apiFetch<Collaborator[]>(`/actors/${id}/lead-collaborators`)
}

export interface Blockbuster {
  title: string
  release_year: number
  poster_url: string | null
  box_office_crore: number
  budget_crore: number | null
  box_office_source: string
  budget_source: string | null
}

export async function getActorBlockbusters(id: number | string): Promise<Blockbuster[]> {
  return apiFetch<Blockbuster[]>(`/actors/${id}/blockbusters`)
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

export async function getTopDirectors(
  industry?: string,
  limit = 30
): Promise<DirectorStat[]> {
  const params = new URLSearchParams({ limit: String(limit) })
  if (industry && industry !== 'all') params.set('industry', industry)
  return apiFetch<DirectorStat[]>(`/analytics/directors?${params}`)
}

export async function getTopProductionHouses(
  industry?: string,
  limit = 20
): Promise<ProductionHouseStat[]> {
  const params = new URLSearchParams({ limit: String(limit) })
  if (industry && industry !== 'all') params.set('industry', industry)
  return apiFetch<ProductionHouseStat[]>(`/analytics/production-houses?${params}`)
}

// ── Stats for Nerds  (Sprint 21) ─────────────────────────────────────────────

export interface StatsOverview {
  total_movies: number
  total_actors: number
  total_links:  number
  industries:   number
}

export interface ConnectedActor {
  id:            number
  name:          string
  industry:      string
  tier:          string
  unique_costars: number
  film_count:    number
}

export interface IndustryBucket {
  industry: string
  total:    number
  pre_1980: number
  s1980s:   number
  s2000s:   number
  s2010s:   number
  s2020s:   number
}

export interface DirectorPartnership {
  actor:      string
  director:   string
  film_count: number
  industry:   string
  films:      string[]
}

export interface TimelinePoint { year: number; count: number }
export interface CareerTimeline {
  actor_id:   number
  actor_name: string
  data:       TimelinePoint[]
}

export interface CoStarStat {
  id:             number
  name:           string
  industry:       string
  unique_costars: number
  film_count:     number
}

export interface ConnectionPath {
  found:       boolean
  depth:       number
  path:        { id: number; name: string }[]
  connections: { movie_id: number; movie_title: string; poster_url: string | null; tmdb_id: number | null }[]
}

export async function getStatsOverview(): Promise<StatsOverview> {
  return apiFetch<StatsOverview>('/stats/overview')
}
export async function getMostConnected(limit = 25): Promise<ConnectedActor[]> {
  return apiFetch<ConnectedActor[]>(`/stats/most-connected?limit=${limit}`)
}
export async function getIndustryDistribution(): Promise<IndustryBucket[]> {
  return apiFetch<IndustryBucket[]>('/stats/industry-distribution')
}
export async function getTopPartnerships(limit = 15): Promise<DirectorPartnership[]> {
  return apiFetch<DirectorPartnership[]>(`/stats/top-partnerships?limit=${limit}`)
}
export async function getCareerTimeline(actorId: number): Promise<CareerTimeline> {
  return apiFetch<CareerTimeline>(`/stats/career-timeline?actor_id=${actorId}`)
}
export async function getTopCoStars(limit = 15): Promise<CoStarStat[]> {
  return apiFetch<CoStarStat[]>(`/stats/top-costars?limit=${limit}`)
}
export async function getActorConnection(
  actor1Id: number, actor2Id: number
): Promise<ConnectionPath> {
  return apiFetch<ConnectionPath>(
    `/stats/connection?actor1_id=${actor1Id}&actor2_id=${actor2Id}`
  )
}

// ── Sprint 22 — Build Your Own Chart / Cinema Universe / Gravity Center ──────

export interface ChartSeries {
  actor_id:   number
  actor_name: string
  points:     { x: string | number; y: number }[]
}

export interface ChartData {
  chart_type: 'line' | 'bar'
  series:     ChartSeries[]
}

export interface UniverseNode {
  id:           number
  name:         string
  industry:     string
  film_count:   number
  costar_count: number
}

export interface UniverseEdge {
  source: number
  target: number
  weight: number
}

export interface CinemaUniverse {
  nodes: UniverseNode[]
  edges: UniverseEdge[]
}

export interface GravityActor {
  id:           number
  name:         string
  industry:     string
  centrality:   number
  film_count:   number
  costar_count: number
}

export async function getChartData(
  xAxis: string,
  yAxis: string,
  actorIds: number[],
  industry?: string,
  yearFrom?: number,
  yearTo?: number,
): Promise<ChartData> {
  const params = new URLSearchParams({ x_axis: xAxis, y_axis: yAxis, actors: actorIds.join(',') })
  if (industry)  params.set('industry',  industry)
  if (yearFrom)  params.set('year_from', String(yearFrom))
  if (yearTo)    params.set('year_to',   String(yearTo))
  return apiFetch<ChartData>(`/stats/chart-data?${params}`)
}

export async function getCinemaUniverse(minFilms = 3): Promise<CinemaUniverse> {
  return apiFetch<CinemaUniverse>(`/stats/cinema-universe?min_films=${minFilms}`)
}

export async function getGravityCenter(limit = 25): Promise<GravityActor[]> {
  return apiFetch<GravityActor[]>(`/stats/gravity-center?limit=${limit}`)
}

// ── Box Office  (Sprint 23) ───────────────────────────────────────────────────

export interface BoxOfficeEntry {
  title: string
  release_year: number
  industry: string
  /** Worldwide gross in INR crore (converted from TMDB USD at 84.0 ₹/USD) */
  box_office_crore: number
  /** Primary actors who appeared in the film (is_primary_actor = TRUE) */
  actor_names: string[]
  actor_ids: number[]
  poster_url: string | null
}

export async function getTopBoxOffice(
  industry?: string,
  limit = 4,
): Promise<BoxOfficeEntry[]> {
  const params = new URLSearchParams({ limit: String(limit) })
  if (industry && industry !== 'all' && industry !== 'explore') {
    params.set('industry', industry)
  }
  return apiFetch<BoxOfficeEntry[]>(`/analytics/top-box-office?${params}`)
}
