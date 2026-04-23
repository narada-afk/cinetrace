// NOTE: Do NOT add `export const dynamic = 'force-dynamic'` here.
// Accessing `searchParams` already makes this page dynamically rendered, but
// keeping it off lets Next.js Data Cache honour the `revalidate` values in
// apiFetch — cutting backend hits dramatically (insights cached 60 s, rest 300 s).

import fs from 'fs'
import path from 'path'
import dynamic from 'next/dynamic'
import Header from '@/components/Header'
import HeroSearch from '@/components/HeroSearch'
import InsightsCarousel from '@/components/InsightsCarousel'
import { type InsightCardData } from '@/components/InsightCard'
import ConnectionFinder from '@/components/stats/ConnectionFinder'
import CompareEntry from '@/components/CompareEntry'
import { getInsights, getActors, getActorCollaborators, getActorLeadCollaborators, getActorDirectors, getActor, type Insight } from '@/lib/api'
import type { TrendingChip } from '@/components/HeroSearch'
import type { NetworkCenter, NetworkNode } from '@/components/GraphPreview'

// GraphPreview is a heavy client component; lazy-load it so it lands in its own
// chunk and does not block the initial page JS bundle.
const GraphPreview = dynamic(() => import('@/components/GraphPreview'), { ssr: false })

// ── Gradient palette ──────────────────────────────────────────────────────────

const GRADIENTS: InsightCardData['gradient'][] = ['red', 'purple', 'orange', 'blue', 'green', 'amber']

const INSIGHT_META: Record<string, { emoji: string; label: string; blurb: string }> = {
  // Legacy insight types
  collaboration:    { emoji: '🔥', label: 'Iconic Duo',                  blurb: 'A pairing South cinema never forgot'           },
  director:         { emoji: '🎬', label: 'Director Partnership',         blurb: 'One director shaped their entire career'       },
  supporting:       { emoji: '⭐', label: 'Character Icon',               blurb: 'Always in the background, never forgettable'  },
  // WOW insight types (insight_engine.py)
  collab_shock:     { emoji: '⚡', label: 'Wait… how many films??',       blurb: 'They just kept making films together'          },
  hidden_dominance: { emoji: '👀', label: 'Still everywhere',             blurb: 'The most overlooked icon in South cinema'     },
  cross_industry:   { emoji: '🌏', label: 'No language barriers',         blurb: 'One actor across every South Indian industry'  },
  career_peak:      { emoji: '🔥', label: 'Golden run',                   blurb: 'Their most explosive creative run, ever'      },
  network_power:    { emoji: '🕸️', label: 'The ultimate connector',      blurb: 'Connected to more actors than anyone else'    },
  director_loyalty:    { emoji: '🤝', label: 'One director. Always.',        blurb: 'A creative bond that defined a career'        },
  director_box_office: { emoji: '💰', label: 'Box office giant',             blurb: 'The director who prints money at the box office' },
}

// ── Static fallbacks ──────────────────────────────────────────────────────────

const FALLBACK_INSIGHT_CARDS: InsightCardData[] = [
  {
    emoji: '🔥',
    label: 'Legendary Duo',
    headline: 'Mohanlal + Mammootty appeared together in',
    stat: '60 films',
    subtext: 'The greatest pair in Malayalam cinema',
    actors: [{ name: 'Mohanlal' }, { name: 'Mammootty' }],
    gradient: 'red',
    href: '/compare',
  },
  {
    emoji: '🎬',
    label: 'Most Prolific',
    headline: 'Rajinikanth has starred in',
    stat: '180+ films',
    subtext: 'Spanning five decades of South Indian cinema',
    actors: [{ name: 'Rajinikanth' }],
    gradient: 'purple',
    href: '/stats',
  },
  {
    emoji: '⭐',
    label: 'Box Office King',
    headline: 'Prabhas — highest-grossing South Indian film',
    stat: '₹2,500 Cr',
    subtext: 'Baahubali 2: The Conclusion (2017)',
    actors: [{ name: 'Prabhas' }],
    gradient: 'orange',
    href: '/stats',
  },
]

// ── Personalised one-liner per insight ───────────────────────────────────────
//
// Uses actor names from the insight so every card reads as a specific story,
// not a generic template. Falls back to meta.blurb when names are unavailable.

function personalizedBlurb(
  type: string,
  actors: string[],
  fallback: string,
): string {
  const a1 = actors[0] ?? ''
  const a2 = actors[1] ?? ''
  if (!a1) return fallback

  switch (type) {
    case 'collab_shock':
      return a2
        ? `${a1} & ${a2} — together so many times it became a ritual`
        : `${a1} — a collaboration nobody could stop watching`
    case 'hidden_dominance':
      return `${a1} was in more films than most stars will ever dream of`
    case 'cross_industry':
      return `${a1} refused to stay in one language`
    case 'career_peak':
      return `${a1} was everywhere — and nothing could slow them down`
    case 'network_power':
      return `${a1} knows everyone. Every. Single. One.`
    case 'director_loyalty':
      return a2
        ? `${a1} kept coming back to ${a2} — film after film`
        : `${a1}'s best work? Always with the same director`
    case 'collaboration':
      return a2
        ? `${a1} & ${a2} — the duo that defined an era`
        : `${a1} — an icon the screen never forgot`
    case 'director':
      return `${a1} — one director changed everything`
    case 'supporting':
      return `${a1} — blink and you'd miss them. But you never did`
    case 'director_box_office':
      return `${a1} — every film a blockbuster, every release an event`
    default:
      return fallback
  }
}

// ── Industry diversity selection ──────────────────────────────────────────────
//
// Ensures the carousel shows at most 1 card per industry before repeating.
// Algorithm:
//   1. Assign each insight to a Tamil/Telugu/Malayalam/Kannada bucket (or "other")
//   2. Pick 1 from each available bucket (highest-confidence first within bucket)
//   3. Append remainder in original (engine-scored) order, light shuffle applied

function diversifyInsights(insights: Insight[]): Insight[] {
  if (insights.length <= 4) return insights

  const BUCKETS = ['Tamil', 'Telugu', 'Malayalam', 'Kannada'] as const
  type BucketKey = typeof BUCKETS[number] | 'other'

  const groups: Record<BucketKey, Insight[]> = {
    Tamil: [], Telugu: [], Malayalam: [], Kannada: [], other: [],
  }

  for (const ins of insights) {
    const ind = (ins.industry ?? '').toLowerCase()
    const key = BUCKETS.find(b => ind.includes(b.toLowerCase()))
    groups[key ?? 'other'].push(ins)
  }

  // Pick the top-confidence item from each industry bucket
  const picked: Insight[] = []
  const pickSet = new Set<Insight>()

  for (const bucket of BUCKETS) {
    if (groups[bucket].length > 0) {
      const top = groups[bucket][0]   // engine already sorts by score desc
      picked.push(top)
      pickSet.add(top)
    }
  }

  // Collect all remaining (unpicked) and apply a slight shuffle for variety
  const rest = insights.filter(ins => !pickSet.has(ins))
  for (let i = rest.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [rest[i], rest[j]] = [rest[j], rest[i]]
  }

  return [...picked, ...rest]
}

// ── Avatar helpers ────────────────────────────────────────────────────────────

const AVATARS_DIR = path.join(process.cwd(), 'public', 'avatars')

// Read the avatar directory once at module init instead of calling
// fs.existsSync() per insight card on every request (N→1 disk hit).
const _existingAvatarSlugs: Set<string> = (() => {
  try {
    return new Set(
      fs.readdirSync(AVATARS_DIR)
        .filter(f => f.endsWith('.png'))
        .map(f => f.slice(0, -4))
    )
  } catch {
    return new Set()
  }
})()

/** Returns the slug if a matching PNG exists on disk, undefined otherwise */
function avatarSlugIfExists(name: string): string | undefined {
  const slug = name.toLowerCase().replace(/[^a-z0-9]/g, '')
  return _existingAvatarSlugs.has(slug) ? slug : undefined
}

// ── Data helpers ──────────────────────────────────────────────────────────────

async function fetchPageData(industry: string) {
  try {
    const rawInsights = await getInsights(industry)
    if (!rawInsights.length) return { insightCards: FALLBACK_INSIGHT_CARDS }

    // Reorder so each industry appears before repeats
    const insights = diversifyInsights(rawInsights)

    // No cap — show everything the engine returns, interleaved by type
    const insightCards: InsightCardData[] = insights.map((insight, i) => {
      const meta = INSIGHT_META[insight.type] ?? { emoji: '🎭', label: 'Cinema Fact' }

      function toSlug(name: string) {
        return name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '')
      }

      let href = '#'
      if (
        (insight.type === 'collaboration' || insight.type === 'collab_shock') &&
        insight.actors.length === 2
      ) {
        href = `/compare/${toSlug(insight.actors[0])}-vs-${toSlug(insight.actors[1])}`
      } else if (insight.actors.length > 0) {
        href = `/actors/${toSlug(insight.actors[0])}`
      }

      // WOW subtext takes priority; fall back to legacy director label
      const subtext =
        insight.subtext ??
        (insight.type === 'director' && insight.actors.length >= 2
          ? `With director ${insight.actors[1]}`
          : undefined)

      // career_peak value is already a string like "2005–2010"; all others are numbers
      const stat = typeof insight.value === 'string'
        ? insight.value
        : `${insight.value} ${insight.unit ?? 'films'}`

      return {
        emoji:    meta.emoji,
        label:    meta.label,
        // personalised one-liner using actual actor names from the insight
        headline: personalizedBlurb(insight.type, insight.actors, meta.blurb),
        stat,
        subtext,
        actors:   insight.actors
          .slice(0, insight.type === 'director' ? 1 : 2)
          .map((name) => ({
            name,
            // only set avatarSlug when a matching PNG actually exists on disk
            avatarSlug: avatarSlugIfExists(name),
          })),
        gradient: GRADIENTS[i % GRADIENTS.length],
        href,
      }
    })

    return { insightCards }
  } catch (err) {
    console.error('[homepage] insights fetch failed:', err)
    return { insightCards: FALLBACK_INSIGHT_CARDS }
  }
}

// ── Trending chips — top hero of each of the 4 industries ────────────────────

const INDUSTRY_HEROES: TrendingChip[] = [
  { id: 11,   name: 'Rajinikanth'   },   // Tamil
  { id: 206,  name: 'Chiranjeevi'   },   // Telugu
  { id: 381,  name: 'Mohanlal'      },   // Malayalam
  { id: 1939, name: 'Puneet Rajkumar' }, // Kannada
]

function fetchTrendingChips(): TrendingChip[] {
  return INDUSTRY_HEROES
}

// ── Network graph data — top collaborators for the graph center actor ─────────

// Fallback if no trending actors are available
const FALLBACK_CENTER: NetworkCenter = { id: 1, name: 'Rajinikanth', gender: 'M' }

async function fetchNetworkData(
  first?: { id: number; name: string } | null,
): Promise<{ center: NetworkCenter; nodes: NetworkNode[] } | null> {
  const centerId   = first?.id   ?? FALLBACK_CENTER.id
  const centerName = first?.name ?? FALLBACK_CENTER.name

  try {
    // Fetch collaborators, lead collaborators, directors + actor list in parallel
    const [collaborators, leadCollabs, directors, actors] = await Promise.all([
      getActorCollaborators(centerId),
      getActorLeadCollaborators(centerId).catch(() => []),
      getActorDirectors(centerId).catch(() => []),
      getActors(true),
    ])

    // Resolve gender from actors list — drives the pronoun in GraphPreview subtitle
    const centerActor = actors.find(a => a.id === centerId)
    const center: NetworkCenter = {
      id:     centerId,
      name:   centerName,
      gender: centerActor?.gender ?? FALLBACK_CENTER.gender,
    }

    // Build a name → id lookup (case-insensitive) so collaborators get navigable IDs
    const nameToId  = new Map(actors.map(a => [a.name.toLowerCase().trim(), a.id]))
    const leadNames = new Set(leadCollabs.map(l => l.actor.toLowerCase().trim()))
    const dirNames  = new Set(directors.slice(0, 8).map(d => d.director.toLowerCase().trim()))

    // Remove directors from collab list so they don't double-count
    const eligibleCollabs = collaborators.filter(c => !dirNames.has(c.actor.toLowerCase().trim()))

    // Find minimum film threshold so we show ~50 nodes (matches GraphPreview client logic)
    const TARGET = 50
    let threshold = 1
    for (let t = 1; t <= (eligibleCollabs[0]?.films ?? 1); t++) {
      if (eligibleCollabs.filter(c => c.films >= t).length <= TARGET) { threshold = t; break }
    }

    // Director nodes first, then collaborators above threshold
    const nodes: NetworkNode[] = [
      ...directors.slice(0, 8).map(d => ({
        id:    nameToId.get(d.director.toLowerCase().trim()) ?? null,
        name:  d.director,
        films: d.films,
        kind:  'director' as const,
      })),
      ...eligibleCollabs
        .filter(c => c.films >= threshold)
        .map(c => ({
          id:    nameToId.get(c.actor.toLowerCase().trim()) ?? null,
          name:  c.actor,
          films: c.films,
          kind:  leadNames.has(c.actor.toLowerCase().trim()) ? 'lead' as const : 'supporting' as const,
        })),
    ]

    if (nodes.length === 0) return null
    return { center, nodes }
  } catch (err) {
    console.error('[homepage] network data fetch failed:', err)
    return null
  }
}

// ── Page ──────────────────────────────────────────────────────────────────────

export default async function HomePage({
  searchParams,
}: {
  searchParams?: { actor?: string }
}) {
  // Parse ?actor= override upfront
  const actorIdOverride = searchParams?.actor ? Number(searchParams.actor) : NaN
  const hasActorOverride = !Number.isNaN(actorIdOverride)

  const trendingChips = fetchTrendingChips()

  // Fire all three fetches in parallel:
  //  • fetchPageData  — insights (cached 60 s)
  //  • actorOverride  — only when ?actor= is in the URL (rare, cached 300 s)
  //  • fetchNetworkData — can start immediately when no actor override is present
  //    (the center is always trendingChips[0] in that case)
  const [{ insightCards }, actorOverride, baseNetworkData] = await Promise.all([
    fetchPageData('all'),
    hasActorOverride ? getActor(actorIdOverride).catch(() => null) : Promise.resolve(null),
    !hasActorOverride ? fetchNetworkData(trendingChips[0] ?? null) : Promise.resolve(null),
  ])

  // If ?actor= was supplied, we now know the override actor and can fetch
  // its network data (single sequential step only in the rare override path).
  const networkData = baseNetworkData ?? await fetchNetworkData(
    actorOverride
      ? { id: actorOverride.id, name: actorOverride.name }
      : trendingChips[0] ?? null
  )

  return (
    <>
      <Header />

      <main className="max-w-[1200px] mx-auto px-6 pb-24">

        {/* ── 1. Hero ───────────────────────────────────────────────────────── */}
        <HeroSearch trendingActors={trendingChips} />

        {/* ── 2. Insights (moved above fold — viral engine) ────────────────── */}
        <section className="mt-12 mb-12">
          <h2 className="text-xs font-semibold uppercase tracking-widest text-white/40 mb-5">
            🔥 Did you know?
          </h2>
          <InsightsCarousel cards={insightCards} />
        </section>

        {/* ── 3. Compare Entry ─────────────────────────────────────────────── */}
        <section className="mt-0">
          <CompareEntry />
        </section>

        {/* ── 4. Connection Finder ─────────────────────────────────────────── */}
        <section className="mt-14">
          <ConnectionFinder />
        </section>

        {/* ── 5. Graph Preview ─────────────────────────────────────────────── */}
        <section className="mt-14">
          <h2 className="text-xs font-semibold uppercase tracking-widest text-white/40 mb-1">
            ✦ Cinema Network
          </h2>
          <p className="text-xs text-white/25 mb-4">Tap any node to explore connections</p>
          <GraphPreview networkData={networkData} suggestions={trendingChips} />
        </section>


      </main>
    </>
  )
}
