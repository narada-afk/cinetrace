// Force dynamic rendering so searchParams (?industry=…) is always fresh
export const dynamic = 'force-dynamic'

import Header from '@/components/Header'
import HeroSearch from '@/components/HeroSearch'
import GraphPreview from '@/components/GraphPreview'
import InsightsCarousel from '@/components/InsightsCarousel'
import { type InsightCardData } from '@/components/InsightCard'
import ConnectionFinder from '@/components/stats/ConnectionFinder'
import { getInsights, getActors, getActorCollaborators, getActor } from '@/lib/api'
import type { TrendingChip } from '@/components/HeroSearch'
import type { NetworkCenter, NetworkNode } from '@/components/GraphPreview'

// ── Gradient palette ──────────────────────────────────────────────────────────

const GRADIENTS: InsightCardData['gradient'][] = ['red', 'purple', 'orange', 'blue']

const INSIGHT_META: Record<string, { emoji: string; label: string }> = {
  // Legacy insight types
  collaboration:    { emoji: '🔥', label: 'Iconic Duo' },
  director:         { emoji: '🎬', label: 'Director Partnership' },
  supporting:       { emoji: '⭐', label: 'Character Icon' },
  // WOW insight types (insight_engine.py)
  collab_shock:     { emoji: '⚡', label: 'Collaboration Shock' },
  hidden_dominance: { emoji: '👑', label: 'Hidden Dominance' },
  cross_industry:   { emoji: '🌏', label: 'Cross-Industry' },
  career_peak:      { emoji: '📈', label: 'Career Peak' },
  network_power:    { emoji: '🕸️', label: 'Network Power' },
  director_loyalty: { emoji: '🤝', label: 'Director Loyalty' },
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

// ── Data helpers ──────────────────────────────────────────────────────────────

async function fetchPageData(industry: string) {
  try {
    const insights = await getInsights(industry)
    if (!insights.length) return { insightCards: FALLBACK_INSIGHT_CARDS }

    // Build insight cards — take first 3 (any type)
    const insightCards: InsightCardData[] = insights.slice(0, 3).map((insight, i) => {
      const meta = INSIGHT_META[insight.type] ?? { emoji: '🎭', label: 'Cinema Fact' }

      let href = '#'
      if (
        (insight.type === 'collaboration' || insight.type === 'collab_shock') &&
        insight.actor_ids.length === 2
      ) {
        href = `/compare/${insight.actor_ids[0]}-vs-${insight.actor_ids[1]}`
      } else if (insight.actor_ids.length > 0) {
        href = `/actors/${insight.actor_ids[0]}`
      }

      // WOW subtext takes priority; fall back to legacy director label
      const subtext =
        insight.subtext ??
        (insight.type === 'director' && insight.actors.length >= 2
          ? `With director ${insight.actors[1]}`
          : undefined)

      return {
        emoji:    meta.emoji,
        label:    meta.label,
        headline: insight.title,
        stat:     `${insight.value} ${insight.value === 1 ? 'film' : insight.unit}`,
        subtext,
        actors:   insight.actors
          .slice(0, insight.type === 'director' ? 1 : 2)
          .map((name) => ({ name })),
        gradient: GRADIENTS[i % GRADIENTS.length],
        href,
      }
    })

    return { insightCards }
  } catch {
    return { insightCards: FALLBACK_INSIGHT_CARDS }
  }
}

// ── Trending chips — one actor per industry for natural variety ───────────────

async function fetchTrendingChips(): Promise<TrendingChip[]> {
  try {
    const actors = await getActors(true)
    // Pick first actor per industry so chips span Telugu / Tamil / Malayalam / Kannada
    const seen = new Set<string>()
    const chips: TrendingChip[] = []
    for (const a of actors) {
      const ind = (a.industry ?? 'other').toLowerCase()
      if (!seen.has(ind)) {
        seen.add(ind)
        chips.push({ id: a.id, name: a.name })
      }
      if (chips.length >= 6) break
    }
    // If deduplication left fewer than 4, fill straight from the top of the list
    if (chips.length < 4) {
      for (const a of actors) {
        if (!chips.some((c) => c.id === a.id)) chips.push({ id: a.id, name: a.name })
        if (chips.length >= 6) break
      }
    }
    return chips
  } catch {
    return []
  }
}

// ── Network graph data — top collaborators for the graph center actor ─────────

// Fallback if no trending actors are available
const FALLBACK_CENTER: NetworkCenter = { id: 1, name: 'Rajinikanth', gender: 'M' }

async function fetchNetworkData(
  first?: { id: number; name: string } | null,
): Promise<{ center: NetworkCenter; nodes: NetworkNode[] } | null> {
  // Use first trending actor as center; fall back to Rajinikanth
  const center: NetworkCenter = first
    ? { id: first.id, name: first.name }
    : FALLBACK_CENTER

  try {
    // Fetch collaborators + actor list in parallel for ID resolution
    const [collaborators, actors] = await Promise.all([
      getActorCollaborators(center.id),
      getActors(true),
    ])

    // Build a name → id lookup (case-insensitive) so collaborators get navigable IDs
    const nameToId = new Map(actors.map(a => [a.name.toLowerCase().trim(), a.id]))

    const nodes: NetworkNode[] = collaborators
      .slice(0, 8)                          // top 8 by collaboration count (API returns sorted)
      .map(c => ({
        id:    nameToId.get(c.actor.toLowerCase().trim()) ?? null,
        name:  c.actor,
        films: c.films,
      }))

    if (nodes.length === 0) return null
    return { center, nodes }
  } catch {
    return null
  }
}

// ── Page ──────────────────────────────────────────────────────────────────────

export default async function HomePage({
  searchParams,
}: {
  searchParams?: { actor?: string }
}) {
  // Fetch page data + trending chips in parallel; chips[0] drives the graph center
  const [{ insightCards }, trendingChips] = await Promise.all([
    fetchPageData('all'),
    fetchTrendingChips(),
  ])

  // ?actor= URL param overrides the network center (used by Share button on GraphPreview)
  let networkCenter: { id: number; name: string } | null = trendingChips[0] ?? null
  if (searchParams?.actor) {
    const actorId = Number(searchParams.actor)
    if (!Number.isNaN(actorId)) {
      try {
        const profile = await getActor(actorId)
        networkCenter = { id: profile.id, name: profile.name }
      } catch {
        // Fall back to trending actor if ID is invalid / actor not found
      }
    }
  }

  // Network data runs after chips resolve so we can pass the center actor
  const networkData = await fetchNetworkData(networkCenter)

  return (
    <div className="min-h-screen bg-[#0a0a0f]">
      <Header />

      <main className="max-w-[1200px] mx-auto px-6 pb-24">

        {/* ── 1. Hero ───────────────────────────────────────────────────────── */}
        <HeroSearch trendingActors={trendingChips} />

        {/* ── 2. Connection Finder ─────────────────────────────────────────── */}
        <section className="mt-16">
          <h2 className="text-xs font-semibold uppercase tracking-widest text-white/40 mb-5">
            🔗 Connection Finder
          </h2>
          <ConnectionFinder />
        </section>

        {/* ── 3. Graph Preview ─────────────────────────────────────────────── */}
        <section className="mt-16">
          <GraphPreview networkData={networkData} />
        </section>

        {/* ── 4. Insights (auto-scroll carousel) ───────────────────────────── */}
        <section className="mt-16">
          <h2 className="text-xs font-semibold uppercase tracking-widest text-white/40 mb-5">
            🔥 Cinema Insights
          </h2>
          <InsightsCarousel cards={insightCards} />
        </section>

      </main>
    </div>
  )
}
