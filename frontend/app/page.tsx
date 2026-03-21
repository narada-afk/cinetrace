// Force dynamic rendering so searchParams (?industry=…) is always fresh
// and never served from the Next.js full-route cache.
export const dynamic = 'force-dynamic'

import Header from '@/components/Header'
import NavTabs from '@/components/NavTabs'
import InsightCard, { InsightCardData } from '@/components/InsightCard'
import TrendingActors, { TrendingActor } from '@/components/TrendingActors'
import { getInsights, getActors, getTopBoxOffice, BoxOfficeEntry } from '@/lib/api'

// Gradient palette — cycle through for variety
const GRADIENTS: InsightCardData['gradient'][] = [
  'red',
  'purple',
  'orange',
  'blue',
]

// Static fallback cards shown even when API is unavailable
const FALLBACK_CARDS: InsightCardData[] = [
  {
    emoji: '🔥',
    label: 'Legendary Duo',
    headline: 'Mohanlal + Mammootty appeared together in',
    stat: '60 films',
    subtext: 'The greatest pair in Malayalam cinema',
    actors: [{ name: 'Mohanlal' }, { name: 'Mammootty' }],
    gradient: 'red',
    href: '/compare/mohanlal-vs-mammootty',
  },
  {
    emoji: '🎬',
    label: 'Most Prolific',
    headline: 'Rajinikanth has starred in',
    stat: '180+ films',
    subtext: 'Spanning five decades of South Indian cinema',
    actors: [{ name: 'Rajinikanth' }],
    gradient: 'purple',
    href: '/compare/rajinikanth',
  },
  {
    emoji: '⭐',
    label: 'Box Office King',
    headline: 'Prabhas — highest-grossing South Indian film',
    stat: '₹2,500 Cr',
    subtext: 'Baahubali 2: The Conclusion (2017)',
    actors: [{ name: 'Prabhas' }],
    gradient: 'orange',
    href: '/compare/prabhas',
  },
  {
    emoji: '🏆',
    label: 'Director Icon',
    headline: 'Kamal Haasan has worked with the most directors',
    stat: '150+',
    subtext: 'Across Tamil, Telugu, Malayalam, Hindi & more',
    actors: [{ name: 'Kamal Haasan', avatarSlug: 'kamalhaasan' }],
    gradient: 'blue',
    href: '/compare/kamal-haasan',
  },
]

// Fallback trending actors shown when /actors API is unavailable
const FALLBACK_TRENDING: TrendingActor[] = [
  { id: 1,  name: 'Rajinikanth',  avatarSlug: 'rajinikanth' },
  { id: 2,  name: 'Mohanlal',     avatarSlug: 'mohanlal' },
  { id: 3,  name: 'Kamal Haasan', avatarSlug: 'kamalhaasan' },
  { id: 4,  name: 'Mammootty',    avatarSlug: 'mammootty' },
  { id: 5,  name: 'Prabhas',      avatarSlug: 'prabhas' },
  { id: 6,  name: 'Mahesh Babu',  avatarSlug: 'maheshbabu' },
  { id: 7,  name: 'Allu Arjun',   avatarSlug: 'alluarjun' },
  { id: 8,  name: 'Vijay',        avatarSlug: 'vijay' },
]

const INSIGHT_META: Record<
  string,
  { emoji: string; label: string }
> = {
  collaboration: { emoji: '🔥', label: 'Iconic Duo' },
  director:      { emoji: '🎬', label: 'Director Partnership' },
  supporting:    { emoji: '⭐', label: 'Character Icon' },
}

async function fetchInsightCards(industry?: string): Promise<InsightCardData[]> {
  try {
    const insights = await getInsights(industry)
    if (!insights.length) return FALLBACK_CARDS

    return insights.map((insight, i) => {
      const meta = INSIGHT_META[insight.type] ?? { emoji: '🎭', label: 'Cinema Fact' }

      // Build URL using actor IDs — avoids name→slug roundtrip issues
      // (e.g. "N. T. Rama Rao Jr." loses dots when slugified → search fails)
      // collaboration → /compare/id1-vs-id2
      // director      → /actors/actor_id  (profile shows filmography + directors)
      // supporting    → /actors/actor_id
      let href = '#'
      if (insight.type === 'collaboration' && insight.actor_ids.length === 2) {
        href = `/compare/${insight.actor_ids[0]}-vs-${insight.actor_ids[1]}`
      } else if (insight.actor_ids.length > 0) {
        href = `/actors/${insight.actor_ids[0]}`
      }

      // For director cards, surface the director's name in the subtext
      const subtext =
        insight.type === 'director' && insight.actors.length >= 2
          ? `With director ${insight.actors[1]}`
          : undefined

      return {
        emoji:    meta.emoji,
        label:    meta.label,
        headline: insight.headline,
        stat:     `${insight.value} ${insight.value === 1 ? 'film' : insight.unit}`,
        subtext,
        // Show up to 2 actor avatars; for director cards show only the actor (index 0)
        actors:   insight.actors
          .slice(0, insight.type === 'director' ? 1 : 2)
          .map((name) => ({ name })),
        gradient: GRADIENTS[i % GRADIENTS.length],
        href,
      }
    })
  } catch {
    return FALLBACK_CARDS
  }
}

// Box office card gradients — cycle through for visual variety
const BO_GRADIENTS: InsightCardData['gradient'][] = [
  'green',
  'orange',
  'blue',
  'purple',
]

/** Format INR crore for display: 2357.9 → "₹2,358 Cr" */
function formatCrore(crore: number): string {
  return `₹${Math.round(crore).toLocaleString('en-IN')} Cr`
}

async function fetchBoxOfficeCards(industry?: string): Promise<InsightCardData[]> {
  try {
    const entries = await getTopBoxOffice(industry, 4)
    if (!entries.length) return []

    return entries.map((entry: BoxOfficeEntry, i: number) => {
      const leadName = entry.actor_names[0] ?? null
      const leadId   = entry.actor_ids[0]  ?? null
      return {
        emoji:    '💰',
        label:    `Box Office #${i + 1}`,
        headline: `${entry.title} earned worldwide`,
        stat:     formatCrore(entry.box_office_crore),
        subtext:  `${entry.industry} · ${entry.release_year}`,
        actors:   leadName ? [{ name: leadName }] : [],
        gradient: BO_GRADIENTS[i % BO_GRADIENTS.length],
        href:     leadId ? `/actors/${leadId}` : '#',
      }
    })
  } catch {
    return []
  }
}

async function fetchTrendingActors(industry?: string): Promise<TrendingActor[]> {
  try {
    // gender='M' → lead actors only (excludes lead actresses from this row)
    const actors = await getActors(true, 'M')
    if (!actors.length) return FALLBACK_TRENDING

    // Filter by industry when a tab is selected (case-insensitive match)
    const filtered =
      industry && industry !== 'all'
        ? actors.filter(
            (a) => a.industry?.toLowerCase() === industry.toLowerCase()
          )
        : actors

    // Cap at 50 — all primary actors fit comfortably in the scroll row
    return filtered.slice(0, 50).map((a) => ({
      id: a.id,
      name: a.name,
    }))
  } catch {
    return FALLBACK_TRENDING
  }
}

async function fetchLeadingLadies(industry?: string): Promise<TrendingActor[]> {
  try {
    const actresses = await getActors(true, 'F')
    if (!actresses.length) return []

    const filtered =
      industry && industry !== 'all'
        ? actresses.filter(
            (a) => a.industry?.toLowerCase() === industry.toLowerCase()
          )
        : actresses

    return filtered.slice(0, 50).map((a) => ({
      id: a.id,
      name: a.name,
    }))
  } catch {
    return []
  }
}

export default async function HomePage({
  searchParams,
}: {
  searchParams?: { industry?: string }
}) {
  // Read the active industry from the URL (?industry=telugu) or default to 'all'
  const industry = searchParams?.industry ?? 'all'

  const [insightCards, boxOfficeCards, trendingActors, leadingLadies] = await Promise.all([
    fetchInsightCards(industry),
    fetchBoxOfficeCards(industry),
    fetchTrendingActors(industry),
    fetchLeadingLadies(industry),
  ])

  return (
    <div className="min-h-screen bg-[#0a0a0f]">
      {/* Header */}
      <Header />

      {/* Glass Nav — aligned to same container */}
      <div className="max-w-[1200px] mx-auto px-6">
        <NavTabs activeTab={industry} />
      </div>

      {/* Page content */}
      <main className="max-w-[1200px] mx-auto px-6 mt-10 pb-20">
        {/* Section title */}
        <h1 className="text-xl font-bold text-white/80 mt-10 mb-6">
          🔥 Cinema Insights
        </h1>

        {/* 2×N Insight Cards Grid */}
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          {insightCards.map((card, i) => (
            <InsightCard key={i} {...card} />
          ))}
        </div>

        {/* Box Office Leaderboard */}
        {boxOfficeCards.length > 0 && (
          <>
            <h2 className="text-xl font-bold text-white/80 mt-12 mb-6">
              💰 Box Office Leaderboard
            </h2>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
              {boxOfficeCards.map((card, i) => (
                <InsightCard key={i} {...card} />
              ))}
            </div>
          </>
        )}
      </main>

      {/* Lead Actors Row */}
      <TrendingActors actors={trendingActors} title="🎬 Lead Actors" />

      {/* Leading Ladies Row — only shown when there are results */}
      {leadingLadies.length > 0 && (
        <TrendingActors actors={leadingLadies} title="🌟 Leading Ladies" />
      )}

      <div className="h-16" />
    </div>
  )
}
