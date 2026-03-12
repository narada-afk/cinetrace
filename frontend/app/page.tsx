import Header from '@/components/Header'
import NavTabs from '@/components/NavTabs'
import InsightCard, { InsightCardData } from '@/components/InsightCard'
import TrendingActors, { TrendingActor } from '@/components/TrendingActors'
import { getTopCollaborations, getActors } from '@/lib/api'

// Gradient palette — cycle through for variety
const GRADIENTS: InsightCardData['gradient'][] = [
  'red',
  'purple',
  'orange',
  'blue',
]

// Slugify a name for compare URL
function toSlug(name: string) {
  return name.toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '')
}

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
  { id: 1, name: 'Rajinikanth', avatarSlug: 'rajinikanth' },
  { id: 2, name: 'Mohanlal', avatarSlug: 'mohanlal' },
  { id: 3, name: 'Kamal Haasan', avatarSlug: 'kamalhaasan' },
  { id: 4, name: 'Mammootty', avatarSlug: 'mammootty' },
  { id: 5, name: 'Prabhas', avatarSlug: 'prabhas' },
  { id: 6, name: 'Mahesh Babu', avatarSlug: 'maheshbabu' },
  { id: 7, name: 'Allu Arjun', avatarSlug: 'alluarjun' },
  { id: 8, name: 'Vijay', avatarSlug: 'vijay' },
]

async function fetchInsightCards(): Promise<InsightCardData[]> {
  try {
    const collabs = await getTopCollaborations(4)
    if (!collabs.length) return FALLBACK_CARDS

    return collabs.map((c, i) => ({
      emoji: i === 0 ? '🔥' : i === 1 ? '🎭' : i === 2 ? '🌟' : '🎬',
      label: i === 0 ? 'Legendary Duo' : 'Iconic Pair',
      headline: `${c.actor_1} + ${c.actor_2} appeared together in`,
      stat: `${c.films} films`,
      subtext: 'Top co-starring pair',
      actors: [{ name: c.actor_1 }, { name: c.actor_2 }],
      gradient: GRADIENTS[i % GRADIENTS.length],
      href: `/compare/${toSlug(c.actor_1)}-vs-${toSlug(c.actor_2)}`,
    }))
  } catch {
    return FALLBACK_CARDS
  }
}

async function fetchTrendingActors(): Promise<TrendingActor[]> {
  try {
    const actors = await getActors()
    if (!actors.length) return FALLBACK_TRENDING

    return actors.map((a) => ({
      id: a.id,
      name: a.name,
    }))
  } catch {
    return FALLBACK_TRENDING
  }
}

export default async function HomePage() {
  const [insightCards, trendingActors] = await Promise.all([
    fetchInsightCards(),
    fetchTrendingActors(),
  ])

  return (
    <div className="min-h-screen bg-[#0a0a0f]">
      {/* Header */}
      <Header />

      {/* Glass Nav — aligned to same container */}
      <div className="max-w-[1200px] mx-auto px-6">
        <NavTabs />
      </div>

      {/* Page content */}
      <main className="max-w-[1200px] mx-auto px-6 mt-10 pb-20">
        {/* Section title */}
        <h1 className="text-xl font-bold text-white/80 mt-10 mb-6">
          🔥 Cinema Insights
        </h1>

        {/* 2×2 Insight Cards Grid */}
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          {insightCards.map((card, i) => (
            <InsightCard key={i} {...card} />
          ))}
        </div>
      </main>

      {/* Trending Actors Row — same container */}
      <TrendingActors actors={trendingActors} />

      <div className="h-16" />
    </div>
  )
}
