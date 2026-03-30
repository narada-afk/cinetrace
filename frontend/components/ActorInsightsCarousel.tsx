import type { ActorProfile, Actor, ActorMovie, Collaborator, DirectorCollab, Blockbuster } from '@/lib/api'
import type { InsightCardData } from './InsightCard'
import InsightsCarousel from './InsightsCarousel'

interface ActorInsightsCarouselProps {
  actor:             ActorProfile
  actorGender:       string | null
  collaborators:     Collaborator[]
  leadCollaborators: Collaborator[]
  directors:         DirectorCollab[]
  blockbusters:      Blockbuster[]
  allFemaleActors:   Actor[]
  movies:            ActorMovie[]
}

function slugify(name: string) {
  return name.toLowerCase().replace(/[^a-z0-9]/g, '')
}

function formatCrore(val: number) {
  if (val >= 1000) return `₹${(val / 1000).toFixed(1)}K Cr`
  return `₹${Math.round(val)} Cr`
}

function ordinal(n: number) {
  const s = ['th','st','nd','rd'], v = n % 100
  return n + (s[(v - 20) % 10] ?? s[v] ?? s[0])
}

export default function ActorInsightsCarousel({
  actor,
  actorGender,
  collaborators,
  leadCollaborators,
  directors,
  blockbusters,
  allFemaleActors,
  movies,
}: ActorInsightsCarouselProps) {
  const cards: InsightCardData[] = []
  const currentYear = new Date().getFullYear()
  const isFemale    = actorGender === 'F'

  // Pre-computed lookups
  const femaleNames = new Set(allFemaleActors.map(a => a.name.toLowerCase()))
  const datedMovies = movies.filter(m => m.release_year > 0 && m.release_year <= currentYear)
  const ratedMovies = datedMovies.filter(m => m.vote_average && m.vote_average > 0)

  // ── PRIORITY 1 — Lead pair / actress-actor ────────────────────────────────

  const leadPairs = isFemale
    ? leadCollaborators.filter(c => !femaleNames.has(c.actor.toLowerCase()))
    : leadCollaborators.filter(c =>  femaleNames.has(c.actor.toLowerCase()))

  const topPair = leadPairs[0]

  // 1a. Top lead pair
  if (topPair) {
    cards.push({
      emoji:    '🎭',
      label:    'Lead Pair',
      headline: `Most films with ${topPair.actor}`,
      stat:     topPair.films,
      subtext:  `${topPair.films} films together`,
      actors:   [
        { name: actor.name,     avatarSlug: slugify(actor.name) },
        { name: topPair.actor,  avatarSlug: slugify(topPair.actor) },
      ],
      gradient: 'red',
      href:     `/actors/${actor.id}`,
    })
  }

  // 1b. Legendary duo — if top pair has ≥ 8 films
  if (topPair && topPair.films >= 8) {
    cards.push({
      emoji:    '⭐',
      label:    'Legendary Duo',
      headline: `${actor.name} & ${topPair.actor} — one of cinema's most iconic pairs`,
      stat:     `${topPair.films}×`,
      subtext:  `silver screen partners`,
      actors:   [
        { name: actor.name,     avatarSlug: slugify(actor.name) },
        { name: topPair.actor,  avatarSlug: slugify(topPair.actor) },
      ],
      gradient: 'orange',
      href:     `/actors/${actor.id}`,
    })
  }

  // 1c. Second iconic pair (≥ 5 films)
  const secondPair = leadPairs[1]
  if (secondPair && secondPair.films >= 5) {
    cards.push({
      emoji:    '💫',
      label:    'Iconic Pair',
      headline: `Another beloved pairing — with ${secondPair.actor}`,
      stat:     secondPair.films,
      subtext:  `${secondPair.films} films together`,
      actors:   [
        { name: actor.name,       avatarSlug: slugify(actor.name) },
        { name: secondPair.actor, avatarSlug: slugify(secondPair.actor) },
      ],
      gradient: 'purple',
      href:     `/actors/${actor.id}`,
    })
  }

  // 1d. Lead pair diversity — how many unique lead co-stars
  if (leadPairs.length >= 5) {
    cards.push({
      emoji:    '🌟',
      label:    isFemale ? 'Leading Men' : 'Leading Ladies',
      headline: `Starred opposite ${leadPairs.length} different ${isFemale ? 'leading men' : 'leading ladies'}`,
      stat:     leadPairs.length,
      subtext:  `unique lead ${isFemale ? 'co-stars' : 'heroines'}`,
      actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
      gradient: 'red',
      href:     `/actors/${actor.id}`,
    })
  }

  // ── PRIORITY 2 — Director stats ───────────────────────────────────────────

  const topDir    = directors[0]
  const secondDir = directors[1]

  // 2a. Top director
  if (topDir && topDir.films >= 2) {
    cards.push({
      emoji:    '🎬',
      label:    'Director Bond',
      headline: `Most films directed by ${topDir.director}`,
      stat:     topDir.films,
      subtext:  `films with ${topDir.director}`,
      actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
      gradient: 'blue',
      href:     `/actors/${actor.id}`,
    })
  }

  // 2b. Auteur partnership — if top director has a huge count
  if (topDir && topDir.films >= 8) {
    cards.push({
      emoji:    '🏛️',
      label:    'Auteur Partnership',
      headline: `${topDir.director} — a defining creative partnership`,
      stat:     `${topDir.films}×`,
      subtext:  `times directed`,
      actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
      gradient: 'blue',
      href:     `/actors/${actor.id}`,
    })
  }

  // 2c. Second director (≥ 4 films)
  if (secondDir && secondDir.films >= 4) {
    cards.push({
      emoji:    '🎥',
      label:    'Another Bond',
      headline: `Also a frequent collaborator — ${secondDir.director}`,
      stat:     secondDir.films,
      subtext:  `films with ${secondDir.director}`,
      actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
      gradient: 'blue',
      href:     `/actors/${actor.id}`,
    })
  }

  // 2d. Director range — total unique directors
  if (directors.length >= 10) {
    cards.push({
      emoji:    '🗂️',
      label:    'Director Range',
      headline: `Worked with ${directors.length} different directors`,
      stat:     directors.length,
      subtext:  `unique directors`,
      actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
      gradient: 'blue',
      href:     `/actors/${actor.id}`,
    })
  }

  // ── PRIORITY 3 — Individual stats ─────────────────────────────────────────

  // 3a. Biggest blockbuster
  const topHit = blockbusters[0]
  if (topHit) {
    cards.push({
      emoji:    '💰',
      label:    'Biggest Hit',
      headline: `${topHit.title} · ${topHit.release_year}`,
      stat:     formatCrore(topHit.box_office_crore),
      subtext:  `highest grossing film · Source: TMDB`,
      actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
      gradient: 'green',
      href:     `/actors/${actor.id}`,
    })
  }

  // 3b. Cumulative box office (if ≥ 3 blockbusters)
  if (blockbusters.length >= 3) {
    const total = blockbusters.reduce((s, b) => s + b.box_office_crore, 0)
    cards.push({
      emoji:    '📊',
      label:    'Box Office Total',
      headline: `Top ${blockbusters.length} films combined`,
      stat:     formatCrore(total),
      subtext:  `combined collection · Source: TMDB`,
      actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
      gradient: 'green',
      href:     `/actors/${actor.id}`,
    })
  }

  // 3c-ROI. Best ROI film (needs budget data)
  const roiFilms = blockbusters.filter(b => b.budget_crore && b.budget_crore > 0)
  if (roiFilms.length > 0) {
    const best = roiFilms.reduce((a, b) =>
      (b.box_office_crore / b.budget_crore!) > (a.box_office_crore / a.budget_crore!) ? b : a
    )
    const roi = (best.box_office_crore / best.budget_crore!).toFixed(1)
    cards.push({
      emoji:    '🚀',
      label:    'Best ROI',
      headline: `${best.title} · ${best.release_year}`,
      stat:     `${roi}x`,
      subtext:  `return on ₹${Math.round(best.budget_crore!)} Cr budget · Source: TMDB / Wikipedia`,
      actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
      gradient: 'amber',
      href:     `/actors/${actor.id}`,
    })
  }

  // 3c. Best rated film
  if (ratedMovies.length > 0) {
    const best = [...ratedMovies].sort((a, b) => (b.vote_average ?? 0) - (a.vote_average ?? 0))[0]
    if (best.vote_average && best.vote_average >= 7) {
      cards.push({
        emoji:    '⭐',
        label:    'Critics\' Favourite',
        headline: `${best.title} · ${best.release_year}`,
        stat:     `${best.vote_average.toFixed(1)}/10`,
        subtext:  `highest audience rating`,
        actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
        gradient: 'orange',
        href:     `/actors/${actor.id}`,
      })
    }
  }

  // 3d. Peak decade — which decade had most releases
  if (datedMovies.length >= 5) {
    const byDecade: Record<number, number> = {}
    for (const m of datedMovies) {
      const dec = Math.floor(m.release_year / 10) * 10
      byDecade[dec] = (byDecade[dec] ?? 0) + 1
    }
    const peakDec  = Number(Object.keys(byDecade).sort((a, b) => byDecade[Number(b)] - byDecade[Number(a)])[0])
    const peakCount = byDecade[peakDec]
    if (peakCount >= 3) {
      cards.push({
        emoji:    '📅',
        label:    'Peak Decade',
        headline: `Most active in the ${peakDec}s`,
        stat:     `${peakDec}s`,
        subtext:  `${peakCount} films that decade`,
        actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
        gradient: 'purple',
        href:     `/actors/${actor.id}`,
      })
    }
  }

  // 3e. Most prolific year
  if (datedMovies.length >= 5) {
    const byYear: Record<number, number> = {}
    for (const m of datedMovies) byYear[m.release_year] = (byYear[m.release_year] ?? 0) + 1
    const peakYear  = Number(Object.keys(byYear).sort((a, b) => byYear[Number(b)] - byYear[Number(a)])[0])
    const peakYearN = byYear[peakYear]
    if (peakYearN >= 3) {
      cards.push({
        emoji:    '🔥',
        label:    'Prolific Year',
        headline: `${peakYearN} films released in a single year`,
        stat:     peakYear,
        subtext:  `${peakYearN} films in one year`,
        actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
        gradient: 'orange',
        href:     `/actors/${actor.id}`,
      })
    }
  }

  // 3f. Total screen time (avg_runtime × film_count)
  if (actor.avg_runtime && actor.avg_runtime > 0 && actor.film_count >= 5) {
    const totalMins = Math.round(actor.avg_runtime * actor.film_count)
    const totalHrs  = Math.round(totalMins / 60)
    cards.push({
      emoji:    '⏱',
      label:    'Screen Time',
      headline: `Across ${actor.film_count} films on screen`,
      stat:     `${totalHrs}hrs`,
      subtext:  `total on-screen time`,
      actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
      gradient: 'purple',
      href:     `/actors/${actor.id}`,
    })
  }

  // 3g. Language diversity
  const languages = new Set(datedMovies.map(m => m.language).filter(Boolean))
  if (languages.size >= 3) {
    cards.push({
      emoji:    '🌐',
      label:    'Multi-lingual',
      headline: `Films across ${languages.size} languages`,
      stat:     languages.size,
      subtext:  `languages worked in`,
      actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
      gradient: 'blue',
      href:     `/actors/${actor.id}`,
    })
  }

  // 3h. Recent activity — films in last 5 years
  const recentFilms = datedMovies.filter(m => m.release_year >= currentYear - 5)
  if (recentFilms.length >= 2) {
    cards.push({
      emoji:    '⚡',
      label:    'Recent Activity',
      headline: `${recentFilms.length} films in the last 5 years`,
      stat:     recentFilms.length,
      subtext:  `films since ${currentYear - 5}`,
      actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
      gradient: 'green',
      href:     `/actors/${actor.id}`,
    })
  }

  // 3i. Career span
  if (actor.first_film_year && actor.last_film_year) {
    const lastYear = Math.min(actor.last_film_year, currentYear)
    const span     = lastYear - actor.first_film_year
    if (span > 0) {
      cards.push({
        emoji:    '⏳',
        label:    'Career Span',
        headline: `${actor.first_film_year} – ${lastYear} · ${actor.industry} cinema`,
        stat:     `${span}yr`,
        subtext:  `Active for ${span} years`,
        actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
        gradient: 'orange',
        href:     `/actors/${actor.id}`,
      })
    }
  }

  // 3j. Total films milestone
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

  // 3k. Avg runtime
  if (actor.avg_runtime && actor.avg_runtime > 0) {
    const hrs   = Math.floor(actor.avg_runtime / 60)
    const mins  = Math.round(actor.avg_runtime % 60)
    const label = hrs > 0 ? `${hrs}h ${mins}m` : `${mins}m`
    cards.push({
      emoji:    '🎞',
      label:    'Avg Runtime',
      headline: `Average runtime per film`,
      stat:     label,
      subtext:  `across ${actor.film_count} films`,
      actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
      gradient: 'purple',
      href:     `/actors/${actor.id}`,
    })
  }

  // ── NEW PATTERNS from cross-actor analysis ────────────────────────────────

  // N1. Hit rate (% of films that were blockbusters ≥₹100Cr)
  const boFilms = datedMovies.filter(m => m.box_office != null && m.box_office! > 0)
  const hitFilms = boFilms.filter(m => m.box_office! >= 100)
  if (boFilms.length >= 5) {
    const hitRate = Math.round((hitFilms.length / boFilms.length) * 100)
    if (hitRate >= 15) {
      cards.push({
        emoji:    '🎯',
        label:    'Hit Rate',
        headline: `${hitFilms.length} of ${boFilms.length} films crossed ₹100 Cr`,
        stat:     `${hitRate}%`,
        subtext:  `commercial success rate · Source: TMDB`,
        actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
        gradient: 'green',
        href:     `/actors/${actor.id}`,
      })
    }
  }

  // N2. Critical darling — films rated ≥7.5
  const acclaimedFilms = ratedMovies.filter(m => (m.vote_average ?? 0) >= 7.5)
  if (acclaimedFilms.length >= 3) {
    cards.push({
      emoji:    '🎭',
      label:    'Critical Darling',
      headline: `${acclaimedFilms.length} films rated 7.5+ by audiences`,
      stat:     acclaimedFilms.length,
      subtext:  `critically acclaimed performances · Source: TMDB`,
      actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
      gradient: 'purple',
      href:     `/actors/${actor.id}`,
    })
  }

  // N3. Unicorn films — both critically acclaimed (≥7.0) AND blockbuster (≥₹100 Cr)
  const unicornFilms = datedMovies.filter(m =>
    (m.vote_average ?? 0) >= 7.0 && m.box_office != null && m.box_office! >= 100
  )
  if (unicornFilms.length >= 1) {
    cards.push({
      emoji:    '🦄',
      label:    'Unicorn Films',
      headline: `${unicornFilms.length === 1 ? unicornFilms[0].title : `${unicornFilms.length} films`} — critic & commercial hit`,
      stat:     unicornFilms.length,
      subtext:  `films that won both critics and box office · Source: TMDB`,
      actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
      gradient: 'amber',
      href:     `/actors/${actor.id}`,
    })
  }

  // N4. Director loyalty — % of films with top director
  if (directors.length > 0 && datedMovies.length >= 5) {
    const top = directors[0]
    const loyaltyPct = Math.round((top.films / datedMovies.length) * 100)
    if (loyaltyPct >= 10) {
      cards.push({
        emoji:    '🎬',
        label:    'Director Loyalty',
        headline: `${loyaltyPct}% of films directed by ${top.director}`,
        stat:     `${loyaltyPct}%`,
        subtext:  `${top.films} films together`,
        actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
        gradient: 'blue',
        href:     `/actors/${actor.id}`,
      })
    }
  }

  // N5. Box office growth — compare first-half avg vs second-half avg BO
  if (boFilms.length >= 6) {
    const sorted = [...boFilms].sort((a, b) => a.release_year - b.release_year)
    const mid = Math.floor(sorted.length / 2)
    const earlyAvg = sorted.slice(0, mid).reduce((s, m) => s + m.box_office!, 0) / mid
    const lateAvg  = sorted.slice(mid).reduce((s, m) => s + m.box_office!, 0) / (sorted.length - mid)
    const growthX  = (lateAvg / earlyAvg).toFixed(1)
    if (lateAvg > earlyAvg * 1.5) {
      cards.push({
        emoji:    '📈',
        label:    'Box Office Growth',
        headline: `Later films earn ${growthX}× more than early career`,
        stat:     `${growthX}×`,
        subtext:  `avg BO: ₹${Math.round(earlyAvg)}Cr early → ₹${Math.round(lateAvg)}Cr recent · Source: TMDB`,
        actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
        gradient: 'green',
        href:     `/actors/${actor.id}`,
      })
    }
  }

  // N6. Rating arc — early vs recent career ratings
  if (ratedMovies.length >= 8) {
    const sorted = [...ratedMovies].sort((a, b) => a.release_year - b.release_year)
    const mid = Math.floor(sorted.length / 2)
    const earlyAvg = sorted.slice(0, mid).reduce((s, m) => s + (m.vote_average ?? 0), 0) / mid
    const lateAvg  = sorted.slice(mid).reduce((s, m) => s + (m.vote_average ?? 0), 0) / (sorted.length - mid)
    const diff = lateAvg - earlyAvg
    if (Math.abs(diff) >= 0.3) {
      cards.push({
        emoji:    diff > 0 ? '⬆️' : '📉',
        label:    'Rating Arc',
        headline: diff > 0 ? `Improving — recent films rated higher` : `Early films rated higher`,
        stat:     `${earlyAvg.toFixed(1)} → ${lateAvg.toFixed(1)}`,
        subtext:  `audience rating: early vs recent career · Source: TMDB`,
        actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
        gradient: diff > 0 ? 'green' : 'orange',
        href:     `/actors/${actor.id}`,
      })
    }
  }

  // N7. Cross-industry — worked in multiple film languages/industries
  const industries = new Set(datedMovies.map(m => m.language).filter(Boolean))
  if (industries.size >= 3) {
    const homeCount = datedMovies.filter(m => m.language === actor.industry).length
    const crossover = datedMovies.length - homeCount
    cards.push({
      emoji:    '🌍',
      label:    'Pan-Indian',
      headline: `Worked across ${industries.size} film industries`,
      stat:     industries.size,
      subtext:  `${crossover} crossover films outside ${actor.industry}`,
      actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
      gradient: 'blue',
      href:     `/actors/${actor.id}`,
    })
  }

  // N8. Comeback — largest mid-career gap
  if (datedMovies.length >= 5) {
    const years = [...new Set(datedMovies.map(m => m.release_year))].sort((a, b) => a - b)
    let maxGap = 0, comebackYear = 0
    for (let i = 1; i < years.length; i++) {
      const gap = years[i] - years[i - 1]
      if (gap > maxGap) { maxGap = gap; comebackYear = years[i] }
    }
    if (maxGap >= 4) {
      cards.push({
        emoji:    '🔄',
        label:    'Comeback',
        headline: `Returned in ${comebackYear} after ${maxGap}-year break`,
        stat:     `${maxGap}yr`,
        subtext:  `longest mid-career gap`,
        actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
        gradient: 'purple',
        href:     `/actors/${actor.id}`,
      })
    }
  }

  // N9. High impact — avg BO per film (selective filmography)
  if (boFilms.length >= 3 && boFilms.length <= 20) {
    const avgBO = boFilms.reduce((s, m) => s + m.box_office!, 0) / boFilms.length
    if (avgBO >= 150) {
      cards.push({
        emoji:    '⚡',
        label:    'High Impact',
        headline: `Avg ${formatCrore(avgBO)} per film — selective filmography`,
        stat:     formatCrore(avgBO),
        subtext:  `average box office per film · Source: TMDB`,
        actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
        gradient: 'amber',
        href:     `/actors/${actor.id}`,
      })
    }
  }

  // ── PRIORITY 4 — Supporting / same-gender co-stars ───────────────────────

  const sameGenderPairs = isFemale
    ? leadCollaborators.filter(c =>  femaleNames.has(c.actor.toLowerCase()))
    : leadCollaborators.filter(c => !femaleNames.has(c.actor.toLowerCase()))

  // 4a. Top same-gender lead co-star (e.g. Mahesh + Prakash Raj)
  const topSameGender = sameGenderPairs[0]
  if (topSameGender && topSameGender.films >= 5) {
    cards.push({
      emoji:    '🤝',
      label:    'Top Co-Star',
      headline: `Frequently shares screen with ${topSameGender.actor}`,
      stat:     topSameGender.films,
      subtext:  `films together`,
      actors:   [
        { name: actor.name,           avatarSlug: slugify(actor.name) },
        { name: topSameGender.actor,  avatarSlug: slugify(topSameGender.actor) },
      ],
      gradient: 'purple',
      href:     `/actors/${actor.id}`,
    })
  }

  // 4b. Total unique co-stars (all collaborators)
  if (collaborators.length >= 20) {
    cards.push({
      emoji:    '👥',
      label:    'Co-Star Universe',
      headline: `${collaborators.length} different actors shared the screen`,
      stat:     collaborators.length,
      subtext:  `unique co-stars`,
      actors:   [{ name: actor.name, avatarSlug: slugify(actor.name) }],
      gradient: 'orange',
      href:     `/actors/${actor.id}`,
    })
  }

  // 4c. Most frequent overall collaborator (any role, not already shown)
  const topOverall = collaborators.find(c =>
    c.actor !== topPair?.actor &&
    c.actor !== topSameGender?.actor &&
    c.films >= 8
  )
  if (topOverall) {
    cards.push({
      emoji:    '🎞',
      label:    'Key Collaborator',
      headline: `Key on-screen partner — ${topOverall.actor}`,
      stat:     topOverall.films,
      subtext:  `films together`,
      actors:   [
        { name: actor.name,       avatarSlug: slugify(actor.name) },
        { name: topOverall.actor, avatarSlug: slugify(topOverall.actor) },
      ],
      gradient: 'orange',
      href:     `/actors/${actor.id}`,
    })
  }

  // 4d. Supporting depth — how many supporting collaborators have 5+ films
  const deepSupport = collaborators.filter(c => c.films >= 5).length
  if (deepSupport >= 5) {
    cards.push({
      emoji:    '🎭',
      label:    'Ensemble Depth',
      headline: `${deepSupport} actors in 5+ films together`,
      stat:     deepSupport,
      subtext:  `actors with 5+ films together`,
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
