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

function crore(val: number) {
  if (val >= 1000) return `₹${(val / 1000).toFixed(1)}K Cr`
  return `₹${Math.round(val)} Cr`
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
  const pronoun     = isFemale ? 'her' : 'his'

  const femaleNames  = new Set(allFemaleActors.map(a => a.name.toLowerCase()))
  const datedMovies  = movies.filter(m => m.release_year > 0 && m.release_year <= currentYear)
  const ratedMovies  = datedMovies.filter(m => m.vote_average && m.vote_average > 0)

  function card(c: InsightCardData) { cards.push(c) }
  function actorAvatar() { return [{ name: actor.name, avatarSlug: slugify(actor.name) }] }
  function duo(other: string) {
    return [
      { name: actor.name, avatarSlug: slugify(actor.name) },
      { name: other,      avatarSlug: slugify(other) },
    ]
  }

  // ── 1. Lead co-star pairing ────────────────────────────────────────────────
  // One card per pair — no separate "Legendary Duo" duplicate.

  const leadPairs = isFemale
    ? leadCollaborators.filter(c => !femaleNames.has(c.actor.toLowerCase()))
    : leadCollaborators.filter(c =>  femaleNames.has(c.actor.toLowerCase()))

  const topPair = leadPairs[0]
  if (topPair) {
    card({
      emoji: '🎭', insightType: 'collab_shock', gradient: 'red',
      label:    topPair.films >= 8 ? 'Iconic Pair' : 'Top Co-Star',
      headline: `${actor.name} and ${topPair.actor} have done ${topPair.films} films together`,
      stat:     topPair.films,
      subtext:  `films together`,
      actors:   duo(topPair.actor),
      href:     `/compare/${slugify(actor.name)}-vs-${slugify(topPair.actor)}`,
    })
  }

  // Second lead pair (≥ 6 films — raised from 5)
  const secondPair = leadPairs[1]
  if (secondPair && secondPair.films >= 6) {
    card({
      emoji: '💫', insightType: 'collab_shock', gradient: 'red',
      label:    'Popular Pair',
      headline: `Also a fan favourite pair — with ${secondPair.actor}`,
      stat:     secondPair.films,
      subtext:  `films together`,
      actors:   duo(secondPair.actor),
      href:     `/compare/${slugify(actor.name)}-vs-${slugify(secondPair.actor)}`,
    })
  }

  // How many unique lead co-stars
  if (leadPairs.length >= 5) {
    card({
      emoji: '🌟', insightType: 'network_power', gradient: 'blue',
      label:    isFemale ? 'Leading Men' : 'Leading Ladies',
      headline: `Has starred opposite ${leadPairs.length} different ${isFemale ? 'leading men' : 'heroines'}`,
      stat:     leadPairs.length,
      subtext:  `unique lead co-stars`,
      actors:   actorAvatar(),
      href:     `/actors/${actor.id}`,
    })
  }

  // ── 2. Director bond ───────────────────────────────────────────────────────
  // One card for top director — no separate "Auteur Partnership" duplicate.

  const topDir    = directors[0]
  const secondDir = directors[1]

  if (topDir && topDir.films >= 2) {
    card({
      emoji: '🎬', insightType: 'director_loyalty', gradient: 'amber',
      label:    topDir.films >= 8 ? 'Favourite Director' : 'Top Director',
      headline: `${topDir.films} films directed by ${topDir.director}`,
      stat:     topDir.films,
      subtext:  `films together`,
      actors:   actorAvatar(),
      href:     `/actors/${actor.id}`,
    })
  }

  // Second director (≥ 5 films — raised from 4)
  if (secondDir && secondDir.films >= 5) {
    card({
      emoji: '🎥', insightType: 'director_loyalty', gradient: 'amber',
      label:    'Another Bond',
      headline: `${secondDir.films} films with ${secondDir.director} as well`,
      stat:     secondDir.films,
      subtext:  `films together`,
      actors:   actorAvatar(),
      href:     `/actors/${actor.id}`,
    })
  }

  // Total unique directors
  if (directors.length >= 10) {
    card({
      emoji: '🗂️', insightType: 'network_power', gradient: 'blue',
      label:    'Worked With',
      headline: `Has worked with ${directors.length} different directors`,
      stat:     directors.length,
      subtext:  `directors`,
      actors:   actorAvatar(),
      href:     `/actors/${actor.id}`,
    })
  }

  // Director loyalty % (raised minimum to 20%)
  if (topDir && datedMovies.length >= 5) {
    const pct = Math.round((topDir.films / datedMovies.length) * 100)
    if (pct >= 20) {
      card({
        emoji: '🤝', insightType: 'director_loyalty', gradient: 'amber',
        label:    'Director Loyalty',
        headline: `${pct}% of ${pronoun} films were directed by ${topDir.director}`,
        stat:     `${pct}%`,
        subtext:  `of all films`,
        actors:   actorAvatar(),
        href:     `/actors/${actor.id}`,
      })
    }
  }

  // ── 3. Box office ─────────────────────────────────────────────────────────

  const topHit = blockbusters[0]
  if (topHit) {
    card({
      emoji: '💰', insightType: 'career_peak', gradient: 'green',
      label:    'Biggest Hit',
      headline: `${topHit.title} (${topHit.release_year}) is ${pronoun} biggest film`,
      stat:     crore(topHit.box_office_crore),
      subtext:  `box office collection`,
      actors:   actorAvatar(),
      href:     `/actors/${actor.id}`,
    })
  }

  if (blockbusters.length >= 3) {
    const total = blockbusters.reduce((s, b) => s + b.box_office_crore, 0)
    card({
      emoji: '📊', insightType: 'career_peak', gradient: 'green',
      label:    'Box Office Total',
      headline: `Top ${blockbusters.length} films have collected a combined ${crore(total)}`,
      stat:     crore(total),
      subtext:  `combined collection`,
      actors:   actorAvatar(),
      href:     `/actors/${actor.id}`,
    })
  }

  // Best returns on investment
  const roiFilms = blockbusters.filter(b => b.budget_crore && b.budget_crore > 0)
  if (roiFilms.length > 0) {
    const best = roiFilms.reduce((a, b) =>
      (b.box_office_crore / b.budget_crore!) > (a.box_office_crore / a.budget_crore!) ? b : a
    )
    const roi = (best.box_office_crore / best.budget_crore!).toFixed(1)
    card({
      emoji: '🚀', insightType: 'career_peak', gradient: 'green',
      label:    'Best Returns',
      headline: `${best.title} made ${roi}× its budget back`,
      stat:     `${roi}×`,
      subtext:  `return on ₹${Math.round(best.budget_crore!)} Cr budget`,
      actors:   actorAvatar(),
      href:     `/actors/${actor.id}`,
    })
  }

  // Hit rate (raised minimum to 20%)
  const boFilms  = datedMovies.filter(m => m.box_office != null && m.box_office! > 0)
  const hitFilms = boFilms.filter(m => m.box_office! >= 100)
  if (boFilms.length >= 5) {
    const rate = Math.round((hitFilms.length / boFilms.length) * 100)
    if (rate >= 20) {
      card({
        emoji: '🎯', insightType: 'career_peak', gradient: 'green',
        label:    'Success Rate',
        headline: `${hitFilms.length} out of ${boFilms.length} films crossed ₹100 Cr`,
        stat:     `${rate}%`,
        subtext:  `films that were blockbusters`,
        actors:   actorAvatar(),
        href:     `/actors/${actor.id}`,
      })
    }
  }

  // Box office growth — later career vs early career
  if (boFilms.length >= 6) {
    const sorted   = [...boFilms].sort((a, b) => a.release_year - b.release_year)
    const mid      = Math.floor(sorted.length / 2)
    const earlyAvg = sorted.slice(0, mid).reduce((s, m) => s + m.box_office!, 0) / mid
    const lateAvg  = sorted.slice(mid).reduce((s, m) => s + m.box_office!, 0) / (sorted.length - mid)
    if (lateAvg > earlyAvg * 1.5) {
      const growthX = (lateAvg / earlyAvg).toFixed(1)
      card({
        emoji: '📈', insightType: 'career_peak', gradient: 'green',
        label:    'Getting Bigger',
        headline: `Recent films earn ${growthX}× more than ${pronoun} early films`,
        stat:     `${growthX}×`,
        subtext:  `box office growth over career`,
        actors:   actorAvatar(),
        href:     `/actors/${actor.id}`,
      })
    }
  }

  // ── 4. Ratings & critical reception ──────────────────────────────────────

  if (ratedMovies.length > 0) {
    const best = [...ratedMovies].sort((a, b) => (b.vote_average ?? 0) - (a.vote_average ?? 0))[0]
    if (best.vote_average && best.vote_average >= 7) {
      card({
        emoji: '⭐', insightType: 'hidden_dominance', gradient: 'orange',
        label:    'Audience Favourite',
        headline: `${best.title} (${best.release_year}) is ${pronoun} most loved film`,
        stat:     `${best.vote_average.toFixed(1)}/10`,
        subtext:  `fan rating`,
        actors:   actorAvatar(),
        href:     `/actors/${actor.id}`,
      })
    }
  }

  // Films rated 7.5+
  const acclaimedFilms = ratedMovies.filter(m => (m.vote_average ?? 0) >= 7.5)
  if (acclaimedFilms.length >= 3) {
    card({
      emoji: '🎭', insightType: 'hidden_dominance', gradient: 'purple',
      label:    'Fan Favourite',
      headline: `${acclaimedFilms.length} films rated 7.5 or above by fans`,
      stat:     acclaimedFilms.length,
      subtext:  `highly rated films`,
      actors:   actorAvatar(),
      href:     `/actors/${actor.id}`,
    })
  }

  // Hit AND loved by fans
  const unicornFilms = datedMovies.filter(m =>
    (m.vote_average ?? 0) >= 7.0 && m.box_office != null && m.box_office! >= 100
  )
  if (unicornFilms.length >= 1) {
    card({
      emoji: '🦄', insightType: 'hidden_dominance', gradient: 'purple',
      label:    'Hit and Loved',
      headline: unicornFilms.length === 1
        ? `${unicornFilms[0].title} was both a blockbuster and a fan favourite`
        : `${unicornFilms.length} films that were blockbusters and fan favourites`,
      stat:     unicornFilms.length,
      subtext:  `blockbuster + fan favourite`,
      actors:   actorAvatar(),
      href:     `/actors/${actor.id}`,
    })
  }

  // ── 5. Career stats ───────────────────────────────────────────────────────

  if (actor.film_count >= 10) {
    card({
      emoji: '🏆', insightType: 'cross_industry', gradient: 'blue',
      label:    'Films and Counting',
      headline: `${actor.film_count} films in the database — and still going`,
      stat:     actor.film_count,
      subtext:  `films in the database`,
      actors:   actorAvatar(),
      href:     `/actors/${actor.id}`,
    })
  }

  if (actor.first_film_year && actor.last_film_year) {
    const lastYear = Math.min(actor.last_film_year, currentYear)
    const span     = lastYear - actor.first_film_year
    if (span > 0) {
      card({
        emoji: '⏳', insightType: 'cross_industry', gradient: 'orange',
        label:    'Career Span',
        headline: `${span} years in ${actor.industry} cinema — from ${actor.first_film_year} to ${lastYear}`,
        stat:     `${span} yrs`,
        subtext:  `years in cinema`,
        actors:   actorAvatar(),
        href:     `/actors/${actor.id}`,
      })
    }
  }

  // Peak decade
  if (datedMovies.length >= 5) {
    const byDecade: Record<number, number> = {}
    for (const m of datedMovies) {
      const dec = Math.floor(m.release_year / 10) * 10
      byDecade[dec] = (byDecade[dec] ?? 0) + 1
    }
    const peakDec   = Number(Object.keys(byDecade).sort((a, b) => byDecade[Number(b)] - byDecade[Number(a)])[0])
    const peakCount = byDecade[peakDec]
    if (peakCount >= 3) {
      card({
        emoji: '📅', insightType: 'career_peak', gradient: 'purple',
        label:    'Golden Decade',
        headline: `${peakCount} films in the ${peakDec}s — ${pronoun} most active decade`,
        stat:     `${peakDec}s`,
        subtext:  `${peakCount} films that decade`,
        actors:   actorAvatar(),
        href:     `/actors/${actor.id}`,
      })
    }
  }

  // Busiest single year (raised threshold to ≥ 4 films)
  if (datedMovies.length >= 5) {
    const byYear: Record<number, number> = {}
    for (const m of datedMovies) byYear[m.release_year] = (byYear[m.release_year] ?? 0) + 1
    const peakYear  = Number(Object.keys(byYear).sort((a, b) => byYear[Number(b)] - byYear[Number(a)])[0])
    const peakYearN = byYear[peakYear]
    if (peakYearN >= 4) {
      card({
        emoji: '🔥', insightType: 'career_peak', gradient: 'orange',
        label:    'Busiest Year',
        headline: `Released ${peakYearN} films in ${peakYear} alone`,
        stat:     peakYearN,
        subtext:  `films in one year`,
        actors:   actorAvatar(),
        href:     `/actors/${actor.id}`,
      })
    }
  }

  // Still active — raised to ≥ 4 films in last 5 years
  const recentFilms = datedMovies.filter(m => m.release_year >= currentYear - 5)
  if (recentFilms.length >= 4) {
    card({
      emoji: '⚡', insightType: 'cross_industry', gradient: 'green',
      label:    'Still Going Strong',
      headline: `${recentFilms.length} films in the last 5 years`,
      stat:     recentFilms.length,
      subtext:  `films since ${currentYear - 5}`,
      actors:   actorAvatar(),
      href:     `/actors/${actor.id}`,
    })
  }

  // Pan-India (replaces the duplicate Multi-lingual card)
  const industries = new Set(datedMovies.map(m => m.language).filter(Boolean))
  if (industries.size >= 3) {
    const homeCount = datedMovies.filter(m => m.language === actor.industry).length
    const crossover = datedMovies.length - homeCount
    card({
      emoji: '🌍', insightType: 'cross_industry', gradient: 'blue',
      label:    'Pan-India Star',
      headline: `${crossover} films outside ${actor.industry} — a true Pan-India star`,
      stat:     industries.size,
      subtext:  `film industries`,
      actors:   actorAvatar(),
      href:     `/actors/${actor.id}`,
    })
  }

  // Comeback — raised to ≥ 5 year gap
  if (datedMovies.length >= 5) {
    const years = [...new Set(datedMovies.map(m => m.release_year))].sort((a, b) => a - b)
    let maxGap = 0, comebackYear = 0
    for (let i = 1; i < years.length; i++) {
      const gap = years[i] - years[i - 1]
      if (gap > maxGap) { maxGap = gap; comebackYear = years[i] }
    }
    if (maxGap >= 5) {
      card({
        emoji: '🔄', insightType: 'hidden_dominance', gradient: 'purple',
        label:    'The Comeback',
        headline: `Came back in ${comebackYear} after a ${maxGap}-year break`,
        stat:     `${maxGap} yrs`,
        subtext:  `break from films`,
        actors:   actorAvatar(),
        href:     `/actors/${actor.id}`,
      })
    }
  }

  // ── 6. Co-star universe ───────────────────────────────────────────────────

  // Top same-gender co-star (raised to ≥ 6 films)
  const sameGenderPairs = isFemale
    ? leadCollaborators.filter(c =>  femaleNames.has(c.actor.toLowerCase()))
    : leadCollaborators.filter(c => !femaleNames.has(c.actor.toLowerCase()))

  const topSameGender = sameGenderPairs[0]
  if (topSameGender && topSameGender.films >= 6) {
    card({
      emoji: '🤝', insightType: 'collab_shock', gradient: 'purple',
      label:    'Co-Star Bond',
      headline: `Shares screen with ${topSameGender.actor} more than anyone else — ${topSameGender.films} times`,
      stat:     topSameGender.films,
      subtext:  `films together`,
      actors:   duo(topSameGender.actor),
      href:     `/compare/${slugify(actor.name)}-vs-${slugify(topSameGender.actor)}`,
    })
  }

  // Most frequent overall collaborator not already shown
  const topOverall = collaborators.find(c =>
    c.actor !== topPair?.actor &&
    c.actor !== topSameGender?.actor &&
    c.films >= 8
  )
  if (topOverall) {
    card({
      emoji: '🎞', insightType: 'collab_shock', gradient: 'orange',
      label:    'Key Co-Star',
      headline: `${topOverall.actor} is another person you keep seeing with ${actor.name.split(' ')[0]}`,
      stat:     topOverall.films,
      subtext:  `films together`,
      actors:   duo(topOverall.actor),
      href:     `/compare/${slugify(actor.name)}-vs-${slugify(topOverall.actor)}`,
    })
  }

  // Total unique co-stars
  if (collaborators.length >= 20) {
    card({
      emoji: '👥', insightType: 'network_power', gradient: 'orange',
      label:    'Knows Everyone',
      headline: `Has shared screen with ${collaborators.length} different actors`,
      stat:     collaborators.length,
      subtext:  `unique co-stars`,
      actors:   actorAvatar(),
      href:     `/actors/${actor.id}`,
    })
  }

  if (cards.length === 0) return null

  return (
    <div className="flex flex-col gap-4">
      <h2 className="text-xs font-semibold uppercase tracking-widest text-white/40">
        ✨ By the numbers
      </h2>
      <InsightsCarousel cards={cards} />
    </div>
  )
}
