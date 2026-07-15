/**
 * /social/director-loyalty/[slug]
 *
 * Social screenshot surface — NOT a product page.
 * Renders a single cinematic card: actor × their most loyal director.
 *
 * Design intent (Variation B — "The Witness"):
 *   Actor portrait → chapter title → monumental count → director reveal → era
 *
 * Playwright target: [data-section="director-loyalty"]
 * Capture: domcontentloaded + 1.5s (SSR only, no client-side fetches)
 */

import { notFound } from 'next/navigation'
import Image from 'next/image'
import { searchActors, getActor, getActorDirectors, getActorMovies, toAvatarSlug } from '@/lib/api'
import type { DirectorCollab, ActorMovie } from '@/lib/api'

interface PageProps {
  params: { slug: string }
}

export default async function DirectorLoyaltyPage({ params }: PageProps) {
  const slug = params.slug

  // Resolve slug → numeric actor ID (mirrors chart page logic)
  let id: number | string = slug
  if (!/^\d+$/.test(slug)) {
    const nameFromSlug = slug.replace(/-/g, ' ')
    const results = await searchActors(nameFromSlug).catch(() => [] as Awaited<ReturnType<typeof searchActors>>)
    if (!results.length) notFound()
    id = results[0].id
  }

  const [actor, directors, movies] = await Promise.all([
    getActor(id).catch(() => null),
    getActorDirectors(id).catch(() => [] as DirectorCollab[]),
    getActorMovies(id).catch(() => [] as ActorMovie[]),
  ])

  if (!actor || !directors.length) notFound()

  // Top director — API returns sorted by film_count desc
  const top = directors[0]

  // Derive year range client-free: filter movies by director name
  const years = movies
    .filter(m => m.director === top.director && m.release_year > 0)
    .map(m => m.release_year)
    .sort((a, b) => a - b)

  const firstYear = years[0]                    ?? null
  const lastYear  = years[years.length - 1]     ?? null
  // Em dash with spaces — feels like a cinematic date range, not a data field
  const yearRange = firstYear && lastYear
    ? `${firstYear} — ${lastYear}`   // thin-space + em-dash + thin-space
    : null

  // Avatar filename slug (no separators) — NOT the dash-based route slug.
  const avatarSlug = toAvatarSlug(actor.name)
  const initial    = actor.name.charAt(0).toUpperCase()

  return (
    <div className="min-h-screen bg-[#0a0a0f] p-8">
      {/*
        data-section attr is the Playwright target.
        flex-col + items-center gives the centered column layout.
        Width is capped so the card doesn't spread to 800px — tight negative space.
      */}
      <div
        data-section="director-loyalty"
        className="mx-auto flex flex-col items-center"
        style={{ maxWidth: '560px', paddingTop: '36px', paddingBottom: '52px' }}
      >

        {/* ── Actor: the documentary subject ─────────────────────────────── */}
        <div className="flex flex-col items-center gap-3 mb-12">

          {/* Avatar — amber ring, initials underneath as SSR fallback */}
          <div
            className="relative rounded-full overflow-hidden flex-shrink-0 flex items-center justify-center"
            style={{
              width:  '88px',
              height: '88px',
              background: 'rgba(245, 158, 11, 0.08)',
              boxShadow: '0 0 0 1px rgba(245, 158, 11, 0.25)',
            }}
          >
            {/* Initials — visible if image 404s */}
            <span
              className="select-none font-medium absolute"
              style={{ color: 'rgba(245, 158, 11, 0.4)', fontSize: '28px' }}
            >
              {initial}
            </span>
            <Image
              src={`/avatars/${avatarSlug}.png`}
              alt={actor.name}
              fill
              sizes="88px"
              className="object-cover"
            />
          </div>

          {/* Actor name — 14px so it survives 50% feed compression */}
          <p
            className="text-white/80 uppercase text-center"
            style={{ fontSize: '14px', letterSpacing: '0.2em', fontWeight: 400 }}
          >
            {actor.name}
          </p>
        </div>

        {/* ── Chapter title — subliminal at feed scale, intentional ───────── */}
        {/* At 47% compression this reads as visual rhythm, not literal text.  */}
        {/* Tracking reduced from 0.52em → 0.3em so letters survive as a unit */}
        <p
          className="text-center"
          style={{
            color:         'rgba(245, 158, 11, 0.65)',
            fontSize:      '12px',
            letterSpacing: '0.3em',
            textTransform: 'uppercase',
            marginBottom:  '48px',
          }}
        >
          ONE&nbsp;DIRECTOR.&nbsp;ALWAYS.
        </p>

        {/* ── The count: monumental ──────────────────────────────────────── */}
        <p
          className="font-bold leading-none tabular-nums text-center"
          style={{
            fontSize:   '128px',
            color:      '#f59e0b',
            textShadow: '0 0 80px rgba(245, 158, 11, 0.20)',
            marginBottom: '10px',
          }}
        >
          {top.films}
        </p>

        {/* "films together" — quiet, subordinate */}
        <p
          className="text-center uppercase"
          style={{
            color:         '#4b5563',
            fontSize:      '11px',
            letterSpacing: '0.35em',
            marginBottom:  '56px',
          }}
        >
          films&nbsp;together
        </p>

        {/* ── Director name: the revelation ──────────────────────────────── */}
        {/* weight 300 → 400: light type at ~13px after compression blurs    */}
        <p
          className="text-white uppercase text-center"
          style={{
            fontSize:      '28px',
            letterSpacing: '0.18em',
            fontWeight:    400,
            marginBottom:  '12px',
          }}
        >
          {top.director}
        </p>

        {/* Year range — the era, not a stat */}
        {yearRange && (
          <p
            className="text-center"
            style={{
              color:         'rgba(180, 120, 30, 0.75)',
              fontSize:      '13px',
              letterSpacing: '0.12em',
            }}
          >
            {yearRange}
          </p>
        )}

        {/* ── Branding — bottom left, nearly invisible ────────────────────── */}
        <div
          className="self-start"
          style={{ marginTop: '56px' }}
        >
          <p
            style={{
              color:         '#374151',
              fontSize:      '9px',
              letterSpacing: '0.3em',
              textTransform: 'uppercase',
            }}
          >
            ◆&nbsp;cinetrace.in
          </p>
        </div>

      </div>
    </div>
  )
}
