/**
 * /social/stat-card/[slug]
 *
 * Social screenshot surface for tweet attachments.
 * Shows a prominent actor portrait in the StatMuse style —
 * large avatar filling most of the card, name + branding below.
 *
 * Playwright target: [data-section="stat-card"]
 * Capture: domcontentloaded + 1.5s (SSR only)
 */

import { notFound } from 'next/navigation'
import Image from 'next/image'
import { searchActors, getActor } from '@/lib/api'

interface PageProps {
  params: { slug: string }
}

export default async function StatCardPage({ params }: PageProps) {
  const slug = params.slug

  let id: number | string = slug
  if (!/^\d+$/.test(slug)) {
    const nameFromSlug = slug.replace(/-/g, ' ')
    const results = await searchActors(nameFromSlug).catch(() => [] as Awaited<ReturnType<typeof searchActors>>)
    if (!results.length) notFound()
    id = results[0].id
  }

  const actor = await getActor(id).catch(() => null)
  if (!actor) notFound()

  const avatarSlug = actor.name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '')
  const initial    = actor.name.charAt(0).toUpperCase()

  return (
    <div
      className="min-h-screen"
      style={{ background: '#0a0a0f' }}
    >
      <div
        data-section="stat-card"
        style={{
          width:    '560px',
          height:   '560px',
          margin:   '0 auto',
          position: 'relative',
          overflow: 'hidden',
          background: 'linear-gradient(160deg, #111118 0%, #0a0a0f 60%)',
        }}
      >

        {/* ── Large actor portrait ─────────────────────────────────────────── */}
        <div
          style={{
            position: 'absolute',
            bottom:   0,
            left:     '50%',
            transform: 'translateX(-50%)',
            width:    '420px',
            height:   '480px',
          }}
        >
          {/* Initials fallback — sits behind the image */}
          <div
            style={{
              position:       'absolute',
              inset:          0,
              display:        'flex',
              alignItems:     'center',
              justifyContent: 'center',
              fontSize:       '120px',
              fontWeight:     700,
              color:          'rgba(245, 158, 11, 0.15)',
              userSelect:     'none',
              letterSpacing:  '-0.02em',
            }}
          >
            {initial}
          </div>
          <Image
            src={`/avatars/${avatarSlug}.png`}
            alt={actor.name}
            fill
            sizes="420px"
            style={{ objectFit: 'cover', objectPosition: 'top center' }}
          />
          {/* Bottom fade — so name text reads cleanly */}
          <div
            style={{
              position:   'absolute',
              bottom:     0,
              left:       0,
              right:      0,
              height:     '180px',
              background: 'linear-gradient(to top, #0a0a0f 0%, transparent 100%)',
            }}
          />
        </div>

        {/* ── Subtle top vignette so branding reads ──────────────────────── */}
        <div
          style={{
            position:   'absolute',
            top:        0,
            left:       0,
            right:      0,
            height:     '120px',
            background: 'linear-gradient(to bottom, rgba(10,10,15,0.7) 0%, transparent 100%)',
            pointerEvents: 'none',
          }}
        />

        {/* ── CineTrace branding — top-right ─────────────────────────────── */}
        <div
          style={{
            position:    'absolute',
            top:         '20px',
            right:       '24px',
            display:     'flex',
            alignItems:  'center',
            gap:         '6px',
          }}
        >
          <div
            style={{
              width:        '6px',
              height:       '6px',
              borderRadius: '50%',
              background:   'rgba(245, 158, 11, 0.6)',
            }}
          />
          <span
            style={{
              color:         'rgba(255,255,255,0.55)',
              fontSize:      '11px',
              letterSpacing: '0.28em',
              textTransform: 'uppercase',
              fontWeight:    400,
            }}
          >
            cinetrace.in
          </span>
        </div>

        {/* ── Actor name — bottom, above the fade ────────────────────────── */}
        <div
          style={{
            position:       'absolute',
            bottom:         '28px',
            left:           0,
            right:          0,
            textAlign:      'center',
          }}
        >
          <p
            style={{
              color:         'rgba(255, 255, 255, 0.90)',
              fontSize:      '18px',
              letterSpacing: '0.22em',
              textTransform: 'uppercase',
              fontWeight:    400,
              margin:        0,
            }}
          >
            {actor.name}
          </p>
        </div>

      </div>
    </div>
  )
}
