/**
 * /social/connections/[actor1]/[actor2]
 *
 * Connection Finder social screenshot surface — NOT a product page.
 * Concept B ("The Path"): vertical chain, minimum metadata, cinematic receipts.
 *
 * Design intent:
 *   - Each step is a beat: face → film → face → film → face
 *   - Bridge actors are revealed by position, never labeled
 *   - Film titles are cinematic receipts — amber, tiny, uppercase
 *   - The thread is a whisper, not a diagram
 *   - Endpoint actors carry full visual weight; bridges are quieter
 *
 * Playwright target: [data-section="connection-finder"]
 * Capture: domcontentloaded + 1.5s (pure SSR — no client fetches)
 */

import { notFound } from 'next/navigation'
import Image from 'next/image'
import { searchActors, getActorConnection, toAvatarSlug } from '@/lib/api'
import type { ConnectionPath } from '@/lib/api'

interface PageProps {
  params: { actor1: string; actor2: string }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function truncate(title: string, max = 26): string {
  return title.length > max ? title.slice(0, max - 1) + '…' : title
}

function footerLine(connection: ConnectionPath): string {
  if (!connection.found)    return 'No thread found.'
  if (connection.depth === 1) return 'One film apart.'
  return `Connected in ${connection.depth} steps.`
}

// ── Sub-components (server-side, no hooks) ────────────────────────────────────

function ActorNode({
  name,
  isEndpoint,
}: {
  name: string
  isEndpoint: boolean
}) {
  const slug    = toAvatarSlug(name)  // avatar filename slug (no separators)
  const initial = name.charAt(0).toUpperCase()
  const size    = isEndpoint ? 64 : 52

  return (
    <div className="flex flex-col items-center" style={{ gap: '10px' }}>
      {/* Avatar circle — amber ring, initials as SSR fallback */}
      <div
        className="relative rounded-full overflow-hidden flex items-center justify-center flex-shrink-0"
        style={{
          width:     `${size}px`,
          height:    `${size}px`,
          background: 'rgba(245, 158, 11, 0.07)',
          boxShadow:  isEndpoint
            ? '0 0 0 1px rgba(245, 158, 11, 0.35)'
            : '0 0 0 1px rgba(245, 158, 11, 0.18)',
        }}
      >
        <span
          className="select-none font-medium absolute"
          style={{
            color:    'rgba(245, 158, 11, 0.32)',
            fontSize: `${Math.round(size * 0.34)}px`,
          }}
        >
          {initial}
        </span>
        <Image
          src={`/avatars/${slug}.png`}
          alt={name}
          fill
          sizes={`${size}px`}
          className="object-cover"
        />
      </div>

      {/* Name */}
      <p
        className="text-center uppercase"
        style={{
          color:       isEndpoint ? 'rgba(255,255,255,0.85)' : 'rgba(255,255,255,0.45)',
          fontSize:    isEndpoint ? '12px' : '11px',
          letterSpacing: '0.22em',
          fontWeight:  isEndpoint ? 400 : 300,
          lineHeight:  1.3,
        }}
      >
        {name}
      </p>
    </div>
  )
}

/** One segment of the amber thread between two nodes */
function Thread() {
  return (
    <div
      className="flex-shrink-0"
      style={{
        width:      '1px',
        height:     '28px',
        background: 'rgba(245, 158, 11, 0.18)',
      }}
    />
  )
}

/** Film title — the cinematic receipt */
function FilmReceipt({ title }: { title: string }) {
  return (
    <p
      className="text-center uppercase"
      style={{
        color:         'rgba(245, 158, 11, 0.60)',
        fontSize:      '10px',
        letterSpacing: '0.18em',
        lineHeight:    1.4,
        maxWidth:      '200px',
      }}
    >
      {truncate(title)}
    </p>
  )
}

/** Shown when connection.found === false */
function NoThreadCard({
  name1,
  name2,
}: {
  name1: string
  name2: string
}) {
  return (
    <div className="flex flex-col items-center" style={{ gap: '28px' }}>
      <ActorNode name={name1} isEndpoint />

      {/* Broken thread indicator */}
      <p
        style={{
          color:         'rgba(245, 158, 11, 0.22)',
          fontSize:      '11px',
          letterSpacing: '0.5em',
        }}
      >
        ·  ·  ·  ·  ·
      </p>

      <ActorNode name={name2} isEndpoint />
    </div>
  )
}

// ── Page ──────────────────────────────────────────────────────────────────────

export default async function ConnectionFinderPage({ params }: PageProps) {
  const { actor1: slug1, actor2: slug2 } = params

  // Resolve both slugs → actor IDs in parallel
  const [results1, results2] = await Promise.all([
    searchActors(slug1.replace(/-/g, ' ')).catch(
      () => [] as Awaited<ReturnType<typeof searchActors>>
    ),
    searchActors(slug2.replace(/-/g, ' ')).catch(
      () => [] as Awaited<ReturnType<typeof searchActors>>
    ),
  ])

  if (!results1.length || !results2.length) notFound()

  const id1 = results1[0].id
  const id2 = results2[0].id

  const connection = await getActorConnection(id1, id2).catch(() => null)
  if (!connection) notFound()

  return (
    <div className="min-h-screen bg-[#0a0a0f] p-8">
      <div
        data-section="connection-finder"
        className="mx-auto flex flex-col items-center"
        style={{ maxWidth: '480px', paddingTop: '52px', paddingBottom: '52px' }}
      >

        {/* ── Chain or no-thread card ──────────────────────────────────── */}
        {connection.found ? (
          /*
           * Interleave path[] and connections[]:
           *   path[0] → connections[0] → path[1] → connections[1] → path[2]
           *
           * Each actor segment owns the thread + receipt that follows it.
           * The last actor has no trailing connector (connections[last] is undefined).
           */
          connection.path.map((actor, i) => {
            const isEndpoint = i === 0 || i === connection.path.length - 1
            const film       = connection.connections[i]

            return (
              <div key={`${actor.id}-${i}`} className="flex flex-col items-center">
                <ActorNode name={actor.name} isEndpoint={isEndpoint} />
                {film && (
                  <>
                    <Thread />
                    <FilmReceipt title={film.movie_title} />
                    <Thread />
                  </>
                )}
              </div>
            )
          })
        ) : (
          <NoThreadCard
            name1={results1[0].name}
            name2={results2[0].name}
          />
        )}

        {/* ── Footer ──────────────────────────────────────────────────── */}
        <div
          className="flex w-full items-center justify-between"
          style={{ marginTop: '44px' }}
        >
          <p
            className="uppercase"
            style={{
              color:         '#4b5563',
              fontSize:      '10px',
              letterSpacing: '0.25em',
            }}
          >
            {footerLine(connection)}
          </p>
          <p
            className="uppercase"
            style={{
              color:         '#374151',
              fontSize:      '9px',
              letterSpacing: '0.3em',
            }}
          >
            ◆&nbsp;cinetrace.in
          </p>
        </div>

      </div>
    </div>
  )
}
