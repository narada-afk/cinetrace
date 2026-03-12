import Image from 'next/image'

export type MissingDataType =
  | 'poster'
  | 'poster_old'
  | 'cast'
  | 'director'
  | 'rating'
  | 'backdrop'

interface MissingDataProps {
  type: MissingDataType
  /** Movie title — used to personalise humour lines */
  title?: string
  /** Release year — reserved for future context-aware lines */
  releaseYear?: number
  /** For backdrop: primary backdrop image url */
  backdropUrl?: string | null
  /** For backdrop: poster used as blurred fallback */
  posterUrl?: string | null
}

// ── Humour lines ──────────────────────────────────────────────────────────────
// Each template has a titled variant (used when a title is passed) and a
// generic fallback. The line is chosen via a deterministic string hash so the
// same movie always gets the same line — stable across ISR revalidations.

type LineTemplate = {
  withTitle: (t: string) => string
  generic: string
}

const LINE_TEMPLATES: LineTemplate[] = [
  {
    withTitle: (t) => `Even TMDB drew a blank for ${t}.`,
    generic: 'Even TMDB drew a blank.',
  },
  {
    withTitle: (t) => `The movie exists. The poster does not.`,
    generic: 'The movie exists. The poster does not.',
  },
  {
    withTitle: (t) => `Archive copy required for ${t}.`,
    generic: 'Archive copy required.',
  },
  {
    withTitle: () => 'Cult classic energy detected.',
    generic: 'Cult classic energy detected.',
  },
  {
    withTitle: (t) => `Even TMDB couldn't find it.`,
    generic: "Even TMDB couldn't find it.",
  },
]

/** djb2-style hash — maps a string to a stable positive integer. */
function hashString(s: string): number {
  let h = 5381
  for (let i = 0; i < s.length; i++) {
    h = ((h << 5) + h) ^ s.charCodeAt(i)
  }
  return Math.abs(h)
}

function pickLine(title?: string): string {
  const seed = title ? hashString(title) : Math.floor(Math.random() * 1_000_000)
  const template = LINE_TEMPLATES[seed % LINE_TEMPLATES.length]
  return title ? template.withTitle(title) : template.generic
}

// ── Component ─────────────────────────────────────────────────────────────────

export default function MissingData({
  type,
  title,
  backdropUrl,
  posterUrl,
}: MissingDataProps) {

  // ── poster ────────────────────────────────────────────────────
  if (type === 'poster') {
    const line = pickLine(title)
    return (
      <div
        className="
          w-full h-full
          bg-gradient-to-br from-slate-800 to-slate-900
          flex flex-col items-center justify-center text-center gap-1 p-4
        "
      >
        <span className="text-3xl select-none" aria-hidden>🎬</span>
        <span className="text-xs font-semibold text-white/50 mt-1">
          Poster missing
        </span>
        <span className="text-[10px] text-white/30 leading-snug">
          Narada is investigating.
        </span>
        <span className="text-[10px] text-white/20 italic leading-snug">
          {line}
        </span>
        <span className="text-[9px] text-white/15 mt-1 uppercase tracking-wide">
          Source: TMDB
        </span>
      </div>
    )
  }

  // ── poster_old ────────────────────────────────────────────────
  if (type === 'poster_old') {
    return (
      <div
        className="
          w-full h-full
          bg-gradient-to-br from-amber-950 to-stone-900
          flex flex-col items-center justify-center text-center gap-1 p-4
        "
      >
        <span className="text-3xl select-none" aria-hidden>🎞</span>
        <span className="text-xs font-semibold text-amber-300/60 mt-1">
          Vintage film
        </span>
        <span className="text-[10px] text-white/30 leading-snug">
          This movie may be older<br />than the internet.
        </span>
        <span className="text-[10px] text-white/20 italic leading-snug mt-1">
          Narada is investigating.
        </span>
      </div>
    )
  }

  // ── cast ──────────────────────────────────────────────────────
  if (type === 'cast') {
    return (
      <p className="text-sm text-white/30 py-4">
        Cast list missing.{' '}
        <span className="text-white/20">
          Narada hasn&apos;t tracked everyone down yet.
        </span>
      </p>
    )
  }

  // ── director ──────────────────────────────────────────────────
  if (type === 'director') {
    return (
      <p className="text-sm text-white/30 py-4">
        Director unknown.{' '}
        <span className="text-white/20">Narada couldn&apos;t confirm it yet.</span>
      </p>
    )
  }

  // ── rating ────────────────────────────────────────────────────
  if (type === 'rating') {
    return (
      <span
        className="text-[10px] text-white/25 italic"
        title="Critics haven't voted yet."
      >
        Not yet rated. Critics are still deciding.
      </span>
    )
  }

  // ── backdrop ──────────────────────────────────────────────────
  if (type === 'backdrop') {
    // 1 — real backdrop
    if (backdropUrl) {
      return (
        <div className="relative w-full aspect-video overflow-hidden rounded-xl">
          <Image
            src={backdropUrl}
            alt="Movie backdrop"
            fill
            sizes="100vw"
            className="object-cover"
            priority
          />
        </div>
      )
    }

    // 2 — poster blown up + blurred
    if (posterUrl) {
      return (
        <div className="relative w-full aspect-video overflow-hidden rounded-xl">
          <Image
            src={posterUrl}
            alt="Backdrop (poster fallback)"
            fill
            sizes="100vw"
            className="object-cover scale-110 blur-2xl opacity-50"
          />
          <div className="absolute inset-0 bg-gradient-to-t from-[#0a0a0f] via-transparent to-transparent" />
        </div>
      )
    }

    // 3 — nothing at all
    return (
      <div
        className="
          w-full aspect-video rounded-xl
          bg-gradient-to-br from-slate-800/60 to-slate-950
          flex flex-col items-center justify-center gap-2
        "
      >
        <span className="text-2xl select-none" aria-hidden>🌌</span>
        <span className="text-sm text-white/30">Backdrop unavailable.</span>
        <span className="text-xs text-white/20 italic">
          Narada is investigating.
        </span>
      </div>
    )
  }

  return null
}
