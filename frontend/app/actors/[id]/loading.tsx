/**
 * Actor page loading skeleton — shown by Next.js while [id]/page.tsx awaits server data.
 * Matches the approximate shape of: Hero · FilmographyPreview · Collaborations ·
 * Compare · Connections · Insights · FullFilmography
 */
export default function Loading() {
  return (
    <div className="min-h-screen bg-[#0a0a0f]">
      {/* Header placeholder */}
      <div className="h-14 border-b border-white/[0.06]" />

      <main className="max-w-[1200px] mx-auto px-6 pb-24 flex flex-col gap-14">

        {/* Hero skeleton */}
        <div
          className="rounded-2xl p-10 flex flex-col sm:flex-row items-center sm:items-end gap-6 animate-pulse"
          style={{ background: '#1a1a2e' }}
        >
          <div className="w-[120px] h-[120px] rounded-full bg-white/[0.10] flex-shrink-0" />
          <div className="flex flex-col gap-3 flex-1">
            <div className="h-4 w-40 rounded-full bg-white/[0.08]" />
            <div className="h-12 w-64 rounded-xl bg-white/[0.12]" />
            <div className="h-4 w-52 rounded-full bg-white/[0.06]" />
          </div>
        </div>

        {/* Filmography strip skeleton */}
        <div className="flex flex-col gap-4">
          <div className="h-4 w-32 rounded-full bg-white/[0.05] animate-pulse" />
          <div className="flex gap-3 overflow-hidden">
            {[1, 2, 3, 4, 5, 6].map(i => (
              <div
                key={i}
                className="flex-shrink-0 rounded-xl bg-white/[0.04] animate-pulse"
                style={{ width: 100, aspectRatio: '2/3' }}
              />
            ))}
          </div>
        </div>

        {/* Collaborations skeleton */}
        <div className="flex flex-col gap-4">
          <div className="h-4 w-36 rounded-full bg-white/[0.05] animate-pulse" />
          <div className="h-40 rounded-2xl bg-white/[0.04] animate-pulse" />
        </div>

        {/* Compare skeleton */}
        <div className="h-48 rounded-3xl bg-white/[0.04] animate-pulse" />

        {/* Connections skeleton */}
        <div className="h-56 rounded-3xl bg-white/[0.04] animate-pulse" />

        {/* Insights skeleton */}
        <div className="flex flex-col gap-4">
          <div className="h-4 w-24 rounded-full bg-white/[0.05] animate-pulse" />
          <div className="flex gap-4 overflow-hidden">
            {[1, 2, 3].map(i => (
              <div
                key={i}
                className="flex-shrink-0 h-[168px] rounded-2xl bg-white/[0.04] animate-pulse"
                style={{ width: 360 }}
              />
            ))}
          </div>
        </div>

        {/* Full filmography grid skeleton */}
        <div className="flex flex-col gap-4">
          <div className="h-4 w-28 rounded-full bg-white/[0.05] animate-pulse" />
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-5 lg:grid-cols-6 gap-4">
            {Array.from({ length: 12 }).map((_, i) => (
              <div
                key={i}
                className="rounded-xl bg-white/[0.04] animate-pulse"
                style={{ aspectRatio: '2/3' }}
              />
            ))}
          </div>
        </div>

      </main>
    </div>
  )
}
