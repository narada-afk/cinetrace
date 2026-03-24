/**
 * Homepage loading skeleton — shown by Next.js while page.tsx awaits server data.
 * Matches the approximate shape of: HeroSearch · ConnectionFinder · GraphPreview · InsightsCarousel
 */
export default function Loading() {
  return (
    <div className="min-h-screen bg-[#0a0a0f]">
      {/* Header placeholder */}
      <div className="h-14 border-b border-white/[0.06]" />

      <main className="max-w-[1200px] mx-auto px-6 pb-24">

        {/* Hero skeleton */}
        <div className="flex flex-col items-center text-center pt-10 pb-4 gap-4">
          <div className="h-10 w-72 rounded-full bg-white/[0.07] animate-pulse" />
          <div className="h-4 w-56 rounded-full bg-white/[0.04] animate-pulse" />
          <div className="h-14 w-full max-w-lg rounded-full bg-white/[0.06] animate-pulse mt-4" />
          {/* Trending chip skeletons */}
          <div className="flex gap-2 mt-2">
            {[80, 96, 72, 88].map((w, i) => (
              <div
                key={i}
                className="h-8 rounded-full bg-white/[0.05] animate-pulse"
                style={{ width: w }}
              />
            ))}
          </div>
        </div>

        {/* Connection Finder skeleton */}
        <div className="mt-16">
          <div className="h-3 w-36 rounded-full bg-white/[0.05] animate-pulse mb-5" />
          <div className="h-52 rounded-3xl bg-white/[0.04] animate-pulse" />
        </div>

        {/* Graph Preview skeleton */}
        <div className="mt-16">
          <div className="h-72 rounded-3xl bg-white/[0.04] animate-pulse" />
        </div>

        {/* Insights Carousel skeleton */}
        <div className="mt-16">
          <div className="h-3 w-28 rounded-full bg-white/[0.05] animate-pulse mb-5" />
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

      </main>
    </div>
  )
}
