/** @type {import('next').NextConfig} */
const nextConfig = {
  async rewrites() {
    const backendUrl = process.env.API_URL || 'http://localhost:8000'
    return [
      {
        source: '/api/backend/:path*',
        destination: `${backendUrl}/:path*`,
      },
    ]
  },
  images: {
    // Skip optimisation in local dev — speeds up the dev server significantly.
    // Production always optimises (WebP / AVIF conversion, resizing).
    unoptimized: process.env.NODE_ENV === 'development',

    // Serve AVIF first (best compression), fall back to WebP.
    // Browsers that support neither get the original PNG/JPEG.
    formats: ['image/avif', 'image/webp'],

    // Cache optimised images for 7 days at the CDN/edge layer.
    // Avatar PNGs and TMDB posters are stable — no need to re-optimise on every request.
    minimumCacheTTL: 60 * 60 * 24 * 7,

    // Declare the exact sizes we actually render so Next.js picks the
    // nearest bucket instead of up-scaling to a needlessly large image.
    // Covers: 92px duo circles, 130-155px single portraits, 160px hero avatar,
    //         40-56px ActorAvatar chips, 100px film poster thumbnails.
    imageSizes: [48, 64, 96, 128, 160],
    deviceSizes: [640, 750, 828, 1080, 1200],

    remotePatterns: [
      {
        protocol: 'https',
        hostname: 'image.tmdb.org',
        pathname: '/t/p/**',
      },
    ],
  },
}

// withSentryConfig is a no-op when SENTRY_DSN / NEXT_PUBLIC_SENTRY_DSN is
// absent — it only injects the SDK when a DSN is configured at build time.
const { withSentryConfig } = require('@sentry/nextjs')

module.exports = withSentryConfig(nextConfig, {
  // Suppress the Sentry CLI upload step unless SENTRY_AUTH_TOKEN is set.
  // This keeps local builds and CI clean when source maps aren't needed.
  silent: true,
  disableLogger: true,
  // Only upload source maps if auth token is present (opt-in for prod).
  authToken: process.env.SENTRY_AUTH_TOKEN,
  telemetry: false,
})
