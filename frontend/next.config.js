/** @type {import('next').NextConfig} */
const nextConfig = {
  images: {
    unoptimized: process.env.NODE_ENV === 'development',
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
