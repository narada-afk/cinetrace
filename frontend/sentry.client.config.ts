import * as Sentry from '@sentry/nextjs'

const dsn = process.env.NEXT_PUBLIC_SENTRY_DSN

// Only initialise when a DSN is provided — no-op in dev / CI without it.
if (dsn) {
  Sentry.init({
    dsn,
    tracesSampleRate: 0,     // error capture only — no performance tracing
    replaysSessionSampleRate: 0,
    replaysOnErrorSampleRate: 0,
    debug: false,
  })
}
