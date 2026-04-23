import type { Metadata } from 'next'
import { Suspense } from 'react'
import Script from 'next/script'
import './globals.css'
import StarBackground from '@/components/StarBackground'
import PostHogProvider from '@/components/PostHogProvider'

export const metadata: Metadata = {
  title: 'CineTrace',
  description: 'Trace connections across South Indian cinema — explore actors, collaborations and insights across Telugu, Tamil, Malayalam and Kannada films.',
  metadataBase: new URL('https://cinetrace.in'),
  openGraph: {
    title: 'CineTrace — South Indian Cinema Intelligence',
    description: 'Trace connections across South Indian cinema — 8,000+ actors, 4 industries, infinite connections.',
    url: 'https://cinetrace.in',
    siteName: 'CineTrace',
    locale: 'en_IN',
    type: 'website',
  },
  twitter: {
    card: 'summary_large_image',
    title: 'CineTrace — South Indian Cinema Intelligence',
    description: 'Trace connections across South Indian cinema — 8,000+ actors, 4 industries, infinite connections.',
  },
  icons: {
    icon: '/narada.png',
    apple: '/narada.png',
  },
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  const gaId      = process.env.NEXT_PUBLIC_GA_ID
  const clarityId = process.env.NEXT_PUBLIC_CLARITY_ID

  return (
    <html lang="en">
      <body className="bg-[#0a0a0f] text-white min-h-screen antialiased">

        {/* ── Google Analytics 4 ───────────────────────────────────────
            Loads after page is interactive so it never blocks rendering.
            send_page_view:false → we fire page_view manually on every
            SPA route change from PostHogProvider to avoid double-counts.
        ── */}
        {gaId && (
          <>
            <Script
              src={`https://www.googletagmanager.com/gtag/js?id=${gaId}`}
              strategy="afterInteractive"
            />
            <Script id="ga4-init" strategy="afterInteractive">
              {`
                window.dataLayer = window.dataLayer || [];
                function gtag(){dataLayer.push(arguments);}
                gtag('js', new Date());
                gtag('config', '${gaId}', { send_page_view: false });
              `}
            </Script>
          </>
        )}

        {/* ── Microsoft Clarity (session recording + heatmaps) ────────
            Fully passive — no custom event calls needed.
        ── */}
        {clarityId && (
          <Script id="clarity-init" strategy="afterInteractive">
            {`
              (function(c,l,a,r,i,t,y){
                c[a]=c[a]||function(){(c[a].q=c[a].q||[]).push(arguments)};
                t=l.createElement(r);t.async=1;t.src="https://www.clarity.ms/tag/"+i;
                y=l.getElementsByTagName(r)[0];y.parentNode.insertBefore(t,y);
              })(window,document,"clarity","script","${clarityId}");
            `}
          </Script>
        )}

        {/* ── Global star background — fixed, z:0, behind content ── */}
        <StarBackground />

        {/* ── PostHog pageview tracking (useSearchParams needs Suspense) ── */}
        <Suspense fallback={null}>
          <PostHogProvider>

        {/* ── All page content sits above the canvas (z:1) ── */}
        <div className="relative" style={{ zIndex: 1 }}>
        {children}

        {/* ── Global attribution footer ─────────────────────────── */}
        <footer className="border-t border-white/[0.04] py-8 px-4">
          <div className="max-w-[1000px] mx-auto flex flex-col items-center gap-3 text-center">

            {/* ── Creator attribution — subtle signature ──────────── */}
            {/* TODO: replace href values with your actual channel / profile URLs */}
            <div className="flex items-center justify-center gap-2.5">
              {/* Avatar */}
              {/* eslint-disable-next-line @next/next/no-img-element */}
              <img
                src="/narada.png"
                alt="Mr Narada"
                width={20}
                height={20}
                className="rounded-full object-cover flex-shrink-0"
                style={{ opacity: 0.80 }}
              />
              <span className="text-xs text-white/50">Built by</span>
              <span className="text-xs text-white/80 font-medium">Mr Narada</span>
              <span className="text-white/20 text-xs select-none">·</span>

              {/* YouTube */}
              <a
                href="https://www.youtube.com/@MrNarada"
                target="_blank"
                rel="noopener noreferrer"
                aria-label="Mr Narada on YouTube"
                className="text-white/50 hover:text-white transition-colors duration-200"
              >
                <svg className="w-4 h-4" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
                  <path d="M23.498 6.186a3.016 3.016 0 0 0-2.122-2.136C19.505 3.545 12 3.545 12 3.545s-7.505 0-9.377.505A3.017 3.017 0 0 0 .502 6.186C0 8.07 0 12 0 12s0 3.93.502 5.814a3.016 3.016 0 0 0 2.122 2.136c1.871.505 9.376.505 9.376.505s7.505 0 9.377-.505a3.015 3.015 0 0 0 2.122-2.136C24 15.93 24 12 24 12s0-3.93-.502-5.814zM9.545 15.568V8.432L15.818 12l-6.273 3.568z" />
                </svg>
              </a>

              {/* Instagram */}
              <a
                href="https://www.instagram.com/misternarada/"
                target="_blank"
                rel="noopener noreferrer"
                aria-label="Mr Narada on Instagram"
                className="text-white/50 hover:text-white transition-colors duration-200"
              >
                <svg className="w-4 h-4" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
                  <path d="M12 2.163c3.204 0 3.584.012 4.85.07 3.252.148 4.771 1.691 4.919 4.919.058 1.265.069 1.645.069 4.849 0 3.205-.012 3.584-.069 4.849-.149 3.225-1.664 4.771-4.919 4.919-1.266.058-1.644.07-4.85.07-3.204 0-3.584-.012-4.849-.07-3.26-.149-4.771-1.699-4.919-4.92-.058-1.265-.07-1.644-.07-4.849 0-3.204.013-3.583.07-4.849.149-3.227 1.664-4.771 4.919-4.919 1.266-.057 1.645-.069 4.849-.069zM12 0C8.741 0 8.333.014 7.053.072 2.695.272.273 2.69.073 7.052.014 8.333 0 8.741 0 12c0 3.259.014 3.668.072 4.948.2 4.358 2.618 6.78 6.98 6.98C8.333 23.986 8.741 24 12 24c3.259 0 3.668-.014 4.948-.072 4.354-.2 6.782-2.618 6.979-6.98.059-1.28.073-1.689.073-4.948 0-3.259-.014-3.667-.072-4.947-.196-4.354-2.617-6.78-6.979-6.98C15.668.014 15.259 0 12 0zm0 5.838a6.162 6.162 0 1 0 0 12.324 6.162 6.162 0 0 0 0-12.324zM12 16a4 4 0 1 1 0-8 4 4 0 0 1 0 8zm6.406-11.845a1.44 1.44 0 1 0 0 2.881 1.44 1.44 0 0 0 0-2.881z" />
                </svg>
              </a>
            </div>

            {/* Separator */}
            <div className="w-16 h-px" style={{ background: 'rgba(255,255,255,0.06)' }} />

            {/* ── TMDB logo + source list ─────────────────────────── */}
            <div className="flex items-center justify-center gap-2">
              {/* TMDB colour-mark — matches their official blue */}
              <svg width="32" height="14" viewBox="0 0 500 220" aria-hidden="true">
                <defs>
                  <linearGradient id="tmdbGrad" x1="0" y1="0" x2="1" y2="0">
                    <stop offset="0%"   stopColor="#90cea1" />
                    <stop offset="47%"  stopColor="#01b4e4" />
                    <stop offset="100%" stopColor="#0d253f" />
                  </linearGradient>
                </defs>
                <rect width="500" height="220" rx="30" fill="url(#tmdbGrad)" />
                <text x="50%" y="155" textAnchor="middle"
                  fontFamily="-apple-system,system-ui,sans-serif" fontWeight="800"
                  fontSize="160" fill="white" letterSpacing="-4">
                  TMDB
                </text>
              </svg>

              <p className="text-[11px] text-white/30">
                Data sources:{' '}
                <a
                  href="https://www.themoviedb.org/"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-[#01b4e4]/80 hover:text-[#01b4e4] transition-colors underline-offset-2 hover:underline"
                >
                  TMDB
                </a>
                {' '}·{' '}Wikipedia{' '}·{' '}Wikidata
              </p>
            </div>

            <p className="text-[10px] text-white/18">
              Movie posters and metadata provided by{' '}
              <a
                href="https://www.themoviedb.org/"
                target="_blank"
                rel="noopener noreferrer"
                className="text-[#01b4e4]/60 hover:text-[#01b4e4]/90 transition-colors hover:underline underline-offset-2"
              >
                The Movie Database (TMDB)
              </a>
              . This product uses the TMDB API but is not endorsed or certified by TMDB.
            </p>

          </div>
        </footer>
        </div>{/* end z:1 content wrapper */}

          </PostHogProvider>
        </Suspense>

      </body>
    </html>
  )
}
