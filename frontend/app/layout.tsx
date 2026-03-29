import type { Metadata } from 'next'
import './globals.css'
import StarBackground from '@/components/StarBackground'

export const metadata: Metadata = {
  title: 'South Cinema Analytics',
  description: 'A cinema curiosity engine for South Indian films',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body className="bg-[#0a0a0f] text-white min-h-screen antialiased">
        {/* ── Global star background — fixed, z:0, behind content ── */}
        <StarBackground />

        {/* ── All page content sits above the canvas (z:1) ── */}
        <div className="relative" style={{ zIndex: 1 }}>
        {children}

        {/* ── Global attribution footer ─────────────────────────── */}
        <footer className="border-t border-white/[0.04] py-8 px-4">
          <div className="max-w-[1000px] mx-auto flex flex-col items-center gap-2 text-center">

            {/* TMDB logo + source list */}
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

      </body>
    </html>
  )
}
