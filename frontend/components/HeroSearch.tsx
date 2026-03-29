'use client'

import { useState } from 'react'
import { useRouter } from 'next/navigation'
import Link from 'next/link'
import ActorAvatar from '@/components/ActorAvatar'

export interface TrendingChip {
  id: number
  name: string
}

export default function HeroSearch({ trendingActors = [] }: { trendingActors?: TrendingChip[] }) {
  const [query, setQuery]       = useState('')
  const [loading, setLoading]   = useState(false)
  const [notFound, setNotFound] = useState(false)
  const [focused, setFocused]   = useState(false)
  const router = useRouter()

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    const q = query.trim()
    if (!q) return

    setLoading(true)
    setNotFound(false)

    try {
      const apiUrl = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000'
      const res     = await fetch(`${apiUrl}/actors/search?q=${encodeURIComponent(q)}`)
      const results = await res.json()

      if (results.length > 0) {
        const slug = results[0].name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '')
        router.push(`/actors/${slug}`)
      } else {
        setNotFound(true)
        setLoading(false)
      }
    } catch {
      setLoading(false)
    }
  }

  return (
    <section className="flex flex-col items-center text-center pt-10 pb-4">
      <h1 className="text-[2rem] sm:text-[2.75rem] font-black text-white leading-[1.15] tracking-[-0.02em] max-w-xl">
        Explore South Indian<br />Cinema Connections
      </h1>
      <p className="mt-4 text-sm text-white/40 max-w-sm leading-relaxed">
        Discover how actors are connected across Telugu, Tamil, Malayalam &amp; Kannada cinema.
      </p>

      {/* Search bar — focus glow ring */}
      <form onSubmit={handleSubmit} className="relative w-full max-w-lg mt-8">
        <span className="absolute left-5 top-1/2 -translate-y-1/2 text-white/30 pointer-events-none transition-colors duration-200"
          style={{ color: focused ? 'rgba(255,255,255,0.5)' : undefined }}>
          {loading ? (
            <svg className="animate-spin" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <path d="M21 12a9 9 0 1 1-6.219-8.56" />
            </svg>
          ) : (
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="11" cy="11" r="8" />
              <line x1="21" y1="21" x2="16.65" y2="16.65" />
            </svg>
          )}
        </span>

        <input
          id="hero-search-input"
          type="text"
          value={query}
          onChange={(e) => { setQuery(e.target.value); setNotFound(false) }}
          onFocus={() => setFocused(true)}
          onBlur={() => setFocused(false)}
          placeholder="Search an actor… (Try: Rajinikanth, Prabhas)"
          disabled={loading}
          autoComplete="off"
          className="
            w-full pl-12 pr-5 py-4 rounded-full text-sm
            bg-white/[0.07] border border-white/[0.12]
            text-white placeholder-white/25
            focus:outline-none
            disabled:opacity-60
            transition-all duration-300
          "
          style={focused ? {
            borderColor:  'rgba(120,150,255,0.45)',
            background:   'rgba(255,255,255,0.10)',
            boxShadow:    '0 0 0 3px rgba(99,120,255,0.12), 0 0 20px rgba(99,120,255,0.08)',
          } : {}}
        />

        {notFound && (
          <span className="absolute right-5 top-1/2 -translate-y-1/2 text-xs text-white/35">
            No results
          </span>
        )}
      </form>

      {/* Actor chips — lift + glow on hover */}
      {trendingActors.length > 0 && (
        <div className="flex flex-wrap gap-2 justify-center mt-5">
          {trendingActors.map((actor) => (
            <Link
              key={actor.id}
              href={`/actors/${actor.name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '')}`}
              className="
                inline-flex items-center gap-2
                pl-1.5 pr-4 py-1.5 rounded-full
                bg-white/[0.05] border border-white/[0.09]
                text-white/50
                transition-all duration-200
                hover:text-white/85
                hover:bg-white/[0.10]
                hover:border-white/[0.20]
                hover:-translate-y-0.5
              "
              style={{ transition: 'all 0.2s ease' }}
              onMouseEnter={e => {
                (e.currentTarget as HTMLAnchorElement).style.boxShadow =
                  '0 4px 16px rgba(120,150,255,0.12), 0 0 0 1px rgba(255,255,255,0.08)'
              }}
              onMouseLeave={e => {
                (e.currentTarget as HTMLAnchorElement).style.boxShadow = 'none'
              }}
            >
              <ActorAvatar name={actor.name} size={22} />
              <span className="text-xs">{actor.name}</span>
            </Link>
          ))}
        </div>
      )}
    </section>
  )
}
