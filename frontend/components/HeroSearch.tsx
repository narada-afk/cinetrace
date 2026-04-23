'use client'

import { useState, useEffect, useRef, type ReactNode } from 'react'
import { useRouter } from 'next/navigation'
import Link from 'next/link'
import ActorAvatar from '@/components/ActorAvatar'
import { trackSearch } from '@/lib/analytics'
import { toActorSlug } from '@/lib/api'

export interface TrendingChip {
  id: number
  name: string
}

interface SearchResult {
  id: number
  name: string
}

const HEADLINES: ReactNode[] = [
  <><span key="a1">How connected</span><br key="b1"/><span key="a2">is South cinema?</span></>,
  <><span key="a1">Your stars are closer</span><br key="b1"/><span key="a2">than you think 👀</span></>,
  <><span key="a1">Hidden connections</span><br key="b1"/><span key="a2">in South cinema</span></>,
  <><span key="a1">Six degrees of</span><br key="b1"/><span key="a2">South Indian cinema</span></>,
]

export default function HeroSearch({ trendingActors = [] }: { trendingActors?: TrendingChip[] }) {
  const [query, setQuery]         = useState('')
  const [loading, setLoading]     = useState(false)
  const [notFound, setNotFound]   = useState(false)
  const [focused, setFocused]     = useState(false)
  const [results, setResults]     = useState<SearchResult[]>([])
  const [searching, setSearching] = useState(false)
  const [activeIdx, setActiveIdx] = useState(-1)

  // Rotating headline
  const [headlineIdx, setHeadlineIdx] = useState(0)
  const [fading, setFading]           = useState(false)

  const inputRef = useRef<HTMLInputElement>(null)
  const dropRef  = useRef<HTMLDivElement>(null)
  const router   = useRouter()

  // Rotate headline every 4s
  useEffect(() => {
    const timer = setInterval(() => {
      setFading(true)
      setTimeout(() => {
        setHeadlineIdx(i => (i + 1) % HEADLINES.length)
        setFading(false)
      }, 350)
    }, 4000)
    return () => clearInterval(timer)
  }, [])

  // Live search — debounced 180 ms
  useEffect(() => {
    if (!query.trim()) { setResults([]); setNotFound(false); return }
    setSearching(true)
    const tid = setTimeout(async () => {
      try {
        const apiUrl = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000'
        const res    = await fetch(`${apiUrl}/actors/search?q=${encodeURIComponent(query.trim())}`)
        const data: SearchResult[] = await res.json()
        // Prioritise exact match → starts-with → contains
        const q = query.trim().toLowerCase()
        const sorted = [...data].sort((a, b) => {
          const an = a.name.toLowerCase()
          const bn = b.name.toLowerCase()
          if (an === q && bn !== q) return -1
          if (bn === q && an !== q) return 1
          if (an.startsWith(q) && !bn.startsWith(q)) return -1
          if (bn.startsWith(q) && !an.startsWith(q)) return 1
          return 0
        })
        setResults(sorted.slice(0, 7))
        setNotFound(data.length === 0)
      } catch {
        setResults([])
      } finally {
        setSearching(false)
      }
    }, 180)
    return () => clearTimeout(tid)
  }, [query])

  // Close dropdown on outside click
  useEffect(() => {
    const h = (e: MouseEvent) => {
      if (
        dropRef.current?.contains(e.target as Node) ||
        inputRef.current?.contains(e.target as Node)
      ) return
      setFocused(false)
    }
    document.addEventListener('mousedown', h)
    return () => document.removeEventListener('mousedown', h)
  }, [])

  const dropItems     = query.trim() ? results : trendingActors
  const isDefaultList = !query.trim()
  const showDropdown  = focused && dropItems.length > 0

  function navigate(name: string) {
    trackSearch(name)
    setFocused(false)
    setQuery('')
    router.push(`/actors/${toActorSlug(name)}`)
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    if (activeIdx >= 0 && dropItems[activeIdx]) {
      navigate(dropItems[activeIdx].name); return
    }
    const q = query.trim()
    if (!q) return
    setLoading(true); setNotFound(false)
    try {
      const apiUrl = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000'
      const res    = await fetch(`${apiUrl}/actors/search?q=${encodeURIComponent(q)}`)
      const data: SearchResult[] = await res.json()
      if (data.length > 0) router.push(`/actors/${toActorSlug(data[0].name)}`)
      else { setNotFound(true); setLoading(false) }
    } catch { setLoading(false) }
  }

  function handleKeyDown(e: React.KeyboardEvent) {
    if (!showDropdown || dropItems.length === 0) return
    if (e.key === 'ArrowDown') { e.preventDefault(); setActiveIdx(i => Math.min(i + 1, dropItems.length - 1)) }
    if (e.key === 'ArrowUp')   { e.preventDefault(); setActiveIdx(i => Math.max(i - 1, -1)) }
    if (e.key === 'Escape')    { setFocused(false); setActiveIdx(-1) }
  }

  return (
    <section className="relative flex flex-col items-center text-center pt-10 pb-4 overflow-hidden">

      {/* ── Ambient center glow — behind all text, purely decorative ─────── */}
      <div
        aria-hidden="true"
        className="absolute pointer-events-none select-none"
        style={{
          top:       '-80px',
          left:      '50%',
          transform: 'translateX(-50%)',
          width:     '720px',
          height:    '420px',
          background:'radial-gradient(ellipse at 50% 38%, rgba(79,172,254,0.07) 0%, rgba(168,85,247,0.05) 38%, transparent 68%)',
          filter:    'blur(36px)',
          zIndex:    0,
        }}
      />

      {/* Rotating headline — always 2 lines via explicit <br/> */}
      <h1
        className="relative text-[1.9rem] sm:text-[2.6rem] font-black text-white leading-[1.25] tracking-[-0.02em] max-w-lg"
        style={{
          opacity:    fading ? 0 : 1,
          transition: 'opacity 0.35s ease',
          minHeight:  '2.6em',
          zIndex:     1,
        }}
      >
        {HEADLINES[headlineIdx]}
      </h1>

      {/* Stats line */}
      <p
        className="relative mt-4 text-xs"
        style={{ color: 'rgba(255,255,255,0.22)', zIndex: 1 }}
      >
        8,000+ actors · 4 industries · infinite connections
      </p>

      {/* Search bar + dropdown wrapper */}
      <div className="relative w-full max-w-3xl mt-8" style={{ zIndex: 1 }}>
        <form onSubmit={handleSubmit}>
          <span
            className="absolute left-5 top-[22px] -translate-y-1/2 text-white/30 pointer-events-none z-10 transition-colors duration-200"
            style={{ color: focused ? 'rgba(255,255,255,0.5)' : undefined }}
          >
            {loading || searching ? (
              <svg className="animate-spin" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <path d="M21 12a9 9 0 1 1-6.219-8.56" />
              </svg>
            ) : (
              <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <circle cx="11" cy="11" r="8" /><line x1="21" y1="21" x2="16.65" y2="16.65" />
              </svg>
            )}
          </span>

          <input
            ref={inputRef}
            id="hero-search-input"
            type="text"
            value={query}
            onChange={e => { setQuery(e.target.value); setNotFound(false); setActiveIdx(-1) }}
            onFocus={() => setFocused(true)}
            onKeyDown={handleKeyDown}
            placeholder="Search an actor… (e.g. Rajinikanth, Prabhas)"
            disabled={loading}
            autoComplete="off"
            className="w-full pl-12 pr-5 py-4 text-sm bg-white/[0.07] border border-white/[0.12] text-white placeholder-white/25 focus:outline-none disabled:opacity-60 transition-all duration-200"
            style={{
              borderRadius: showDropdown ? '1.5rem 1.5rem 0 0' : '9999px',
              ...(focused ? {
                borderColor: 'rgba(120,150,255,0.45)',
                background:  'rgba(255,255,255,0.10)',
                boxShadow:   '0 0 0 3px rgba(99,120,255,0.12), 0 0 20px rgba(99,120,255,0.08)',
              } : {}),
            }}
          />
        </form>

        {/* Dropdown */}
        {showDropdown && (
          <div
            ref={dropRef}
            className="absolute left-0 right-0 z-50 overflow-hidden"
            style={{
              top:           'calc(100% - 1px)',
              background:    'rgba(10,10,20,0.97)',
              border:        '1px solid rgba(120,150,255,0.25)',
              borderTop:     '1px solid rgba(120,150,255,0.08)',
              borderRadius:  '0 0 1.5rem 1.5rem',
              boxShadow:     '0 20px 50px rgba(0,0,0,0.7)',
              backdropFilter:'blur(24px)',
            }}
          >
            {isDefaultList && (
              <div className="px-5 pt-3 pb-1">
                <span className="text-[10px] uppercase tracking-widest text-white/25 font-semibold">
                  🔥 Quick picks
                </span>
              </div>
            )}
            {dropItems.map((item, idx) => (
              <button
                key={item.id}
                data-testid={`actor-suggestion-${item.id}`}
                onMouseDown={() => navigate(item.name)}
                onMouseEnter={() => setActiveIdx(idx)}
                className="w-full flex items-center gap-3 px-4 py-2.5 text-left transition-colors duration-100"
                style={{
                  background: idx === activeIdx ? 'rgba(99,120,255,0.10)' : 'transparent',
                  position: 'relative',
                  zIndex: 1,
                  pointerEvents: 'auto',
                }}
              >
                <ActorAvatar name={item.name} size={30} />
                <span className="text-sm text-white/80 pointer-events-none">{item.name}</span>
              </button>
            ))}
            <div className="h-2" />
          </div>
        )}

        {/* No results */}
        {notFound && !searching && query.trim() && (
          <p className="mt-2 text-xs text-white/30 text-center">
            No actors found for &ldquo;{query}&rdquo;
          </p>
        )}
      </div>

      {/* "Try this" chips */}
      {trendingActors.length > 0 && (
        <div className="flex flex-wrap gap-2 justify-center mt-5">
          <div className="w-full flex justify-center mb-1">
            <span className="text-[10px] uppercase tracking-widest text-white/25 font-semibold">
              🔥 Try this:
            </span>
          </div>
          {trendingActors.map((actor) => (
            <Link
              key={actor.id}
              href={`/actors/${toActorSlug(actor.name)}`}
              className="inline-flex items-center gap-2 pl-1.5 pr-4 py-1.5 rounded-full bg-white/[0.05] border border-white/[0.09] text-white/50 transition-all duration-200 hover:text-white/90 hover:bg-white/[0.10] hover:border-white/[0.25] hover:-translate-y-0.5 hover:scale-105"
              onMouseEnter={e => { (e.currentTarget as HTMLAnchorElement).style.boxShadow = '0 4px 20px rgba(120,150,255,0.18)' }}
              onMouseLeave={e => { (e.currentTarget as HTMLAnchorElement).style.boxShadow = 'none' }}
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
