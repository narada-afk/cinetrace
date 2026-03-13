'use client'

// /compare — Actor picker page
// Lets the viewer choose two actors and navigate to their compare page.
// Client Component because it owns live search state + dropdown UX.

import { useState, useEffect, useRef, useCallback } from 'react'
import { useRouter } from 'next/navigation'
import Header from '@/components/Header'
import NavTabs from '@/components/NavTabs'
import ActorAvatar from '@/components/ActorAvatar'
import { searchActors, type Actor } from '@/lib/api'

// ── ActorPicker sub-component ─────────────────────────────────────────────────

interface ActorPickerProps {
  label: string          // "Actor 1" or "Actor 2"
  selected: Actor | null
  onSelect: (actor: Actor) => void
  onClear: () => void
}

function ActorPicker({ label, selected, onSelect, onClear }: ActorPickerProps) {
  const [query, setQuery]       = useState('')
  const [results, setResults]   = useState<Actor[]>([])
  const [loading, setLoading]   = useState(false)
  const [open, setOpen]         = useState(false)
  const inputRef                = useRef<HTMLInputElement>(null)
  const dropdownRef             = useRef<HTMLDivElement>(null)

  // Debounced search
  useEffect(() => {
    if (!query.trim() || selected) {
      setResults([])
      setOpen(false)
      return
    }
    setLoading(true)
    const tid = setTimeout(async () => {
      try {
        const actors = await searchActors(query, true)
        setResults(actors.slice(0, 8))
        setOpen(actors.length > 0)
      } catch {
        setResults([])
      } finally {
        setLoading(false)
      }
    }, 250)
    return () => clearTimeout(tid)
  }, [query, selected])

  // Close dropdown on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (
        dropdownRef.current &&
        !dropdownRef.current.contains(e.target as Node) &&
        !inputRef.current?.contains(e.target as Node)
      ) {
        setOpen(false)
      }
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  function handleSelect(actor: Actor) {
    onSelect(actor)
    setQuery('')
    setResults([])
    setOpen(false)
  }

  function handleClear() {
    onClear()
    setQuery('')
    setTimeout(() => inputRef.current?.focus(), 0)
  }

  return (
    <div className="flex-1 min-w-0">
      <p className="text-xs font-semibold text-white/40 uppercase tracking-widest mb-3">
        {label}
      </p>

      {selected ? (
        /* ── Selected state ── */
        <div className="glass rounded-2xl p-4 flex items-center gap-4">
          <ActorAvatar name={selected.name} size={56} />
          <div className="flex-1 min-w-0">
            <p className="text-white font-semibold truncate">{selected.name}</p>
            {selected.industry && (
              <p className="text-white/40 text-sm">{selected.industry}</p>
            )}
          </div>
          <button
            onClick={handleClear}
            className="text-white/30 hover:text-white/70 transition-colors text-xl leading-none flex-shrink-0"
            aria-label={`Remove ${selected.name}`}
          >
            ✕
          </button>
        </div>
      ) : (
        /* ── Search state ── */
        <div className="relative">
          <div className="glass rounded-2xl flex items-center px-4 gap-3 focus-within:ring-1 focus-within:ring-white/20 transition-all">
            <span className="text-white/30 text-lg flex-shrink-0">🔍</span>
            <input
              ref={inputRef}
              type="text"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder={`Search actor…`}
              className="flex-1 bg-transparent py-4 text-white placeholder-white/25 outline-none text-sm"
            />
            {loading && (
              <span className="text-white/30 text-xs animate-pulse flex-shrink-0">
                searching…
              </span>
            )}
          </div>

          {/* Dropdown */}
          {open && results.length > 0 && (
            <div
              ref={dropdownRef}
              className="absolute top-full left-0 right-0 mt-2 rounded-2xl overflow-hidden z-50 shadow-2xl border border-white/[0.10]"
              style={{ background: '#1e1e2c' }}
            >
              {results.map((actor) => (
                <button
                  key={actor.id}
                  onMouseDown={(e) => e.preventDefault()} // keep focus on input
                  onClick={() => handleSelect(actor)}
                  className="w-full flex items-center gap-3 px-4 py-3 hover:bg-white/[0.08] transition-colors text-left border-b border-white/[0.05] last:border-0"
                >
                  <ActorAvatar name={actor.name} size={36} />
                  <div className="min-w-0">
                    <p className="text-white text-sm font-medium truncate">
                      {actor.name}
                    </p>
                    {actor.industry && (
                      <p className="text-white/40 text-xs">{actor.industry}</p>
                    )}
                  </div>
                </button>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

// ── Main page ─────────────────────────────────────────────────────────────────

export default function ComparePicker() {
  const router = useRouter()
  const [actor1, setActor1] = useState<Actor | null>(null)
  const [actor2, setActor2] = useState<Actor | null>(null)

  const canCompare = actor1 !== null && actor2 !== null && actor1.id !== actor2.id

  function handleCompare() {
    if (!canCompare) return
    router.push(`/compare/${actor1.id}-vs-${actor2.id}`)
  }

  return (
    <div className="min-h-screen bg-[#0a0a0f]">
      <Header />

      <div className="max-w-[1200px] mx-auto px-6">
        <NavTabs activeTab="compare" />
      </div>

      <main className="max-w-[800px] mx-auto px-6 mt-16 pb-20">
        {/* Page title */}
        <div className="text-center mb-12">
          <h1 className="text-3xl font-bold text-white mb-3">
            ⚔️ Compare Actors
          </h1>
          <p className="text-white/40 text-sm">
            Pick two South Indian stars and see their stats head to head
          </p>
        </div>

        {/* Picker card */}
        <div className="rounded-3xl p-8 border border-white/[0.08]" style={{ background: '#13131a' }}>
          {/* Two actor pickers */}
          <div className="flex flex-col sm:flex-row items-stretch gap-6">
            <ActorPicker
              label="Actor 1"
              selected={actor1}
              onSelect={setActor1}
              onClear={() => setActor1(null)}
            />

            {/* VS divider */}
            <div className="flex sm:flex-col items-center justify-center gap-2 sm:gap-0 flex-shrink-0">
              <div className="h-px sm:h-12 w-full sm:w-px bg-white/10" />
              <span className="text-white/20 font-bold text-xs tracking-widest px-3 sm:py-3">
                VS
              </span>
              <div className="h-px sm:h-12 w-full sm:w-px bg-white/10" />
            </div>

            <ActorPicker
              label="Actor 2"
              selected={actor2}
              onSelect={setActor2}
              onClear={() => setActor2(null)}
            />
          </div>

          {/* Compare button */}
          <div className="mt-8 flex justify-center">
            <button
              onClick={handleCompare}
              disabled={!canCompare}
              className={`
                px-10 py-3.5 rounded-full font-semibold text-sm transition-all
                ${
                  canCompare
                    ? 'bg-white text-black hover:bg-white/90 active:scale-95 shadow-lg shadow-white/10'
                    : 'bg-white/10 text-white/30 cursor-not-allowed'
                }
              `}
            >
              {canCompare
                ? `Compare ${actor1.name.split(' ')[0]} vs ${actor2.name.split(' ')[0]} →`
                : 'Select two actors to compare'}
            </button>
          </div>
        </div>

        {/* Quick-start suggestions */}
        <div className="mt-10">
          <p className="text-white/30 text-xs font-semibold uppercase tracking-widest mb-4 text-center">
            Popular matchups
          </p>
          <div className="flex flex-wrap justify-center gap-2">
            {[
              { label: 'Mohanlal vs Mammootty',       href: '/compare/mohanlal-vs-mammootty' },
              { label: 'Rajinikanth vs Kamal Haasan', href: '/compare/rajinikanth-vs-kamal-haasan' },
              { label: 'Prabhas vs Allu Arjun',       href: '/compare/prabhas-vs-allu-arjun' },
              { label: 'Vijay vs Ajith',               href: '/compare/vijay-vs-ajith' },
              { label: 'Mahesh Babu vs Ram Charan',   href: '/compare/mahesh-babu-vs-ram-charan' },
            ].map(({ label, href }) => (
              <a
                key={href}
                href={href}
                className="px-4 py-2 rounded-full text-sm text-white/50 glass hover:text-white/80 hover:bg-white/[0.08] transition-all"
              >
                {label}
              </a>
            ))}
          </div>
        </div>
      </main>
    </div>
  )
}
