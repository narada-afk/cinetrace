'use client'

/**
 * ConnectionFinder — Six Degrees of South Indian Cinema
 *
 * Actor A → Film → Actor B → Film → ... → Actor Z
 *
 * Uses BFS via GET /stats/connection.
 * Actor search boxes reuse the same debounced-search pattern as ComparePicker.
 */

import { useState, useEffect, useRef, useCallback } from 'react'
import ActorAvatar from '@/components/ActorAvatar'
import ConnectionResult from '@/components/ConnectionResult'
import { searchActors, getActorConnection, type Actor, type ConnectionPath } from '@/lib/api'

// ── Tiny actor search box ─────────────────────────────────────────────────────

function ActorBox({
  label,
  selected,
  onSelect,
  onClear,
  colorClass,
  placeholder = 'Search actor…',
}: {
  label: string
  selected: Actor | null
  onSelect: (a: Actor) => void
  onClear: () => void
  colorClass: string
  placeholder?: string
}) {
  const [query, setQuery]     = useState('')
  const [results, setResults] = useState<Actor[]>([])
  const [loading, setLoading] = useState(false)
  const [open, setOpen]       = useState(false)
  const inputRef              = useRef<HTMLInputElement>(null)
  const dropRef               = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!query.trim() || selected) { setResults([]); setOpen(false); return }
    setLoading(true)
    const tid = setTimeout(async () => {
      try {
        const res = await searchActors(query)
        setResults(res.slice(0, 7))
        setOpen(res.length > 0)
      } catch { setResults([]) }
      finally  { setLoading(false) }
    }, 220)
    return () => clearTimeout(tid)
  }, [query, selected])

  useEffect(() => {
    const h = (e: MouseEvent) => {
      if (dropRef.current?.contains(e.target as Node) ||
          inputRef.current?.contains(e.target as Node)) return
      setOpen(false)
    }
    document.addEventListener('mousedown', h)
    return () => document.removeEventListener('mousedown', h)
  }, [])

  return (
    <div className="flex-1 min-w-0">
      <p className={`text-[10px] font-bold uppercase tracking-[0.18em] mb-2 ${colorClass}`}>
        {label}
      </p>

      {selected ? (
        <div className="flex items-center gap-3 glass rounded-2xl px-4 py-3">
          <ActorAvatar name={selected.name} size={44} />
          <div className="flex-1 min-w-0">
            <p className="text-white font-semibold text-sm truncate">{selected.name}</p>
            {selected.industry && (
              <p className="text-white/40 text-xs">{selected.industry}</p>
            )}
          </div>
          <button
            onClick={() => { onClear(); setQuery(''); setTimeout(() => inputRef.current?.focus(), 0) }}
            className="text-white/30 hover:text-white/70 transition-colors flex-shrink-0"
            aria-label="Remove"
          >✕</button>
        </div>
      ) : (
        <div className="relative">
          <div className="glass rounded-2xl flex items-center px-4 gap-2 focus-within:ring-1 focus-within:ring-white/20 transition-all">
            <span className="text-white/25 flex-shrink-0">🔍</span>
            <input
              ref={inputRef}
              value={query}
              onChange={e => setQuery(e.target.value)}
              placeholder={placeholder}
              className="flex-1 bg-transparent py-3.5 text-white placeholder-white/25 outline-none text-sm"
            />
            {loading && <span className="text-white/30 text-xs animate-pulse flex-shrink-0">…</span>}
          </div>
          {open && results.length > 0 && (
            <div
              ref={dropRef}
              className="absolute top-full left-0 right-0 mt-1.5 rounded-2xl overflow-hidden z-50 shadow-2xl border border-white/[0.10]"
              style={{ background: '#1e1e2c' }}
            >
              {results.map(a => (
                <button
                  key={a.id}
                  onMouseDown={e => e.preventDefault()}
                  onClick={() => { onSelect(a); setQuery(''); setResults([]); setOpen(false) }}
                  className="w-full flex items-center gap-3 px-4 py-2.5 hover:bg-white/[0.08] transition-colors text-left border-b border-white/[0.05] last:border-0"
                >
                  <ActorAvatar name={a.name} size={32} />
                  <div className="min-w-0">
                    <p className="text-white text-sm font-medium truncate">{a.name}</p>
                    {a.industry && <p className="text-white/35 text-xs">{a.industry}</p>}
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

// ── Main component ────────────────────────────────────────────────────────────

const EXAMPLE_PAIRS: { label: string; a: Actor; b: Actor }[] = [
  {
    label: 'Rajinikanth → Prabhas',
    a: { id: 1,  name: 'Rajinikanth',  industry: 'Tamil' },
    b: { id: 2,  name: 'Prabhas',      industry: 'Telugu' },
  },
]

export default function ConnectionFinder({
  defaultActors,
}: {
  defaultActors?: { id: number; name: string; industry: string }[]
}) {
  const [actor1, setActor1] = useState<Actor | null>(null)
  const [actor2, setActor2] = useState<Actor | null>(null)
  const [result, setResult] = useState<ConnectionPath | null>(null)
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState<string | null>(null)

  const canSearch = actor1 !== null && actor2 !== null && actor1.id !== actor2.id

  async function handleFind() {
    if (!canSearch) return
    setLoading(true)
    setResult(null)
    setError(null)
    try {
      const res = await getActorConnection(actor1!.id, actor2!.id)
      setResult(res)
    } catch {
      setError('Failed to fetch connection — please try again.')
    } finally {
      setLoading(false)
    }
  }

  // Quick-start with example pairs
  async function tryExample() {
    const a = defaultActors?.[0]
    const b = defaultActors?.[1]
    if (!a || !b) return
    setActor1(a)
    setActor2(b)
  }

  return (
    <section className="rounded-3xl p-6 sm:p-8 border border-white/[0.08]" style={{ background: '#13131a' }}>
      {/* Header */}
      <div className="mb-6">
        <h2 className="text-white font-bold text-lg flex items-center gap-2">
          🔗 Actor Connection Finder
        </h2>
        <p className="text-white/40 text-sm mt-1">
          Try connecting actors you wouldn&apos;t expect 👀
        </p>
      </div>

      {/* Actor pickers */}
      <div className="flex flex-col sm:flex-row items-stretch gap-4">
        <ActorBox
          label="From"
          selected={actor1}
          onSelect={setActor1}
          onClear={() => { setActor1(null); setResult(null) }}
          colorClass="text-amber-400/70"
          placeholder="Try Rajinikanth"
        />

        <div className="flex sm:flex-col items-center justify-center gap-1 flex-shrink-0 py-2">
          <div className="h-px sm:h-8 w-12 sm:w-px bg-white/10" />
          <span className="text-white/20 text-xs font-mono">→</span>
          <div className="h-px sm:h-8 w-12 sm:w-px bg-white/10" />
        </div>

        <ActorBox
          label="To"
          selected={actor2}
          onSelect={setActor2}
          onClear={() => { setActor2(null); setResult(null) }}
          colorClass="text-cyan-400/70"
          placeholder="Try Prabhas"
        />
      </div>

      {/* Find button */}
      <div className="mt-5 flex flex-col sm:flex-row items-center gap-3">
        <button
          onClick={handleFind}
          disabled={loading}
          className={`
            px-8 py-3 rounded-full font-bold text-sm transition-all duration-200
            ${canSearch && !loading
              ? 'bg-white text-[#0a0a0f] hover:scale-[1.03] hover:shadow-lg hover:shadow-white/20 active:scale-95'
              : 'bg-white/[0.08] text-white/50 border border-white/[0.12] cursor-default'}
          `}
        >
          {loading ? 'Searching path…' : 'Find Connection'}
        </button>

        {(!actor1 || !actor2) && defaultActors && defaultActors.length >= 2 && (
          <button
            onClick={() => { setActor1(defaultActors[0]); setActor2(defaultActors[1]); }}
            className="text-white/35 hover:text-white/60 text-xs transition-colors underline underline-offset-2"
          >
            Try {defaultActors[0].name} → {defaultActors[1].name}
          </button>
        )}
      </div>

      {/* Error */}
      {error && <p className="mt-4 text-red-400 text-sm text-center">{error}</p>}

      {/* Loading shimmer */}
      {loading && (
        <div className="mt-6 flex justify-center gap-2">
          {[0,1,2,3,4].map(i => (
            <div
              key={i}
              className="w-2 h-2 rounded-full bg-white/30 animate-bounce"
              style={{ animationDelay: `${i * 0.1}s` }}
            />
          ))}
        </div>
      )}

      {/* Result — key forces full remount (animation reset) on each new search */}
      {result && !loading && (
        <ConnectionResult
          key={`${result.path[0]?.id}-${result.path.at(-1)?.id}-${result.depth}`}
          result={result}
        />
      )}
    </section>
  )
}
