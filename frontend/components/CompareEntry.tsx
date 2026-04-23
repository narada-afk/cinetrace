'use client'

/**
 * CompareEntry — lightweight compare entry point on the homepage.
 *
 * Two actor search inputs + "Compare Now" button + popular suggestion chips.
 * Navigates to /compare/:slug1-vs-:slug2 on submit.
 */

import { useState, useEffect, useRef } from 'react'
import { useRouter } from 'next/navigation'
import { searchActors, toActorSlug, type Actor } from '@/lib/api'

const POPULAR = [
  { label: 'Vijay vs Ajith',          a: 'vijay',       b: 'ajith',       tag: 'Tamil' },
  { label: 'Prabhas vs Mahesh Babu',  a: 'prabhas',     b: 'mahesh-babu', tag: 'Telugu' },
  { label: 'Mammootty vs Mohanlal',   a: 'mammootty',   b: 'mohanlal',    tag: 'Malayalam' },
  { label: 'Rajini vs Chiranjeevi',   a: 'rajinikanth', b: 'chiranjeevi', tag: 'Cross' },
]

// ── Minimal actor search input ────────────────────────────────────────────────

function ActorPill({
  actor,
  placeholder,
  onSelect,
  onClear,
}: {
  actor: Actor | null
  placeholder: string
  onSelect: (a: Actor) => void
  onClear: () => void
}) {
  const [q, setQ]           = useState('')
  const [results, setResults] = useState<Actor[]>([])
  const [open, setOpen]       = useState(false)
  const inputRef = useRef<HTMLInputElement>(null)
  const dropRef  = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!q.trim()) { setResults([]); setOpen(false); return }
    const tid = setTimeout(async () => {
      try {
        const res = await searchActors(q)
        setResults(res.slice(0, 5))
        setOpen(res.length > 0)
      } catch { setResults([]) }
    }, 200)
    return () => clearTimeout(tid)
  }, [q])

  useEffect(() => {
    const h = (e: MouseEvent) => {
      if (
        dropRef.current?.contains(e.target as Node) ||
        inputRef.current?.contains(e.target as Node)
      ) return
      setOpen(false)
    }
    document.addEventListener('mousedown', h)
    return () => document.removeEventListener('mousedown', h)
  }, [])

  if (actor) {
    return (
      <div className="flex-1 flex items-center gap-2.5 px-4 py-3 rounded-2xl border min-w-0"
           style={{ background: 'rgba(255,255,255,0.08)', borderColor: 'rgba(255,255,255,0.18)' }}>
        <span className="text-white text-sm font-semibold flex-1 truncate">{actor.name}</span>
        <button
          onClick={() => { onClear(); setTimeout(() => inputRef.current?.focus(), 0) }}
          className="text-white/30 hover:text-white/60 transition-colors flex-shrink-0 text-xs leading-none"
          aria-label="Clear"
        >✕</button>
      </div>
    )
  }

  return (
    <div className="relative flex-1 min-w-0">
      <input
        ref={inputRef}
        value={q}
        onChange={e => setQ(e.target.value)}
        onFocus={() => q.trim() && results.length > 0 && setOpen(true)}
        placeholder={placeholder}
        className="w-full px-4 py-3 rounded-2xl text-sm text-white placeholder-white/30 outline-none transition-all duration-200"
        style={{
          background: 'rgba(255,255,255,0.06)',
          border:     '1px solid rgba(255,255,255,0.10)',
        }}
      />
      {open && results.length > 0 && (
        <div
          ref={dropRef}
          className="absolute left-0 right-0 top-full mt-1.5 rounded-2xl overflow-hidden z-50 shadow-2xl border border-white/[0.10]"
          style={{ background: '#1e1e2c' }}
        >
          {results.map(a => (
            <button
              key={a.id}
              onMouseDown={e => e.preventDefault()}
              onClick={() => { onSelect(a); setQ(''); setOpen(false) }}
              className="w-full flex items-center justify-between px-4 py-2.5 hover:bg-white/[0.08] transition-colors text-left border-b border-white/[0.05] last:border-0"
            >
              <span className="text-white text-sm truncate">{a.name}</span>
              {a.industry && (
                <span className="text-white/30 text-xs ml-2 flex-shrink-0">{a.industry}</span>
              )}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

// ── Main component ────────────────────────────────────────────────────────────

export default function CompareEntry() {
  const router = useRouter()
  const [a1, setA1] = useState<Actor | null>(null)
  const [a2, setA2] = useState<Actor | null>(null)

  const canCompare = a1 !== null && a2 !== null && a1.id !== a2.id

  function handleCompare() {
    if (!canCompare) return
    router.push(`/compare/${toActorSlug(a1!.name)}-vs-${toActorSlug(a2!.name)}`)
  }

  return (
    <div
      className="rounded-3xl p-6 sm:p-8 border border-white/[0.08]"
      style={{ background: '#13131a' }}
    >
      <div className="mb-5">
        <h2 className="text-white font-bold text-lg">⚔️ Compare Actors</h2>
        <p className="text-white/40 text-sm mt-1">
          Who dominated more? See stats, films &amp; box office head-to-head.
        </p>
      </div>

      {/* Inputs */}
      <div className="flex flex-col sm:flex-row items-stretch sm:items-center gap-3">
        <ActorPill
          actor={a1}
          placeholder="Actor 1 (e.g. Vijay)"
          onSelect={setA1}
          onClear={() => setA1(null)}
        />
        <span className="text-white/25 font-bold text-sm text-center flex-shrink-0 sm:px-1">
          vs
        </span>
        <ActorPill
          actor={a2}
          placeholder="Actor 2 (e.g. Ajith)"
          onSelect={setA2}
          onClear={() => setA2(null)}
        />
      </div>

      {/* Button + chips row */}
      <div className="mt-4 flex flex-col sm:flex-row items-start sm:items-center gap-4">
        <button
          onClick={handleCompare}
          disabled={!canCompare}
          className={[
            'px-8 py-3 rounded-full font-bold text-sm transition-all duration-200 flex-shrink-0',
            canCompare
              ? 'bg-white text-[#0a0a0f] hover:scale-[1.03] active:scale-95'
              : 'bg-white/[0.08] text-white/40 border border-white/[0.12] cursor-default',
          ].join(' ')}
          onMouseEnter={e => {
            if (canCompare) (e.currentTarget as HTMLButtonElement).style.boxShadow = '0 0 24px rgba(255,255,255,0.30), 0 4px 16px rgba(255,255,255,0.12)'
          }}
          onMouseLeave={e => { (e.currentTarget as HTMLButtonElement).style.boxShadow = 'none' }}
        >
          Compare Now
        </button>

        <div className="flex flex-wrap items-center gap-2">
          <span className="text-[10px] uppercase tracking-widest text-white/25 font-semibold whitespace-nowrap">
            🔥 Fan Battles:
          </span>
          {POPULAR.map(p => (
            <button
              key={p.label}
              onClick={() => router.push(`/compare/${p.a}-vs-${p.b}`)}
              className="px-3 py-1.5 rounded-full text-xs text-white/50 border border-white/[0.09] bg-white/[0.03] transition-all duration-200 hover:text-white/85 hover:bg-white/[0.10] hover:border-white/[0.22] hover:scale-105"
              onMouseEnter={e => { (e.currentTarget as HTMLButtonElement).style.boxShadow = '0 0 14px rgba(255,255,255,0.08)' }}
              onMouseLeave={e => { (e.currentTarget as HTMLButtonElement).style.boxShadow = 'none' }}
              title={p.tag}
            >
              {p.label}
            </button>
          ))}
        </div>
      </div>
    </div>
  )
}
