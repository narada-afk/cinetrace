'use client'

import { useState } from 'react'
import { useRouter } from 'next/navigation'

export default function SearchBar() {
  const [query, setQuery] = useState('')
  const [loading, setLoading] = useState(false)
  const [notFound, setNotFound] = useState(false)
  const router = useRouter()

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    const q = query.trim()
    if (!q) return

    setLoading(true)
    setNotFound(false)

    try {
      const apiUrl = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000'
      const res = await fetch(`${apiUrl}/actors/search?q=${encodeURIComponent(q)}`)
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
    <form onSubmit={handleSubmit} className="relative w-full">
      <span className="absolute left-3 top-1/2 -translate-y-1/2 text-white/30 pointer-events-none">
        {loading ? (
          <svg
            className="animate-spin"
            width="16"
            height="16"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
          >
            <path d="M21 12a9 9 0 1 1-6.219-8.56" />
          </svg>
        ) : (
          <svg
            width="16"
            height="16"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <circle cx="11" cy="11" r="8" />
            <line x1="21" y1="21" x2="16.65" y2="16.65" />
          </svg>
        )}
      </span>
      <input
        type="text"
        value={query}
        onChange={(e) => { setQuery(e.target.value); setNotFound(false) }}
        placeholder="Search actors, movies, stats..."
        disabled={loading}
        className="
          w-full pl-9 pr-4 py-2 rounded-full text-sm
          bg-white/[0.06] border border-white/10
          text-white placeholder-white/30
          focus:outline-none focus:border-white/25 focus:bg-white/[0.08]
          transition-colors disabled:opacity-60
        "
      />
      {notFound && (
        <span className="absolute right-4 top-1/2 -translate-y-1/2 text-xs text-white/40">
          No results
        </span>
      )}
    </form>
  )
}
