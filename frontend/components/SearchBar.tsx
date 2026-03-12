'use client'

import { useState } from 'react'

export default function SearchBar() {
  const [query, setQuery] = useState('')

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    // TODO Sprint 12: route to search results page
    if (query.trim()) {
      console.log('Search:', query)
    }
  }

  return (
    <form onSubmit={handleSubmit} className="relative w-full">
      <span className="absolute left-3 top-1/2 -translate-y-1/2 text-white/30 pointer-events-none">
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
      </span>
      <input
        type="text"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder="Search actors, movies, stats..."
        className="
          w-full pl-9 pr-4 py-2 rounded-full text-sm
          bg-white/[0.06] border border-white/10
          text-white placeholder-white/30
          focus:outline-none focus:border-white/25 focus:bg-white/[0.08]
          transition-colors
        "
      />
    </form>
  )
}
