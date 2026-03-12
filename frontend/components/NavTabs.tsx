'use client'

import { useState } from 'react'

const TABS = [
  { label: 'All Cinema', value: 'all' },
  { label: 'Telugu', value: 'telugu' },
  { label: 'Tamil', value: 'tamil' },
  { label: 'Malayalam', value: 'malayalam' },
  { label: 'Kannada', value: 'kannada' },
  { label: 'Explore', value: 'explore' },
]

interface NavTabsProps {
  onTabChange?: (tab: string) => void
}

export default function NavTabs({ onTabChange }: NavTabsProps) {
  const [active, setActive] = useState('all')

  function handleClick(value: string) {
    setActive(value)
    onTabChange?.(value)
  }

  return (
    <nav className="flex justify-center mt-4">
      <div
        className="
          inline-flex items-center gap-1 px-2 py-2 rounded-full
          glass
        "
      >
        {TABS.map((tab) => (
          <button
            key={tab.value}
            onClick={() => handleClick(tab.value)}
            className={`
              px-4 py-1.5 rounded-full text-sm font-medium transition-all
              ${
                active === tab.value
                  ? 'bg-white/15 text-white'
                  : 'text-white/50 hover:text-white/80 hover:bg-white/[0.06]'
              }
            `}
          >
            {tab.label}
          </button>
        ))}
      </div>
    </nav>
  )
}
