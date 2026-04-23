// NavTabs — Server Component
// Uses <Link> so every tab click is a real navigation.
// Industry tabs update the ?industry= search param on the home page.
// Compare and More tabs navigate to dedicated pages.

import Link from 'next/link'

const TABS = [
  { label: 'All Cinema', value: 'all',       href: '/' },
  { label: 'Telugu',     value: 'telugu',    href: '/?industry=telugu' },
  { label: 'Tamil',      value: 'tamil',     href: '/?industry=tamil' },
  { label: 'Malayalam',  value: 'malayalam', href: '/?industry=malayalam' },
  { label: 'Kannada',    value: 'kannada',   href: '/?industry=kannada' },
]

interface NavTabsProps {
  /**
   * Active tab identifier.
   * - For the home page: the industry slug ('all' | 'telugu' | 'tamil' | 'malayalam' | 'kannada')
   * - For dedicated pages: 'compare' | 'more'
   */
  activeTab?: string
}

export default function NavTabs({ activeTab = 'all' }: NavTabsProps) {
  return (
    <nav className="flex justify-center mt-4">
      <div className="inline-flex items-center gap-1 px-2 py-2 rounded-full glass">
        {TABS.map((tab) => (
          <Link
            key={tab.value}
            href={tab.href}
            className={`
              px-4 py-1.5 rounded-full text-sm font-medium transition-all
              ${
                activeTab === tab.value
                  ? 'bg-white/15 text-white'
                  : 'text-white/50 hover:text-white/80 hover:bg-white/[0.06]'
              }
            `}
          >
            {tab.label}
          </Link>
        ))}
      </div>
    </nav>
  )
}
