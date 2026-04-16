'use client'

import { useState, useEffect } from 'react'
import Image from 'next/image'
import Link from 'next/link'
import { usePathname } from 'next/navigation'

// ── Nav config ────────────────────────────────────────────────────────────────

const NAV_LINKS = [
  { label: 'Insights', href: '/' },
  { label: 'Compare',  href: '/compare' },
  { label: 'Stats',    href: '/stats' },
]

// ── Sub-components ────────────────────────────────────────────────────────────

function GitHubIcon() {
  return (
    <svg width="15" height="15" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
      <path d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z" />
    </svg>
  )
}

function NavItem({ label, href, isActive }: { label: string; href: string; isActive: boolean }) {
  return (
    <Link
      href={href}
      className={`
        relative px-3.5 py-2 rounded-lg text-sm font-medium
        transition-colors duration-200
        hover:bg-white/[0.055]
        ${isActive ? 'text-white/90' : 'text-white/45 hover:text-white/80'}
      `}
    >
      {label}
      {/* Active indicator — glowing dot below the label */}
      {isActive && (
        <span
          className="absolute bottom-[5px] left-1/2 -translate-x-1/2 w-[3px] h-[3px] rounded-full"
          style={{
            background: '#a5b4fc',
            boxShadow: '0 0 5px 1px rgba(165,180,252,0.7)',
          }}
        />
      )}
    </Link>
  )
}

// ── Main component ────────────────────────────────────────────────────────────

export default function Header() {
  const [scrolled, setScrolled] = useState(false)
  const pathname = usePathname()

  useEffect(() => {
    const onScroll = () => setScrolled(window.scrollY > 20)
    onScroll() // initialise on mount
    window.addEventListener('scroll', onScroll, { passive: true })
    return () => window.removeEventListener('scroll', onScroll)
  }, [])

  return (
    <header
      className="sticky top-0 z-50 w-full transition-all duration-300"
      style={{
        background:          scrolled ? 'rgba(10,10,15,0.72)' : 'transparent',
        backdropFilter:      scrolled ? 'blur(18px) saturate(160%)' : 'none',
        WebkitBackdropFilter:scrolled ? 'blur(18px) saturate(160%)' : 'none',
        borderBottom:        scrolled
          ? '1px solid rgba(255,255,255,0.06)'
          : '1px solid transparent',
        boxShadow:           scrolled ? '0 2px 32px rgba(0,0,0,0.4)' : 'none',
      }}
    >
      <div className="max-w-[1200px] mx-auto px-6 h-[66px] flex items-center justify-between">

        {/* ── LEFT: logo + brand ─────────────────────────────────────── */}
        <Link href="/" className="group flex items-center gap-3 flex-shrink-0">

          {/* Logo orb */}
          <div className="relative flex-shrink-0">
            {/* Ambient glow — expands on hover */}
            <div
              className="absolute inset-0 rounded-full pointer-events-none opacity-0 group-hover:opacity-100 transition-opacity duration-500"
              style={{
                background: 'radial-gradient(circle, rgba(99,102,241,0.55) 0%, transparent 68%)',
                transform:  'scale(2.2)',
                filter:     'blur(6px)',
              }}
            />
            {/* Logo ring */}
            <div
              className="relative w-[44px] h-[44px] rounded-full overflow-hidden flex-shrink-0 transition-all duration-300 group-hover:scale-[1.07]"
              style={{
                background: 'rgba(255,255,255,0.055)',
                border:     '1px solid rgba(255,255,255,0.13)',
                boxShadow:  'inset 0 1px 0 rgba(255,255,255,0.10), 0 0 0 1px rgba(255,255,255,0.04)',
              }}
            >
              <Image
                src="/narada.png"
                alt="SouthCineStats"
                width={44}
                height={44}
                className="object-cover w-full h-full scale-110"
                priority
              />
            </div>
          </div>

          {/* Brand text */}
          <div className="flex flex-col gap-[3px]">

            {/* Name + BETA badge */}
            <div className="flex items-center gap-[7px]">
              <span className="text-[15px] font-bold tracking-[-0.01em] leading-none select-none">
                <span className="text-white/88 group-hover:text-white/95 transition-colors duration-200">
                  SouthCine
                </span>
                <span
                  className="transition-colors duration-200"
                  style={{ color: '#a5b4fc' }}
                >
                  Stats
                </span>
              </span>
              <span
                className="text-[9px] font-semibold tracking-[0.07em] leading-none px-[6px] py-[3px] rounded-full select-none"
                style={{
                  background: 'rgba(165,180,252,0.10)',
                  border:     '1px solid rgba(165,180,252,0.20)',
                  color:      'rgba(165,180,252,0.60)',
                }}
              >
                BETA
              </span>
            </div>

            {/* Subtitle */}
            <span
              className="text-[10px] leading-none select-none tracking-[0.055em]"
              style={{ color: 'rgba(255,255,255,0.30)' }}
            >
              South Indian Cinema Analytics
            </span>
          </div>
        </Link>

        {/* ── RIGHT: nav + github ────────────────────────────────────── */}
        <nav className="flex items-center gap-0.5">
          {NAV_LINKS.map(({ label, href }) => {
            const isActive =
              href === '/' ? pathname === '/' : pathname.startsWith(href)
            return (
              <NavItem key={href} label={label} href={href} isActive={isActive} />
            )
          })}

          {/* Divider */}
          <div className="w-px h-[14px] mx-2 bg-white/[0.09]" />

          {/* GitHub */}
          <a
            href="https://github.com/narada-afk/south-cinema-analytics"
            target="_blank"
            rel="noopener noreferrer"
            aria-label="GitHub"
            className="
              p-2 rounded-lg
              text-white/35 hover:text-white/70
              hover:bg-white/[0.055]
              transition-all duration-200
            "
          >
            <GitHubIcon />
          </a>
        </nav>

      </div>
    </header>
  )
}
