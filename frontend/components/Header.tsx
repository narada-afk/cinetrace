'use client'

import { useState, useEffect } from 'react'
import Image from 'next/image'
import Link from 'next/link'

// ── Main component ────────────────────────────────────────────────────────────

export default function Header() {
  const [scrolled, setScrolled] = useState(false)

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
                background: 'radial-gradient(circle, rgba(79,172,254,0.45) 0%, rgba(168,85,247,0.30) 50%, transparent 72%)',
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
                alt="CineTrace"
                width={44}
                height={44}
                className="object-cover w-full h-full scale-110"
                priority
              />
            </div>
          </div>

          {/* Brand text */}
          <div className="flex flex-col gap-[4px]">

            {/* Name + BETA badge */}
            <div className="flex items-center gap-[8px]">
              <span className="text-[16px] font-extrabold tracking-[0.015em] leading-none select-none">
                <span className="text-white/90 group-hover:text-white transition-colors duration-200">
                  Cine
                </span>
                <span
                  className="bg-gradient-to-r from-[#4FACFE] to-[#A855F7] bg-clip-text text-transparent"
                  style={{ filter: 'drop-shadow(0 0 8px rgba(168,85,247,0.40))' }}
                >
                  Trace
                </span>
              </span>
              <span
                className="text-[9px] font-semibold tracking-[0.07em] leading-none px-[6px] py-[3px] rounded-full select-none"
                style={{
                  background: 'rgba(79,172,254,0.08)',
                  border:     '1px solid rgba(79,172,254,0.22)',
                  color:      'rgba(79,172,254,0.70)',
                }}
              >
                BETA
              </span>
            </div>

            {/* Subtitle */}
            <span
              className="text-[10px] leading-none select-none tracking-[0.06em] italic"
              style={{ color: 'rgba(255,255,255,0.28)' }}
            >
              South Indian Cinema… traced.
            </span>
          </div>
        </Link>


      </div>
    </header>
  )
}
