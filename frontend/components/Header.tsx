'use client'

import { useState, useEffect } from 'react'
import Link from 'next/link'
import Image from 'next/image'

export default function Header() {
  const [scrolled, setScrolled] = useState(false)

  useEffect(() => {
    const onScroll = () => setScrolled(window.scrollY > 20)
    onScroll()
    window.addEventListener('scroll', onScroll, { passive: true })
    return () => window.removeEventListener('scroll', onScroll)
  }, [])

  return (
    <header
      className="sticky top-0 z-50 w-full transition-all duration-300"
      style={{
        background:           scrolled ? 'rgba(10,10,15,0.72)' : 'transparent',
        backdropFilter:       scrolled ? 'blur(18px) saturate(160%)' : 'none',
        WebkitBackdropFilter: scrolled ? 'blur(18px) saturate(160%)' : 'none',
        borderBottom:         scrolled ? '1px solid rgba(255,255,255,0.06)' : '1px solid transparent',
        boxShadow:            scrolled ? '0 2px 32px rgba(0,0,0,0.4)' : 'none',
      }}
    >
      <div className="max-w-[1200px] mx-auto px-6 h-[66px] flex items-center">

        {/* ── Logo ─────────────────────────────────────────────── */}
        <Link href="/" className="group">
          <Image
            src="/cinetrace-logo.png"
            alt="CineTrace"
            width={160}
            height={40}
            className="object-contain opacity-90 group-hover:opacity-100 transition-opacity duration-200 w-[120px] sm:w-[160px] h-auto"
            priority
          />
        </Link>

      </div>
    </header>
  )
}
