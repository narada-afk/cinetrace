'use client'

import Image from 'next/image'
import type { ActorProfile } from '@/lib/api'

interface ActorHeroProps {
  actor: ActorProfile
  collaboratorCount: number
  directorCount: number
  firstFilm?: { title: string; year: number } | null
}

export default function ActorHero({
  actor,
  collaboratorCount,
  directorCount,
  firstFilm,
}: ActorHeroProps) {
  const slug = actor.name.toLowerCase().replace(/[^a-z0-9]/g, '')

  return (
    <div className="relative w-full overflow-hidden rounded-3xl" style={{ background: '#0f0f1a' }}>

      {/* Ambient gradient — behind text side only */}
      <div className="absolute inset-0 bg-gradient-to-br from-[#1c1c35] via-[#0f0f1a] to-[#0a0a0f]" />
      <div
        className="absolute right-0 inset-y-0 w-2/3 pointer-events-none"
        style={{
          background: 'radial-gradient(ellipse at 80% 50%, rgba(120,80,255,0.07) 0%, transparent 70%)',
        }}
      />

      <div className="relative z-10 flex flex-row items-end" style={{ gap: 0 }}>

        {/* ── Avatar — large, rounded rectangle, breaks out of bottom ── */}
        <div
          className="flex-shrink-0 self-end group"
          style={{ marginBottom: -1, transform: 'translateX(8px)' }}
        >
          {/* Subtle glow behind the cutout — no visible box */}
          <div
            style={{
              width: 160,
              height: 200,
              position: 'relative',
              background: 'radial-gradient(circle at 50% 60%, rgba(255,255,255,0.04), transparent 70%)',
            }}
          >
            <Image
              src={`/avatars/${slug}.png`}
              alt={actor.name}
              fill
              priority
              sizes="160px"
              className="object-cover object-top transition-transform duration-300 group-hover:scale-[1.02]"
              style={{
                maskImage: 'radial-gradient(ellipse 85% 90% at 50% 55%, rgba(0,0,0,1) 60%, rgba(0,0,0,0) 100%)',
                WebkitMaskImage: 'radial-gradient(ellipse 85% 90% at 50% 55%, rgba(0,0,0,1) 60%, rgba(0,0,0,0) 100%)',
                filter: 'drop-shadow(0 10px 24px rgba(0,0,0,0.45))',
                transform: 'scale(1.05)',
                opacity: 0.98,
              }}
              onError={(e) => {
                const el = e.currentTarget as HTMLImageElement
                el.style.display = 'none'
                const parent = el.parentElement
                if (parent) {
                  parent.style.background = initialsColor(actor.name)
                  parent.innerHTML = `<span style="font-size:52px;color:rgba(255,255,255,0.9);font-weight:700;display:flex;align-items:center;justify-content:center;width:100%;height:100%">${initials(actor.name)}</span>`
                }
              }}
            />
          </div>
        </div>

        {/* ── Text block ─────────────────────────────────────────── */}
        <div className="flex flex-col gap-3 px-4 py-10 flex-1 min-w-0">

          {/* Industry pill */}
          <div>
            <span className="text-[11px] font-semibold uppercase tracking-widest text-white/40 px-2.5 py-1 rounded-full border border-white/[0.08]">
              {actor.industry}
            </span>
          </div>

          {/* Name */}
          <h1 className="text-4xl sm:text-5xl font-bold text-white tracking-tight leading-[1.05]">
            {actor.name}
          </h1>

          {/* Stats */}
          <div className="flex items-center gap-2 text-sm flex-wrap mt-0.5">
            <HeroStat value={actor.film_count} label="films" />
            <Dot />
            <HeroStat value={collaboratorCount} label="co-actors" />
            <Dot />
            <HeroStat value={directorCount} label="directors" />
          </div>

          {/* First film */}
          {firstFilm && (
            <p className="text-white/35 text-xs mt-0.5">
              First film{' '}
              <span className="text-white/55 font-medium">
                {firstFilm.title}
              </span>
              {firstFilm.year > 0 && (
                <span className="text-white/25"> ({firstFilm.year})</span>
              )}
            </p>
          )}
        </div>
      </div>
    </div>
  )
}

function HeroStat({ value, label }: { value: number; label: string }) {
  return (
    <span>
      <strong className="text-white font-semibold">{value}</strong>
      <span className="text-white/40"> {label}</span>
    </span>
  )
}

function Dot() {
  return <span className="text-white/20">•</span>
}

function initials(name: string) {
  return name.split(' ').map(w => w[0]).join('').slice(0, 2).toUpperCase()
}

const COLORS = ['#7c3aed','#db2777','#ea580c','#2563eb','#16a34a','#0891b2']
function initialsColor(name: string) {
  let h = 0
  for (let i = 0; i < name.length; i++) h = name.charCodeAt(i) + ((h << 5) - h)
  return COLORS[Math.abs(h) % COLORS.length]
}
