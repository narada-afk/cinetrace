/**
 * /stats — Stats for Nerds  (Sprint 22 rewrite)
 *
 * Page order:
 *   1. Hero metrics
 *   2. Build Your Own Chart        ← new
 *   3. Actor Connection Finder
 *   4. Cinema Universe             ← new (force graph)
 *   5. Cinema Gravity Center       ← new (betweenness centrality)
 *   ──── existing dashboards ────
 *   6. Most Connected Actors
 *   7. Industry Distribution
 *   8. Actor–Director Partnerships
 *   9. Career Timeline
 *  10. Top Co-Star Networks
 */

import Header             from '@/components/Header'
import NavTabs            from '@/components/NavTabs'
import ConnectionFinder   from '@/components/stats/ConnectionFinder'
import MostConnectedPanel from '@/components/stats/MostConnectedPanel'
import IndustryChart      from '@/components/stats/IndustryChart'
import DirectorPartnerships from '@/components/stats/DirectorPartnerships'
import CareerTimeline     from '@/components/stats/CareerTimeline'
import TopCoStarsChart    from '@/components/stats/TopCoStarsChart'
import ChartBuilder       from '@/components/stats/ChartBuilder'
import CinemaUniverse     from '@/components/stats/CinemaUniverse'
import GravityCenter      from '@/components/stats/GravityCenter'

import {
  getStatsOverview,
  getMostConnected,
  getIndustryDistribution,
  getTopPartnerships,
  getCareerTimeline,
  getTopCoStars,
  getCinemaUniverse,
  getGravityCenter,
} from '@/lib/api'

// ── Hero metric pill ───────────────────────────────────────────────────────────

function StatPill({ value, label, emoji }: { value: string; label: string; emoji: string }) {
  return (
    <div className="flex flex-col items-center gap-1 px-6 py-4 glass rounded-2xl flex-1 min-w-[110px]">
      <span className="text-2xl">{emoji}</span>
      <span className="text-white font-bold text-xl tabular-nums">{value}</span>
      <span className="text-white/40 text-xs text-center leading-tight">{label}</span>
    </div>
  )
}

// ── Section wrapper ────────────────────────────────────────────────────────────

function Section({ id, children, className = '' }: { id: string; children: React.ReactNode; className?: string }) {
  return <section id={id} className={`scroll-mt-24 ${className}`}>{children}</section>
}

// ── Divider ────────────────────────────────────────────────────────────────────

function Divider({ label }: { label: string }) {
  return (
    <div className="flex items-center gap-4 my-8">
      <div className="flex-1 h-px bg-white/[0.07]" />
      <span className="text-white/25 text-xs uppercase tracking-widest px-2">{label}</span>
      <div className="flex-1 h-px bg-white/[0.07]" />
    </div>
  )
}

// ── Page ───────────────────────────────────────────────────────────────────────

interface PageProps { searchParams?: { actor?: string } }

export default async function StatsPage({ searchParams }: PageProps) {
  // Pre-fetch all data in parallel
  const [overview, connected, industry, partnerships, costars, universe, gravity] = await Promise.all([
    getStatsOverview(),
    getMostConnected(20),
    getIndustryDistribution(),
    getTopPartnerships(14),
    getTopCoStars(15),
    getCinemaUniverse(2),
    getGravityCenter(25),
  ])

  // Career timeline: respect ?actor= param, else default to most-connected actor
  const actorIdParam    = searchParams?.actor ? parseInt(searchParams.actor, 10) : null
  const timelineActorId = actorIdParam ?? (connected[0]?.id ?? null)
  const timeline = timelineActorId
    ? await getCareerTimeline(timelineActorId).catch(() => null)
    : null

  const defaultActors = [
    { id: 1,   name: 'Allu Arjun',  industry: 'Telugu'    },
    { id: 381, name: 'Mohanlal',    industry: 'Malayalam' },
  ]

  return (
    <main className="min-h-screen" style={{ background: '#0a0a0f' }}>
      <div className="max-w-6xl mx-auto px-4 pb-24">
        <Header />
        <NavTabs activeTab="stats" />

        {/* ── Page title ── */}
        <div className="text-center mt-10 mb-8">
          <h1 className="text-white text-2xl sm:text-3xl font-bold tracking-tight">Stats for Nerds</h1>
          <p className="text-white/35 text-sm mt-2">
            Explore {overview.total_movies.toLocaleString()} films ·{' '}
            {overview.total_actors} actors · {overview.total_links.toLocaleString()} collaborations
            across {overview.industries} South Indian industries
          </p>
        </div>

        {/* ── Hero metrics ── */}
        <div className="flex flex-wrap gap-3 justify-center mb-10">
          <StatPill emoji="🎬" value={overview.total_movies.toLocaleString()} label="Films indexed" />
          <StatPill emoji="🎭" value={overview.total_actors.toLocaleString()} label="Fully ingested actors" />
          <StatPill emoji="🤝" value={overview.total_links.toLocaleString()} label="Actor–film links" />
          <StatPill emoji="🏭" value={String(overview.industries)} label="Industries" />
        </div>

        {/* ══════════════════════════════════════════════════════════════ */}
        {/* INTERACTIVE PLAYGROUND                                        */}
        {/* ══════════════════════════════════════════════════════════════ */}

        {/* 1 — Build Your Own Chart */}
        <Section id="chart-builder" className="mb-6">
          <ChartBuilder />
        </Section>

        {/* 2 — Actor Connection Finder */}
        <Section id="connection-finder" className="mb-6">
          <ConnectionFinder defaultActors={defaultActors} />
        </Section>

        {/* 3 — Cinema Universe */}
        <Section id="cinema-universe" className="mb-6">
          <CinemaUniverse data={universe} />
        </Section>

        {/* 4 — Cinema Gravity Center */}
        <Section id="gravity-center" className="mb-6">
          <GravityCenter data={gravity} />
        </Section>

        {/* ══════════════════════════════════════════════════════════════ */}
        {/* EXISTING DASHBOARDS                                           */}
        {/* ══════════════════════════════════════════════════════════════ */}

        <Divider label="Analytics Dashboards" />

        {/* 5+6 — Most Connected | Industry Distribution */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
          <Section id="most-connected">
            <MostConnectedPanel data={connected} />
          </Section>
          <Section id="industry">
            <IndustryChart data={industry} />
          </Section>
        </div>

        {/* 7 — Director Partnerships */}
        <Section id="partnerships" className="mb-6">
          <DirectorPartnerships data={partnerships} />
        </Section>

        {/* 8+9 — Career Timeline | Top Co-Stars */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <Section id="career-timeline">
            {timeline ? (
              <CareerTimeline initialData={timeline} />
            ) : (
              <div className="glass rounded-3xl p-6 flex items-center justify-center min-h-[280px]">
                <p className="text-white/30 text-sm">Career timeline unavailable</p>
              </div>
            )}
          </Section>
          <Section id="top-costars">
            <TopCoStarsChart data={costars} />
          </Section>
        </div>

        {/* Footer */}
        <p className="text-center text-white/20 text-xs mt-12">
          Data sourced from TMDB · {overview.total_actors} actors fully ingested ·
          Collaboration graph: {overview.total_links.toLocaleString()} edges
        </p>
      </div>
    </main>
  )
}
