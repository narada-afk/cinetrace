// Screenshot-friendly comparison summary card.
// Designed to fit within one viewport for easy sharing.

interface ActorSummary {
  name: string
  filmCount: number
  collaboratorCount: number
  directorCount: number
}

interface CompareSummaryProps {
  actorA: ActorSummary
  actorB: ActorSummary
}

const STATS: Array<{
  label: string
  keyA: keyof ActorSummary
  keyB: keyof ActorSummary
}> = [
  { label: 'Films',         keyA: 'filmCount',         keyB: 'filmCount' },
  { label: 'Collaborators', keyA: 'collaboratorCount',  keyB: 'collaboratorCount' },
  { label: 'Directors',     keyA: 'directorCount',      keyB: 'directorCount' },
]

export default function CompareSummary({ actorA, actorB }: CompareSummaryProps) {
  return (
    <div
      className="
        max-w-[900px] mx-auto mt-10 mb-12 px-6 py-8
        rounded-xl shadow-lg
        bg-gradient-to-br from-slate-900 to-slate-800
      "
    >
      {/* ── Header ──────────────────────────────────────────────── */}
      <p className="text-xl font-semibold text-white text-center mb-8">
        {actorA.name}{' '}
        <span className="text-white/25">vs</span>{' '}
        {actorB.name}
      </p>

      {/* ── Column headers ──────────────────────────────────────── */}
      <div className="grid grid-cols-3 gap-6 mb-1">
        <div />
        <p className="text-center text-xs font-medium text-white/40 uppercase tracking-wide truncate">
          {actorA.name}
        </p>
        <p className="text-center text-xs font-medium text-white/40 uppercase tracking-wide truncate">
          {actorB.name}
        </p>
      </div>

      {/* ── Stat rows ───────────────────────────────────────────── */}
      <div className="flex flex-col divide-y divide-white/[0.06]">
        {STATS.map(({ label, keyA, keyB }) => {
          const a = actorA[keyA] as number
          const b = actorB[keyB] as number
          const lead = a > b ? 'A' : b > a ? 'B' : null

          return (
            <div key={label} className="grid grid-cols-3 gap-6 items-center py-5">
              <span className="text-sm text-white/40">{label}</span>
              <span
                className={`text-center text-2xl font-bold tabular-nums ${
                  lead === 'A' ? 'text-emerald-400' : 'text-white/55'
                }`}
              >
                {a.toLocaleString()}
              </span>
              <span
                className={`text-center text-2xl font-bold tabular-nums ${
                  lead === 'B' ? 'text-emerald-400' : 'text-white/55'
                }`}
              >
                {b.toLocaleString()}
              </span>
            </div>
          )
        })}
      </div>

      {/* ── Footer branding ─────────────────────────────────────── */}
      <p className="text-xs text-white/30 text-center mt-6">
        southcinemaanalytics.com
      </p>
    </div>
  )
}
