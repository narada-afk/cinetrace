interface StatCard {
  label: string
  value: string | number
  emoji: string
  gradient: string
  border: string
}

interface ActorStatsProps {
  filmCount: number
  collaboratorCount: number
  directorCount: number
  industry: string
}

export default function ActorStats({
  filmCount,
  collaboratorCount,
  directorCount,
  industry,
}: ActorStatsProps) {
  const cards: StatCard[] = [
    {
      label: 'Films',
      value: filmCount,
      emoji: '🎬',
      gradient: 'from-red-700/60 to-red-900/40',
      border: 'border-red-500/20',
    },
    {
      label: 'Co-Actors',
      value: collaboratorCount,
      emoji: '🤝',
      gradient: 'from-purple-700/60 to-purple-900/40',
      border: 'border-purple-500/20',
    },
    {
      label: 'Directors',
      value: directorCount,
      emoji: '🎥',
      gradient: 'from-orange-600/60 to-orange-900/40',
      border: 'border-orange-500/20',
    },
    {
      label: 'Industry',
      value: industry,
      emoji: '🌟',
      gradient: 'from-blue-700/60 to-blue-900/40',
      border: 'border-blue-500/20',
    },
  ]

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
      {cards.map((card) => (
        <div
          key={card.label}
          className={`
            relative rounded-xl p-5 overflow-hidden
            bg-gradient-to-br ${card.gradient}
            border ${card.border}
          `}
        >
          <div className="absolute inset-0 bg-[#0a0a0f]/20 pointer-events-none" />
          <div className="relative z-10 flex flex-col gap-1">
            <div className="flex items-center gap-1.5">
              <span className="text-base">{card.emoji}</span>
              <span className="text-xs font-semibold uppercase tracking-widest text-white/40">
                {card.label}
              </span>
            </div>
            <div className="text-3xl font-bold text-white leading-none mt-1">
              {card.value}
            </div>
          </div>
        </div>
      ))}
    </div>
  )
}
