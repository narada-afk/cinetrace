import type { DirectorCollab } from '@/lib/api'

interface DirectorListProps {
  directors: DirectorCollab[]
}

export default function DirectorList({ directors }: DirectorListProps) {
  if (!directors.length) {
    return <p className="text-sm text-white/30 py-4">No director data available.</p>
  }

  return (
    <div className="flex flex-wrap gap-2">
      {directors.map((d) => (
        <div
          key={d.director}
          className="
            glass rounded-full px-4 py-2
            flex items-center gap-2
            hover:bg-white/[0.08] transition-colors duration-200
          "
        >
          <span className="text-sm text-white/80">{d.director}</span>
          <span className="text-xs text-white/30 font-medium">
            {d.films} film{d.films !== 1 ? 's' : ''}
          </span>
        </div>
      ))}
    </div>
  )
}
