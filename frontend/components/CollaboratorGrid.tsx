import Link from 'next/link'
import ActorAvatar from './ActorAvatar'
import type { Collaborator } from '@/lib/api'

interface CollaboratorGridProps {
  collaborators: Collaborator[]
  // Map of name → id for linking (populated from /actors list)
  actorIdMap: Record<string, number>
}

export default function CollaboratorGrid({
  collaborators,
  actorIdMap,
}: CollaboratorGridProps) {
  if (!collaborators.length) {
    return <EmptyState message="No collaborator data available." />
  }

  const top = collaborators.slice(0, 8)

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
      {top.map((collab) => {
        const actorId = actorIdMap[collab.actor]
        const inner = (
          <div
            className="
              glass rounded-xl p-4 flex flex-col items-center gap-3 text-center
              hover:bg-white/[0.08] hover:scale-[1.02]
              transition-all duration-200 cursor-pointer
            "
          >
            <ActorAvatar name={collab.actor} size={52} />
            <div className="flex flex-col gap-0.5">
              <span className="text-sm font-medium text-white/90 leading-snug">
                {collab.actor}
              </span>
              <span className="text-xs text-white/40">
                {collab.films} film{collab.films !== 1 ? 's' : ''} together
              </span>
            </div>
          </div>
        )

        return actorId ? (
          <Link key={collab.actor} href={`/actors/${actorId}`}>
            {inner}
          </Link>
        ) : (
          <div key={collab.actor}>{inner}</div>
        )
      })}
    </div>
  )
}

function EmptyState({ message }: { message: string }) {
  return (
    <p className="text-sm text-white/30 py-4">{message}</p>
  )
}
