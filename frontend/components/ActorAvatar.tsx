'use client'

import Image from 'next/image'

interface ActorAvatarProps {
  name: string
  avatarSlug?: string | null
  size?: number
}

function getInitials(name: string): string {
  return name
    .split(' ')
    .map((w) => w[0])
    .join('')
    .slice(0, 2)
    .toUpperCase()
}

// Deterministic color from name for initials background
const COLORS = [
  'bg-red-600',
  'bg-purple-600',
  'bg-orange-500',
  'bg-blue-600',
  'bg-green-600',
  'bg-pink-600',
  'bg-indigo-600',
  'bg-teal-600',
]

function colorForName(name: string): string {
  let hash = 0
  for (let i = 0; i < name.length; i++) {
    hash = name.charCodeAt(i) + ((hash << 5) - hash)
  }
  return COLORS[Math.abs(hash) % COLORS.length]
}

export default function ActorAvatar({
  name,
  avatarSlug,
  size = 40,
}: ActorAvatarProps) {
  if (!name) return null
  const slug = avatarSlug ?? name.toLowerCase().replace(/\s+/g, '')
  const src = `/avatars/${slug}.png`

  return (
    <div
      className={`relative flex-shrink-0 rounded-full overflow-hidden border-2 border-white/10`}
      style={{ width: size, height: size }}
    >
      <Image
        src={src}
        alt={name}
        width={size}
        height={size}
        className="object-cover w-full h-full"
        onError={(e) => {
          const el = e.currentTarget as HTMLImageElement
          el.style.display = 'none'
          const parent = el.parentElement
          if (parent) {
            parent.classList.add(colorForName(name))
            parent.innerHTML = `<span style="font-size:${Math.round(size * 0.36)}px" class="text-white font-bold flex items-center justify-center w-full h-full">${getInitials(name)}</span>`
          }
        }}
      />
    </div>
  )
}
