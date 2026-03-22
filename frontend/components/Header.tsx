import Image from 'next/image'
import Link from 'next/link'

export default function Header() {
  return (
    <header className="w-full px-6 py-4 max-w-[1200px] mx-auto">
      {/* Narada avatar — links to homepage */}
      <Link href="/" className="inline-flex flex-shrink-0">
        <div className="w-11 h-11 rounded-full overflow-hidden border border-white/10 hover:scale-105 transition-transform duration-200">
          <Image
            src="/narada.jpeg"
            alt="South Cinema Analytics"
            width={44}
            height={44}
            className="object-cover w-full h-full"
            style={{ objectPosition: '50% 25%' }}
            priority
          />
        </div>
      </Link>
    </header>
  )
}
