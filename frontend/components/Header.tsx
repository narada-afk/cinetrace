import Image from 'next/image'
import Link from 'next/link'

export default function Header() {
  return (
    <header className="w-full px-6 py-4 max-w-[1200px] mx-auto">
      <Link href="/" className="inline-flex flex-shrink-0 group" aria-label="Home">
        <div
          className="
            w-[52px] h-[52px] rounded-full overflow-hidden
            bg-white/[0.06] border border-white/[0.12]
            transition-all duration-300
            group-hover:scale-105
            group-hover:border-white/25
            group-hover:shadow-[0_0_18px_4px_rgba(99,130,255,0.22),0_0_6px_1px_rgba(99,130,255,0.15)]
          "
        >
          <Image
            src="/narada.png"
            alt="South Cinema Analytics"
            width={52}
            height={52}
            className="object-cover w-full h-full scale-110"
            priority
          />
        </div>
      </Link>
    </header>
  )
}
