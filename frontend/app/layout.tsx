import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'South Cinema Analytics',
  description: 'A cinema curiosity engine for South Indian films',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body className="bg-[#0a0a0f] text-white min-h-screen antialiased">
        {children}
      </body>
    </html>
  )
}
