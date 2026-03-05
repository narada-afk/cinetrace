export const metadata = {
  title: 'South Cinema Analytics',
  description: 'Compare South Indian actors side by side',
}

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body style={{ fontFamily: 'sans-serif', backgroundColor: '#f5f5f5', margin: 0 }}>
        {children}
      </body>
    </html>
  )
}
