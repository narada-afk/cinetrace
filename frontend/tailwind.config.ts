import type { Config } from 'tailwindcss'

const config: Config = {
  content: [
    './pages/**/*.{js,ts,jsx,tsx,mdx}',
    './components/**/*.{js,ts,jsx,tsx,mdx}',
    './app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      colors: {
        cinema: {
          bg: '#0a0a0f',
          surface: '#13131a',
          card: '#1a1a24',
          border: 'rgba(255,255,255,0.08)',
          muted: '#6b7280',
        },
      },
      backgroundImage: {
        'gradient-red': 'linear-gradient(135deg, #e63946 0%, #c1121f 100%)',
        'gradient-purple': 'linear-gradient(135deg, #7c3aed 0%, #4c1d95 100%)',
        'gradient-orange': 'linear-gradient(135deg, #f97316 0%, #c2410c 100%)',
        'gradient-blue': 'linear-gradient(135deg, #2563eb 0%, #1e3a8a 100%)',
        'gradient-green': 'linear-gradient(135deg, #16a34a 0%, #14532d 100%)',
      },
    },
  },
  plugins: [],
}

export default config
