/**
 * MilkyWayBand
 * ─────────────────────────────────────────────────────────────
 * Four layered CSS gradient divs simulate a faint galactic band:
 *   1. Core band  — thin diagonal blue-white streak (the spine)
 *   2. Warm dust  — orange/gold cloud, lower-left (galactic core side)
 *   3. Cool haze  — purple/blue cloud, upper-right (outer arm side)
 *   4. Density accent — warm ellipse thickening the band's midsection
 *
 * Zero JS, zero images, SSR-safe. Total opacity: ~8–12% of screen.
 * blend-mode: screen ensures it integrates cleanly on any dark surface.
 */

export default function MilkyWayBand() {
  return (
    <div
      aria-hidden="true"
      className="fixed inset-0 pointer-events-none overflow-hidden"
      style={{ zIndex: 0 }}
    >
      {/* ── 1. Core band — diagonal blue-white spine ───────────────────────── */}
      <div
        style={{
          position: 'absolute',
          inset: '-50%',
          background: `
            linear-gradient(
              0deg,
              transparent          0%,
              transparent         28%,
              rgba(110,145,255,0.11) 38%,
              rgba(175,198,255,0.18) 46%,
              rgba(210,225,255,0.16) 50%,
              rgba(165,188,255,0.14) 54%,
              rgba(100,138,240,0.09) 62%,
              transparent         72%,
              transparent        100%
            )
          `,
          transform: 'rotate(-34deg)',
          filter: 'blur(38px)',
          mixBlendMode: 'screen',
        }}
      />

      {/* ── 2. Warm dust cloud — lower-left, galactic-core warmth ─────────── */}
      <div
        style={{
          position: 'absolute',
          width: '80%',
          height: '60%',
          left: '-8%',
          top: '40%',
          background: `
            radial-gradient(
              ellipse 62% 52% at 42% 58%,
              rgba(255,138,48,0.14) 0%,
              rgba(230,105,32,0.07) 42%,
              transparent          75%
            )
          `,
          filter: 'blur(52px)',
          mixBlendMode: 'screen',
        }}
      />

      {/* ── 3. Cool nebula haze — upper-right, outer spiral arm ───────────── */}
      <div
        style={{
          position: 'absolute',
          width: '70%',
          height: '60%',
          right: '-6%',
          top: '-4%',
          background: `
            radial-gradient(
              ellipse 58% 62% at 62% 38%,
              rgba(100,72,240,0.12) 0%,
              rgba(118,88,240,0.06) 45%,
              transparent          75%
            )
          `,
          filter: 'blur(55px)',
          mixBlendMode: 'screen',
        }}
      />

      {/* ── 4. Density accent — warm ellipse fattening the band's midsection ── */}
      <div
        style={{
          position: 'absolute',
          inset: '-50%',
          background: `
            radial-gradient(
              ellipse 75% 14% at 38% 58%,
              rgba(255,200,90,0.09) 0%,
              transparent          68%
            )
          `,
          transform: 'rotate(-34deg)',
          filter: 'blur(48px)',
          mixBlendMode: 'screen',
        }}
      />

      {/* ── 5. Subtle second warm streak — offset for organic width variation ── */}
      <div
        style={{
          position: 'absolute',
          inset: '-50%',
          background: `
            linear-gradient(
              0deg,
              transparent          0%,
              transparent         40%,
              rgba(255,170,80,0.06) 52%,
              rgba(255,150,60,0.08) 56%,
              rgba(200,120,50,0.04) 62%,
              transparent         70%,
              transparent        100%
            )
          `,
          transform: 'rotate(-34deg) translateY(-3%)',
          filter: 'blur(44px)',
          mixBlendMode: 'screen',
        }}
      />
    </div>
  )
}
