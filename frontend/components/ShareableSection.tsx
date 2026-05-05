'use client'

/**
 * ShareableSection — wraps any compare-page section with a screenshot share button.
 *
 * Replaces the inline <SectionLabel> + <section> pair.
 * A small "Share" button sits right-aligned in the label row.
 * On click it:
 *   1. Lazy-loads html2canvas from unpkg CDN (no npm install needed).
 *   2. Screenshots the section content div.
 *   3. On mobile: tries navigator.share with the PNG file (native sheet).
 *   4. Falls back to a modal with thumbnail preview + Download / Twitter / WhatsApp / Copy Link.
 */

import { useRef, useState } from 'react'

// ── CDN loader for html2canvas ─────────────────────────────────────────────────
// Avoids adding html2canvas to package.json (which would break npm ci without
// regenerating the lock file). The script is injected once and cached by the browser.

type Html2CanvasFn = (el: HTMLElement, opts?: Record<string, unknown>) => Promise<HTMLCanvasElement>

declare global {
  interface Window { html2canvas?: Html2CanvasFn }
}

function loadHtml2Canvas(): Promise<Html2CanvasFn> {
  if (typeof window === 'undefined') return Promise.reject('SSR')
  if (window.html2canvas) return Promise.resolve(window.html2canvas)

  return new Promise((resolve, reject) => {
    const script = document.createElement('script')
    script.src = 'https://unpkg.com/html2canvas@1.4.1/dist/html2canvas.min.js'
    script.onload  = () => window.html2canvas ? resolve(window.html2canvas) : reject('not found')
    script.onerror = () => reject('failed to load html2canvas')
    document.head.appendChild(script)
  })
}

export default function ShareableSection({
  children,
  label,
  name1,
  name2,
  subtitle,
  className,
}: {
  children: React.ReactNode
  label: string
  /** Optional — used on compare page for "Actor1 vs Actor2" share text */
  name1?: string
  name2?: string
  /** Optional subtitle shown in the share modal when name1/name2 not provided */
  subtitle?: string
  /** Extra classes on the outer <section> element (e.g. spacing) */
  className?: string
}) {
  const contentRef = useRef<HTMLDivElement>(null)
  const [capturing, setCapturing] = useState(false)
  const [open, setOpen]           = useState(false)
  const [imgUrl, setImgUrl]       = useState<string | null>(null)
  const [copied, setCopied]       = useState(false)

  const pageUrl    = typeof window !== 'undefined' ? window.location.href : ''
  const shareText  = name1 && name2
    ? `${name1} vs ${name2} on CineTrace`
    : subtitle
    ? `${subtitle} — cinetrace.in`
    : `${label} — cinetrace.in`
  const modalSub   = name1 && name2 ? `${name1} vs ${name2}` : (subtitle ?? label)
  const filename   = (name1 && name2 ? `${name1}-vs-${name2}` : (subtitle ?? label))
    .replace(/[^a-z0-9]/gi, '-').toLowerCase().replace(/--+/g, '-') + '.png'

  // ── Capture ──────────────────────────────────────────────────────────────────

  async function captureAndShare() {
    if (!contentRef.current || capturing) return
    setCapturing(true)
    try {
      // Lazy-load html2canvas from CDN (injected once, cached by browser)
      const html2canvas = await loadHtml2Canvas()
      const canvas = await html2canvas(contentRef.current, {
        backgroundColor: '#0a0a0f',
        useCORS: true,      // needed for TMDB poster images
        allowTaint: false,
        scale: 2,
        logging: false,
      })

      const dataUrl = canvas.toDataURL('image/png')

      // Try native share with PNG file on mobile
      const blob = await new Promise<Blob | null>(res => canvas.toBlob(res, 'image/png'))
      if (blob) {
        const file = new File([blob], filename, { type: 'image/png' })
        if (typeof navigator !== 'undefined' && navigator.canShare?.({ files: [file] })) {
          try {
            await navigator.share({ files: [file], title: shareText, url: pageUrl })
            return // native sheet handled it
          } catch {
            // user cancelled — fall through to custom modal
          }
        }
      }

      setImgUrl(dataUrl)
      setOpen(true)
    } catch (err) {
      console.error('[ShareableSection] screenshot failed:', err)
    } finally {
      setCapturing(false)
    }
  }

  // ── Actions ───────────────────────────────────────────────────────────────────

  function downloadPng() {
    if (!imgUrl) return
    const a = document.createElement('a')
    a.href     = imgUrl
    a.download = filename
    a.click()
  }

  async function copyLink() {
    try {
      await navigator.clipboard.writeText(pageUrl)
      setCopied(true)
      setTimeout(() => setCopied(false), 2200)
    } catch { /* clipboard unavailable */ }
  }

  const tweetUrl = `https://twitter.com/intent/tweet?text=${encodeURIComponent(shareText + '\n')}&url=${encodeURIComponent(pageUrl)}`
  const waUrl    = `https://wa.me/?text=${encodeURIComponent(shareText + '\n' + pageUrl)}`

  // ── Render ────────────────────────────────────────────────────────────────────

  return (
    <section className={className}>

      {/* ── Section label row with share button ── */}
      <div className="flex items-center justify-between mb-4">
        <p className="text-[10px] font-bold text-white/25 uppercase tracking-[0.2em]">
          {label}
        </p>

        <button
          onClick={captureAndShare}
          disabled={capturing}
          aria-label={`Share ${label} section`}
          className="flex items-center gap-1.5 text-white/25 hover:text-white/55 transition-colors
            text-[11px] px-2.5 py-1 rounded-full hover:bg-white/[0.05] disabled:opacity-50"
        >
          {capturing ? (
            /* Loading dots */
            <span className="flex gap-[3px] items-center h-[13px]">
              {[0, 1, 2].map(i => (
                <span
                  key={i}
                  className="w-[3px] h-[3px] rounded-full bg-current animate-bounce"
                  style={{ animationDelay: `${i * 0.15}s` }}
                />
              ))}
            </span>
          ) : (
            <>
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"
                strokeLinecap="round" strokeLinejoin="round" width="13" height="13">
                <circle cx="18" cy="5"  r="3" />
                <circle cx="6"  cy="12" r="3" />
                <circle cx="18" cy="19" r="3" />
                <line x1="8.59"  y1="13.51" x2="15.42" y2="17.49" />
                <line x1="15.41" y1="6.51"  x2="8.59"  y2="10.49" />
              </svg>
              <span>Share</span>
            </>
          )}
        </button>
      </div>

      {/* ── Section content — screenshotted area ── */}
      <div ref={contentRef}>
        {children}
      </div>

      {/* ── Share modal ── */}
      {open && imgUrl && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center p-4"
          style={{ background: 'rgba(0,0,0,0.78)', backdropFilter: 'blur(10px)' }}
          onClick={e => { if (e.target === e.currentTarget) setOpen(false) }}
        >
          <style>{`
            @keyframes sectionSheetIn {
              from { opacity: 0; transform: scale(0.95) translateY(10px); }
              to   { opacity: 1; transform: scale(1)    translateY(0);    }
            }
          `}</style>

          <div
            className="relative w-full max-w-[360px] rounded-3xl p-5 flex flex-col gap-4"
            style={{
              background:  'linear-gradient(155deg, rgba(16,16,28,0.99), rgba(22,22,38,0.99))',
              border:      '1px solid rgba(255,255,255,0.09)',
              boxShadow:   '0 32px 80px rgba(0,0,0,0.65), inset 0 1px 0 rgba(255,255,255,0.05)',
              animation:   'sectionSheetIn 180ms cubic-bezier(0.34,1.56,0.64,1) both',
            }}
          >
            {/* Close */}
            <button
              onClick={() => setOpen(false)}
              className="absolute top-4 right-4 w-8 h-8 rounded-full flex items-center justify-center
                text-white/30 hover:text-white/70 hover:bg-white/[0.08] transition-all text-sm"
              aria-label="Close"
            >✕</button>

            {/* Title */}
            <p className="text-sm font-semibold text-white/80 text-center pr-6">
              Share snapshot
            </p>
            <p className="text-[11px] text-white/30 text-center -mt-2">
              {modalSub}
            </p>

            {/* Preview thumbnail */}
            <div className="rounded-xl overflow-hidden border border-white/[0.07] max-h-48 flex items-center justify-center bg-[#0a0a0f]">
              {/* eslint-disable-next-line @next/next/no-img-element */}
              <img src={imgUrl} alt="Section preview" className="w-full h-auto object-contain" />
            </div>

            {/* Download button */}
            <button
              onClick={downloadPng}
              className="w-full py-3 rounded-2xl text-sm font-semibold text-white
                transition-all duration-150 hover:opacity-90 active:scale-[0.98]"
              style={{ background: 'linear-gradient(135deg, rgba(139,92,246,0.85), rgba(99,102,241,0.85))' }}
            >
              ↓ Save as PNG
            </button>

            {/* Platform row */}
            <div className="grid grid-cols-3 gap-2">

              {/* Twitter / X */}
              <a
                href={tweetUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="flex flex-col items-center gap-2 py-3 rounded-2xl
                  border border-white/[0.07] hover:bg-white/[0.07] transition-colors"
              >
                <svg viewBox="0 0 24 24" fill="currentColor" width="18" height="18" style={{ color: '#e7e7e7' }}>
                  <path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-4.714-6.231-5.401 6.231H2.744l7.73-8.835L1.254 2.25H8.08l4.258 5.622L18.244 2.25zm-1.161 17.52h1.833L7.084 4.126H5.117L17.083 19.77z" />
                </svg>
                <span className="text-[9.5px] text-white/40">Twitter</span>
              </a>

              {/* WhatsApp */}
              <a
                href={waUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="flex flex-col items-center gap-2 py-3 rounded-2xl
                  border border-white/[0.07] hover:bg-white/[0.07] transition-colors"
              >
                <svg viewBox="0 0 24 24" fill="currentColor" width="18" height="18" style={{ color: '#25D366' }}>
                  <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 0 1-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 0 1-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 0 1 2.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0 0 12.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 0 0 5.683 1.448h.005c6.554 0 11.89-5.335 11.893-11.893a11.821 11.821 0 0 0-3.48-8.413Z" />
                </svg>
                <span className="text-[9.5px] text-white/40">WhatsApp</span>
              </a>

              {/* Copy Link */}
              <button
                onClick={copyLink}
                className="flex flex-col items-center gap-2 py-3 rounded-2xl
                  border border-white/[0.07] hover:bg-white/[0.07] transition-colors"
              >
                {copied ? (
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"
                    strokeLinecap="round" strokeLinejoin="round" width="18" height="18"
                    style={{ color: '#22c55e' }}>
                    <polyline points="20 6 9 17 4 12" />
                  </svg>
                ) : (
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"
                    strokeLinecap="round" strokeLinejoin="round" width="18" height="18"
                    style={{ color: 'rgba(255,255,255,0.5)' }}>
                    <path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71" />
                    <path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71" />
                  </svg>
                )}
                <span className="text-[9.5px]" style={{ color: copied ? '#22c55e' : 'rgba(255,255,255,0.4)' }}>
                  {copied ? 'Copied!' : 'Copy Link'}
                </span>
              </button>

            </div>
          </div>
        </div>
      )}

    </section>
  )
}
