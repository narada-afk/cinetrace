'use client'
/**
 * ShareSheet — "Share" button + platform popup modal.
 *
 * On mobile (Web Share API available):  native share sheet is opened directly.
 * On desktop (or if Web Share cancelled): custom modal shows 6 platform tiles.
 *
 * Platforms:
 *   Twitter/X  Reddit  Facebook
 *   WhatsApp   Instagram (download PNG)  Copy Link
 */

import { useState, useEffect, useRef } from 'react'
import { buildCanvas, downloadCanvas, type ShareCardData } from '@/lib/shareCard'

// ── Icons ─────────────────────────────────────────────────────────────────────

function IconShare() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"
      strokeLinecap="round" strokeLinejoin="round" width="15" height="15">
      <circle cx="18" cy="5"  r="3" />
      <circle cx="6"  cy="12" r="3" />
      <circle cx="18" cy="19" r="3" />
      <line x1="8.59"  y1="13.51" x2="15.42" y2="17.49" />
      <line x1="15.41" y1="6.51"  x2="8.59"  y2="10.49" />
    </svg>
  )
}

function IconX() {
  return (
    <svg viewBox="0 0 24 24" fill="currentColor" width="22" height="22">
      <path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-4.714-6.231-5.401 6.231H2.744l7.73-8.835L1.254 2.25H8.08l4.258 5.622L18.244 2.25zm-1.161 17.52h1.833L7.084 4.126H5.117L17.083 19.77z" />
    </svg>
  )
}

function IconReddit() {
  return (
    <svg viewBox="0 0 24 24" fill="currentColor" width="22" height="22">
      <path d="M12 0A12 12 0 0 0 0 12a12 12 0 0 0 12 12 12 12 0 0 0 12-12A12 12 0 0 0 12 0zm5.01 4.744c.688 0 1.25.561 1.25 1.249a1.25 1.25 0 0 1-2.498.056l-2.597-.547-.8 3.747c1.824.07 3.48.632 4.674 1.488.308-.309.73-.491 1.207-.491.968 0 1.754.786 1.754 1.754 0 .716-.435 1.333-1.01 1.614a3.111 3.111 0 0 1 .042.52c0 2.694-3.13 4.87-7.004 4.87-3.874 0-7.004-2.176-7.004-4.87 0-.183.015-.366.043-.534A1.748 1.748 0 0 1 4.028 12c0-.968.786-1.754 1.754-1.754.463 0 .898.196 1.207.49 1.207-.883 2.878-1.43 4.744-1.487l.885-4.182a.342.342 0 0 1 .14-.197.35.35 0 0 1 .238-.042l2.906.617a1.214 1.214 0 0 1 1.108-.701zM9.25 12C8.561 12 8 12.562 8 13.25c0 .687.561 1.248 1.25 1.248.687 0 1.248-.561 1.248-1.249 0-.688-.561-1.249-1.249-1.249zm5.5 0c-.687 0-1.248.561-1.248 1.25 0 .687.561 1.248 1.249 1.248.688 0 1.249-.561 1.249-1.249 0-.687-.562-1.249-1.25-1.249zm-5.466 3.99a.327.327 0 0 0-.231.094.33.33 0 0 0 0 .463c.842.842 2.484.913 2.961.913.477 0 2.105-.056 2.961-.913a.361.361 0 0 0 .029-.463.33.33 0 0 0-.464 0c-.547.533-1.684.73-2.512.73-.828 0-1.979-.196-2.512-.73a.326.326 0 0 0-.232-.095z" />
    </svg>
  )
}

function IconFacebook() {
  return (
    <svg viewBox="0 0 24 24" fill="currentColor" width="22" height="22">
      <path d="M24 12.073c0-6.627-5.373-12-12-12s-12 5.373-12 12c0 5.99 4.388 10.954 10.125 11.854v-8.385H7.078v-3.47h3.047V9.43c0-3.007 1.792-4.669 4.533-4.669 1.312 0 2.686.235 2.686.235v2.953H15.83c-1.491 0-1.956.925-1.956 1.874v2.25h3.328l-.532 3.47h-2.796v8.385C19.612 23.027 24 18.062 24 12.073z" />
    </svg>
  )
}

function IconWhatsApp() {
  return (
    <svg viewBox="0 0 24 24" fill="currentColor" width="22" height="22">
      <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 0 1-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 0 1-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 0 1 2.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0 0 12.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 0 0 5.683 1.448h.005c6.554 0 11.89-5.335 11.893-11.893a11.821 11.821 0 0 0-3.48-8.413Z" />
    </svg>
  )
}

function IconInstagram() {
  // Camera outline icon — signals "photo / download"
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8"
      strokeLinecap="round" strokeLinejoin="round" width="22" height="22">
      <rect x="2" y="2" width="20" height="20" rx="5" ry="5" />
      <circle cx="12" cy="12" r="4" />
      <circle cx="17.5" cy="6.5" r="1" fill="currentColor" strokeWidth="0" />
    </svg>
  )
}

function IconLink() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"
      strokeLinecap="round" strokeLinejoin="round" width="22" height="22">
      <path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71" />
      <path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71" />
    </svg>
  )
}

function IconCheck() {
  return (
    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"
      strokeLinecap="round" strokeLinejoin="round" width="22" height="22">
      <polyline points="20 6 9 17 4 12" />
    </svg>
  )
}

// ── Component ─────────────────────────────────────────────────────────────────

export default function ShareSheet(props: ShareCardData) {
  const [open,   setOpen]   = useState(false)
  const [copied, setCopied] = useState(false)
  const [dlBusy, setDlBusy] = useState(false)
  const overlayRef = useRef<HTMLDivElement>(null)

  // Scroll-lock while modal is open
  useEffect(() => {
    document.body.style.overflow = open ? 'hidden' : ''
    return () => { document.body.style.overflow = '' }
  }, [open])

  // Escape key closes modal
  useEffect(() => {
    if (!open) return
    const handler = (e: KeyboardEvent) => { if (e.key === 'Escape') setOpen(false) }
    document.addEventListener('keydown', handler)
    return () => document.removeEventListener('keydown', handler)
  }, [open])

  // ── Sharing helpers ────────────────────────────────────────────────────────

  const pageUrl = typeof window !== 'undefined' ? window.location.href : ''

  const shareText = props.winner
    ? `${props.name1} vs ${props.name2} — ${props.winner} leads in ${props.winnerLeads} of 5 metrics!`
    : `${props.name1} vs ${props.name2} — all square!`

  const tweetBody  = `${shareText}\n\nSee the full comparison:`
  const waBody     = `${shareText}\n\nSee the full comparison: ${pageUrl}`

  function openUrl(url: string) {
    window.open(url, '_blank', 'noopener,noreferrer,width=600,height=480')
  }

  async function handleShareButton() {
    if (typeof navigator !== 'undefined' && navigator.share) {
      try {
        await navigator.share({
          title: `${props.name1} vs ${props.name2} · CineTrace`,
          text:  shareText,
          url:   pageUrl,
        })
        return
      } catch {
        // User cancelled native sheet — fall through to custom popup
      }
    }
    setOpen(true)
  }

  async function copyLink() {
    try {
      await navigator.clipboard.writeText(pageUrl)
      setCopied(true)
      setTimeout(() => setCopied(false), 2200)
    } catch {
      // Clipboard not available — silently skip
    }
  }

  async function downloadForInstagram() {
    if (dlBusy) return
    setDlBusy(true)
    try {
      const canvas   = buildCanvas(props)
      const filename = `${props.name1.replace(/\s+/g, '-')}-vs-${props.name2.replace(/\s+/g, '-')}.png`
      await downloadCanvas(canvas, filename)
      setOpen(false)
    } finally {
      setDlBusy(false)
    }
  }

  // ── Platform definitions ───────────────────────────────────────────────────

  type Platform = {
    key:    string
    icon:   React.ReactNode
    label:  string
    color:  string
    glow:   string
    action: () => void
  }

  const PLATFORMS: Platform[] = [
    {
      key:    'twitter',
      icon:   <IconX />,
      label:  'Twitter / X',
      color:  '#e7e7e7',
      glow:   'rgba(231,231,231,0.2)',
      action: () => openUrl(
        `https://twitter.com/intent/tweet?text=${encodeURIComponent(tweetBody)}&url=${encodeURIComponent(pageUrl)}`
      ),
    },
    {
      key:    'reddit',
      icon:   <IconReddit />,
      label:  'Reddit',
      color:  '#FF4500',
      glow:   'rgba(255,69,0,0.35)',
      action: () => openUrl(
        `https://www.reddit.com/submit?url=${encodeURIComponent(pageUrl)}&title=${encodeURIComponent(`${props.name1} vs ${props.name2} — data comparison`)}`
      ),
    },
    {
      key:    'facebook',
      icon:   <IconFacebook />,
      label:  'Facebook',
      color:  '#1877F2',
      glow:   'rgba(24,119,242,0.35)',
      action: () => openUrl(
        `https://www.facebook.com/sharer/sharer.php?u=${encodeURIComponent(pageUrl)}`
      ),
    },
    {
      key:    'whatsapp',
      icon:   <IconWhatsApp />,
      label:  'WhatsApp',
      color:  '#25D366',
      glow:   'rgba(37,211,102,0.35)',
      action: () => openUrl(
        `https://wa.me/?text=${encodeURIComponent(waBody)}`
      ),
    },
    {
      key:    'instagram',
      icon:   <IconInstagram />,
      label:  dlBusy ? 'Downloading…' : 'Download for\nInstagram',
      color:  '#E1306C',
      glow:   'rgba(225,48,108,0.35)',
      action: downloadForInstagram,
    },
    {
      key:    'copy',
      icon:   copied ? <IconCheck /> : <IconLink />,
      label:  copied ? 'Copied!' : 'Copy Link',
      color:  copied ? '#22c55e' : 'rgba(255,255,255,0.55)',
      glow:   copied ? 'rgba(34,197,94,0.35)' : 'rgba(255,255,255,0.12)',
      action: copyLink,
    },
  ]

  // ── Render ─────────────────────────────────────────────────────────────────

  return (
    <>
      <style>{`
        @keyframes sheetIn {
          from { opacity: 0; transform: scale(0.95) translateY(10px); }
          to   { opacity: 1; transform: scale(1)    translateY(0);    }
        }
      `}</style>

      {/* ── Trigger button ──────────────────────────────────────── */}
      <button
        onClick={handleShareButton}
        className="inline-flex items-center gap-2 px-6 py-3 rounded-full font-semibold text-sm
          border border-white/10 text-white/80
          hover:text-white hover:border-white/25 hover:bg-white/6
          transition-all duration-200 active:scale-95"
        style={{ background: 'rgba(255,255,255,0.05)' }}
      >
        <IconShare />
        Share
      </button>

      {/* ── Modal overlay ───────────────────────────────────────── */}
      {open && (
        <div
          ref={overlayRef}
          className="fixed inset-0 z-50 flex items-center justify-center p-4"
          style={{ background: 'rgba(0,0,0,0.78)', backdropFilter: 'blur(10px)' }}
          onClick={(e) => { if (e.target === overlayRef.current) setOpen(false) }}
        >
          <div
            className="relative w-full max-w-[340px] rounded-3xl p-6"
            style={{
              background:  'linear-gradient(155deg, rgba(16,16,28,0.99), rgba(22,22,38,0.99))',
              border:      '1px solid rgba(255,255,255,0.09)',
              boxShadow:   '0 32px 80px rgba(0,0,0,0.65), inset 0 1px 0 rgba(255,255,255,0.05)',
              animation:   'sheetIn 180ms cubic-bezier(0.34,1.56,0.64,1) both',
            }}
          >
            {/* Close button */}
            <button
              onClick={() => setOpen(false)}
              className="absolute top-4 right-4 w-8 h-8 rounded-full flex items-center justify-center
                text-white/30 hover:text-white/70 hover:bg-white/8 transition-all duration-150 text-sm"
              aria-label="Close"
            >
              ✕
            </button>

            {/* Title */}
            <p className="text-sm font-semibold text-white/80 text-center mb-5 pr-6">
              Share this comparison
            </p>

            {/* Comparison sub-label */}
            <p className="text-[11px] text-white/30 text-center mb-5 leading-snug">
              {props.name1} vs {props.name2}
              {props.winner && (
                <span> · <span style={{ color: props.winner === props.name1 ? '#f59e0b' : '#06b6d4' }}>
                  {props.winner} leads
                </span></span>
              )}
            </p>

            {/* 3×2 platform grid */}
            <div className="grid grid-cols-3 gap-2.5">
              {PLATFORMS.map((p) => (
                <PlatformTile key={p.key} platform={p} />
              ))}
            </div>
          </div>
        </div>
      )}
    </>
  )
}

// ── Platform tile ─────────────────────────────────────────────────────────────

type Platform = {
  key:    string
  icon:   React.ReactNode
  label:  string
  color:  string
  glow:   string
  action: () => void
}

function PlatformTile({ platform: p }: { platform: Platform }) {
  const [hov, setHov] = useState(false)

  return (
    <button
      onClick={p.action}
      onMouseEnter={() => setHov(true)}
      onMouseLeave={() => setHov(false)}
      className="flex flex-col items-center gap-2 py-3.5 px-2 rounded-2xl
        transition-all duration-150 active:scale-95"
      style={{
        background:  hov ? 'rgba(255,255,255,0.07)' : 'rgba(255,255,255,0.04)',
        border:      `1px solid ${hov ? `${p.color}55` : 'rgba(255,255,255,0.07)'}`,
        boxShadow:   hov ? `0 0 18px ${p.glow}` : 'none',
        transform:   hov ? 'scale(1.05)' : 'scale(1)',
        color:       p.color,
      }}
    >
      {p.icon}
      <span
        className="text-[9.5px] font-medium text-center leading-tight whitespace-pre-line"
        style={{ color: 'rgba(255,255,255,0.55)' }}
      >
        {p.label}
      </span>
    </button>
  )
}
