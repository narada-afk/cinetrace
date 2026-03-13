'use client'

/**
 * CinemaUniverse v2 — Force-Directed Collaboration Graph
 *
 * Improvements over v1:
 *  - Industry cluster layout  → Tamil ↖ · Telugu ↙ · Malayalam ↗ · Kannada ↘
 *  - √ node sizing            → costar_count on a sqrt scale (no giant nodes)
 *  - Edge opacity 0.08        → very subtle at rest, brightens on hover
 *  - Hover                    → highlight actor + direct co-stars, fade everything else
 *  - Click zoom               → viewport scales to fit the actor's neighborhood
 *  - Initial view             → top-80 actors, toggle to show all
 */

import { useEffect, useRef, useState, useCallback, useMemo } from 'react'
import { type CinemaUniverse as UniverseData, type UniverseNode, type UniverseEdge } from '@/lib/api'

// ── Colors ─────────────────────────────────────────────────────────────────────

const IND_COLOR: Record<string, string> = {
  Tamil:     '#f43f5e',
  Telugu:    '#f59e0b',
  Malayalam: '#06b6d4',
  Kannada:   '#8b5cf6',
  Unknown:   '#6b7280',
}

// ── Cluster centers (0–1 normalised) ──────────────────────────────────────────

const CLUSTER_NX: Record<string, number> = {
  Tamil: 0.24, Telugu: 0.24, Malayalam: 0.76, Kannada: 0.76, Unknown: 0.50,
}
const CLUSTER_NY: Record<string, number> = {
  Tamil: 0.28, Telugu: 0.74, Malayalam: 0.28, Kannada: 0.74, Unknown: 0.50,
}

const INITIAL_LIMIT = 80

// ── Simulation ─────────────────────────────────────────────────────────────────

interface SimNode extends UniverseNode {
  x: number; y: number
}

// ── Force parameters ──────────────────────────────────────────────────────────
const NODE_REPULSION = 10000  // Coulomb constant — how hard nodes push apart
const LINK_DIST      = 150    // preferred edge length in pixels
const LINK_STR       = 0.06   // spring stiffness (weak; many edges → keep low)
const CLUSTER_STR    = 0.020  // industry cluster pull strength

function runForceLayout(
  nodes: SimNode[],
  edges: UniverseEdge[],
  W: number,
  H: number,
  iters = 2000,
) {
  const N = nodes.length
  if (N === 0) return

  const idx: Record<number, number> = {}
  nodes.forEach((n, i) => { idx[n.id] = i })

  // Larger initial max-displacement so nodes can travel further before cooling
  const maxD = Math.min(W, H) / 5
  const fdx  = new Float64Array(N)
  const fdy  = new Float64Array(N)

  for (let it = 0; it < iters; it++) {
    // Power-1.5 cooling: fast at first, slow fine-tuning at the tail
    const temp = maxD * Math.pow(Math.max(0, 1 - it / iters), 1.5)
    // Cluster pull: strong early (holds industry groups apart), eases to a floor
    const clF = CLUSTER_STR * Math.max(0.35, 1 - (it / iters) * 0.65)
    fdx.fill(0); fdy.fill(0)

    // ── Repulsion: F = NODE_REPULSION / d  (Coulomb 1/d, not 1/d²) ──────────
    for (let i = 0; i < N; i++) {
      for (let j = i + 1; j < N; j++) {
        const ex = nodes[j].x - nodes[i].x
        const ey = nodes[j].y - nodes[i].y
        const d  = Math.sqrt(ex * ex + ey * ey) || 0.1
        const f  = NODE_REPULSION / d
        fdx[i] -= f * ex / d;  fdy[i] -= f * ey / d
        fdx[j] += f * ex / d;  fdy[j] += f * ey / d
      }
    }

    // ── Link spring: pulls toward LINK_DIST (attractive only when d > LINK_DIST)
    for (const e of edges) {
      const si = idx[e.source], ti = idx[e.target]
      if (si == null || ti == null) continue
      const ex = nodes[ti].x - nodes[si].x
      const ey = nodes[ti].y - nodes[si].y
      const d  = Math.sqrt(ex * ex + ey * ey) || 0.1
      if (d <= LINK_DIST) continue                 // already within ideal range
      const f  = LINK_STR * (d - LINK_DIST)
      fdx[si] += f * ex / d;  fdy[si] += f * ey / d
      fdx[ti] -= f * ex / d;  fdy[ti] -= f * ey / d
    }

    // ── Apply displacement + industry cluster pull + boundary clamp ───────────
    for (let i = 0; i < N; i++) {
      const disp = Math.sqrt(fdx[i] * fdx[i] + fdy[i] * fdy[i]) || 0.1
      nodes[i].x += (fdx[i] / disp) * Math.min(disp, temp)
      nodes[i].y += (fdy[i] / disp) * Math.min(disp, temp)

      const ind = CLUSTER_NX[nodes[i].industry] !== undefined ? nodes[i].industry : 'Unknown'
      nodes[i].x += (CLUSTER_NX[ind] * W - nodes[i].x) * clF
      nodes[i].y += (CLUSTER_NY[ind] * H - nodes[i].y) * clF

      nodes[i].x = Math.max(22, Math.min(W - 22, nodes[i].x))
      nodes[i].y = Math.max(22, Math.min(H - 22, nodes[i].y))
    }
  }
}

// ── Zoom transform ─────────────────────────────────────────────────────────────

interface ZoomT { scale: number; tx: number; ty: number }

function zoomForNodes(nodes: SimNode[], nodeIds: Set<number>, W: number, H: number): ZoomT | null {
  const vis = nodes.filter(n => nodeIds.has(n.id))
  if (!vis.length) return null

  if (vis.length === 1) {
    const scale = 2.5
    return { scale, tx: W / 2 - vis[0].x * scale, ty: H / 2 - vis[0].y * scale }
  }

  const pad  = 72
  const xs   = vis.map(n => n.x)
  const ys   = vis.map(n => n.y)
  const minX = Math.min(...xs) - pad, maxX = Math.max(...xs) + pad
  const minY = Math.min(...ys) - pad, maxY = Math.max(...ys) + pad
  const scale = Math.min(W / (maxX - minX), H / (maxY - minY), 2.8)
  return {
    scale,
    tx: W / 2 - ((minX + maxX) / 2) * scale,
    ty: H / 2 - ((minY + maxY) / 2) * scale,
  }
}

// ── Canvas renderer ────────────────────────────────────────────────────────────

function drawGraph(
  ctx: CanvasRenderingContext2D,
  nodes: SimNode[],
  edges: UniverseEdge[],
  idx: Record<number, number>,
  W: number,
  H: number,
  hovered: number | null,
  zoomed: { actorId: number; nodeIds: Set<number> } | null,
  dpr: number,
) {
  ctx.clearRect(0, 0, W * dpr, H * dpr)
  ctx.save()
  ctx.scale(dpr, dpr)

  const maxCS = Math.max(...nodes.map(n => n.costar_count), 1)
  const maxWt = Math.max(...edges.map(e => e.weight), 1)
  const nodeR = (n: SimNode) => 3 + Math.sqrt(n.costar_count / maxCS) * 13

  const focusId = zoomed?.actorId ?? hovered
  const visIds  = zoomed?.nodeIds ?? null

  // Neighbour set of focused node
  const nbrs = new Set<number>()
  if (focusId != null) {
    for (const e of edges) {
      if (e.source === focusId) nbrs.add(e.target)
      if (e.target === focusId) nbrs.add(e.source)
    }
  }

  // Apply zoom transform
  if (zoomed) {
    const zt = zoomForNodes(nodes, zoomed.nodeIds, W, H)
    if (zt) ctx.transform(zt.scale, 0, 0, zt.scale, zt.tx, zt.ty)
  }

  // ── Edges ──────────────────────────────────────────────────────────────────
  for (const e of edges) {
    if (visIds && !(visIds.has(e.source) && visIds.has(e.target))) continue
    const si = idx[e.source], ti = idx[e.target]
    if (si == null || ti == null) continue
    const sn = nodes[si], tn = nodes[ti]

    const isConn = focusId != null && (e.source === focusId || e.target === focusId)
    const alpha = focusId == null
      ? 0.05 + (e.weight / maxWt) * 0.07      // at rest: ~0.08 avg, very subtle
      : isConn
        ? 0.55 + (e.weight / maxWt) * 0.35    // focused: vivid
        : 0.012                                 // non-connected: nearly invisible

    ctx.beginPath()
    ctx.moveTo(sn.x, sn.y)
    ctx.lineTo(tn.x, tn.y)
    ctx.strokeStyle = `rgba(255,255,255,${alpha})`
    ctx.lineWidth   = 0.4 + (e.weight / maxWt) * 2.0
    ctx.stroke()
  }

  // ── Nodes ──────────────────────────────────────────────────────────────────
  for (const n of nodes) {
    if (visIds && !visIds.has(n.id)) continue

    const r       = nodeR(n)
    const color   = IND_COLOR[n.industry] ?? IND_COLOR.Unknown
    const isFocus = n.id === focusId
    const isNbr   = nbrs.has(n.id)
    const dimmed  = focusId != null && !isFocus && !isNbr

    ctx.globalAlpha = dimmed ? 0.10 : 1

    if (isFocus) { ctx.shadowColor = color; ctx.shadowBlur = 22 }

    ctx.beginPath()
    ctx.arc(n.x, n.y, r, 0, Math.PI * 2)
    ctx.fillStyle = color
    ctx.fill()

    if (isFocus) {
      ctx.strokeStyle = 'rgba(255,255,255,0.9)'
      ctx.lineWidth   = 2
      ctx.stroke()
      ctx.shadowBlur  = 0
    }

    ctx.globalAlpha = 1

    // Show label for focused, neighbours (when in focus mode), or large nodes
    const showLabel = isFocus || (isNbr && focusId != null) || r > 7.5
    if (showLabel && !dimmed) {
      ctx.globalAlpha = isFocus ? 1 : r > 8 ? 0.85 : 0.65
      ctx.font        = `${isFocus ? 600 : 400} ${Math.max(9, Math.min(r * 0.85, 13))}px Inter,sans-serif`
      ctx.fillStyle   = 'white'
      ctx.textAlign   = 'center'
      ctx.fillText(n.name.split(' ')[0], n.x, n.y + r + 11)
      ctx.globalAlpha = 1
    }
  }

  ctx.restore()
}

// ── Tooltip ───────────────────────────────────────────────────────────────────

function Tooltip({ node, x, y }: { node: UniverseNode; x: number; y: number }) {
  return (
    <div
      className="absolute pointer-events-none z-20 px-3 py-2 rounded-xl text-xs text-white shadow-xl"
      style={{
        left: Math.min(x + 14, window.innerWidth - 200),
        top:  Math.max(y - 56, 8),
        background: 'rgba(15,15,28,0.95)',
        border: `1px solid ${IND_COLOR[node.industry] ?? '#444'}55`,
      }}
    >
      <div className="font-semibold">{node.name}</div>
      <div className="flex gap-3 mt-0.5 text-white/50">
        <span>{node.industry}</span>
        <span>{node.film_count} films</span>
        <span>{node.costar_count} co-stars</span>
      </div>
    </div>
  )
}

// ── Legend ────────────────────────────────────────────────────────────────────

function GraphLegend() {
  return (
    <div className="flex flex-wrap gap-4 text-xs text-white/50 mt-3">
      {Object.entries(IND_COLOR).filter(([k]) => k !== 'Unknown').map(([ind, col]) => (
        <div key={ind} className="flex items-center gap-1.5">
          <div className="w-2.5 h-2.5 rounded-full" style={{ background: col }} />
          {ind}
        </div>
      ))}
      <div className="ml-auto text-white/30">Node size = √collaborators · Click to zoom into neighborhood</div>
    </div>
  )
}

// ── Cluster zone corner labels ─────────────────────────────────────────────────

const ZONE_LABELS = [
  { ind: 'Tamil',     cls: 'top-3 left-3'    },
  { ind: 'Telugu',    cls: 'bottom-3 left-3'  },
  { ind: 'Malayalam', cls: 'top-3 right-3'   },
  { ind: 'Kannada',   cls: 'bottom-3 right-3' },
]

// ── Main component ─────────────────────────────────────────────────────────────

interface ZoomedState { actorId: number; nodeIds: Set<number> }

export default function CinemaUniverse({ data }: { data: UniverseData }) {
  const canvasRef    = useRef<HTMLCanvasElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)

  const [simNodes,   setSimNodes]   = useState<SimNode[] | null>(null)
  const [idxMap,     setIdxMap]     = useState<Record<number, number>>({})
  const [hovered,    setHovered]    = useState<number | null>(null)
  const [zoomed,     setZoomed]     = useState<ZoomedState | null>(null)
  const [tooltip,    setTooltip]    = useState<{ node: UniverseNode; x: number; y: number } | null>(null)
  const [simulating, setSimulating] = useState(true)
  const [showAll,    setShowAll]    = useState(false)

  const W = 900, H = 560

  // ── Visible subset (top-80 or all) ─────────────────────────────────────────

  const visibleNodes = useMemo(() => {
    const sorted = [...data.nodes].sort((a, b) => b.costar_count - a.costar_count)
    return showAll ? sorted : sorted.slice(0, INITIAL_LIMIT)
  }, [data.nodes, showAll])

  const visibleNodeIds = useMemo(
    () => new Set(visibleNodes.map(n => n.id)),
    [visibleNodes],
  )

  const visibleEdges = useMemo(
    () => data.edges.filter(e => visibleNodeIds.has(e.source) && visibleNodeIds.has(e.target)),
    [data.edges, visibleNodeIds],
  )

  // ── Force simulation ────────────────────────────────────────────────────────

  useEffect(() => {
    if (!visibleNodes.length) return
    setSimulating(true)
    setZoomed(null)
    setHovered(null)

    setTimeout(() => {
      const sn: SimNode[] = visibleNodes.map(n => {
        const ind    = CLUSTER_NX[n.industry] !== undefined ? n.industry : 'Unknown'
        const jitter = Math.min(W, H) * 0.10
        return {
          ...n,
          x: CLUSTER_NX[ind] * W + (Math.random() - 0.5) * jitter,
          y: CLUSTER_NY[ind] * H + (Math.random() - 0.5) * jitter,
        }
      })
      runForceLayout(sn, visibleEdges, W, H, 300)

      const im: Record<number, number> = {}
      sn.forEach((n, i) => { im[n.id] = i })
      setSimNodes(sn)
      setIdxMap(im)
      setSimulating(false)
    }, 50)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [visibleNodes, visibleEdges])

  // ── Redraw ──────────────────────────────────────────────────────────────────

  useEffect(() => {
    if (!simNodes || !canvasRef.current) return
    const canvas = canvasRef.current
    const ctx    = canvas.getContext('2d')
    if (!ctx) return
    const dpr           = window.devicePixelRatio || 1
    canvas.width        = W * dpr
    canvas.height       = H * dpr
    canvas.style.width  = W + 'px'
    canvas.style.height = H + 'px'
    drawGraph(ctx, simNodes, visibleEdges, idxMap, W, H, hovered, zoomed, dpr)
  }, [simNodes, visibleEdges, idxMap, hovered, zoomed])

  // ── Hit detection (zoom-aware) ──────────────────────────────────────────────

  const findNode = useCallback((ex: number, ey: number): SimNode | null => {
    if (!simNodes || !canvasRef.current) return null
    const rect = canvasRef.current.getBoundingClientRect()
    let mx = (ex - rect.left) * (W / rect.width)
    let my = (ey - rect.top)  * (H / rect.height)

    if (zoomed) {
      const zt = zoomForNodes(simNodes, zoomed.nodeIds, W, H)
      if (zt) { mx = (mx - zt.tx) / zt.scale; my = (my - zt.ty) / zt.scale }
    }

    const maxCS = Math.max(...simNodes.map(n => n.costar_count), 1)
    const pool  = zoomed ? simNodes.filter(n => zoomed.nodeIds.has(n.id)) : simNodes
    let best: SimNode | null = null, bestD = 32

    for (const n of pool) {
      const r = 3 + Math.sqrt(n.costar_count / maxCS) * 16
      const d = Math.hypot(n.x - mx, n.y - my)
      if (d < r + 6 && d < bestD) { best = n; bestD = d }
    }
    return best
  }, [simNodes, zoomed])

  const onMouseMove = useCallback((e: React.MouseEvent) => {
    const n = findNode(e.clientX, e.clientY)
    setHovered(n?.id ?? null)
    setTooltip(n ? { node: n, x: e.clientX, y: e.clientY } : null)
  }, [findNode])

  const onMouseLeave = useCallback(() => { setHovered(null); setTooltip(null) }, [])

  const onClick = useCallback((e: React.MouseEvent) => {
    const n = findNode(e.clientX, e.clientY)

    if (!n)                          { setZoomed(null); return }     // empty area → exit zoom
    if (zoomed?.actorId === n.id)    { setZoomed(null); return }     // same actor → toggle off

    // Zoom into this actor's neighbourhood
    const nbrs = new Set<number>([n.id])
    for (const ed of visibleEdges) {
      if (ed.source === n.id) nbrs.add(ed.target)
      if (ed.target === n.id) nbrs.add(ed.source)
    }
    setZoomed({ actorId: n.id, nodeIds: nbrs })
  }, [findNode, zoomed, visibleEdges])

  // ── Derived UI data ─────────────────────────────────────────────────────────

  const industryStats = useMemo(() =>
    Object.entries(
      visibleNodes.reduce<Record<string, number>>((acc, n) => {
        const k = IND_COLOR[n.industry] && n.industry !== 'Unknown' ? n.industry : null
        if (k) acc[k] = (acc[k] ?? 0) + 1
        return acc
      }, {}),
    ).sort((a, b) => b[1] - a[1]),
  [visibleNodes])

  const zoomedActor = zoomed ? simNodes?.find(n => n.id === zoomed.actorId) : null

  // ── Render ──────────────────────────────────────────────────────────────────

  return (
    <div className="glass rounded-3xl p-6 sm:p-8">
      {/* Header */}
      <div className="flex items-start justify-between mb-2">
        <div>
          <div className="flex items-center gap-3 mb-1">
            <span className="text-2xl">🌐</span>
            <h2 className="text-white font-bold text-lg">Cinema Universe</h2>
          </div>
          <p className="text-white/40 text-sm">
            {visibleNodes.length} actors · {visibleEdges.length} edges (2+ shared films) · hover or click to explore
          </p>
        </div>

        <div className="flex flex-col items-end gap-2">
          {/* Industry pills */}
          <div className="hidden sm:flex gap-2 flex-wrap justify-end">
            {industryStats.map(([ind, cnt]) => (
              <div
                key={ind}
                className="flex items-center gap-1.5 text-xs text-white/60 px-2 py-1 rounded-full"
                style={{
                  background: (IND_COLOR[ind] ?? '#666') + '22',
                  border: `1px solid ${(IND_COLOR[ind] ?? '#666')}44`,
                }}
              >
                <div className="w-1.5 h-1.5 rounded-full" style={{ background: IND_COLOR[ind] ?? '#666' }} />
                {ind} ({cnt})
              </div>
            ))}
          </div>

          {/* Expand / collapse toggle */}
          {data.nodes.length > INITIAL_LIMIT && (
            <button
              onClick={() => setShowAll(v => !v)}
              className="text-xs px-3 py-1.5 rounded-full border border-white/[0.12] text-white/50 hover:text-white hover:border-white/25 transition-all"
            >
              {showAll ? `↩ Top ${INITIAL_LIMIT}` : `＋ Show all ${data.nodes.length} actors`}
            </button>
          )}
        </div>
      </div>

      {/* Graph canvas */}
      <div
        ref={containerRef}
        className="relative mt-4 rounded-2xl overflow-hidden"
        style={{ background: '#0d0d1a' }}
      >
        {simulating ? (
          <div className="flex items-center justify-center" style={{ height: H }}>
            <div className="text-center text-white/30">
              <div className="text-3xl mb-3">⚡</div>
              <p className="text-sm">Clustering {visibleNodes.length} actors by industry…</p>
              <p className="text-xs mt-1 text-white/20">Tamil ↖ · Telugu ↙ · Malayalam ↗ · Kannada ↘</p>
            </div>
          </div>
        ) : (
          <canvas
            ref={canvasRef}
            style={{ display: 'block', cursor: hovered ? 'pointer' : 'default', maxWidth: '100%' }}
            onMouseMove={onMouseMove}
            onMouseLeave={onMouseLeave}
            onClick={onClick}
          />
        )}

        {tooltip && <Tooltip node={tooltip.node} x={tooltip.x} y={tooltip.y} />}

        {/* Zoom mode: actor info + back button */}
        {zoomed && zoomedActor && (
          <div className="absolute top-3 left-3 right-3 flex items-center justify-between pointer-events-none">
            <div
              className="text-xs text-white/80 px-3 py-1.5 rounded-full backdrop-blur-sm pointer-events-auto"
              style={{
                background: `${IND_COLOR[zoomedActor.industry] ?? '#888'}2a`,
                border:     `1px solid ${IND_COLOR[zoomedActor.industry] ?? '#888'}44`,
              }}
            >
              📍 {zoomedActor.name} · {zoomed.nodeIds.size - 1} direct co-stars
            </div>
            <button
              onClick={() => setZoomed(null)}
              className="text-xs px-3 py-1.5 rounded-full bg-white/10 text-white/60 hover:text-white hover:bg-white/15 transition-all pointer-events-auto"
            >
              ← Full graph
            </button>
          </div>
        )}

        {/* Industry cluster zone corner labels */}
        {!simulating && !zoomed && ZONE_LABELS.map(({ ind, cls }) => (
          <div
            key={ind}
            className={`absolute ${cls} text-xs font-semibold tracking-widest pointer-events-none select-none`}
            style={{ color: IND_COLOR[ind], opacity: 0.22 }}
          >
            {ind.toUpperCase()}
          </div>
        ))}
      </div>

      <GraphLegend />
    </div>
  )
}
