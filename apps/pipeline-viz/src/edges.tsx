// Custom edge that can carry a travelling delta dot: the base bezier plus, while a pulse is
// active, an SVG circle + weight label (+1/−1/±1) animated along the path — one shot per trace
// event, keyed by the id of the event that created the pulse, so later events never restart dots
// already in flight. Staging (`pulse.delayMs`) is done by DELAYED INSERTION of the animated group:
// SMIL `begin` offsets resolve against the SVG's global timeline (its zero is page load, long in
// the past), which silently skips the travel — inserting the element when its stage starts makes
// the browser begin the motion right then, exactly like the original unstaged behavior.

import { BaseEdge, getBezierPath, type EdgeProps } from '@xyflow/react'
import { useEffect, useLayoutEffect, useRef, useState } from 'react'

import type { EdgePulse } from './trace-anim'

export interface PulseEdgeData extends Record<string, unknown> {
  pulse?: EdgePulse | undefined
  baseStyle?: React.CSSProperties
}

/** The travelling dot + weight label, driven by requestAnimationFrame over the edge path
 *  (`getPointAtLength`). Deliberately NOT SMIL: `begin` offsets resolve against the edge-svg's
 *  long-running timeline and browsers increasingly don't tick SMIL at all — a rAF loop moves in
 *  every engine and starts exactly when this group mounts (its stage). */
function TravelDot({ pulse, path }: { pulse: EdgePulse; path: string }) {
  const groupRef = useRef<SVGGElement | null>(null)
  const measureRef = useRef<SVGPathElement | null>(null)
  useLayoutEffect(() => {
    const p = measureRef.current
    const g = groupRef.current
    if (!p || !g) return
    const total = p.getTotalLength()
    const t0 = performance.now()
    let raf = 0
    // A query-back BOUNCE ping-pongs between the two endpoints (a few round trips) instead of
    // travelling once — the Δ node "asks" the source and the moved rows come back. Otherwise the
    // dot travels once, source → target.
    const BOUNCE_CYCLES = 2
    const tick = (now: number) => {
      const f = (now - t0) / pulse.durMs // elapsed fraction over the whole duration
      let k: number
      if (pulse.bounce) {
        const phase = Math.min(f, 1) * BOUNCE_CYCLES * 2 // half-legs elapsed
        k = Math.abs((phase % 2) - 1) // triangle: 1 → 0 → 1 → 0 → 1 (starts/ends at the target = Δ node)
      } else {
        k = Math.min(1, f)
      }
      const pt = p.getPointAtLength(total * k)
      g.setAttribute('transform', `translate(${pt.x}, ${pt.y})`)
      g.style.opacity = '1'
      if (f < 1) raf = requestAnimationFrame(tick)
    }
    raf = requestAnimationFrame(tick)
    return () => cancelAnimationFrame(raf)
    // eslint-disable-next-line react-hooks/exhaustive-deps -- one shot per mount; the group is keyed by pulse id
  }, [])
  return (
    <>
      {/* invisible copy of the edge path, used only to measure travel positions */}
      <path ref={measureRef} d={path} fill="none" stroke="none" />
      <g ref={groupRef} style={{ opacity: 0 }}>
        {/* Derived (query-back) moves get a hollow ring, matching the dashed edge, so the dot reads
            as "carried in from another table" rather than the table's own solid data delta. */}
        {pulse.derived ? (
          <circle r={5} fill="#fff" stroke={pulse.color} strokeWidth={2} opacity={0.95} />
        ) : (
          <circle r={5} fill={pulse.color} opacity={0.95} />
        )}
        {pulse.label ? (
          <text fontSize={11} fontWeight={700} fill={pulse.color} dy={-8}>
            {pulse.label}
          </text>
        ) : null}
      </g>
    </>
  )
}

export function PulseEdge(props: EdgeProps) {
  const { sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition } = props
  const data = (props.data ?? {}) as PulseEdgeData
  const [path] = getBezierPath({ sourceX, sourceY, sourcePosition, targetX, targetY, targetPosition })
  const pulse = data.pulse

  // The pulse becomes visible (and its motion starts) when its stage begins, and is removed a
  // short linger after it arrives — a dot frozen at the path end sits half-hidden under the
  // target node's card, which reads as a stray number lying around the canvas.
  const LINGER_MS = 500
  const [staged, setStaged] = useState<number | null>(null) // pulse.id currently shown
  useEffect(() => {
    if (!pulse) {
      setStaged(null)
      return
    }
    const timers: ReturnType<typeof setTimeout>[] = []
    if (pulse.delayMs <= 0) {
      setStaged(pulse.id)
    } else {
      timers.push(setTimeout(() => setStaged(pulse.id), pulse.delayMs))
    }
    timers.push(setTimeout(() => setStaged(null), pulse.delayMs + pulse.durMs + LINGER_MS))
    return () => timers.forEach(clearTimeout)
  }, [pulse?.id, pulse?.delayMs, pulse?.durMs])

  const show = pulse != null && staged === pulse.id
  return (
    <>
      <BaseEdge
        id={props.id}
        path={path}
        style={{
          ...data.baseStyle,
          ...(pulse ? { stroke: pulse.color, strokeWidth: 2.5, opacity: 1 } : {}),
          // A query-back-derived move-in/out dashes the lit path — this data crossed from another
          // table's change via a Postgres query-back, not this edge's own stream.
          ...(pulse?.derived ? { strokeDasharray: '6 4' } : {}),
          // The recolor is delayed to the pulse's stage so the path lights up as the dot leaves.
          transition: pulse ? `stroke 0.2s ${pulse.delayMs}ms` : 'stroke 0.2s',
        }}
      />
      {show ? <TravelDot key={pulse.id} pulse={pulse} path={path} /> : null}
    </>
  )
}

export const edgeTypes = { pulse: PulseEdge }
