// Custom edge that can carry a travelling delta dot: the base bezier plus, while a pulse is
// active, an SVG circle + weight label (+1/−1/±1) animated along the path — one shot per trace
// event, keyed by the id of the event that created the pulse, so later events never restart dots
// already in flight. Staging (`pulse.delayMs`) is done by DELAYED INSERTION of the animated group:
// SMIL `begin` offsets resolve against the SVG's global timeline (its zero is page load, long in
// the past), which silently skips the travel — inserting the element when its stage starts makes
// the browser begin the motion right then, exactly like the original unstaged behavior.

import { BaseEdge, getBezierPath, type EdgeProps } from '@xyflow/react'
import { useEffect, useState } from 'react'

import type { EdgePulse } from './trace-anim'

export interface PulseEdgeData extends Record<string, unknown> {
  pulse?: EdgePulse | undefined
  baseStyle?: React.CSSProperties
}

export function PulseEdge(props: EdgeProps) {
  const { sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition } = props
  const data = (props.data ?? {}) as PulseEdgeData
  const [path] = getBezierPath({ sourceX, sourceY, sourcePosition, targetX, targetY, targetPosition })
  const pulse = data.pulse

  // The pulse becomes visible (and its motion starts) when its stage begins.
  const [staged, setStaged] = useState<number | null>(null) // pulse.id currently shown
  useEffect(() => {
    if (!pulse) {
      setStaged(null)
      return
    }
    if (pulse.delayMs <= 0) {
      setStaged(pulse.id)
      return
    }
    const t = setTimeout(() => setStaged(pulse.id), pulse.delayMs)
    return () => clearTimeout(t)
  }, [pulse?.id, pulse?.delayMs])

  const show = pulse != null && staged === pulse.id
  const dur = pulse ? `${pulse.durMs}ms` : '0.8s'
  return (
    <>
      <BaseEdge
        id={props.id}
        path={path}
        style={{
          ...data.baseStyle,
          ...(pulse ? { stroke: pulse.color, strokeWidth: 2.5, opacity: 1 } : {}),
          // The recolor is delayed to the pulse's stage so the path lights up as the dot leaves.
          transition: pulse ? `stroke 0.2s ${pulse.delayMs}ms` : 'stroke 0.2s',
        }}
      />
      {show ? (
        <g key={pulse.id}>
          <circle r={5} fill={pulse.color} opacity={0.95}>
            <animateMotion dur={dur} repeatCount="1" fill="freeze" path={path} />
          </circle>
          {pulse.label ? (
            <text fontSize={11} fontWeight={700} fill={pulse.color} dy={-8}>
              <animateMotion dur={dur} repeatCount="1" fill="freeze" path={path} />
              {pulse.label}
            </text>
          ) : null}
        </g>
      ) : null}
    </>
  )
}

export const edgeTypes = { pulse: PulseEdge }
