// Custom edge that can carry a travelling delta dot: the base bezier plus, while a pulse is
// active, an SVG circle animated along the path (one shot per trace event — keyed by decor id so
// consecutive events restart the motion).

import { BaseEdge, getBezierPath, type EdgeProps } from '@xyflow/react'

import type { EdgePulse } from './trace-anim.ts'

export interface PulseEdgeData extends Record<string, unknown> {
  pulse?: EdgePulse | undefined
  baseStyle?: React.CSSProperties
}

export function PulseEdge(props: EdgeProps) {
  const { sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition } = props
  const data = (props.data ?? {}) as PulseEdgeData
  const [path] = getBezierPath({ sourceX, sourceY, sourcePosition, targetX, targetY, targetPosition })
  const pulse = data.pulse
  return (
    <>
      <BaseEdge
        id={props.id}
        path={path}
        style={{
          ...data.baseStyle,
          ...(pulse ? { stroke: pulse.color, strokeWidth: 2.5, opacity: 1 } : {}),
          transition: 'stroke 0.2s',
        }}
      />
      {pulse ? (
        <g key={pulse.id}>
          <circle r={pulse.foreign ? 3 : 5} fill={pulse.color} opacity={pulse.foreign ? 0.5 : 0.95}>
            <animateMotion dur={pulse.foreign ? '1.2s' : '0.8s'} repeatCount="1" fill="freeze" path={path} />
          </circle>
          {pulse.label && !pulse.foreign ? (
            <text fontSize={11} fontWeight={700} fill={pulse.color} dy={-8}>
              <animateMotion dur="0.8s" repeatCount="1" fill="freeze" path={path} />
              {pulse.label}
            </text>
          ) : null}
        </g>
      ) : null}
    </>
  )
}

export const edgeTypes = { pulse: PulseEdge }
