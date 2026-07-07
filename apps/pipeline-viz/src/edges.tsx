// Custom edge that can carry a travelling delta dot: the base bezier plus, while a pulse is
// active, an SVG circle animated along the path — one shot per trace event, keyed by the id of
// the event that created the pulse, so later events never restart dots already in flight. The
// dot is STAGED (`pulse.delayMs`): it stays hidden until the change actually reaches this edge's
// source node, so a delta visibly propagates through the pipeline rank by rank.

import { BaseEdge, getBezierPath, type EdgeProps } from '@xyflow/react'

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
  const begin = pulse ? `${pulse.delayMs}ms` : '0ms'
  const dur = pulse ? `${pulse.durMs}ms` : '0.8s'
  return (
    <>
      <BaseEdge
        id={props.id}
        path={path}
        style={{
          ...data.baseStyle,
          ...(pulse ? { stroke: pulse.color, strokeWidth: 2.5, opacity: 1 } : {}),
          // The recolor is delayed to the pulse's stage so the path lights up as the dot arrives.
          transition: pulse ? `stroke 0.2s ${begin}` : 'stroke 0.2s',
        }}
      />
      {pulse ? (
        <g key={pulse.id}>
          {/* Hidden until its stage begins (a frozen dot sitting at the path start before the
              change "arrives" reads as a glitch), then revealed as the motion starts. */}
          <circle r={5} fill={pulse.color} opacity={0}>
            <set attributeName="opacity" to="0.95" begin={begin} fill="freeze" />
            <animateMotion dur={dur} begin={begin} repeatCount="1" fill="freeze" path={path} />
          </circle>
          {pulse.label ? (
            <text fontSize={11} fontWeight={700} fill={pulse.color} opacity={0} dy={-8}>
              <set attributeName="opacity" to="1" begin={begin} fill="freeze" />
              <animateMotion dur={dur} begin={begin} repeatCount="1" fill="freeze" path={path} />
              {pulse.label}
            </text>
          ) : null}
        </g>
      ) : null}
    </>
  )
}

export const edgeTypes = { pulse: PulseEdge }
