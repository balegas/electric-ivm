import { useVirtualizer } from '@tanstack/react-virtual'
import { useEffect, useRef } from 'react'

/**
 * Windowed renderer: mounts only the rows currently in view (plus a small overscan) instead of one
 * DOM node per row. At 20k rows this turns a ~350k-node render into a few dozen nodes, so the list/
 * board paints immediately and stays responsive while scrolling. The synced collection is unchanged —
 * this is purely a render-layer optimization.
 *
 * `className` styles the scroll viewport (it owns `overflow-y: auto` + a bounded height). Each row is
 * absolutely positioned and self-measured, so variable heights (wrapping titles) are handled. `gap`
 * adds vertical spacing between rows and is included in the measured height.
 */
export function Virtual<T>({
  items,
  getKey,
  estimateSize,
  renderItem,
  className,
  gap = 0,
  overscan = 12,
  onEndReached,
}: {
  items: T[]
  getKey: (item: T, index: number) => string | number
  estimateSize: number
  renderItem: (item: T, index: number) => React.ReactNode
  className?: string
  gap?: number
  overscan?: number
  /** Fired when the rendered window reaches the end of `items` (for infinite-scroll paging). */
  onEndReached?: () => void
}): JSX.Element {
  const parentRef = useRef<HTMLDivElement>(null)
  const virtualizer = useVirtualizer({
    count: items.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => estimateSize + gap,
    overscan,
  })

  // Trigger paging when the last rendered row is within a screenful of the end. Keyed on the last
  // rendered index + item count so it re-checks as the user scrolls and as pages arrive.
  const virtualItems = virtualizer.getVirtualItems()
  const lastIndex = virtualItems.length ? virtualItems[virtualItems.length - 1]!.index : 0
  useEffect(() => {
    if (onEndReached && items.length > 0 && lastIndex >= items.length - 1 - overscan) onEndReached()
  }, [onEndReached, lastIndex, items.length, overscan])

  return (
    <div ref={parentRef} className={className}>
      {/* flexShrink:0 — when the viewport is itself a flex container (e.g. .board-col-body), the sizer
          must keep its full virtual height instead of being shrunk to fit, or scrolling collapses. */}
      <div style={{ height: virtualizer.getTotalSize(), position: 'relative', width: '100%', flexShrink: 0 }}>
        {virtualItems.map((vi) => (
          <div
            key={getKey(items[vi.index]!, vi.index)}
            data-index={vi.index}
            ref={virtualizer.measureElement}
            style={{
              position: 'absolute',
              top: 0,
              left: 0,
              width: '100%',
              transform: `translateY(${vi.start}px)`,
              paddingBottom: gap || undefined,
            }}
          >
            {renderItem(items[vi.index]!, vi.index)}
          </div>
        ))}
      </div>
    </div>
  )
}
