// Scene 6's card: a subset query — an ORDERED page over the data, positioned at an exact LSN.
// Ordering/windowing deliberately live OUTSIDE shape maintenance; the board stays pinned at the
// LSN it was fetched at while the shapes around it keep flowing. Refresh re-pins.

import { useCallback, useEffect, useState } from 'react'

import { api } from './api.ts'

export function SubsetBoard({ workspaceId }: { workspaceId: string | undefined }) {
  const [rows, setRows] = useState<Record<string, unknown>[]>([])
  const [lsn, setLsn] = useState<string | null>(null)
  const [err, setErr] = useState<string | null>(null)

  const load = useCallback(async () => {
    if (!workspaceId) return
    try {
      const r = await api.subset(workspaceId, { col: 'total', desc: true }, 5)
      setRows(r.rows)
      setLsn(r.lsn)
      setErr(null)
    } catch (e) {
      setErr(String((e as Error).message ?? e))
    }
  }, [workspaceId])

  useEffect(() => {
    void load()
  }, [load])

  return (
    <div className="device device-tile subset">
      <div className="device-h">
        <span className="device-title">🏆 Top 5 orders by total</span>
        <button className="mini" onClick={() => void load()}>
          ↻ re-pin
        </button>
      </div>
      <div className="device-pred">subset query · ORDER BY total DESC LIMIT 5 · pinned at LSN {lsn ?? '…'}</div>
      {err ? <div className="device-err">{err}</div> : null}
      <div className="device-rows">
        {rows.map((r, i) => (
          <div key={String(r.id ?? i)} className="device-row">
            <span>
              {i + 1}. {String(r.dish ?? '')}
            </span>
            <span className="device-row-r">€{Number(r.total ?? 0).toFixed(2)}</span>
          </div>
        ))}
        {rows.length === 0 && !err ? <div className="device-empty">no orders yet</div> : null}
      </div>
    </div>
  )
}
