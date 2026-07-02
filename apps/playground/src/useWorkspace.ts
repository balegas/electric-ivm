// Workspace lifecycle for the app: localStorage identity, provisioning, polling the world state,
// action dispatch, and reset recovery. Any workspace-scoped 404 flips status to 'reset-needed';
// the reset overlay then re-provisions (keeping the id if the server still knows it, minting a
// fresh one otherwise).

import { useCallback, useEffect, useRef, useState } from 'react'

import type { DeviceRole, ShapeSpec, Verb, WorkspaceState } from '../shared/types.ts'
import { api, ApiError, storedWorkspaceId, storeWorkspaceId } from './api.ts'

export type WsStatus = 'booting' | 'ready' | 'reset-needed' | 'error'

export interface Workspace {
  state: WorkspaceState | null
  status: WsStatus
  error: string | null
  /** Bumped after every successful action — device cards use it to poll immediately. */
  actionTick: number
  refresh(): Promise<void>
  reprovision(): Promise<void>
  act(verb: Verb): Promise<void>
  createShape(spec: ShapeSpec, label: string, role: DeviceRole): Promise<void>
  deleteShape(id: string): Promise<void>
  enterScene(n: number): Promise<void>
}

const POLL_MS = 2500

export function useWorkspace(): Workspace {
  const [state, setState] = useState<WorkspaceState | null>(null)
  const [status, setStatus] = useState<WsStatus>('booting')
  const [error, setError] = useState<string | null>(null)
  const [actionTick, setActionTick] = useState(0)
  const idRef = useRef<string | undefined>(storedWorkspaceId())

  const handleError = useCallback((e: unknown) => {
    if (e instanceof ApiError && e.status === 404) {
      setStatus('reset-needed')
    } else if (e instanceof ApiError && e.status === 429) {
      setError('Rate limited — slow down a little.')
      setTimeout(() => setError(null), 2500)
    } else {
      setError(String((e as Error).message ?? e))
      setStatus((s) => (s === 'booting' ? 'error' : s))
    }
  }, [])

  const provision = useCallback(
    async (keepId: boolean) => {
      try {
        const st = await api.provision(keepId ? idRef.current : undefined)
        idRef.current = st.workspace.id
        storeWorkspaceId(st.workspace.id)
        setState(st)
        setStatus('ready')
        setError(null)
      } catch (e) {
        handleError(e)
      }
    },
    [handleError],
  )

  useEffect(() => {
    void provision(true)
  }, [provision])

  const refresh = useCallback(async () => {
    if (!idRef.current) return
    try {
      setState(await api.workspace(idRef.current))
      setStatus('ready')
    } catch (e) {
      handleError(e)
    }
  }, [handleError])

  useEffect(() => {
    if (status !== 'ready') return
    const t = setInterval(() => void refresh(), POLL_MS)
    return () => clearInterval(t)
  }, [status, refresh])

  const withWs = useCallback(
    async (fn: (ws: string) => Promise<unknown>) => {
      if (!idRef.current) return
      try {
        await fn(idRef.current)
        setActionTick((n) => n + 1)
        await refresh()
      } catch (e) {
        handleError(e)
      }
    },
    [refresh, handleError],
  )

  return {
    state,
    status,
    error,
    actionTick,
    refresh,
    reprovision: () => provision(false),
    act: (verb) => withWs((ws) => api.action(ws, verb)),
    createShape: (spec, label, role) => withWs((ws) => api.createShape(ws, spec, label, role)),
    deleteShape: (id) => withWs((ws) => api.deleteShape(ws, id)),
    enterScene: (n) => withWs((ws) => api.scene(ws, n)),
  }
}
