// Typed fetch helpers for the playground server's /api surface. A 404 on workspace-scoped calls
// means "unknown or reset workspace" — surfaced as ApiError(404) so useWorkspace can trigger the
// reset-recovery flow.

import type {
  GraphResponse,
  PlaygroundShape,
  SceneShapeResult,
  ShapeSpec,
  Verb,
  WorkspaceState,
  DeviceRole,
} from '../shared/types.ts'

export class ApiError extends Error {
  constructor(
    public status: number,
    msg: string,
    public epoch?: number,
  ) {
    super(msg)
  }
}

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, { headers: { 'content-type': 'application/json' }, ...init })
  const body = (await res.json().catch(() => ({}))) as Record<string, unknown>
  if (!res.ok) {
    throw new ApiError(res.status, String(body.error ?? res.statusText), body.epoch as number | undefined)
  }
  return body as T
}

export const api = {
  provision: (existingId?: string) =>
    req<WorkspaceState>('/api/workspace', { method: 'POST', body: JSON.stringify({ existingId }) }),
  workspace: (id: string) => req<WorkspaceState>(`/api/workspace/${encodeURIComponent(id)}`),
  action: (workspace: string, verb: Verb) =>
    req<{ ok: true }>('/api/action', { method: 'POST', body: JSON.stringify({ workspace, ...verb }) }),
  scene: (workspace: string, scene: number) =>
    req<SceneShapeResult>('/api/scene', { method: 'POST', body: JSON.stringify({ workspace, scene }) }),
  createShape: (workspace: string, spec: ShapeSpec, label: string, role: DeviceRole) =>
    req<PlaygroundShape>('/api/shape', { method: 'POST', body: JSON.stringify({ workspace, spec, label, role }) }),
  deleteShape: (workspace: string, id: string) =>
    req<{ ok: true }>(`/api/shape/${encodeURIComponent(id)}?workspace=${encodeURIComponent(workspace)}`, {
      method: 'DELETE',
    }),
  graph: (workspace: string) => req<GraphResponse>(`/api/graph?workspace=${encodeURIComponent(workspace)}`),
  shapeRows: (workspace: string, id: string, limit = 100) =>
    req<{ rows: { key: string; value: Record<string, unknown> }[]; count: number; changesOnly: boolean }>(
      `/api/shapes/${encodeURIComponent(id)}/rows?workspace=${encodeURIComponent(workspace)}&limit=${limit}`,
    ),
  nodeIndex: (workspace: string, sig: string) =>
    req<{
      sig: string
      distinctValues: number
      refcount: number
      values: { value: unknown; contributors: number }[]
      truncated: boolean
    }>(`/api/graph/node?sig=${encodeURIComponent(sig)}&workspace=${encodeURIComponent(workspace)}`),
  subset: (workspace: string, orderBy: { col: string; desc?: boolean }, limit: number) =>
    req<{ rows: Record<string, unknown>[]; lsn: string }>('/api/subset', {
      method: 'POST',
      body: JSON.stringify({ workspace, orderBy, limit }),
    }),
}

const WS_KEY = 'playground-workspace'
export const storedWorkspaceId = (): string | undefined => localStorage.getItem(WS_KEY) ?? undefined
export const storeWorkspaceId = (id: string): void => localStorage.setItem(WS_KEY, id)
