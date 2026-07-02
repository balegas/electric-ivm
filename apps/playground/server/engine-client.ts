// Thin typed client for the engine's control-plane HTTP API (apps/engine/src/http.rs). The
// playground server is the only thing that talks to the engine; browsers never reach it directly.

import type { Predicate } from '@electric-ivm/protocol'

export interface EngineShapeResp {
  shapeId: string
  table: string
  streamPath: string
  streamUrl: string
}

export class EngineClient {
  constructor(readonly baseUrl: string) {}

  private async req<T>(path: string, init?: RequestInit): Promise<T> {
    const res = await fetch(`${this.baseUrl}${path}`, {
      ...init,
      headers: { 'content-type': 'application/json', ...init?.headers },
    })
    if (!res.ok) {
      const body = await res.text().catch(() => '')
      throw new Error(`engine ${init?.method ?? 'GET'} ${path} → ${res.status} ${body}`)
    }
    return (await res.json()) as T
  }

  createShape(table: string, where: Predicate, columns?: string[]): Promise<EngineShapeResp> {
    return this.req('/shapes', { method: 'POST', body: JSON.stringify({ table, where, columns }) })
  }

  createAggregate(table: string, where: Predicate, fn: string, col?: string | null): Promise<EngineShapeResp> {
    return this.req('/aggregate', {
      method: 'POST',
      body: JSON.stringify({ table, where, fn, ...(col ? { col } : {}) }),
    })
  }

  async shapeExists(id: string): Promise<boolean> {
    const res = await fetch(`${this.baseUrl}/shapes/${encodeURIComponent(id)}`)
    return res.ok
  }

  async deleteShape(id: string): Promise<void> {
    await fetch(`${this.baseUrl}/shapes/${encodeURIComponent(id)}`, { method: 'DELETE' }).catch(() => {})
  }

  graph(): Promise<unknown> {
    return this.req('/graph')
  }

  shapeRows(id: string, limit: number): Promise<unknown> {
    return this.req(`/shapes/${encodeURIComponent(id)}/rows?limit=${limit}`)
  }

  query(body: unknown): Promise<{ rows: unknown[]; lsn: string }> {
    return this.req('/query', { method: 'POST', body: JSON.stringify(body) })
  }

  traceUrl(): string {
    return `${this.baseUrl}/trace`
  }
}
