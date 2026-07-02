// The State-Protocol change-event envelope that travels on every table/shape durable stream
// and that `@durable-streams/state`'s createStreamDB consumes. `type` is the table name (the
// collection discriminator), `key` is the stringified primary key, `headers.operation` is the
// op. See decisions D4.

import type { Op, Row, Value } from './types.js'

export type Operation = 'insert' | 'update' | 'delete' | 'upsert'

export interface StreamEnvelope {
  type: string
  key: string
  /** Present for insert/update/upsert; omitted for delete. */
  value?: Row
  headers: {
    operation: Operation
    txid?: string
    /** Stamped by the server on read; never sent by producers. */
    offset?: string
    /**
     * Postgres commit LSN (`"HI/LO"` hex) of the change, stamped by the engine on live shape/feed
     * envelopes. Lets a subset client position its live tail at the page snapshot — drop deltas with
     * `lsn < snapshotLsn`. Absent on backfill rows and in library (no-Postgres) mode.
     */
    lsn?: string
    /**
     * Position of the change within its transaction, stamped by the replication ingestor on
     * table-stream envelopes. `(lsn, seq)` uniquely identifies a change so the engine tailer can
     * skip duplicates from the ingestor's at-least-once redelivery. Not present on shape streams.
     */
    seq?: number
  }
}

/** Build the table-stream envelope for an ingest write. */
export function toTableEnvelope(table: string, op: Op, pk: Value, row?: Row, txid?: string): StreamEnvelope {
  const headers: StreamEnvelope['headers'] = { operation: op }
  if (txid !== undefined) headers.txid = txid
  const env: StreamEnvelope = { type: table, key: String(pk), headers }
  if (op !== 'delete' && row !== undefined) env.value = row
  return env
}
