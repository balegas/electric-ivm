// Global test setup: build the engine once (so parallel workers don't race the cargo lock) and boot
// one ephemeral Postgres with logical replication enabled. Each harness then creates its own database
// + slot inside it (logical slots are per-database), so test files stay isolated. The admin
// connection string is exported via ELECTRIC_CIRCUITS_TEST_PG_URL (inherited by forked workers).
import { execFileSync } from 'node:child_process'
import { appendFileSync, mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

export default function setup() {
  execFileSync('cargo', ['build', '-p', 'electric-circuits-engine'], { stdio: 'inherit' })
  process.env.ELECTRIC_CIRCUITS_ENGINE_PREBUILT = '1'

  const dir = mkdtempSync(join(tmpdir(), 'el-pg-'))
  const data = join(dir, 'data')
  execFileSync('initdb', ['-D', data, '-U', 'postgres', '--auth=trust', '--no-sync'], { stdio: 'ignore' })

  // Try a few ports; initdb is done, only the chosen port differs per attempt.
  let port = 0
  let started = false
  for (let attempt = 0; attempt < 8 && !started; attempt++) {
    port = 55432 + Math.floor(Math.random() * 4000)
    appendFileSync(
      join(data, 'postgresql.conf'),
      `\n# test config (attempt ${attempt})\n` +
        `wal_level = logical\nmax_replication_slots = 80\nmax_wal_senders = 80\n` +
        `listen_addresses = '127.0.0.1'\nunix_socket_directories = '/tmp'\nport = ${port}\n` +
        `fsync = off\nsynchronous_commit = off\nfull_page_writes = off\n`,
    )
    try {
      execFileSync('pg_ctl', ['-D', data, '-l', join(dir, 'log'), '-w', 'start'], { stdio: 'ignore' })
      started = true
    } catch {
      /* port likely in use; loop appends a new port (last one wins in postgresql.conf) */
    }
  }
  if (!started) throw new Error('failed to start ephemeral postgres for tests')

  process.env.ELECTRIC_CIRCUITS_TEST_PG_URL = `postgres://postgres@127.0.0.1:${port}/postgres`

  return () => {
    try {
      execFileSync('pg_ctl', ['-D', data, '-m', 'immediate', '-w', 'stop'], { stdio: 'ignore' })
    } catch {
      /* already down */
    }
    try {
      rmSync(dir, { recursive: true, force: true })
    } catch {
      /* ignore */
    }
  }
}
