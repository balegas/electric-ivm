// Drop-in replacement for `@durable-streams/server`'s DurableStreamTestServer, backed by the
// Rust durable-streams server (https://crates.io/crates/durable-streams). Same constructor
// options and `start()` / `stop()` surface, but the server is a spawned native binary instead
// of an in-process Node store — the same wire protocol the production server speaks.
//
// Binary resolution (first hit wins):
//   1. $DS_RUST_BIN                                  (explicit path override)
//   2. `durable-streams-server` on $PATH
//   3. ~/.cargo/bin/durable-streams-server
//   4. self-provision: `cargo install durable-streams --version <PIN> --locked`
//      (guarded by an exclusive mkdir lock so parallel vitest workers install once)
//
// Semantics mapping vs the Node test server:
//   - `dataDir` omitted (the Node "in-memory" mode) → a fresh temp dir, deleted on stop().
//     On Linux we additionally pass `--durability memory` (no WAL/fsync — matches the Node
//     server's non-durable semantics); the flag is Linux-only, so macOS runs `wal`.
//   - `port: 0` → the wrapper picks a free port itself (the binary logs the *requested*
//     address, so OS-assigned ports would be unreadable); bind races are retried.

import { type ChildProcess, execFileSync, spawn } from 'node:child_process'
import { existsSync, mkdirSync, mkdtempSync, rmdirSync, rmSync } from 'node:fs'
import { createServer } from 'node:net'
import { tmpdir } from 'node:os'
import { delimiter, join } from 'node:path'

const CRATE_VERSION = '0.1.4'
const BIN_NAME = 'durable-streams-server'

export interface TestServerOptions {
  /** Listen port; 0 (default) picks a free port. */
  port?: number
  /** Listen address (default 127.0.0.1). */
  host?: string
  /** Storage directory. Omitted = ephemeral temp dir, removed on stop() (the Node server's in-memory mode). */
  dataDir?: string
  /** `live=long-poll` block time in ms (server default 30000). */
  longPollTimeout?: number
}

function cargoBin(): string {
  const home = process.env.CARGO_HOME ?? join(process.env.HOME ?? '', '.cargo')
  return join(home, 'bin', BIN_NAME)
}

function onPath(): string | undefined {
  for (const dir of (process.env.PATH ?? '').split(delimiter)) {
    if (dir && existsSync(join(dir, BIN_NAME))) return join(dir, BIN_NAME)
  }
  return undefined
}

/** Locate the server binary, installing it via cargo if absent (once across processes). */
export function ensureServerBinary(): string {
  const override = process.env.DS_RUST_BIN
  if (override) {
    if (!existsSync(override)) throw new Error(`DS_RUST_BIN=${override} does not exist`)
    return override
  }
  const found = onPath() ?? (existsSync(cargoBin()) ? cargoBin() : undefined)
  if (found) return found
  // Exclusive install lock: mkdir is atomic; losers spin until the winner's install lands.
  const lock = join(tmpdir(), `ds-rust-install-${CRATE_VERSION}.lock`)
  try {
    mkdirSync(lock)
  } catch {
    const deadline = Date.now() + 300_000
    while (Date.now() < deadline) {
      if (existsSync(cargoBin())) return cargoBin()
      execFileSync('sleep', ['1'])
    }
    throw new Error(`timed out waiting for concurrent 'cargo install durable-streams' (lock: ${lock})`)
  }
  try {
    // eslint-disable-next-line no-console
    console.error(`[ds-rust] installing durable-streams ${CRATE_VERSION} (one-time cargo install)…`)
    execFileSync('cargo', ['install', 'durable-streams', '--version', CRATE_VERSION, '--locked'], {
      stdio: ['ignore', 'inherit', 'inherit'],
    })
  } finally {
    try {
      rmdirSync(lock)
    } catch {
      /* ignore */
    }
  }
  if (!existsSync(cargoBin())) throw new Error(`cargo install did not produce ${cargoBin()}`)
  return cargoBin()
}

/** Ask the OS for a currently-free port (tiny race window; bind failures are retried). */
function freePort(host: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createServer()
    srv.once('error', reject)
    srv.listen(0, host, () => {
      const addr = srv.address()
      if (addr === null || typeof addr === 'string') {
        srv.close(() => reject(new Error('could not allocate a port')))
        return
      }
      srv.close(() => resolve(addr.port))
    })
  })
}

export class DurableStreamTestServer {
  private readonly opts: TestServerOptions
  private proc: ChildProcess | undefined
  private tempDir: string | undefined
  private url_: string | undefined

  constructor(opts: TestServerOptions = {}) {
    this.opts = opts
  }

  get url(): string | undefined {
    return this.url_
  }

  /** Spawn the server and resolve with its base URL once it reports listening. */
  async start(): Promise<string> {
    if (this.proc) throw new Error('already started')
    const bin = ensureServerBinary()
    const host = this.opts.host ?? '127.0.0.1'
    let dataDir = this.opts.dataDir
    const ephemeral = dataDir === undefined
    if (dataDir === undefined) {
      dataDir = mkdtempSync(join(tmpdir(), 'ds-rust-'))
      this.tempDir = dataDir
    }
    // Bind-conflict retry: freePort()'s reservation is released before the spawn, so another
    // process can steal it; the binary exits immediately on a failed bind and we re-roll.
    let lastErr: unknown
    for (let attempt = 0; attempt < 5; attempt++) {
      const port = this.opts.port && this.opts.port !== 0 ? this.opts.port : await freePort(host)
      const args = ['--host', host, '--port', String(port), '--data-dir', dataDir]
      if (this.opts.longPollTimeout !== undefined) {
        args.push('--long-poll-timeout-ms', String(this.opts.longPollTimeout))
      }
      // The Node server's dataDir-omitted mode is non-durable; mirror it where the flag exists.
      if (ephemeral && process.platform === 'linux') args.push('--durability', 'memory')
      try {
        this.url_ = await this.spawnOnce(bin, args, host, port)
        return this.url_
      } catch (e) {
        lastErr = e
        if (this.opts.port && this.opts.port !== 0) break // fixed port: don't re-roll
      }
    }
    throw new Error(`durable-streams-server failed to start: ${String(lastErr)}`)
  }

  private spawnOnce(bin: string, args: string[], host: string, port: number): Promise<string> {
    return new Promise((resolve, reject) => {
      const proc = spawn(bin, args, { stdio: ['ignore', 'pipe', 'pipe'] })
      let out = ''
      let settled = false
      const timer = setTimeout(() => {
        if (settled) return
        settled = true
        proc.kill('SIGKILL')
        reject(new Error(`did not report listening within 15s\n${out}`))
      }, 15_000)
      const onData = (chunk: Buffer) => {
        out += chunk.toString()
        if (!settled && out.includes('listening on')) {
          settled = true
          clearTimeout(timer)
          this.proc = proc
          resolve(`http://${host}:${port}`)
        }
      }
      proc.stdout?.on('data', onData)
      proc.stderr?.on('data', onData)
      proc.once('exit', (code) => {
        if (settled) return
        settled = true
        clearTimeout(timer)
        reject(new Error(`exited early (code ${code})\n${out}`))
      })
      proc.once('error', (e) => {
        if (settled) return
        settled = true
        clearTimeout(timer)
        reject(e)
      })
    })
  }

  /** Terminate the server (SIGTERM, escalating to SIGKILL) and remove an ephemeral data dir. */
  async stop(): Promise<void> {
    const proc = this.proc
    this.proc = undefined
    if (proc && proc.exitCode === null && !proc.killed) {
      await new Promise<void>((resolve) => {
        const hardKill = setTimeout(() => {
          proc.kill('SIGKILL')
        }, 3_000)
        proc.once('exit', () => {
          clearTimeout(hardKill)
          resolve()
        })
        proc.kill('SIGTERM')
      })
    }
    if (this.tempDir) {
      rmSync(this.tempDir, { recursive: true, force: true })
      this.tempDir = undefined
    }
    this.url_ = undefined
  }
}
