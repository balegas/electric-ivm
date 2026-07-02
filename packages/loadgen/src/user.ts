// A simulated LinearLite user as a state machine — no rendering, no DOM. It uses the real
// @electric-ivm/client for reads (browse subset feeds + a live COUNT aggregation + board status
// shapes that exercise the visibility subquery) and writes mutations straight to Postgres (the system
// of record). Each user holds a bounded set of live subscriptions (≈ its open connections) and, on a
// think-timed loop, either mutates or navigates.

import type { AggregateSubscription, ElectricIvmClient, ShapeMaterialization, SubsetSubscription } from '@electric-ivm/client'
import type { Predicate, ShapeDef, SubsetDef } from '@electric-ivm/protocol'

import type { Config } from './config'
import { MEMBERSHIP, PRIORITIES, STATUSES, USERS } from './infra'

const LIST_COLUMNS = ['id', 'title', 'status', 'priority', 'username', 'project_id', 'created', 'modified', 'kanbanorder']
const BOARD_COLUMNS = ['id', 'title', 'priority', 'username', 'project_id', 'kanbanorder']
const PAGE = 200

/** Monotonic, collision-resistant id allocator (unique across processes via a random high base). */
let _idBase = Date.now() * 100000 + Math.floor(Math.random() * 100000)
const genId = () => ++_idBase

const pick = <T,>(a: readonly T[]): T => a[Math.floor(Math.random() * a.length)]!
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

/** Writes issues/comments to Postgres via the protocol DML compiler (the app's write path). */
export type PgWrite = (table: string, op: 'insert' | 'update' | 'delete', pk: number, row?: Record<string, unknown>) => Promise<void>

export interface Counters {
  reads: number // subscriptions opened over the run
  writes: number // mutations applied over the run
  openSubs: number // currently-open subscriptions
  errors: number
}

function subsetDef(projectId: number): SubsetDef {
  return {
    table: 'issues',
    where: { col: 'project_id', op: 'eq', value: projectId },
    columns: LIST_COLUMNS,
    orderBy: { col: 'created', desc: true },
    limit: PAGE,
  }
}
/** The visibility subquery (drives board status shapes → exercises the subquery registry). */
function visibleSubquery(userId: number): Predicate {
  return { col: 'project_id', in: { table: 'project_members', project: 'project_id', where: { col: 'user_id', op: 'eq', value: userId } } }
}
function statusShapeDef(userId: number, status: string): ShapeDef {
  return { table: 'issues', where: { and: [visibleSubquery(userId), { col: 'status', op: 'eq', value: status }] }, columns: BOARD_COLUMNS }
}
/** COUNT aggregation over the user's visible issues — the app's top-of-list counter (non-subquery OR). */
function visibleOr(projectIds: number[]): Predicate {
  return projectIds.length === 1
    ? { col: 'project_id', op: 'eq', value: projectIds[0] }
    : { or: projectIds.map((p) => ({ col: 'project_id', op: 'eq', value: p })) }
}

export class SimUser {
  private subsets = new Map<number, SubsetSubscription>()
  private board: ShapeMaterialization[] = []
  private counter?: AggregateSubscription
  private myIssues: number[] = []
  private running = false
  private readonly projectIds: number[]
  private readonly name: string

  constructor(
    private readonly userId: number,
    private readonly client: ElectricIvmClient,
    private readonly write: PgWrite,
    private readonly cfg: Config,
    private readonly c: Counters,
  ) {
    this.name = USERS[(userId - 1) % USERS.length]!
    this.projectIds = MEMBERSHIP[(userId - 1) % MEMBERSHIP.length]!
  }

  private async openSubset(projectId: number) {
    if (this.subsets.has(projectId)) return
    try {
      const s = await this.client.subset(subsetDef(projectId))
      this.subsets.set(projectId, s)
      this.c.reads++
      this.c.openSubs++
    } catch {
      this.c.errors++
    }
  }
  private async closeSubset(projectId: number) {
    const s = this.subsets.get(projectId)
    if (!s) return
    this.subsets.delete(projectId)
    this.c.openSubs--
    await s.close().catch(() => {})
  }

  /** Open the initial "browse" view: the COUNT aggregation + up to feedsPerUser-1 project feeds. */
  async start() {
    this.running = true
    try {
      this.counter = await this.client.aggregate({ table: 'issues', where: visibleOr(this.projectIds), fn: 'count' })
      this.c.reads++
      this.c.openSubs++
    } catch {
      this.c.errors++
    }
    const n = Math.max(1, this.cfg.feedsPerUser - 1)
    for (const pid of this.projectIds.slice(0, n)) await this.openSubset(pid)
    void this.loop()
  }

  private async openBoard() {
    if (this.board.length) return
    // 5 board columns, each a visibility-subquery shape (shared subquery node).
    for (const st of STATUSES) {
      try {
        const m = await this.client.shape(statusShapeDef(this.userId, st))
        this.board.push(m)
        this.c.reads++
        this.c.openSubs++
      } catch {
        this.c.errors++
      }
    }
  }
  private async closeBoard() {
    const b = this.board
    this.board = []
    for (const m of b) {
      this.c.openSubs--
      await m.close().catch(() => {})
    }
  }

  // --- mutations (write to Postgres) ---
  private async createIssue() {
    const id = genId()
    const now = Date.now()
    await this.write('issues', 'insert', id, {
      id,
      title: `load ${id}`,
      description: 'generated by loadgen',
      status: pick(STATUSES),
      priority: pick(PRIORITIES),
      username: this.name,
      project_id: pick(this.projectIds),
      created: now,
      modified: now,
      kanbanorder: now + Math.random(),
    })
    this.myIssues.push(id)
    if (this.myIssues.length > 500) this.myIssues.shift()
    this.c.writes++
  }
  private async updateIssue() {
    if (!this.myIssues.length) return this.createIssue()
    const id = pick(this.myIssues)
    const now = Date.now()
    await this.write('issues', 'update', id, {
      id,
      title: `load ${id} v${(now % 1000).toString(36)}`,
      description: 'updated by loadgen',
      status: pick(STATUSES),
      priority: pick(PRIORITIES),
      username: this.name,
      project_id: pick(this.projectIds),
      created: now,
      modified: now,
      kanbanorder: now + Math.random(),
    })
    this.c.writes++
  }
  private async deleteIssue() {
    if (!this.myIssues.length) return
    const idx = Math.floor(Math.random() * this.myIssues.length)
    const id = this.myIssues.splice(idx, 1)[0]!
    await this.write('issues', 'delete', id)
    this.c.writes++
  }
  private async addComment() {
    if (!this.myIssues.length) return this.createIssue()
    const id = genId()
    await this.write('comments', 'insert', id, {
      id,
      issue_id: pick(this.myIssues),
      body: 'loadgen comment',
      username: this.name,
      created: Date.now(),
    })
    this.c.writes++
  }

  private async mutate() {
    const r = Math.random()
    if (r < 0.4) await this.createIssue()
    else if (r < 0.78) await this.updateIssue()
    else if (r < 0.93) await this.addComment()
    else await this.deleteIssue()
  }

  private async navigate() {
    const r = Math.random()
    if (r < 0.4) {
      // scroll a random open feed
      const feeds = [...this.subsets.values()]
      if (feeds.length) await pick(feeds).loadMore().catch(() => {})
    } else if (r < 0.7) {
      // switch project: close one feed, open another member project
      const open = [...this.subsets.keys()]
      const closed = this.projectIds.filter((p) => !this.subsets.has(p))
      if (open.length && closed.length) {
        await this.closeSubset(pick(open))
        await this.openSubset(pick(closed))
      }
    } else {
      // toggle the board view (opens/closes the visibility-subquery status shapes)
      if (this.board.length) await this.closeBoard()
      else await this.openBoard()
    }
  }

  private async loop() {
    while (this.running) {
      await sleep(this.cfg.thinkMinMs + Math.random() * (this.cfg.thinkMaxMs - this.cfg.thinkMinMs))
      if (!this.running) break
      try {
        if (Math.random() < this.cfg.writeRate) await this.mutate()
        else await this.navigate()
      } catch {
        this.c.errors++
      }
    }
  }

  async stop() {
    this.running = false
    await this.counter?.close().catch(() => {})
    for (const s of this.subsets.values()) await s.close().catch(() => {})
    for (const m of this.board) await m.close().catch(() => {})
    this.subsets.clear()
    this.board = []
  }
}
