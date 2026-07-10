import { Database } from "bun:sqlite"
import { tool, type Config, type Plugin } from "@opencode-ai/plugin"
import { existsSync, mkdirSync } from "node:fs"
import { homedir } from "node:os"
import { dirname, join } from "node:path"
import { formatDuration, parseInterval, parseLoopCommand } from "./parser"

type LoopStatus = "active" | "cancelled" | "paused"
type RunStatus = "running" | "completed" | "blocked" | "failed" | "cancelled" | "unverified"

type LoopRow = {
  id: string
  sessionId: string
  prompt: string
  promptKey: string
  intervalMs: number
  autoCompact: boolean
  runRequested: boolean
  status: LoopStatus
  createdAt: number
  updatedAt: number
  nextRunAt: number | null
  lastRunId: string | null
  lastStartedAt: number | null
  lastFinishedAt: number | null
  runCount: number
  leaseOwner: string | null
  leaseUntil: number | null
}

type RunRow = {
  id: string
  loopId: string
  sessionId: string
  scheduledFor: number
  startedAt: number | null
  finishedAt: number | null
  durationMs: number | null
  status: RunStatus
  error: string | null
  createdAt: number
  updatedAt: number
}

type LoopClaim = {
  loop: LoopRow
  run: RunRow
  scheduledFor: number
}

type LoopPluginOptions = {
  commandName?: string
  dbPath?: string
  minIntervalMs?: number
  autoCompact?: boolean
}

const MAX_TIMEOUT_MS = 2_147_483_647
const SESSION_BUSY_RETRY_MS = 10_000
const RUN_LEASE_MS = 6 * 60 * 60 * 1_000
function now() {
  return Date.now()
}

function iso(value?: number | null) {
  if (!value) return "never"
  return new Date(value).toISOString()
}

function relativeTime(value?: number | null) {
  if (!value) return "not scheduled"
  const delta = value - now()
  if (Math.abs(delta) < 1_000) return "now"
  const suffix = delta >= 0 ? "from now" : "ago"
  return `${formatDuration(Math.abs(delta))} ${suffix}`
}

function shortId(id: string) {
  return id.split("_").at(-1)?.slice(0, 8) ?? id.slice(0, 8)
}

function makeId(prefix: string) {
  return `${prefix}_${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`
}

function defaultDbPath() {
  const configHome = process.env.XDG_CONFIG_HOME || join(homedir(), ".config")
  return join(configHome, "opencode", "loops", "loops.db")
}

function ensureParent(path: string) {
  const parent = dirname(path)
  if (!existsSync(parent)) mkdirSync(parent, { recursive: true, mode: 0o700 })
}

function normalizeLoopPrompt(prompt: string) {
  return prompt
    .trim()
    .toLowerCase()
    .replace(/^[.,;:!?]+\s*/, "")
    .replace(/["'`]/g, "")
    .replace(/\s+/g, " ")
}

function loopFromRow(row: any): LoopRow {
  return {
    id: row.id,
    sessionId: row.session_id,
    prompt: row.prompt,
    promptKey: row.prompt_key,
    intervalMs: row.interval_ms,
    autoCompact: Boolean(row.auto_compact),
    runRequested: Boolean(row.run_requested),
    status: row.status,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    nextRunAt: row.next_run_at,
    lastRunId: row.last_run_id,
    lastStartedAt: row.last_started_at,
    lastFinishedAt: row.last_finished_at,
    runCount: row.run_count,
    leaseOwner: row.lease_owner,
    leaseUntil: row.lease_until,
  }
}

function runFromRow(row: any): RunRow {
  return {
    id: row.id,
    loopId: row.loop_id,
    sessionId: row.session_id,
    scheduledFor: row.scheduled_for,
    startedAt: row.started_at,
    finishedAt: row.finished_at,
    durationMs: row.duration_ms,
    status: row.status,
    error: row.error,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  }
}

class LoopStore {
  private db: Database

  constructor(path: string) {
    ensureParent(path)
    this.db = new Database(path)
    this.db.exec("PRAGMA journal_mode = WAL")
    this.db.exec("PRAGMA synchronous = NORMAL")
    this.db.exec("PRAGMA busy_timeout = 5000")
    this.migrate()
    this.recoverStaleRuns()
  }

  close() {
    this.db.close()
  }

  createOrGetLoop(
    input: Pick<LoopRow, "sessionId" | "prompt" | "intervalMs"> & { autoCompact?: boolean },
  ): { loop: LoopRow; created: boolean } {
    const timestamp = now()
    const promptKey = normalizeLoopPrompt(input.prompt)
    const loop: LoopRow = {
      id: makeId("loop"),
      sessionId: input.sessionId,
      prompt: input.prompt,
      promptKey,
      intervalMs: input.intervalMs,
      autoCompact: input.autoCompact ?? false,
      runRequested: false,
      status: "active",
      createdAt: timestamp,
      updatedAt: timestamp,
      nextRunAt: null,
      lastRunId: null,
      lastStartedAt: null,
      lastFinishedAt: null,
      runCount: 0,
      leaseOwner: null,
      leaseUntil: null,
    }
    return this.transaction(() => {
      const existing = this.db
        .query("select * from loops where session_id = ? and prompt_key = ? and interval_ms = ? and status in ('active', 'paused') limit 1")
        .get(input.sessionId, promptKey, input.intervalMs)
      if (existing) return { loop: loopFromRow(existing), created: false }

      this.db
        .query(
          `insert into loops
            (id, session_id, prompt, prompt_key, interval_ms, auto_compact, run_requested, status, created_at, updated_at, next_run_at, last_run_id, last_started_at, last_finished_at, run_count, lease_owner, lease_until)
           values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        )
        .run(
          loop.id,
          loop.sessionId,
          loop.prompt,
          loop.promptKey,
          loop.intervalMs,
          loop.autoCompact ? 1 : 0,
          0,
          loop.status,
          loop.createdAt,
          loop.updatedAt,
          loop.nextRunAt,
          loop.lastRunId,
          loop.lastStartedAt,
          loop.lastFinishedAt,
          loop.runCount,
          loop.leaseOwner,
          loop.leaseUntil,
        )
      return { loop, created: true }
    })
  }

  setAutoCompact(loopId: string, autoCompact: boolean): LoopRow | undefined {
    const timestamp = now()
    this.db.query("update loops set auto_compact = ?, updated_at = ? where id = ?").run(autoCompact ? 1 : 0, timestamp, loopId)
    return this.getLoop(loopId)
  }

  createRun(loop: LoopRow, scheduledFor: number): RunRow {
    const timestamp = now()
    const run: RunRow = {
      id: makeId("run"),
      loopId: loop.id,
      sessionId: loop.sessionId,
      scheduledFor,
      startedAt: timestamp,
      finishedAt: null,
      durationMs: null,
      status: "running",
      error: null,
      createdAt: timestamp,
      updatedAt: timestamp,
    }
    this.db
      .query(
        `insert into loop_runs
          (id, loop_id, session_id, scheduled_for, started_at, finished_at, duration_ms, status, error, created_at, updated_at)
         values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        run.id,
        run.loopId,
        run.sessionId,
        run.scheduledFor,
        run.startedAt,
        run.finishedAt,
        run.durationMs,
        run.status,
        run.error,
        run.createdAt,
        run.updatedAt,
      )
    this.db
      .query("update loops set last_run_id = ?, last_started_at = ?, updated_at = ? where id = ?")
      .run(run.id, run.startedAt, timestamp, loop.id)
    return run
  }

  getLoop(id: string): LoopRow | undefined {
    const row = this.db.query("select * from loops where id = ?").get(id)
    return row ? loopFromRow(row) : undefined
  }

  getSessionLoops(sessionId: string, activeOnly = false): LoopRow[] {
    const rows = activeOnly
      ? this.db.query("select * from loops where session_id = ? and status = 'active' order by created_at").all(sessionId)
      : this.db.query("select * from loops where session_id = ? order by created_at").all(sessionId)
    return rows.map(loopFromRow)
  }

  getActiveLoops(): LoopRow[] {
    return this.db.query("select * from loops where status = 'active'").all().map(loopFromRow)
  }

  getRunningRuns(sessionId: string): RunRow[] {
    return this.db
      .query("select * from loop_runs where session_id = ? and status = 'running' order by started_at")
      .all(sessionId)
      .map(runFromRow)
  }

  getRunningRunForLoop(loopId: string): RunRow | undefined {
    const row = this.db.query("select * from loop_runs where loop_id = ? and status = 'running'").get(loopId)
    return row ? runFromRow(row) : undefined
  }

  getRecentRuns(sessionId: string, limit = 10): RunRow[] {
    return this.db
      .query("select * from loop_runs where session_id = ? order by created_at desc limit ?")
      .all(sessionId, limit)
      .map(runFromRow)
  }

  finishRun(run: RunRow, status: Exclude<RunStatus, "running">, error?: string | null): RunRow {
    const finishedAt = now()
    const durationMs = run.startedAt ? finishedAt - run.startedAt : null
    this.db
      .query("update loop_runs set status = ?, finished_at = ?, duration_ms = ?, error = ?, updated_at = ? where id = ?")
      .run(status, finishedAt, durationMs, error ?? null, finishedAt, run.id)
    return { ...run, status, finishedAt, durationMs, error: error ?? null, updatedAt: finishedAt }
  }

  scheduleNext(loop: LoopRow, finishedAt: number): LoopRow {
    const nextRunAt = loop.runRequested ? finishedAt : finishedAt + loop.intervalMs
    const updatedAt = now()
    this.db
      .query(
        "update loops set next_run_at = ?, run_requested = 0, last_finished_at = ?, run_count = run_count + 1, lease_owner = null, lease_until = null, updated_at = ? where id = ?",
      )
      .run(nextRunAt, finishedAt, updatedAt, loop.id)
    return { ...loop, nextRunAt, lastFinishedAt: finishedAt, runCount: loop.runCount + 1, leaseOwner: null, leaseUntil: null, updatedAt }
  }

  setNextRunAt(loopId: string, nextRunAt: number): LoopRow | undefined {
    const timestamp = now()
    this.db.query("update loops set next_run_at = ?, run_requested = 0, lease_owner = null, lease_until = null, updated_at = ? where id = ?").run(nextRunAt, timestamp, loopId)
    return this.getLoop(loopId)
  }

  postponeUnclaimedLoop(loopId: string, nextRunAt: number): LoopRow | undefined {
    const timestamp = now()
    this.db
      .query(
        `update loops
         set next_run_at = ?, run_requested = 0, updated_at = ?
         where id = ?
           and status = 'active'
           and (lease_until is null or lease_until <= ?)
           and not exists (select 1 from loop_runs where loop_id = ? and status = 'running')`,
      )
      .run(nextRunAt, timestamp, loopId, timestamp, loopId)
    return this.getLoop(loopId)
  }

  requestRunNow(loopId: string): LoopRow | undefined {
    const timestamp = now()
    this.db
      .query(
        `update loops
         set next_run_at = case when lease_owner is null then ? else next_run_at end,
             run_requested = case when lease_owner is null then run_requested else 1 end,
             updated_at = ?
         where id = ? and status = 'active'`,
      )
      .run(timestamp, timestamp, loopId)
    return this.getLoop(loopId)
  }

  claimDueRun(loopId: string, owner: string, leaseMs: number): LoopClaim | undefined {
    return this.transaction(() => {
      const timestamp = now()
      const loop = this.getLoop(loopId)
      if (!loop || loop.status !== "active" || !loop.nextRunAt || loop.nextRunAt > timestamp) return undefined

      const result = this.db
        .query(
          `update loops
           set next_run_at = null, lease_owner = ?, lease_until = ?, updated_at = ?
           where id = ?
             and status = 'active'
             and next_run_at is not null
             and next_run_at <= ?
             and (lease_until is null or lease_until <= ?)
             and not exists (
               select 1 from loop_runs
               where loop_runs.session_id = loops.session_id
                 and loop_runs.status = 'running'
             )`,
        )
        .run(owner, timestamp + leaseMs, timestamp, loopId, timestamp, timestamp)
      if (result.changes !== 1) return undefined

      const claimed = this.getLoop(loopId)
      if (!claimed) throw new Error(`claimed loop ${loopId} disappeared`)
      const run = this.createRun(claimed, loop.nextRunAt)
      return { loop: claimed, run, scheduledFor: loop.nextRunAt }
    })
  }

  releaseLease(loopId: string) {
    const timestamp = now()
    this.db.query("update loops set lease_owner = null, lease_until = null, updated_at = ? where id = ?").run(timestamp, loopId)
  }

  pauseLoop(loopId: string) {
    const timestamp = now()
    this.db
      .query("update loops set status = 'paused', next_run_at = null, run_requested = 0, lease_owner = null, lease_until = null, updated_at = ? where id = ?")
      .run(timestamp, loopId)
  }

  activateLoop(loopId: string) {
    const timestamp = now()
    this.db
      .query("update loops set status = 'active', next_run_at = null, run_requested = 0, lease_owner = null, lease_until = null, updated_at = ? where id = ?")
      .run(timestamp, loopId)
  }

  cancelLoops(sessionId: string, target?: string): LoopRow[] {
    const loops = this.matchLoops(sessionId, target).filter((loop) => loop.status === "active" || loop.status === "paused")
    const timestamp = now()
    const update = this.db.query("update loops set status = 'cancelled', next_run_at = null, run_requested = 0, lease_owner = null, lease_until = null, updated_at = ? where id = ?")
    for (const loop of loops) update.run(timestamp, loop.id)
    return loops.map((loop) => ({ ...loop, status: "cancelled", nextRunAt: null, leaseOwner: null, leaseUntil: null, updatedAt: timestamp }))
  }

  cancelRunningRuns(loops: LoopRow[], error = "loop cancelled"): RunRow[] {
    const cancelled: RunRow[] = []
    for (const loop of loops) {
      const run = this.getRunningRunForLoop(loop.id)
      if (!run) continue
      cancelled.push(this.finishRun(run, "cancelled", error))
    }
    return cancelled
  }

  resumeLoops(sessionId: string, target?: string): LoopRow[] {
    const loops = this.matchLoops(sessionId, target).filter((loop) => loop.status === "paused")
    const timestamp = now()
    const update = this.db.query("update loops set status = 'active', next_run_at = null, run_requested = 0, lease_owner = null, lease_until = null, updated_at = ? where id = ?")
    for (const loop of loops) update.run(timestamp, loop.id)
    return loops.map((loop) => ({ ...loop, status: "active", nextRunAt: null, leaseOwner: null, leaseUntil: null, updatedAt: timestamp }))
  }

  findLoops(sessionId: string, target?: string, activeOnly = false): LoopRow[] {
    const loops = this.getSessionLoops(sessionId, activeOnly)
    const clean = target?.trim().toLowerCase()
    if (!clean || clean === "all" || clean === "current" || clean === "current loops") return loops
    return loops.filter((loop) => loop.id.toLowerCase().includes(clean) || shortId(loop.id).includes(clean) || loop.prompt.toLowerCase().includes(clean))
  }

  checkpointForShutdown(owner: string) {
    const timestamp = now()
    this.db
      .query(
        `update loop_runs
         set status = 'failed', finished_at = ?, duration_ms = case when started_at is not null then ? - started_at else null end, error = ?, updated_at = ?
         where status = 'running'
           and loop_id in (select id from loops where lease_owner = ?)`,
      )
      .run(timestamp, timestamp, "opencode exited mid-run", timestamp, owner)
    this.db
      .query(
        "update loops set lease_owner = null, lease_until = null, next_run_at = coalesce(next_run_at, ?), updated_at = ? where status = 'active' and lease_owner = ?",
      )
      .run(timestamp, timestamp, owner)
  }

  private matchLoops(sessionId: string, target?: string): LoopRow[] {
    const loops = this.getSessionLoops(sessionId)
    const clean = target?.trim().toLowerCase()
    if (!clean || clean === "all" || clean === "current" || clean === "current loops") return loops
    return loops.filter((loop) => loop.id.toLowerCase().includes(clean) || shortId(loop.id).includes(clean) || loop.prompt.toLowerCase().includes(clean))
  }

  private migrate() {
    this.transaction(() => {
      this.db.exec(`
        create table if not exists loops (
          id text primary key,
          session_id text not null,
          prompt text not null,
          prompt_key text not null default '',
          interval_ms integer not null,
          auto_compact integer not null default 0,
          run_requested integer not null default 0,
          status text not null,
          created_at integer not null,
          updated_at integer not null,
          next_run_at integer,
          last_run_id text,
          last_started_at integer,
          last_finished_at integer,
          run_count integer not null default 0,
          lease_owner text,
          lease_until integer
        );

        create index if not exists loops_session_status_idx on loops (session_id, status);
        create index if not exists loops_next_run_idx on loops (next_run_at);

        create table if not exists loop_runs (
          id text primary key,
          loop_id text not null,
          session_id text not null,
          scheduled_for integer not null,
          started_at integer,
          finished_at integer,
          duration_ms integer,
          status text not null,
          error text,
          created_at integer not null,
          updated_at integer not null,
          foreign key (loop_id) references loops(id) on delete cascade
        );

        create index if not exists loop_runs_session_status_idx on loop_runs (session_id, status);
        create index if not exists loop_runs_loop_created_idx on loop_runs (loop_id, created_at);
      `)
      this.addColumnIfMissing("loops", "lease_owner", "text")
      this.addColumnIfMissing("loops", "lease_until", "integer")
      this.addColumnIfMissing("loops", "auto_compact", "integer not null default 0")
      this.addColumnIfMissing("loops", "prompt_key", "text not null default ''")
      this.addColumnIfMissing("loops", "run_requested", "integer not null default 0")

      const loops = this.db.query("select id, prompt from loops").all() as Array<{ id: string; prompt: string }>
      const updatePromptKey = this.db.query("update loops set prompt_key = ? where id = ?")
      for (const loop of loops) updatePromptKey.run(normalizeLoopPrompt(loop.prompt), loop.id)
      this.reconcileRunningRuns()
      this.deduplicateLiveLoops()

      this.db.exec(`
        create unique index if not exists loops_one_live_identity_idx
          on loops (session_id, prompt_key, interval_ms)
          where status in ('active', 'paused');
        create unique index if not exists loop_runs_one_running_loop_idx
          on loop_runs (loop_id)
          where status = 'running';
        create unique index if not exists loop_runs_one_running_session_idx
          on loop_runs (session_id)
          where status = 'running';
      `)
    })
  }

  recoverStaleRuns(): LoopRow[] {
    const timestamp = now()
    const running = this.db
      .query(
        `select loop_runs.*
         from loop_runs
         left join loops on loops.id = loop_runs.loop_id
         where loop_runs.status = 'running'
           and (loops.id is null or loops.lease_until is null or loops.lease_until <= ?)`,
      )
      .all(timestamp)
      .map(runFromRow)
    for (const run of running) {
      const durationMs = run.startedAt ? timestamp - run.startedAt : null
      this.db
        .query("update loop_runs set status = 'failed', finished_at = ?, duration_ms = ?, error = ?, updated_at = ? where id = ?")
        .run(timestamp, durationMs, "loop lease expired before run completed", timestamp, run.id)
    }

    this.db
      .query(
        `update loops
         set next_run_at = ?, lease_owner = null, lease_until = null, updated_at = ?
          where status = 'active'
            and next_run_at is null
            and (lease_until is null or lease_until <= ?)
            and not exists (
             select 1 from loop_runs
             where loop_runs.loop_id = loops.id
               and loop_runs.status = 'running'
           )`,
      )
      .run(timestamp, timestamp, timestamp)
    return this.getActiveLoops()
  }

  private deduplicateLiveLoops() {
    const loops = this.db
      .query(
        `select * from loops
         where status in ('active', 'paused')
         order by exists(select 1 from loop_runs where loop_runs.loop_id = loops.id and loop_runs.status = 'running') desc,
                  case status when 'active' then 0 else 1 end, created_at, id`,
      )
      .all()
      .map(loopFromRow)
    const seen = new Set<string>()
    const timestamp = now()
    const cancel = this.db.query(
      "update loops set status = 'cancelled', next_run_at = null, run_requested = 0, lease_owner = null, lease_until = null, updated_at = ? where id = ?",
    )
    for (const loop of loops) {
      const key = `${loop.sessionId}\0${loop.promptKey}\0${loop.intervalMs}`
      if (!seen.has(key)) {
        seen.add(key)
        continue
      }
      cancel.run(timestamp, loop.id)
    }
  }

  private reconcileRunningRuns() {
    const runs = this.db
      .query("select * from loop_runs where status = 'running' order by started_at desc, id")
      .all()
      .map(runFromRow)
    const loopIds = new Set<string>()
    const sessionIds = new Set<string>()
    const timestamp = now()
    for (const run of runs) {
      if (!loopIds.has(run.loopId) && !sessionIds.has(run.sessionId)) {
        loopIds.add(run.loopId)
        sessionIds.add(run.sessionId)
        continue
      }
      this.db
        .query("update loop_runs set status = 'failed', finished_at = ?, duration_ms = case when started_at is not null then ? - started_at else null end, error = ?, updated_at = ? where id = ?")
        .run(timestamp, timestamp, "superseded during migration", timestamp, run.id)
      if (!loopIds.has(run.loopId)) {
        this.db
          .query("update loops set next_run_at = ?, lease_owner = null, lease_until = null, updated_at = ? where id = ? and status = 'active'")
          .run(timestamp, timestamp, run.loopId)
      }
    }
  }

  private addColumnIfMissing(table: string, column: string, definition: string) {
    const columns = this.db.query(`pragma table_info(${table})`).all() as Array<{ name: string }>
    if (columns.some((item) => item.name === column)) return
    this.db.exec(`alter table ${table} add column ${column} ${definition}`)
  }

  private transaction<T>(callback: () => T): T {
    this.db.exec("begin immediate")
    try {
      const result = callback()
      this.db.exec("commit")
      return result
    } catch (error) {
      this.db.exec("rollback")
      throw error
    }
  }
}

function makeTextPart(text: string): any {
  return { type: "text", text }
}

function commandResult(text: string) {
  return [
    "You are displaying a `/loop` command result.",
    "Reply with the result below only. Do not use tools.",
    "",
    text,
  ].join("\n")
}

function smartLoopPrompt(userText: string, commandName: string) {
  return [
    "You are interpreting a fuzzy `/loop` command for this same OpenCode session.",
    "The user text may contain typos or casual wording. Infer intent only when clear.",
    "",
    "Available actions:",
    "- Create a loop: call `loop_start` with `every` and `prompt`.",
    "- If the user asks for compaction after each run, pass `autoCompact: true`.",
    "- Cancel loops: call `loop_cancel`.",
    "- Show status: call `loop_status`.",
    "- Resume paused loops: call `loop_resume`.",
    "- Run an iteration now, skipping the remaining wait: call `loop_now`.",
    "",
    "Rules:",
    "- Loops are tied to the current session.",
    "- Fixed-delay semantics: next run starts interval after prior run finishes.",
    "- Auto-compact means compact this session after each verified iteration.",
    "- If timing is unclear, ask one short clarification instead of guessing.",
    "- If task text is unclear, ask one short clarification instead of guessing.",
    `- Examples: \`/${commandName} remember every day to check this\` => every \"1 day\", prompt \"check this\".`,
    "",
    "User text:",
    userText,
  ].join("\n")
}

function iterationPrompt(loop: LoopRow, run: RunRow) {
  return [
    "<loop_iteration>",
    `loop_id: ${loop.id}`,
    `run_id: ${run.id}`,
    `session_id: ${loop.sessionId}`,
    `iteration: ${loop.runCount + 1}`,
    `interval: ${formatDuration(loop.intervalMs)}`,
    `scheduled_for: ${iso(run.scheduledFor)}`,
    `started_at: ${iso(run.startedAt)}`,
    "task:",
    loop.prompt,
    "</loop_iteration>",
    "",
    "Run exactly one iteration of this recurring loop.",
    "The plugin schedules the next iteration after this turn finishes; do not schedule it yourself.",
    "Use current project/session context. Verify actual state before claiming success.",
    `End the turn with \`[loop:evidence:${run.id}]\` plus a one-line proof of what this iteration did. The marker is for readability only; finishing the turn completes the iteration whether or not it is present.`,
    "Only if you cannot proceed without user input, explain the blocker and end with `[loop:blocked]`; the plugin will pause this loop until resumed.",
  ].join("\n")
}

function formatLoop(loop: LoopRow) {
  const autoCompact = loop.autoCompact ? "; auto-compact on" : ""
  return [
    `- ${shortId(loop.id)} ${loop.status} every ${formatDuration(loop.intervalMs)}${autoCompact}`,
    `  prompt: ${loop.prompt}`,
    `  runs: ${loop.runCount}; last start: ${iso(loop.lastStartedAt)}; last finish: ${iso(loop.lastFinishedAt)}`,
    `  next: ${iso(loop.nextRunAt)} (${relativeTime(loop.nextRunAt)})`,
  ].join("\n")
}

function parseToolInterval(every: string | undefined, intervalMs: number | undefined) {
  if (intervalMs !== undefined) {
    if (!Number.isFinite(intervalMs) || intervalMs <= 0) throw new Error("intervalMs must be a positive number")
    return { intervalMs: Math.round(intervalMs), intervalText: formatDuration(Math.round(intervalMs)) }
  }
  if (!every) throw new Error("Provide `every` like `5 mins`, `daily`, or `1 day`.")
  const parsed = parseInterval(every)
  if (!parsed) throw new Error("Could not parse interval. Use `5 mins`, `1 hour`, `daily`, or `1 day`.")
  return parsed
}

function statusText(store: LoopStore, sessionId: string) {
  const loops = store.getSessionLoops(sessionId)
  if (!loops.length) return "No loops in this session. Start one with `/loop every 5 mins check this`."
  const active = loops.filter((loop) => loop.status === "active")
  const inactive = loops.filter((loop) => loop.status !== "active")
  const lines = [`Loops for this session (${sessionId}):`]
  if (active.length) lines.push("", "Active:", ...active.map(formatLoop))
  if (inactive.length) lines.push("", "Inactive:", ...inactive.map(formatLoop))
  const recent = store.getRecentRuns(sessionId, 5)
  if (recent.length) {
    lines.push(
      "",
      "Recent runs:",
      ...recent.map(
        (run) =>
          `- ${shortId(run.id)} ${run.status}; scheduled ${iso(run.scheduledFor)}; started ${iso(run.startedAt)}; finished ${iso(run.finishedAt)}; duration ${run.durationMs === null ? "n/a" : formatDuration(run.durationMs)}`,
      ),
    )
  }
  return lines.join("\n")
}

function isIdleEvent(event: any) {
  return event?.type === "session.idle" || (event?.type === "session.status" && event?.properties?.status?.type === "idle")
}

function getSessionId(event: any) {
  return event?.properties?.sessionID || event?.properties?.info?.sessionID || event?.properties?.info?.id || event?.properties?.status?.sessionID
}

function isSessionDeletedEvent(event: any) {
  return event?.type === "session.deleted"
}

function isSessionErrorEvent(event: any) {
  return event?.type === "session.error"
}

function isBlocked(text: string) {
  return /(^|\n)\s*(?:\[loop:blocked\]|loop:blocked)\s*$/i.test(text.trimEnd())
}

function sdkErrorMessage(response: any) {
  const error = response && typeof response === "object" ? response.error : undefined
  if (!error) return undefined
  if (typeof error === "string") return error
  if (typeof error.message === "string") return error.message
  try {
    return JSON.stringify(error)
  } catch {
    return String(error)
  }
}

function assertNoSDKError<T>(response: T): T {
  const error = sdkErrorMessage(response)
  if (error) throw new Error(error)
  return response
}

function messagesFromResponse(response: any): any[] {
  assertNoSDKError(response)
  const data = response?.data ?? response
  if (Array.isArray(data)) return data
  if (Array.isArray(data?.messages)) return data.messages
  if (Array.isArray(data?.data)) return data.data
  return []
}

async function promptSession(client: any, sessionId: string, text: string) {
  const session = client.session
  if (typeof session?.promptAsync === "function") {
    return assertNoSDKError(await session.promptAsync({ path: { id: sessionId }, body: { parts: [makeTextPart(text)] } }))
  }
  if (typeof session?.prompt === "function") {
    try {
      return assertNoSDKError(await session.prompt({ path: { id: sessionId }, body: { parts: [makeTextPart(text)] } }))
    } catch (error) {
      return assertNoSDKError(await session.prompt({ sessionID: sessionId, parts: [makeTextPart(text)] }))
    }
  }
  throw new Error("OpenCode session prompt API not found")
}

async function sessionIsIdle(client: any, sessionId: string) {
  const session = client.session
  if (typeof session?.status !== "function") return true
  const response = assertNoSDKError(await session.status()) as any
  const data = response?.data ?? response
  const statuses = data?.statuses ?? data
  const status = statuses?.[sessionId]
  return !status || status.type === "idle"
}

async function latestAssistantText(client: any, sessionId: string) {
  const session = client.session
  if (typeof session?.messages !== "function") throw new Error("OpenCode session messages API not found")
  let messages: any[] = []
  try {
    const response = await session.messages({ path: { id: sessionId } })
    messages = messagesFromResponse(response)
  } catch (error) {
    try {
      const response = await session.messages({ sessionID: sessionId })
      messages = messagesFromResponse(response)
    } catch {
      throw error
    }
  }

  for (const message of [...messages].reverse()) {
    const role = message.info?.role ?? message.role
    if (role !== "assistant") continue
    return (message.parts ?? [])
      .filter((part: any) => part.type === "text" && typeof part.text === "string")
      .map((part: any) => part.text)
      .join("\n")
      .trim()
  }
  return ""
}

function modelFromMessage(message: any) {
  const info = message?.info ?? message
  const model = info?.model
  if (typeof model?.providerID === "string" && typeof model?.modelID === "string") return model
  if (typeof info?.providerID === "string" && typeof info?.modelID === "string") {
    return { providerID: info.providerID, modelID: info.modelID }
  }
  return undefined
}

async function latestSessionModel(client: any, sessionId: string) {
  const session = client.session
  if (typeof session?.messages !== "function") throw new Error("OpenCode session messages API not found")
  let messages: any[] = []
  try {
    const response = await session.messages({ path: { id: sessionId } })
    messages = messagesFromResponse(response)
  } catch (error) {
    try {
      const response = await session.messages({ sessionID: sessionId })
      messages = messagesFromResponse(response)
    } catch {
      throw error
    }
  }

  for (const message of [...messages].reverse()) {
    const model = modelFromMessage(message)
    if (model) return model
  }
  throw new Error("OpenCode session model not found")
}

async function abortSession(client: any, sessionId: string) {
  const session = client.session
  if (typeof session?.abort !== "function") return false
  try {
    assertNoSDKError(await session.abort({ path: { id: sessionId } }))
    return true
  } catch (error) {
    assertNoSDKError(await session.abort({ sessionID: sessionId }))
    return true
  }
}

async function compactSession(client: any, sessionId: string) {
  const session = client.session
  if (typeof session?.summarize !== "function") throw new Error("OpenCode session summarize API not found")
  const model = await latestSessionModel(client, sessionId)
  return assertNoSDKError(await session.summarize({ path: { id: sessionId }, body: model }))
}

function resolveAutoCompact(value: boolean | undefined, fallback: boolean) {
  return value ?? fallback
}

export const LoopPlugin: Plugin = async ({ client }, options?: LoopPluginOptions) => {
  const instanceId = makeId("instance")
  const commandName = options?.commandName?.replace(/^\//, "") || "loop"
  const minIntervalMs = Math.max(1_000, options?.minIntervalMs ?? 1_000)
  const defaultAutoCompact = options?.autoCompact ?? false
  const store = new LoopStore(options?.dbPath || defaultDbPath())
  const timers = new Map<string, ReturnType<typeof setTimeout>>()
  const pendingRuns = new Set<Promise<void>>()
  const eventWaiters: Array<() => void> = []
  let activeEvents = 0
  let disposing = false
  let closed = false

  function clearLoopTimer(loopId: string) {
    const timer = timers.get(loopId)
    if (timer) clearTimeout(timer)
    timers.delete(loopId)
  }

  function scheduleLoop(loop: LoopRow) {
    if (disposing || closed) return
    clearLoopTimer(loop.id)
    if (loop.status !== "active") return
    const wakeAt = loop.leaseUntil ?? loop.nextRunAt
    if (!wakeAt) return
    const delay = Math.max(0, wakeAt - now())
    const timer = setTimeout(
      () => {
        timers.delete(loop.id)
        if (delay > MAX_TIMEOUT_MS) {
          const latest = store.getLoop(loop.id)
          if (latest) scheduleLoop(latest)
          return
        }
        if (loop.nextRunAt) {
          trackRun(startScheduledRun(loop.id))
          return
        }
        for (const recovered of store.recoverStaleRuns()) scheduleLoop(recovered)
      },
      Math.min(delay, MAX_TIMEOUT_MS),
    )
    timers.set(loop.id, timer)
  }

  function queueLoop(loopId: string) {
    const queued = store.requestRunNow(loopId)
    if (queued) scheduleLoop(queued)
    return queued
  }

  function trackRun(run: Promise<void>) {
    pendingRuns.add(run)
    void run.finally(() => pendingRuns.delete(run))
  }

  async function waitForEvents() {
    if (activeEvents === 0) return
    await new Promise<void>((resolve) => eventWaiters.push(resolve))
  }

  function scheduleDueSessionLoops(sessionId: string) {
    for (const loop of store.getSessionLoops(sessionId, true)) {
      if (loop.nextRunAt && loop.nextRunAt <= now()) scheduleLoop(loop)
    }
  }

  function cancelSessionLoops(sessionId: string, reason = "loop cancelled") {
    const cancelled = store.cancelLoops(sessionId)
    const cancelledRuns = store.cancelRunningRuns(cancelled, reason)
    for (const loop of cancelled) clearLoopTimer(loop.id)
    return { cancelled, cancelledRuns }
  }

  async function startScheduledRun(loopId: string) {
    if (disposing || closed) return
    const loop = store.getLoop(loopId)
    if (!loop || loop.status !== "active") return

    try {
      if (!(await sessionIsIdle(client, loop.sessionId))) {
        const postponed = store.postponeUnclaimedLoop(loop.id, now() + SESSION_BUSY_RETRY_MS)
        if (postponed) scheduleLoop(postponed)
        return
      }
    } catch (error) {
      console.warn(`opencode-loop status check failed for ${shortId(loop.id)}: ${error instanceof Error ? error.message : String(error)}`)
      const postponed = store.postponeUnclaimedLoop(loop.id, now() + SESSION_BUSY_RETRY_MS)
      if (postponed) scheduleLoop(postponed)
      return
    }

    if (store.getRunningRuns(loop.sessionId).length > 0) {
      const postponed = store.postponeUnclaimedLoop(loop.id, now() + SESSION_BUSY_RETRY_MS)
      if (postponed) scheduleLoop(postponed)
      return
    }

    const claim = store.claimDueRun(loop.id, instanceId, RUN_LEASE_MS)
    if (!claim) {
      const latest = store.getLoop(loop.id)
      if (!latest || latest.status !== "active") return
      if (latest.nextRunAt && latest.nextRunAt <= now()) {
        const postponed = store.postponeUnclaimedLoop(latest.id, now() + SESSION_BUSY_RETRY_MS)
        if (postponed) scheduleLoop(postponed)
        return
      }
      scheduleLoop(latest)
      return
    }

    const run = claim.run
    try {
      await promptSession(client, claim.loop.sessionId, iterationPrompt(claim.loop, run))
      const latest = store.getLoop(claim.loop.id)
      // Keep a lease-expiry watchdog armed in case OpenCode never emits idle/error.
      if (latest) scheduleLoop(latest)
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      const failedAt = now()
      const failed = store.finishRun(run, "failed", message)
      const latest = store.getLoop(claim.loop.id)
      if (!latest || latest.status !== "active") return
      scheduleLoop(store.scheduleNext(latest, failed?.finishedAt ?? failedAt))
    }
  }

  for (const loop of store.getActiveLoops()) scheduleLoop(loop)

  const checkpoint = () => {
    if (closed) return
    store.checkpointForShutdown(instanceId)
  }
  process.once("exit", checkpoint)

  return {
    tool: {
      loop_start: tool({
        description:
          "Start a session-tied fixed-delay loop when the user clearly asks to repeat or remember a task on an interval. The first iteration is queued immediately.",
        args: {
          prompt: tool.schema.string().describe("The task to run each iteration. Do not include the interval words."),
          every: tool.schema.string().optional().describe("Natural interval, e.g. '5 mins', '1 hour', 'daily', '1 day'."),
          intervalMs: tool.schema.number().optional().describe("Interval in milliseconds. Use only if already known."),
          autoCompact: tool.schema.boolean().optional().describe("Compact this session after each verified iteration."),
        },
        async execute(args, context) {
          const interval = parseToolInterval(args.every, args.intervalMs)
          if (interval.intervalMs < minIntervalMs) return `Interval too short. Minimum is ${formatDuration(minIntervalMs)}.`
          const prompt = args.prompt.trim()
          if (!prompt) return "Missing loop prompt."
          const autoCompact = resolveAutoCompact(args.autoCompact, defaultAutoCompact)

          const result = store.createOrGetLoop({ sessionId: context.sessionID, prompt, intervalMs: interval.intervalMs, autoCompact })
          if (!result.created) {
            let existing = result.loop
            if (existing.status === "paused") {
              store.activateLoop(existing.id)
              existing = store.getLoop(existing.id) ?? existing
            }
            if (args.autoCompact !== undefined && existing.autoCompact !== autoCompact) store.setAutoCompact(existing.id, autoCompact)
            queueLoop(existing.id)
            return `Loop ${shortId(existing.id)} already exists. Next iteration queued.${autoCompact ? " Auto-compact on." : ""}`
          }

          const loop = result.loop
          queueLoop(loop.id)
          return `Started loop ${shortId(loop.id)} every ${formatDuration(loop.intervalMs)}${autoCompact ? " with auto-compact" : ""}. First iteration will run when this session becomes idle.`
        },
      }),

      loop_status: tool({
        description: "Show session-tied OpenCode loop status for the current session.",
        args: {},
        async execute(_args, context) {
          return statusText(store, context.sessionID)
        },
      }),

      loop_cancel: tool({
        description: "Cancel active or paused loops in the current OpenCode session when the user asks to stop/cancel/remove loops.",
        args: {
          target: tool.schema.string().optional().describe("Optional loop short id or prompt text. Omit to cancel all current session loops."),
        },
        async execute(args, context) {
          const cancelled = store.cancelLoops(context.sessionID, args.target)
          const cancelledRuns = store.cancelRunningRuns(cancelled)
          for (const loop of cancelled) clearLoopTimer(loop.id)
          if (!cancelled.length) return "No active or paused loops matched in this session."
          if (cancelledRuns.length) {
            try {
              await abortSession(client, context.sessionID)
            } catch (error) {
              return `Cancelled ${cancelled.length} loop(s), but abort failed: ${error instanceof Error ? error.message : String(error)}`
            }
          }
          return `Cancelled ${cancelled.length} loop(s): ${cancelled.map((loop) => shortId(loop.id)).join(", ")}.`
        },
      }),

      loop_resume: tool({
        description: "Resume paused loops in the current OpenCode session when the user asks to resume/restart loops.",
        args: {
          target: tool.schema.string().optional().describe("Optional loop short id or prompt text. Omit to resume all paused current session loops."),
        },
        async execute(args, context) {
          const resumed = store.resumeLoops(context.sessionID, args.target)
          if (!resumed.length) return "No paused loops matched in this session."
          for (const loop of resumed) queueLoop(loop.id)
          return `Resumed ${resumed.length} loop(s). Next iteration will run when this session becomes idle.`
        },
      }),

      loop_now: tool({
        description:
          "Immediately queue the next iteration of an active loop in the current session, skipping the remaining wait, when the user asks to run/trigger/pick up the loop now.",
        args: {
          target: tool.schema.string().optional().describe("Optional loop short id or prompt text. Omit to trigger all active current session loops."),
        },
        async execute(args, context) {
          const loops = store.findLoops(context.sessionID, args.target, true)
          if (!loops.length) return "No active loops matched in this session."
          for (const loop of loops) queueLoop(loop.id)
          return `Triggering ${loops.length} loop(s) now: ${loops.map((loop) => shortId(loop.id)).join(", ")}. Next iteration runs when this session becomes idle.`
        },
      }),
    },

    dispose: async () => {
      process.off("exit", checkpoint)
      disposing = true
      for (const timer of timers.values()) clearTimeout(timer)
      timers.clear()
      await Promise.allSettled([...pendingRuns])
      await waitForEvents()
      checkpoint()
      closed = true
      store.close()
    },

    config: async (cfg: Config) => {
      cfg.command ??= {}
      cfg.command[commandName] ??= {
        description: "Create, inspect, resume, and cancel session-tied loops.",
        template: "$ARGUMENTS",
      }
    },

    "command.execute.before": async (input, output) => {
      if (input.command !== commandName) return
      const sessionId = input.sessionID
      const parsed = parseLoopCommand(input.arguments || "")

      if (input.arguments?.trim().toLowerCase().match(/^(exit|quit|q)$/)) {
        const { cancelled } = cancelSessionLoops(sessionId, "loop exited")
        output.parts = [
          makeTextPart(
            commandResult(
              cancelled.length
                ? `Cancelled ${cancelled.length} loop(s) in this session: ${cancelled.map((loop) => shortId(loop.id)).join(", ")}.`
                : "No active loops matched in this session.",
            ),
          ),
        ]
        return
      }

      if (parsed.type === "unknown") {
        output.parts = [makeTextPart(smartLoopPrompt(input.arguments || "", commandName))]
        return
      }

      if (parsed.type === "status" || parsed.type === "list") {
        output.parts = [makeTextPart(commandResult(statusText(store, sessionId)))]
        return
      }

      if (parsed.type === "cancel") {
        const cancelled = store.cancelLoops(sessionId, parsed.target)
        const cancelledRuns = store.cancelRunningRuns(cancelled)
        for (const loop of cancelled) clearLoopTimer(loop.id)
        let abortError: string | undefined
        if (cancelledRuns.length) {
          try {
            await abortSession(client, sessionId)
          } catch (error) {
            abortError = error instanceof Error ? error.message : String(error)
          }
        }
        output.parts = [
          makeTextPart(
            commandResult(
              cancelled.length
                ? `Cancelled ${cancelled.length} loop(s) in this session: ${cancelled.map((loop) => shortId(loop.id)).join(", ")}. ${abortError ? `Abort failed: ${abortError}` : "Future iterations will not run."}`
                : "No active loops matched in this session.",
            ),
          ),
        ]
        return
      }

      if (parsed.type === "resume") {
        const resumed = store.resumeLoops(sessionId, parsed.target)
        if (!resumed.length) {
          output.parts = [makeTextPart(commandResult("No paused loops matched in this session."))]
          return
        }
        for (const loop of resumed) queueLoop(loop.id)
        output.parts = [makeTextPart(commandResult(`Resumed ${resumed.length} loop(s). Next iteration queued now.`))]
        return
      }

      if (parsed.type === "now") {
        const loops = store.findLoops(sessionId, parsed.target, true)
        if (!loops.length) {
          output.parts = [makeTextPart(commandResult("No active loops matched in this session."))]
          return
        }
        for (const loop of loops) queueLoop(loop.id)
        output.parts = [
          makeTextPart(
            commandResult(
              `Triggering ${loops.length} loop(s) now: ${loops.map((loop) => shortId(loop.id)).join(", ")}. Next iteration runs when this session becomes idle.`,
            ),
          ),
        ]
        return
      }

      if (parsed.intervalMs < minIntervalMs) {
        output.parts = [makeTextPart(commandResult(`Interval too short. Minimum is ${formatDuration(minIntervalMs)}.`))]
        return
      }

      const result = store.createOrGetLoop({ sessionId, prompt: parsed.prompt, intervalMs: parsed.intervalMs, autoCompact: resolveAutoCompact(parsed.autoCompact, defaultAutoCompact) })
      if (!result.created) {
        let existing = result.loop
        if (existing.status === "paused") {
          store.activateLoop(existing.id)
          existing = store.getLoop(existing.id) ?? existing
        }
        const autoCompact = resolveAutoCompact(parsed.autoCompact, existing.autoCompact)
        if (parsed.autoCompact !== undefined && existing.autoCompact !== autoCompact) store.setAutoCompact(existing.id, autoCompact)
        queueLoop(existing.id)
        output.parts = [makeTextPart(commandResult(`Loop ${shortId(existing.id)} already exists. Next iteration queued.${autoCompact ? " Auto-compact on." : ""}`))]
        return
      }

      const autoCompact = resolveAutoCompact(parsed.autoCompact, defaultAutoCompact)
      const loop = result.loop
      queueLoop(loop.id)
      output.parts = [makeTextPart(commandResult(`Started loop ${shortId(loop.id)} every ${formatDuration(loop.intervalMs)}${autoCompact ? " with auto-compact" : ""}. First iteration queued now.`))]
    },

    event: async ({ event }) => {
      if (disposing || closed) return
      activeEvents++
      try {
      if (isSessionDeletedEvent(event)) {
        const sessionId = getSessionId(event)
        if (!sessionId) return
        cancelSessionLoops(sessionId, "session deleted")
        return
      }

      if (isSessionErrorEvent(event)) {
        const sessionId = getSessionId(event)
        if (!sessionId) return
        const message = sdkErrorMessage(event.properties) || "OpenCode session error"
        for (const run of store.getRunningRuns(sessionId)) {
          store.finishRun(run, "failed", message)
          const loop = store.getLoop(run.loopId)
          if (!loop || loop.status !== "active") continue
          const retry = store.setNextRunAt(loop.id, now() + SESSION_BUSY_RETRY_MS)
          if (retry) scheduleLoop(retry)
        }
        return
      }

      if (!isIdleEvent(event)) return
      const sessionId = getSessionId(event)
      if (!sessionId) return

      const running = store.getRunningRuns(sessionId)
      if (!running.length) {
        scheduleDueSessionLoops(sessionId)
        return
      }

      let latestText = ""
      let latestError: string | undefined
      try {
        latestText = await latestAssistantText(client, sessionId)
      } catch (error) {
        latestError = error instanceof Error ? error.message : String(error)
      }

      for (const run of running) {
        const loop = store.getLoop(run.loopId)
        if (!loop) {
          store.finishRun(run, "failed", "loop was deleted")
          continue
        }
        if (loop.status !== "active") {
          store.finishRun(run, "cancelled", "loop is no longer active")
          clearLoopTimer(loop.id)
          continue
        }

        if (latestError) {
          store.finishRun(run, "unverified", `could not inspect assistant response: ${latestError}`)
          store.pauseLoop(loop.id)
          clearLoopTimer(loop.id)
          continue
        }

        // Claude-parity: a finished turn IS a finished iteration. No evidence required.
        // Only an explicit blocked marker pauses the loop (model signalled it needs input).
        if (isBlocked(latestText)) {
          store.finishRun(run, "blocked")
          store.pauseLoop(loop.id)
          clearLoopTimer(loop.id)
          continue
        }

        const finished = store.finishRun(run, "completed")
        const latest = store.getLoop(loop.id)
        if (!latest || latest.status !== "active") continue
        const scheduled = store.scheduleNext(latest, finished.finishedAt ?? now())
        scheduleLoop(scheduled)
        if (scheduled.autoCompact) {
          try {
            await compactSession(client, sessionId)
          } catch (error) {
            console.warn(`opencode-loop auto-compact failed for ${shortId(loop.id)}: ${error instanceof Error ? error.message : String(error)}`)
          }
        }
      }
      scheduleDueSessionLoops(sessionId)
      } finally {
        activeEvents--
        if (activeEvents === 0) {
          for (const resolve of eventWaiters.splice(0)) resolve()
        }
      }
    },
  }
}

export default LoopPlugin
