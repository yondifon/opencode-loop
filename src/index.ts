import { Database } from "bun:sqlite"
import { tool, type Config, type Plugin } from "@opencode-ai/plugin"
import { existsSync, mkdirSync } from "node:fs"
import { homedir } from "node:os"
import { dirname, join } from "node:path"
import { formatDuration, parseInterval, parseLoopCommand } from "./parser"

type LoopStatus = "active" | "cancelled" | "paused"
type RunStatus = "running" | "completed" | "blocked" | "failed" | "cancelled"

type LoopRow = {
  id: string
  sessionId: string
  prompt: string
  intervalMs: number
  status: LoopStatus
  createdAt: number
  updatedAt: number
  nextRunAt: number | null
  lastRunId: string | null
  lastStartedAt: number | null
  lastFinishedAt: number | null
  runCount: number
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

type LoopPluginOptions = {
  commandName?: string
  dbPath?: string
  minIntervalMs?: number
}

const MAX_TIMEOUT_MS = 2_147_483_647
const SESSION_BUSY_RETRY_MS = 10_000

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

function loopFromRow(row: any): LoopRow {
  return {
    id: row.id,
    sessionId: row.session_id,
    prompt: row.prompt,
    intervalMs: row.interval_ms,
    status: row.status,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    nextRunAt: row.next_run_at,
    lastRunId: row.last_run_id,
    lastStartedAt: row.last_started_at,
    lastFinishedAt: row.last_finished_at,
    runCount: row.run_count,
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
    this.failStaleRuns()
  }

  close() {
    this.db.close()
  }

  createLoop(input: Pick<LoopRow, "sessionId" | "prompt" | "intervalMs">): LoopRow {
    const timestamp = now()
    const loop: LoopRow = {
      id: makeId("loop"),
      sessionId: input.sessionId,
      prompt: input.prompt,
      intervalMs: input.intervalMs,
      status: "active",
      createdAt: timestamp,
      updatedAt: timestamp,
      nextRunAt: null,
      lastRunId: null,
      lastStartedAt: null,
      lastFinishedAt: null,
      runCount: 0,
    }
    this.db
      .query(
        `insert into loops
          (id, session_id, prompt, interval_ms, status, created_at, updated_at, next_run_at, last_run_id, last_started_at, last_finished_at, run_count)
         values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        loop.id,
        loop.sessionId,
        loop.prompt,
        loop.intervalMs,
        loop.status,
        loop.createdAt,
        loop.updatedAt,
        loop.nextRunAt,
        loop.lastRunId,
        loop.lastStartedAt,
        loop.lastFinishedAt,
        loop.runCount,
      )
    return loop
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
    const nextRunAt = finishedAt + loop.intervalMs
    const updatedAt = now()
    this.db
      .query("update loops set next_run_at = ?, last_finished_at = ?, run_count = run_count + 1, updated_at = ? where id = ?")
      .run(nextRunAt, finishedAt, updatedAt, loop.id)
    return { ...loop, nextRunAt, lastFinishedAt: finishedAt, runCount: loop.runCount + 1, updatedAt }
  }

  setNextRunAt(loopId: string, nextRunAt: number): LoopRow | undefined {
    const timestamp = now()
    this.db.query("update loops set next_run_at = ?, updated_at = ? where id = ?").run(nextRunAt, timestamp, loopId)
    return this.getLoop(loopId)
  }

  pauseLoop(loopId: string) {
    const timestamp = now()
    this.db.query("update loops set status = 'paused', next_run_at = null, updated_at = ? where id = ?").run(timestamp, loopId)
  }

  activateLoop(loopId: string) {
    const timestamp = now()
    this.db.query("update loops set status = 'active', next_run_at = null, updated_at = ? where id = ?").run(timestamp, loopId)
  }

  cancelLoops(sessionId: string, target?: string): LoopRow[] {
    const loops = this.matchLoops(sessionId, target).filter((loop) => loop.status === "active" || loop.status === "paused")
    const timestamp = now()
    const update = this.db.query("update loops set status = 'cancelled', next_run_at = null, updated_at = ? where id = ?")
    for (const loop of loops) update.run(timestamp, loop.id)
    return loops.map((loop) => ({ ...loop, status: "cancelled", nextRunAt: null, updatedAt: timestamp }))
  }

  resumeLoops(sessionId: string, target?: string): LoopRow[] {
    const loops = this.matchLoops(sessionId, target).filter((loop) => loop.status === "paused")
    const timestamp = now()
    const update = this.db.query("update loops set status = 'active', next_run_at = null, updated_at = ? where id = ?")
    for (const loop of loops) update.run(timestamp, loop.id)
    return loops.map((loop) => ({ ...loop, status: "active", nextRunAt: null, updatedAt: timestamp }))
  }

  private matchLoops(sessionId: string, target?: string): LoopRow[] {
    const loops = this.getSessionLoops(sessionId)
    const clean = target?.trim().toLowerCase()
    if (!clean || clean === "all" || clean === "current" || clean === "current loops") return loops
    return loops.filter((loop) => loop.id.toLowerCase().includes(clean) || shortId(loop.id).includes(clean) || loop.prompt.toLowerCase().includes(clean))
  }

  private migrate() {
    this.db.exec(`
      create table if not exists loops (
        id text primary key,
        session_id text not null,
        prompt text not null,
        interval_ms integer not null,
        status text not null,
        created_at integer not null,
        updated_at integer not null,
        next_run_at integer,
        last_run_id text,
        last_started_at integer,
        last_finished_at integer,
        run_count integer not null default 0
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
  }

  private failStaleRuns() {
    const timestamp = now()
    const running = this.db.query("select * from loop_runs where status = 'running'").all().map(runFromRow)
    for (const run of running) {
      const durationMs = run.startedAt ? timestamp - run.startedAt : null
      this.db
        .query("update loop_runs set status = 'failed', finished_at = ?, duration_ms = ?, error = ?, updated_at = ? where id = ?")
        .run(timestamp, durationMs, "plugin restarted before run completed", timestamp, run.id)
    }
    this.db.query("update loops set next_run_at = ? where status = 'active' and next_run_at is null").run(timestamp)
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
    "- Cancel loops: call `loop_cancel`.",
    "- Show status: call `loop_status`.",
    "- Resume paused loops: call `loop_resume`.",
    "",
    "Rules:",
    "- Loops are tied to the current session.",
    "- Fixed-delay semantics: next run starts interval after prior run finishes.",
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
    "If this iteration completes, include `[loop:evidence] ...` near the end.",
    "If user input is required, explain the blocker and end with `[loop:blocked]`; the plugin will pause this loop.",
  ].join("\n")
}

function formatLoop(loop: LoopRow) {
  return [
    `- ${shortId(loop.id)} ${loop.status} every ${formatDuration(loop.intervalMs)}`,
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
  return event?.properties?.sessionID || event?.properties?.info?.sessionID || event?.properties?.status?.sessionID
}

function isBlocked(text: string) {
  return /(^|\n)\s*(?:\[loop:blocked\]|loop:blocked)\s*$/i.test(text.trimEnd())
}

async function promptSession(client: any, sessionId: string, text: string) {
  const session = client.session
  if (typeof session?.promptAsync === "function") {
    return session.promptAsync({ path: { id: sessionId }, body: { parts: [makeTextPart(text)] } })
  }
  if (typeof session?.prompt === "function") {
    try {
      return await session.prompt({ path: { id: sessionId }, body: { parts: [makeTextPart(text)] } })
    } catch (error) {
      return session.prompt({ sessionID: sessionId, parts: [makeTextPart(text)] })
    }
  }
  throw new Error("OpenCode session prompt API not found")
}

async function latestAssistantText(client: any, sessionId: string) {
  const session = client.session
  let messages: any[] = []
  try {
    const response = await session.messages({ path: { id: sessionId } })
    messages = response.data ?? []
  } catch {
    try {
      const response = await session.messages({ sessionID: sessionId })
      messages = response.data ?? []
    } catch {
      return ""
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

export const LoopPlugin: Plugin = async ({ client }, options?: LoopPluginOptions) => {
  const commandName = options?.commandName?.replace(/^\//, "") || "loop"
  const minIntervalMs = Math.max(1_000, options?.minIntervalMs ?? 1_000)
  const store = new LoopStore(options?.dbPath || defaultDbPath())
  const timers = new Map<string, ReturnType<typeof setTimeout>>()

  function clearLoopTimer(loopId: string) {
    const timer = timers.get(loopId)
    if (timer) clearTimeout(timer)
    timers.delete(loopId)
  }

  function scheduleLoop(loop: LoopRow) {
    clearLoopTimer(loop.id)
    if (loop.status !== "active" || !loop.nextRunAt) return
    const delay = Math.max(0, loop.nextRunAt - now())
    const timer = setTimeout(
      () => {
        timers.delete(loop.id)
        if (delay > MAX_TIMEOUT_MS) {
          const latest = store.getLoop(loop.id)
          if (latest) scheduleLoop(latest)
          return
        }
        void startScheduledRun(loop.id)
      },
      Math.min(delay, MAX_TIMEOUT_MS),
    )
    timers.set(loop.id, timer)
  }

  async function startScheduledRun(loopId: string) {
    const loop = store.getLoop(loopId)
    if (!loop || loop.status !== "active") return

    if (store.getRunningRuns(loop.sessionId).length > 0) {
      const postponed = store.setNextRunAt(loop.id, now() + SESSION_BUSY_RETRY_MS)
      if (postponed) scheduleLoop(postponed)
      return
    }

    const run = store.createRun(loop, loop.nextRunAt ?? now())
    try {
      await promptSession(client, loop.sessionId, iterationPrompt(loop, run))
    } catch (error) {
      const failed = store.finishRun(run, "failed", error instanceof Error ? error.message : String(error))
      const latest = store.getLoop(loop.id)
      if (!latest || latest.status !== "active") return
      scheduleLoop(store.scheduleNext(latest, failed.finishedAt ?? now()))
    }
  }

  for (const loop of store.getActiveLoops()) scheduleLoop(loop)

  return {
    tool: {
      loop_start: tool({
        description:
          "Start a session-tied fixed-delay loop when the user clearly asks to repeat or remember a task on an interval. The first iteration instructions are returned and should be followed immediately.",
        args: {
          prompt: tool.schema.string().describe("The task to run each iteration. Do not include the interval words."),
          every: tool.schema.string().optional().describe("Natural interval, e.g. '5 mins', '1 hour', 'daily', '1 day'."),
          intervalMs: tool.schema.number().optional().describe("Interval in milliseconds. Use only if already known."),
        },
        async execute(args, context) {
          const interval = parseToolInterval(args.every, args.intervalMs)
          if (interval.intervalMs < minIntervalMs) return `Interval too short. Minimum is ${formatDuration(minIntervalMs)}.`
          const prompt = args.prompt.trim()
          if (!prompt) return "Missing loop prompt."
          if (store.getRunningRuns(context.sessionID).length > 0) {
            return "A loop iteration is already running in this session. Try again after it finishes."
          }

          const loop = store.createLoop({ sessionId: context.sessionID, prompt, intervalMs: interval.intervalMs })
          const run = store.createRun(loop, now())
          return {
            title: `Started loop ${shortId(loop.id)}`,
            output: [
              `Started loop ${shortId(loop.id)} every ${formatDuration(loop.intervalMs)}.`,
              "Run the first iteration now using these instructions:",
              "",
              iterationPrompt(loop, run),
            ].join("\n"),
            metadata: { loopId: loop.id, runId: run.id, sessionId: context.sessionID },
          }
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
          for (const loop of cancelled) clearLoopTimer(loop.id)
          if (!cancelled.length) return "No active or paused loops matched in this session."
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
          const [first, ...rest] = resumed
          for (const loop of rest) {
            const scheduled = store.setNextRunAt(loop.id, now())
            if (scheduled) scheduleLoop(scheduled)
          }
          const current = store.getLoop(first!.id) ?? first!
          const run = store.createRun(current, now())
          return {
            title: `Resumed loop ${shortId(current.id)}`,
            output: [
              `Resumed ${resumed.length} loop(s).`,
              "Run the first resumed iteration now using these instructions:",
              "",
              iterationPrompt(current, run),
            ].join("\n"),
            metadata: { loopId: current.id, runId: run.id, sessionId: context.sessionID },
          }
        },
      }),
    },

    dispose: async () => {
      for (const timer of timers.values()) clearTimeout(timer)
      timers.clear()
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
        for (const loop of cancelled) clearLoopTimer(loop.id)
        output.parts = [
          makeTextPart(
            commandResult(
              cancelled.length
                ? `Cancelled ${cancelled.length} loop(s) in this session: ${cancelled.map((loop) => shortId(loop.id)).join(", ")}. Future iterations will not run.`
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
        const [first, ...rest] = resumed
        for (const loop of rest) {
          const scheduled = store.setNextRunAt(loop.id, now())
          if (scheduled) scheduleLoop(scheduled)
        }
        const current = store.getLoop(first!.id) ?? first!
        const run = store.createRun(current, now())
        output.parts = [makeTextPart(iterationPrompt(current, run))]
        return
      }

      if (parsed.intervalMs < minIntervalMs) {
        output.parts = [makeTextPart(commandResult(`Interval too short. Minimum is ${formatDuration(minIntervalMs)}.`))]
        return
      }

      if (store.getRunningRuns(sessionId).length > 0) {
        output.parts = [makeTextPart(commandResult("A loop iteration is already running in this session. Wait for it to finish, then create another loop."))]
        return
      }

      const loop = store.createLoop({ sessionId, prompt: parsed.prompt, intervalMs: parsed.intervalMs })
      const run = store.createRun(loop, now())
      output.parts = [makeTextPart(iterationPrompt(loop, run))]
    },

    event: async ({ event }) => {
      if (!isIdleEvent(event)) return
      const sessionId = getSessionId(event)
      if (!sessionId) return

      const running = store.getRunningRuns(sessionId)
      if (!running.length) return

      const latestText = await latestAssistantText(client, sessionId)
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

        const finished = store.finishRun(run, isBlocked(latestText) ? "blocked" : "completed")
        if (finished.status === "blocked") {
          store.pauseLoop(loop.id)
          clearLoopTimer(loop.id)
          continue
        }

        const latest = store.getLoop(loop.id)
        if (!latest || latest.status !== "active") continue
        scheduleLoop(store.scheduleNext(latest, finished.finishedAt ?? now()))
      }
    },
  }
}

export default LoopPlugin
