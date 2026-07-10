import { afterEach, describe, expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { LoopPlugin } from "../src"

const cleanups: Array<() => Promise<void> | void> = []

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup()
})

describe("LoopPlugin", () => {
  test("reuses an active loop when agent wording only changes punctuation", async () => {
    const directory = mkdtempSync(join(tmpdir(), "opencode-loop-test-"))
    const hooks = await (LoopPlugin as any)({ client: {} }, { dbPath: join(directory, "loops.db") })
    cleanups.push(async () => {
      await hooks.dispose()
      rmSync(directory, { recursive: true, force: true })
    })

    const context = { sessionID: "session-1" }
    await hooks.tool.loop_start.execute({ prompt: ". say hi", every: "2 mins" }, context)
    const result = await hooks.tool.loop_start.execute({ prompt: 'Say "hi"', every: "2 mins" }, context)

    expect(result).toContain("already exists")
    expect(await hooks.tool.loop_status.execute({}, context)).toContain("Loops for this session")
  })

  test("creates one live loop across concurrent plugin instances", async () => {
    const directory = mkdtempSync(join(tmpdir(), "opencode-loop-test-"))
    const dbPath = join(directory, "loops.db")
    const client = { session: { status: async () => ({ data: { "session-1": { type: "busy" } } }) } }
    const first = await (LoopPlugin as any)({ client }, { dbPath })
    const second = await (LoopPlugin as any)({ client }, { dbPath })
    cleanups.push(async () => {
      await first.dispose()
      await second.dispose()
      rmSync(directory, { recursive: true, force: true })
    })

    const context = { sessionID: "session-1" }
    await Promise.all([
      first.tool.loop_start.execute({ prompt: ". say hi", every: "2 mins" }, context),
      second.tool.loop_start.execute({ prompt: 'Say "hi"', every: "2 mins" }, context),
    ])

    const db = new Database(dbPath, { readonly: true })
    expect((db.query("select count(*) as count from loops where status in ('active', 'paused')").get() as any).count).toBe(1)
    db.close()
  })

  test("migrates existing equivalent loops to one live identity", async () => {
    const directory = mkdtempSync(join(tmpdir(), "opencode-loop-test-"))
    const dbPath = join(directory, "loops.db")
    const client = { session: { status: async () => ({ data: { "session-1": { type: "busy" } } }) } }
    const first = await (LoopPlugin as any)({ client }, { dbPath })
    await first.tool.loop_start.execute({ prompt: ". say hi", every: "2 mins" }, { sessionID: "session-1" })
    await first.dispose()

    const db = new Database(dbPath)
    db.exec("drop index loops_one_live_identity_idx")
    db.exec(`
      insert into loops
        (id, session_id, prompt, prompt_key, interval_ms, auto_compact, run_requested, status, created_at, updated_at, next_run_at, last_run_id, last_started_at, last_finished_at, run_count, lease_owner, lease_until)
      select
        'loop_duplicate', session_id, 'Say "hi"', 'legacy-key', interval_ms, auto_compact, 0, 'active', created_at + 1, updated_at, next_run_at, null, null, null, 0, null, null
      from loops limit 1
    `)
    db.close()

    const second = await (LoopPlugin as any)({ client }, { dbPath })
    cleanups.push(async () => {
      await second.dispose()
      rmSync(directory, { recursive: true, force: true })
    })
    const migrated = new Database(dbPath, { readonly: true })
    expect((migrated.query("select count(*) as count from loops where status in ('active', 'paused')").get() as any).count).toBe(1)
    expect((migrated.query("select status from loops where id = 'loop_duplicate'").get() as any).status).toBe("cancelled")
    migrated.close()
  })

  test("does not prompt while OpenCode reports the session busy", async () => {
    const directory = mkdtempSync(join(tmpdir(), "opencode-loop-test-"))
    let prompts = 0
    const client = {
      session: {
        status: async () => ({ data: { "session-1": { type: "busy" } } }),
        promptAsync: async () => {
          prompts++
        },
      },
    }
    const hooks = await (LoopPlugin as any)({ client }, { dbPath: join(directory, "loops.db") })
    cleanups.push(async () => {
      await hooks.dispose()
      rmSync(directory, { recursive: true, force: true })
    })

    await hooks.tool.loop_start.execute({ prompt: "say hi", every: "2 mins" }, { sessionID: "session-1" })
    await Bun.sleep(20)

    expect(prompts).toBe(0)
  })

  test("cancels loops for the current session.deleted event shape", async () => {
    const directory = mkdtempSync(join(tmpdir(), "opencode-loop-test-"))
    const client = { session: { status: async () => ({ data: { "session-1": { type: "busy" } } }) } }
    const hooks = await (LoopPlugin as any)({ client }, { dbPath: join(directory, "loops.db") })
    cleanups.push(async () => {
      await hooks.dispose()
      rmSync(directory, { recursive: true, force: true })
    })
    const context = { sessionID: "session-1" }
    await hooks.tool.loop_start.execute({ prompt: "say hi", every: "2 mins" }, context)

    await hooks.event({ event: { type: "session.deleted", properties: { info: { id: "session-1" } } } })

    expect(await hooks.tool.loop_status.execute({}, context)).toContain("cancelled")
  })

  test("queues run-now without clearing an active lease", async () => {
    const directory = mkdtempSync(join(tmpdir(), "opencode-loop-test-"))
    const dbPath = join(directory, "loops.db")
    let resolvePrompt!: () => void
    const promptStarted = new Promise<void>((resolve) => {
      resolvePrompt = resolve
    })
    let markPromptStarted!: () => void
    const started = new Promise<void>((resolve) => {
      markPromptStarted = resolve
    })
    const client = {
      session: {
        status: async () => ({ data: {} }),
        promptAsync: async () => {
          markPromptStarted()
          await promptStarted
        },
        messages: async () => ({ data: [{ info: { role: "assistant" }, parts: [{ type: "text", text: "hi" }] }] }),
      },
    }
    const hooks = await (LoopPlugin as any)({ client }, { dbPath })
    cleanups.push(async () => {
      resolvePrompt()
      await hooks.dispose()
      rmSync(directory, { recursive: true, force: true })
    })
    const context = { sessionID: "session-1" }
    await hooks.tool.loop_start.execute({ prompt: "say hi", every: "2 mins" }, context)
    await started

    const db = new Database(dbPath)
    const before = db.query("select lease_owner, lease_until from loops").get() as any
    await hooks.tool.loop_now.execute({}, context)
    const queued = db.query("select lease_owner, lease_until, run_requested from loops").get() as any

    expect(queued.lease_owner).toBe(before.lease_owner)
    expect(queued.lease_until).toBe(before.lease_until)
    expect(queued.run_requested).toBe(1)
    db.close()
    resolvePrompt()
  })

  test("pauses a run when its assistant response cannot be inspected", async () => {
    const directory = mkdtempSync(join(tmpdir(), "opencode-loop-test-"))
    const dbPath = join(directory, "loops.db")
    let prompted!: () => void
    const didPrompt = new Promise<void>((resolve) => {
      prompted = resolve
    })
    const client = {
      session: {
        status: async () => ({ data: {} }),
        promptAsync: async () => prompted(),
        messages: async () => {
          throw new Error("messages unavailable")
        },
      },
    }
    const hooks = await (LoopPlugin as any)({ client }, { dbPath })
    cleanups.push(async () => {
      await hooks.dispose()
      rmSync(directory, { recursive: true, force: true })
    })
    await hooks.tool.loop_start.execute({ prompt: "say hi", every: "2 mins" }, { sessionID: "session-1" })
    await didPrompt

    await hooks.event({ event: { type: "session.idle", properties: { sessionID: "session-1" } } })

    const db = new Database(dbPath, { readonly: true })
    expect((db.query("select status from loops").get() as any).status).toBe("paused")
    expect((db.query("select status from loop_runs").get() as any).status).toBe("unverified")
    db.close()
  })
})
