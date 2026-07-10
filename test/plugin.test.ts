import { afterEach, describe, expect, test } from "bun:test"
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
})
