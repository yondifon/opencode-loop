import { describe, expect, test } from "bun:test"
import { formatDuration, parseInterval, parseLoopCommand } from "../src/parser"

describe("parseLoopCommand", () => {
  test("parses natural fixed-delay loop", () => {
    expect(parseLoopCommand("every 5 mins check this")).toEqual({
      type: "create",
      intervalMs: 300_000,
      prompt: "check this",
      intervalText: "5 mins",
    })
  })

  test("parses cancel current loops", () => {
    expect(parseLoopCommand("cancel the current loops")).toEqual({ type: "cancel", target: undefined })
  })

  test("parses status default", () => {
    expect(parseLoopCommand("")).toEqual({ type: "status" })
  })
})

describe("parseInterval", () => {
  test("parses daily language", () => {
    expect(parseInterval("daily")).toEqual({ intervalMs: 86_400_000, intervalText: "1 day" })
    expect(parseInterval("every day")).toEqual({ intervalMs: 86_400_000, intervalText: "1 day" })
  })
})

describe("formatDuration", () => {
  test("formats common units", () => {
    expect(formatDuration(300_000)).toBe("5m")
    expect(formatDuration(3_600_000)).toBe("1h")
  })
})
