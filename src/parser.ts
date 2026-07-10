export type LoopCommand =
  | { type: "create"; intervalMs: number; prompt: string; intervalText: string; autoCompact?: boolean }
  | { type: "cancel"; target?: string }
  | { type: "status" }
  | { type: "list" }
  | { type: "resume"; target?: string }
  | { type: "now"; target?: string }
  | { type: "unknown"; message: string }

const UNIT_MS: Record<string, number> = {
  s: 1_000,
  sec: 1_000,
  secs: 1_000,
  second: 1_000,
  seconds: 1_000,
  m: 60_000,
  min: 60_000,
  mins: 60_000,
  minute: 60_000,
  minutes: 60_000,
  h: 3_600_000,
  hr: 3_600_000,
  hrs: 3_600_000,
  hour: 3_600_000,
  hours: 3_600_000,
  d: 86_400_000,
  day: 86_400_000,
  days: 86_400_000,
}

export function parseInterval(input: string): { intervalMs: number; intervalText: string } | undefined {
  const text = input.trim().toLowerCase().replace(/\s+/g, " ")
  if (!text) return undefined
  if (text === "daily" || text === "every day" || text === "each day" || text === "once a day") {
    return { intervalMs: 86_400_000, intervalText: "1 day" }
  }
  if (text === "hourly" || text === "every hour" || text === "each hour" || text === "once an hour") {
    return { intervalMs: 3_600_000, intervalText: "1 hour" }
  }

  const match = text.match(/^(?:every\s+)?(\d+(?:\.\d+)?)?\s*([a-zA-Z]+)$/)
  if (!match) return undefined

  const amount = match[1] === undefined ? 1 : Number(match[1])
  const unit = match[2].toLowerCase()
  const unitMs = UNIT_MS[unit]
  if (!Number.isFinite(amount) || amount <= 0 || !unitMs) return undefined

  return {
    intervalMs: Math.round(amount * unitMs),
    intervalText: `${amount} ${unit}`,
  }
}

export function parseLoopCommand(input: string): LoopCommand {
  const options = parseLoopOptions(input)
  const text = options.text.trim()
  const normalized = text.toLowerCase().replace(/\s+/g, " ").trim()

  if (!normalized) return { type: "status" }

  if (/^(status|show|what'?s running|current loops?|running loops?)\b/.test(normalized)) {
    return { type: "status" }
  }

  if (/^(list|ls|show all)\b/.test(normalized)) {
    return { type: "list" }
  }

  const cancelMatch = normalized.match(/^(?:cancel|stop|delete|clear|remove)\b(?:\s+(?:the\s+)?(?:current\s+)?loops?)?(?:\s+(.+))?$/)
  if (cancelMatch) return { type: "cancel", target: cancelMatch[1]?.trim() }

  const resumeMatch = normalized.match(/^(?:resume|restart)\b(?:\s+(?:the\s+)?(?:current\s+)?loops?)?(?:\s+(.+))?$/)
  if (resumeMatch) return { type: "resume", target: resumeMatch[1]?.trim() }

  const nowMatch = normalized.match(/^(?:now|run now|run next|next now|run it now|trigger(?: now)?|fire(?: now)?|pick ?up(?: next)?)\b(?:\s+(?:the\s+)?(?:current\s+)?loops?)?(?:\s+(.+))?$/)
  if (nowMatch) return { type: "now", target: nowMatch[1]?.trim() }

  const createMatch = text.match(
    /^\s*(?:create\s+|start\s+|run\s+)?(?:a\s+)?(?:loop\s+)?every\s+(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\b\s*(?:to\s+)?([\s\S]+?)\s*$/i,
  )
  if (createMatch) {
    const interval = parseInterval(`${createMatch[1]} ${createMatch[2]}`)
    const prompt = createMatch[3].trim()
    if (!interval) {
      return { type: "unknown", message: "Use an interval like `every 5 mins check this`." }
    }
    if (!prompt) return { type: "unknown", message: "Missing loop prompt after interval." }
    return {
      type: "create",
      intervalMs: interval.intervalMs,
      prompt,
      intervalText: interval.intervalText,
      autoCompact: options.autoCompact,
    }
  }

  return { type: "unknown", message: "Try `/loop every 5 mins check this`, `/loop status`, or `/loop cancel`." }
}

function parseLoopOptions(input: string) {
  let autoCompact: boolean | undefined
  const text = input
    .replace(/(^|\s)--no-auto-compact\b/gi, (match, prefix: string) => {
      autoCompact = false
      return prefix
    })
    .replace(/(^|\s)--no-compact\b/gi, (match, prefix: string) => {
      autoCompact = false
      return prefix
    })
    .replace(/(^|\s)--auto-compact\b/gi, (match, prefix: string) => {
      autoCompact = true
      return prefix
    })
    .replace(/(^|\s)--compact\b/gi, (match, prefix: string) => {
      autoCompact = true
      return prefix
    })
  return { text, autoCompact }
}

export function formatDuration(ms: number): string {
  if (ms % 86_400_000 === 0) return `${ms / 86_400_000}d`
  if (ms % 3_600_000 === 0) return `${ms / 3_600_000}h`
  if (ms % 60_000 === 0) return `${ms / 60_000}m`
  if (ms % 1_000 === 0) return `${ms / 1_000}s`
  return `${ms}ms`
}
