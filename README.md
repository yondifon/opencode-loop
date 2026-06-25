# opencode-loop

Session-tied fixed-delay loops for OpenCode.

```json
{
  "plugin": ["opencode-loop"]
}
```

For local testing before publish, copy the built plugin into OpenCode's global plugins directory:

```bash
make install-local
```

Or reference the built file directly in your config:

```json
{
  "plugin": ["file:///Users/malico/tinkerbin/opencode-loop/dist/index.js"]
}
```

Usage:

```text
/loop every 5 mins check this
/loop remember every day to check this
/loop status
/loop cancel the current loops
```

Test locally with `make check`.

Intervals are fixed-delay. If a 5 minute loop takes 10 minutes to run, the next run starts 5 minutes after it finishes.

New loops queue their first iteration after the current session becomes idle. Completed iterations must include the run-specific `[loop:evidence:<run_id>]` marker requested by the plugin; missing evidence pauses the loop as `unverified`.

State is stored in `~/.config/opencode/loops/loops.db`. Loops are tied to OpenCode session IDs.

OpenCode's own `opencode.db` is not used for plugin state because plugin APIs do not expose a stable extension table or migration hook. Loop rows store OpenCode `sessionId` so runs can still be linked back to OpenCode sessions.
