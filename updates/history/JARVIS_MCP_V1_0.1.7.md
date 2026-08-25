# Jarvis MCP v0.1.7 (released)

## Scope

The first MCP surface uses the official Python `mcp` SDK over stdio and exposes:

- `jarvis_create`
- `jarvis_read`
- `jarvis_resume`
- `jarvis_monitor`
- `jarvis_heartbeat`
- `jarvis_notify`

`jarvis_confirm` and `jarvis_run` remain out of scope.

## Released behavior

`jarvis_heartbeat` owns a separate local scheduler database, computer time,
counters, lifecycle, and receipts. It accepts only bounded structured calls to
`JarvisControl.monitor`, `JarvisControl.resume`, or `JarvisControl.notify`.
It never stores or builds a prompt.

`jarvis_notify` uses the established Feishu outbox and requires the existing
bridge's delivery readback with a Feishu `message_id`; an enqueued record is
not reported as delivered. Each heartbeat run has a distinct request ID, so
separate runs are not incorrectly deduplicated.

## Verification

The release passed 78 targeted unit and contract tests. Production local
heartbeat health readback confirmed the background host and local clock.
