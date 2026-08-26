# Jarvis MCP v0.1.8 (in progress)

## Scope

The first MCP surface uses the official Python `mcp` SDK over stdio and exposes:

- `jarvis_create`
- `jarvis_hold`
- `jarvis_read`
- `jarvis_resume`
- `jarvis_monitor`
- `jarvis_heartbeat`
- `jarvis_notify`

`jarvis_confirm` and `jarvis_run` remain out of scope.

## Safety boundary

The MCP server calls only `JarvisControl`. Its App Server provisioning adapter
queues a durable create or resume request for the normal-user Hold Host, then
returns an `accepted` receipt. `jarvis_hold` is the managed lifecycle entry:
Hold owns App Server execution while Monitor reads exact terminal status and
content, then returns the sole `CONTINUE` or `STOP` command that Hold may
execute. The Hold Host reports `holding` once it owns the exact turn and a
terminal receipt only after that exact-turn readback.

Managed holds persist under `task-holds/`; the Hold Host and readback adapter
also accept legacy `task-monitors/` state so accepted work is not stranded.
Milestone and terminal events are durable but disabled by default. If a verified
notification adapter is configured, `jarvis_monitor` can deliver pending events
and records the returned delivery receipt before marking an event sent.

`jarvis_heartbeat` uses a separate local scheduler database and the computer's
clock; it accepts only bounded structured calls to `JarvisControl.monitor`,
`JarvisControl.resume`, or `JarvisControl.notify`. It never stores or builds a
prompt. When the verified Feishu outbox adapter is configured,
`jarvis_notify` enqueues its single record and requires an exact delivery-log
readback with a Feishu `message_id`; a queued record is not a sent receipt.

## Verification

Contract tests connect the official SDK's in-process `Client` to the server.
The local TEST installer starts the official stdio entry point with deployed
existing-thread and task-provisioning adapters. It keeps runtime state under a
separate MCP state directory. Scheduler state and notification delivery each
have their own configured adapter and contract tests.
