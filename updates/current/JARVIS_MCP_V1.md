# Jarvis MCP v1

The v0.1.7 release is archived at
[`updates/history/JARVIS_MCP_V1_0.1.7.md`](../history/JARVIS_MCP_V1_0.1.7.md).

## Scope

The first MCP surface uses the official Python `mcp` SDK over stdio and exposes:

- `jarvis_create`
- `jarvis_read`
- `jarvis_resume`
- `jarvis_monitor`
- `jarvis_heartbeat`
- `jarvis_notify`

`jarvis_confirm` and `jarvis_run` remain out of scope.

## Safety boundary

The MCP server calls only `JarvisControl`. Its App Server provisioning adapter
queues a durable create or resume request for the normal-user Hold Host, then
returns an `accepted` receipt. The Hold Host reports `holding` once it owns the
exact turn and a terminal receipt only after exact-turn readback.

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
