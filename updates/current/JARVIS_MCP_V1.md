# Jarvis MCP v1 (candidate)

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
creates a durable thread, completes its first turn, reads that exact turn back,
then sets its title. `jarvis_notify` and unconfigured heartbeat scheduling
remain structured `unsupported` receipts; they never report a false action.

## Verification

Contract tests connect the official SDK's in-process `Client` to the server.
The local TEST installer starts the official stdio entry point with deployed
existing-thread and task-provisioning adapters. It keeps runtime state under a
separate MCP state directory. Notification delivery and unconfigured heartbeat
scheduling remain unavailable until their verified adapters exist.
