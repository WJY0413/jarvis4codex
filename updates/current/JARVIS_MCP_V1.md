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

The MCP server calls only `JarvisControl`, which adapts the already-versioned
`JarvisCapabilityPort`. It does not contain a harness implementation. Until a
verified adapter is supplied, `jarvis_create` and `jarvis_notify` return a
structured `unsupported` receipt; they never report a false successful action.

## Verification

Contract tests connect the official SDK's in-process `Client` to the server.
The local TEST installer starts the official stdio entry point with the
deployed existing-thread adapter. It keeps runtime state under a separate MCP
state directory. Task creation, notification delivery, and unconfigured
heartbeat scheduling remain unavailable until their verified adapters exist.
