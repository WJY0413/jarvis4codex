---
name: jarvis-heartbeat
description: Manage a configured 0.3.0 Jarvis heartbeat with bounded lifecycle readback.
---

# Jarvis Heartbeat (0.3.0 candidate)

Use `jarvis_heartbeat(action="create"|"update"|"cancel"|"health", request_id, source_ref?, heartbeat_id?, options?)`. Read `health` and the scheduler adapter's required `options` before mutation. A configured scheduler is required; a valid MCP tool declaration alone does not prove one exists. The 0.3.0 MCP does not expose top-level `pause` or `resume`; those older local CLI actions must not be presented as MCP actions.

For creation/update/cancellation, retain the exact target, cadence, expiry, prompt and stop conditions plus authorization. Read the returned heartbeat ID, active state and next run; registration is not execution. Do not operate a daemon, create a second parent scheduler for a native Loop, or edit its SQLite state.
