---
name: jarvis-control-loop-launcher
description: Compile an authorized work package into reviewable 0.3.0 Loop inputs and optional parent heartbeat.
---

# Jarvis Loop Launch Planner (0.3.0 candidate)

Use `jarvis_loop(action="preflight")` and the actual loaded `jarvis_loop` schema. Produce a preview with business prompt, allowed project, source/finite lanes, unique IDs, receipt paths, `result_verification.mode`, selected inline schema or immutable `schema_ref`, seat count, `max_rounds`, `turns_per_thread`, model/effort, expiry, notifications and stop conditions. Use `jarvis-loop-prompt-builder` for startup/continuation text. Never assume the default model or project is the user's choice.

If the user requests a parent heartbeat, separately check `jarvis_heartbeat(action="health", request_id=...)` and prepare only supported `create` options. Native Loop reconciliation already has its own heartbeat; do not create a redundant parent scheduler unless the approved plan needs a distinct parent acceptance wake. Start only the confirmed package, once, then read exact Loop/Hold identities. `accepted` is not completion. If heartbeat creation fails after Loop start, report partial state; do not silently stop or duplicate it. A missing schema, project permission, lane proof or Host readiness is a launch blocker, not a prompt placeholder.
