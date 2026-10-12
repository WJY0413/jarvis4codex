---
name: jarvis-create
description: Create one Jarvis-managed task through the 0.3.0 MCP and verify its Hold lifecycle.
---

# Jarvis Create (0.3.0 candidate)

Read `jarvis_read(subject="capabilities")` and the loaded MCP tool schema. Use `jarvis_create(project, prompt, request_id, title?, source_ref?, model?, reasoning_effort?, max_turns?, auto_continue?, continue_prompt?, hold_id?, notifications?, run_id?, role?, test_only?)`. `project` is an allowed project key, not a path or `project_id`. A stable `request_id` correlates the request and receipt; retain the receipt and returned Hold ID. Ordinary `jarvis_create` does not promise idempotent replay: do not resubmit when the execution status is unknown. TEST creation using `run_id`/`role` has its separate established replay contract; do not generalize it to ordinary creation.

Creation affects a real task; show the exact project, prompt, model, limits and expected effect and obtain authorization for that task. `accepted` is not an executed turn. Read `jarvis_read(subject="hold", hold_id=<receipt hold_id>)`, then exact `jarvis_read(subject="thread", task_id=<receipt task/thread ID>, turn_id=<receipt turn ID>)` when available. `task_id` is required even if an optional same-valued `thread_id` is also supplied; `thread_id` alone returns `invalid_request`. Missing first-turn evidence is not success. Do not use the old `JarvisCapabilityPort.invoke(jarvis.create)` field set or assume worktree creation is a public 0.3.0 parameter.
