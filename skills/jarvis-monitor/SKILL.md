---
name: jarvis-monitor
description: Observe a known task or conditionally resume an exact target through the 0.3.0 MCP.
---

# Jarvis Monitor (0.3.0 candidate)

Use `jarvis_monitor(action="observe"|"terminal_resume"|"status"|"deliver_hold_notifications", request_id, source_ref?, monitor_id?, observed_task_id?, receipt_task_id?, resume_task_id?, prompt?, model?, reasoning_effort?, hold_id?)`. The 0.3.0 fields end in `_task_id`; never forward legacy `observed_thread_id`, `receipt_target_thread_id` or `resume_target_thread_id` unchanged.

For `observe`, supply nonempty `monitor_id`, `observed_task_id` and `receipt_task_id`; it **persists the baseline** and can consume a `terminal_changed` event. Do not call it as a harmless preview immediately before `terminal_resume` on the same monitor/route: the latter may then return `no_change` without dispatch. `terminal_resume` additionally requires nonempty `resume_task_id` and `prompt`; it observes and may dispatch one turn only on a newly completed transition. For pure lifecycle readback use `jarvis_monitor(action="status", request_id=..., hold_id=<exact Hold ID>)` or `jarvis_read`, not `observe`. `deliver_hold_notifications` requires `hold_id` and a configured notifier and can cause delivery. Keep exact IDs, idempotency key and receipt; no one-shot action is a persistent scheduler. Do not create a second continuation owner over native `jarvis_loop` auto-continuation.
