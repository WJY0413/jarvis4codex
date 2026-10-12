---
name: jarvis-resume
description: Resume one exact existing task through the versioned 0.3.0 Jarvis MCP.
---

# Jarvis Resume (0.3.0 candidate)

Use `jarvis_resume(task_id, prompt, request_id, source_ref?, model?, reasoning_effort?, hold_with_monitor?, monitor_id?, hold_id?, max_turns?, auto_continue?, continue_prompt?, notifications?)`. `task_id` is the existing task; this is not the legacy `resume.existing(thread_id)` capability-port request. Read the target and writer state first. Preserve one stable `request_id` across retry/readback; do not change IDs to repeat an uncertain submission.

Show the target, exact prompt and lifecycle limits before a real resume. After calling, retain Hold/turn receipts and read exact Hold and turn state. `accepted` or a missing `turn_id` cannot establish terminal completion or safe release. Never overlap an active writer or substitute a new thread after an ambiguous result.
