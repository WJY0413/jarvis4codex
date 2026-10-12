---
name: jarvis-read
description: Read exact 0.3.0 Jarvis capabilities, Hold, thread or history without mutating runtime state.
---

# Jarvis Read (0.3.0 candidate)

Use `jarvis_read(subject="capabilities"|"thread"|"hold"|"history", task_id?, hold_id?, thread_id?, turn_id?)` with the loaded 0.3.0 MCP. For a managed lifecycle retain the creation receipt's `hold_id` and read `subject="hold", hold_id=<exact id>`. For a native turn use `jarvis_read(subject="thread", task_id=<exact task/thread ID>, turn_id=<exact turn ID>)`. `task_id` is conditionally required for every `subject="thread"` read, including full-history reads without `turn_id`; optional `thread_id`, when supplied, must equal `task_id`. `thread_id` without `task_id` is invalid. A missing or different turn is unresolved, not terminal. `history` is read-only historical evidence.

Do not pass the retired `subject="task", monitor_id=...` shape. Keep Holder lifecycle and native thread evidence distinct: an observer read error or `notLoaded` does not itself override a live Hold. No resume, stop, or inferred cleanup follows from a read.
