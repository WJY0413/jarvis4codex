---
name: jarvis-loop-controller
description: Observe and close an authorized native 0.3.0 Loop without duplicating its continuation owner.
---

# Jarvis Loop Controller (0.3.0 candidate)

Read `jarvis_loop(action="status",loop_id=...)` and exact Hold/turn evidence. A PID, poll timestamp, queued receipt or missing turn ID is not a confirmed terminal. Native `auto_continue` owns next-turn dispatch and rotation; this controller observes, diagnoses, reports and uses only authorized exact lifecycle actions. Never edit ack/state/SQLite or create a replacement writer from an ambiguous state.

Separate execution terminal/release, scheduler cleanup, structural result review and business acceptance. A soft `review` or `failed` output does not automatically mean a safety stop; identity/path/duplicate risk does. For closure, call `jarvis_close(loop_id=...,request_id=...)` once with a stable ID and read its durable report. `closed_unconfirmed` is not release and cannot authorize replacement. Preserve healthy seats. This Skill does not install a watchdog, run the historical `state_machine.py`, or prove live recovery.
