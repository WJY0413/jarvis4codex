---
name: jarvis-self-continuation
description: Gate same-thread automatic continuation on exact 0.3.0 lifecycle and an existing durable executor.
---

# Jarvis Self Continuation (0.3.0 candidate, capability gated)

Read `jarvis_read(subject="capabilities")`, exact Hold/turn state and the loaded `jarvis_monitor` schema. Its `observe`/`terminal_resume` actions are one-shot, not a persistent monitor. A safe same-thread continuation requires an already supported executor that attaches to the current active turn, deduplicates terminal events, persists the total budget across restart and prevents overlapping writers. `jarvis_hold(task_id=...)` may immediately resume and cannot be used as an attach substitute.

If any of those runtime properties or consent for the bounded run is missing, report `unsupported` and do not create a daemon or timer. Only a newly confirmed completed turn with its exact ID can authorize one next prompt; failures, unknown status, active writer, expiry, no-progress or budget exhaustion stop automatic advancement. Isolated logic checks are not evidence of real trigger delivery.
