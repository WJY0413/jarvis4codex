---
name: jarvis-real-test
description: Define a small authorized 0.3.0 product-path acceptance run and separate it from isolated tests.
---

# Jarvis Real Test (0.3.0 candidate)

Prepare a bounded real-product run only from an explicitly approved manifest, project, budget, model, expiry and output boundary. Read capabilities and `jarvis_loop` preflight first; never reuse already claimed companies or silently launch contact mining. A test must invoke the 0.3.0 MCP product path (`jarvis_loop`, `jarvis_read`, `jarvis_close`, and configured delivery readback where applicable), not a direct AppServer staging substitute. The old `jarvis_read(subject="task",monitor_id=...)` call is invalid; use `subject="hold",hold_id=...` and exact `jarvis_read(subject="thread",task_id=<task/thread ID>,turn_id=<turn ID>)` readback.

Record actual request IDs, task/turn/Hold IDs, input bindings, saved results, terminal/release and cleanup receipts. A real new turn and verified result are required to claim E2E; import, schema structure, HTTP 200, fake ports or a queued request are insufficient. Keep business quality review separate and never send external outreach or write production business data under this Skill. This package contains no approved real-run manifest; live E2E is not yet performed.
