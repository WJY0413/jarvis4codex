---
name: jarvis-loop
description: Plan, start, observe and close one bounded native 0.3.0 Jarvis Loop without a duplicate scheduler.
---

# Jarvis Loop (0.3.0 candidate)

Call `jarvis_loop(action="preflight")` and read the loaded tool schema before planning. The native actions are `preflight`, `start`, `status`, `stop`, `schema_register`, `schema_read`; use `jarvis_close(loop_id=...)` for exact closure/report when needed. Do not layer the old dispatcher/parent-continuation sequence over native `auto_continue`.

`start` requires `request_id`, allowed `project`, business `prompt`, `target_thread_count`, `max_rounds`, `expires_at`. Distinguish seat count, total rounds, `turns_per_thread`, and Host capacity. Each explicit thread has unique `slot`, `acquire=create|resume`, and either `title` or exact `task_id`. A lane has unique positive `candidate_ids`, `database_path`, `output_boundary` and `result_verification` with receipt paths and terminal statuses. The Loop injects current identity; do not put it in a static prompt. Review all real launch effects with Cooper before starting.

For a reusable result shape, register an inline `schema` or uploaded `schema_json` via `schema_register(project,schema_id,schema_version,...)`, read back `schema_sha256`, then select with `thread.lane.result_verification.schema_ref={schema_id,schema_version,schema_sha256}`. The pin must be explicit, nonempty and a 64-character hexadecimal digest; `schema_read` may omit it for discovery. Inline `output_schema` remains supported; never supply both. The selected schema is snapshotted into the persisted lane at start; later versions cannot mutate a running Loop. Draft 2020-12 `format=email` is an annotation by default, not an email syntax assertion, and structural validity is not business factual acceptance. `status` and exact Hold/turn receipts govern progress; only confirmed terminal and release justify cleanup. No duplicate resume, new worker, business research or external send follows from an uncertain state.
