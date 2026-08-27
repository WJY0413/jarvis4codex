# Jarvis MCP optimization backlog

## Next: choice-driven loop preflight

- [ ] Add a read-only `jarvis_loop(action="preflight")` response before every state-changing loop start.
- [ ] Return public start-field metadata: requiredness, type, defaults, enums, relations, and the exact `threads[]` create/resume schema.
- [ ] Read the project candidate list from the same `allowed_projects` configuration used by task provisioning; expose names only, never paths.
- [ ] Make project a selection: the preview returns stable `project_id` values and start accepts only a listed `project_id`, never an inferred or arbitrary project name.
- [ ] Add one authoritative model-profile provider for `{profile_id, model, reasoning_efforts, is_default}`. It must read the configured/Host-supported profiles rather than duplicate a hard-coded list in MCP.
- [ ] Make model selection a selection: preview returns compatible `profile_id` and `reasoning_effort` choices; start accepts only that selected compatible pair, not arbitrary model/effort text.
- [ ] Return environment, notification, and heartbeat options with their defaults and cross-field constraints.
- [ ] Validate an optional candidate payload with the same normalizer used by `jarvis_loop(action="start")`; return a normalized preview or structured validation problems.
- [ ] Require explicit user confirmation after a valid preview and before `start`.

## Follow-on MCP improvements

- [ ] Read-only resources for projects, model profiles, loops, task history, and receipts.
- [ ] Stable structured receipts and error codes with trace/task/thread/turn correlation.
- [ ] Progress, cancellation, pagination, filtering, and incremental history reads.
- [ ] Separate test and production identity/configuration readback.
- [ ] Verified notification delivery readback; no external-send claim without it.
