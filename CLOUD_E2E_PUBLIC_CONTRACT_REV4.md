# Jarvis 0.3.0 rev9 — bounded cloud product E2E contract

Filename retained from the historical rev4 packet (3.0.0a2). Historical E2E results belong to that older revision, not this 0.3.0 rev9 recipe. This updated executable parameter contract does not authorize a new paid/model run; require a separately approved finite manifest and this bundle's current code/archive pins before execution.

This is a public, synthetic TEST packet for an operator using the already officially logged-in, independent cloud Codex Home reported by parent dot. It contains no credentials, customer records, internal Home path, or production state. The parent-reported Home and SDK startup are a **precondition supplied by that environment**, not independently verified in this local engineering turn. Do not copy authentication, create another account, change permissions, touch production 027/cloud30, or replay any of the closed 50 companies.

## Exact finite workload and limit

- One fresh TEST workspace, one TEST project, one synthetic row `id=1, marker=rev9-synthetic` in a new local-only SQLite file; no company name, domain, contact, search, outreach or business DB.
- One Schema registration, one native Loop, exactly one `acquire=create` seat, one candidate ID, `max_rounds=1`, `max_turns=1`, `turns_per_thread=1`, `auto_continue=false`, `notifications=false`. At most one real model turn; no `jarvis_resume`, no replacement Hold, no second Loop on an unknown result. Use a model/effort returned as available by the official native model API; prefer a supported low effort to keep cost bounded.
- Worker prompt: `TEST only. Do not browse, read customer data, contact anyone or write files. Return exactly one JSON object: {"marker":"rev9-synthetic","step":1}. Do not add prose or Markdown.` The Holder, not the Worker, saves the final-answer evidence in the new TEST output boundary.
- Use a fresh run nonce for `request_id`/`schema_id` and a UTC `expires_at` no later than 15 minutes after start. Poll only the existing Loop/Hold for at most five minutes, no faster than every two seconds. Stop on a verified exact terminal or any blocked/unknown/timeout condition. Quota below 1% or exhausted stops this run without reset/recharge or automatic retry.

## Public MCP sequence (placeholders resolved at runtime)

1. Start the candidate `jarvis_mcp.entrypoint` over official stdio with the isolated TEST `--config`, `--launcher-config`, `--state-dir`; do not edit the 027 entry. SDK `tools/list` must report 17 tools and server `0.3.0`. Read `jarvis_read(subject="capabilities")` and `jarvis_loop(action="preflight")`; require `readback.verified=true` and an allowlisted TEST project. Read current official `model/list`/capabilities before choosing `model` and `reasoning_effort`.
2. Call `jarvis_loop(action="schema_register",request_id="rev9-e2e-<nonce>:schema",project="<TEST_PROJECT>",schema_id="rev9-synthetic-<nonce>",schema_version="1",schema={"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["marker","step"],"properties":{"marker":{"const":"rev9-synthetic"},"step":{"const":1}},"additionalProperties":false})`. Require `completed` and save the returned 64-hex `schema_sha256`; `schema_read` with the exact hash must return the same schema. Registry identity is project-scoped and immutable; conflicting replay fails.
3. Call `jarvis_loop(action="start",request_id="rev9-e2e-<nonce>",project="<TEST_PROJECT>",title="Jarvis 0.3.0 rev9 synthetic E2E",prompt="<above prompt>",target_thread_count=1,max_rounds=1,max_turns=1,turns_per_thread=1,auto_continue=false,notifications=false,expires_at="<UTC_PLUS_15_MIN>",model="<NATIVE_MODEL_LIST_VALUE>",reasoning_effort="<SUPPORTED_LOW_VALUE>",threads=[{"slot":"worker-1","acquire":"create","title":"rev9 synthetic worker","lane":{"candidate_ids":[1],"database_path":"<NEW_TEST_WORKSPACE>/synthetic.db","output_boundary":"<NEW_TEST_WORKSPACE>/output","result_verification":{"mode":"final_answer_json","receipt_paths":{"1":"1.receipt.json"},"terminal_statuses":["received"],"schema_ref":{"schema_id":"rev9-synthetic-<nonce>","schema_version":"1","schema_sha256":"<RETURNED_64_HEX>"}}}}])`. All path placeholders are fresh paths inside the isolated TEST workspace. A start receipt of `accepted`/`holding`/`running` is **not** evidence of a native turn or terminal.
4. Retain actual `loop_id`, `hold_id`, `task_id`/`thread_id`, and `turn_id` only from product receipts/native readback. Poll `jarvis_loop(action="status",loop_id=...)` and `jarvis_read(subject="hold",hold_id=...)`. Require an actual nonempty turn ID, exact `terminal_confirmed=true`, `hold_released=true`, and `jarvis_read(subject="thread",task_id=<ACTUAL_TASK_ID>,thread_id=<ACTUAL_THREAD_ID>,turn_id=<ACTUAL_TURN_ID>)` showing that same native terminal. Thread read without `task_id` is invalid.
5. Read the Holder-generated receipt, raw answer, parsed payload, and Schema binding under the isolated output boundary. Require `payload_parsed=true`, empty `schema_issues`, parsed `{"marker":"rev9-synthetic","step":1}`, matching Schema hash, and the exact candidate/request/turn receipt identity. `output_verification.status=verified` proves saved-intake integrity only; `review_needed=true`, `verification_status=not_verified` and `research_completion=not_assessed` remain expected. Do **not** call this business-fact acceptance or an email-format assertion.
6. Call `jarvis_loop(action="status",loop_id=...)`, then `jarvis_close(loop_id=...,request_id="rev9-e2e-<nonce>:close")`; read back Loop status and close report. Pass requires no further round, exact terminal/release, saved Schema result, and close `closed`/completed cleanup. If close is `closed_unconfirmed`, or any identity/terminal/release is unknown, fail closed and retain the report; do not kill a shared Host, edit state, or launch another seat.

Failure/cleanup receipt fields: `run_id`, candidate code SHA, archive SHA, TEST project label, MCP version/tool count, model/effort from official API, `schema_id/version/hash`, `loop_id`, `hold_id`, actual `task_id/thread_id/turn_id` if issued, call count, last phase/error code, exact terminal/release booleans, parsed/schema issue status, close/cleanup status, UTC timestamps, quota readback and output hashes. Use `not_run`, `blocked`, `requires_readback`, `closed_unconfirmed`, or `passed` explicitly; missing IDs are null, never inferred. Publish the redacted receipt and paths to TEST artifacts, not authentication material.

## Launcher and state binding contract

The cloud operator should bind the candidate source to a **new TEST state directory** and its already authenticated independent Home using the official SDK path already verified there. The public `jarvis_mcp.entrypoint` parameters are `--config <ISOLATED_HEARTBEAT_CONFIG> --launcher-config <ISOLATED_LAUNCHER_CONFIG> --state-dir <NEW_TEST_STATE> --transport stdio`. Do not point any argument to 027 state, existing cloud30 output, or a production project. The launcher JSON contract, without private values, is:

```json
{
  "dispatcher_thread_id": "<OFFICIAL_THREAD_ID_IN_TEST_HOME>",
  "codex_cli": "<OFFICIAL_SDK_LAUNCHER_COMMAND_ALREADY_VERIFIED>",
  "profile": "jarvis30-rev9-synthetic-test",
  "linux_cloud": true,
  "expected_codex_home": "<EXISTING_OFFICIALLY_LOGGED_IN_INDEPENDENT_HOME>",
  "runtime_environments": [],
  "runtime_constraints": {"model_provider": "openai", "sandbox": "read-only", "approval_policy": "never"},
  "live_creation_enabled": true,
  "worker_capacity": 1,
  "memory_admission_max_used_percent": 95,
  "allowed_projects": {"<TEST_PROJECT>": "<NEW_TEST_WORKSPACE>"}
}
```

This is a parameter contract, **not** a request to overwrite a launcher or create/change credentials. Reuse an existing TEST `dispatcher_thread_id` verified by official thread list/read in the independent Home; if absent, stop this packet rather than create an extra control thread. Available model/effort and actual Worker thread/turn IDs come from official native API/model/read and Jarvis receipts at runtime. Home and workspace absolute paths come from the existing cloud operator/launcher context, not from this package or a model guess. `profile` is only a local label; verify the runtime `codex_home`, project, model, sandbox, approval policy, and Hold Host `pid`, `host_started_at`, `host_instance_id`, `state_dir`, `worker_capacity`, and fresh `observed_at` against their TEST readback before create. An `accepted` request, HTTP 200, SDK handshake, or tool visibility alone is not an E2E pass. The heartbeat transport config must likewise resolve only to new TEST db/health/lock paths, with no Desktop recovery or notification destination enabled for this run.

## Whole-machine memory admission

The launcher field `memory_admission_max_used_percent` defaults to 95 and accepts finite numeric values in (0,100]. At or above the threshold, new create/resume/capacity expansion and actual dispatch are refused using a fresh Windows GlobalMemoryStatusEx or Linux MemTotal/MemAvailable sample. Unknown sampling and invalid policy refuse new admission with explicit errors; existing readback, close and healthy work remain available. This is not a fixed-GB/job quota or a continuously enforced hard cap. Below threshold the next new action resamples; durable rejected capacity attempts retain their saved receipt, so use a new explicit request ID rather than replaying the rejected attempt. No public payload can set an internal read-only observer bypass, and such observers cannot start model turns.
