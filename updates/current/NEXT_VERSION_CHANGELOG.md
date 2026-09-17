# Jarvis 0.2.2

## Integrated official capability snapshot

- Optional `turns_per_thread` adds bounded thread rotation while preserving logical task identity, lane progress and remaining budgets.
- Reviewable business output failures do not independently stop scheduling. Explicit runtime safety stops remain authoritative.
- Host-owned exact-turn stop and recovery requires matching identity, terminal readback, holder/client exit and claim cleanup. Unknown or mismatched evidence remains unreleased; repeat requests are idempotent.
- Preserve production final-answer JSON intake, existing-thread adoption, notification behavior and explicit worker capacities. Active hosts are not restarted for resizing.
- Restore portable package dependencies, generic disabled-by-default configuration examples and current installation documentation.
- Installation, loaded-process activation and release publication are separate checks. Existing MCP connections must be refreshed and healthy work drained before replacing a running source snapshot.

# Jarvis 0.2.0

## Codex CLI discovery

- `codex_cli: "auto"` now discovers the npm global installation with
  `npm prefix -g` when the Codex shim is absent from PATH. On Windows it
  checks npm before falling back to a Desktop executable.
- npm installations are launched with the resolved Node executable and the
  package's declared Codex entrypoint. Every new client checks `--version`
  with a bounded timeout and logs its command and version. Explicit CLI pins
  remain authoritative; broken installations and failed probes are reported
  without silently selecting another version.
- App Server initialization errors include the selected command and version.
  Initialization remains the startup protocol check; this does not certify all
  later task operations against arbitrary future CLI versions.
- No existing launcher configuration is changed. To enable discovery after
  deploying this source, set the intended launcher's `codex_cli` to `"auto"`.
  Existing running clients retain their command; no restart or install is automatic.
- Validated with 189 runtime, adapter, and control contract tests. A live test
  using npm CLI 0.153.4 returned `JARVIS_CLI_AUTO_OK`; the single-turn Hold stopped
  at its configured turn limit and exact thread/turn history retained the answer.

## Managed Hold lifecycle

- Added `jarvis_hold` as the public lifecycle entry. It can create a task or
  resume an existing task while retaining one explicit `hold_id`.
- Moved continuation authority out of Hold. For each exact held turn, Monitor
  reads terminal status and content before emitting one bound `CONTINUE` or
  `STOP` command; Hold validates the command's hold and turn identity before it
  starts another turn.
- Added opt-in milestone and terminal notification events. They are persisted
  locally and can be delivered only through a configured verified notifier with
  a saved delivery readback; no notification is enabled by default.
- New work is stored under `task-holds/`. Status reads and the user-host runner
  retain compatibility with legacy `task-monitors/` state.

## MCP control surface

- Added an official Python SDK MCP server for the six first-priority Jarvis tools:
  create, read, resume, monitor, heartbeat, and notify.
- MCP tools call the harness-neutral `JarvisControl` facade and the existing
  versioned capability port; they do not embed a HostBridge implementation.
- New thread creation is queued through the provisioning adapter to a
  normal-user Hold Host. It creates durable `thread/start` tasks and returns
  phase receipts from `accepted` through `holding` and exact-turn terminal
  readback; resume uses `thread/resume` before `turn/start`. Unavailable notify
  and heartbeat paths return `unsupported`.
- Fixed the production-package import path used by the normal-user Hold Host.

# Jarvis 0.1.3

## Independent Monitor output layer

- Added a durable, bounded Monitor engine separate from Heartbeat scheduling.
- A Monitor observes an existing thread only; a new completed turn can fan out to
  an explicit thread resume, the source thread resume, and/or the Jarvis Bot outbox.
- Output messages use a built-in structured completion summary by default and
  accept per-output `user_message_text` / `notification_text` overrides.
- Resume outputs persist a deterministic `client_user_message_id` before calling
  the runtime adapter; Bot outputs remain `QUEUED` until the existing outbox
  records delivery.

# Jarvis 0.1.2

## Stable Desktop message identity

- Imported the checked-in Jarvis runtime baseline used by the local control plane.
- `wake-now` now requires a caller-provided `client_user_message_id` and passes it
  unchanged to App Server `turn/start.clientUserMessageId`.
- Heartbeat receipts persist that ID separately from `source_event_key`; legacy
  heartbeat databases migrate with a nullable compatibility column.
- The controller rejects missing or whitespace-only message IDs before opening an
  App Server client, so no generated UUID can become a Desktop message identity.

# Jarvis 0.1.1

## Capability input port

- Added `jarvis-capability-request/v1` and `jarvis-capability-receipt/v1`.
- Added a caller-owned `JarvisCapabilityPort` for monitor, resume,
  monitor-to-resume composition, and heartbeat control requests.
- The Port leaves HostBridge adaptation and external delivery outside the
  standard package; each caller supplies its own integration adapter.
- No scheduler, credential, or existing runtime state is migrated by this release.
