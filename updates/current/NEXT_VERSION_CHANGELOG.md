# Jarvis 0.2.1

## Hold, Loop and output contracts

- A shared normal-user Hold host supports capacity 10 with durable ownership, recovery and stop propagation.
- Finite lanes support arbitrary positive batch sizes and partial final batches, with exact candidate IDs and per-item saved JSON output/receipt verification. Open-ended tasks may omit lanes.
- Optional output schemas use Draft 2020-12, document-local references and no `$id`. A missing, mismatched or invalid saved result cannot be treated as verified output.
- This snapshot uses the saved-file/receipt workflow. Automatic final-answer JSON intake is not included.

## Observer and notification semantics

- Reads expose `read_source`, `execution_source` and `execution_status`, distinguishing a native observer from authoritative Hold execution evidence. Observer interruption alone does not establish task termination; insufficient evidence remains unknown.
- Local heartbeats reconcile terminal Holds. Optional notification delivery requires an actual delivery receipt; queued outbox entries are not delivery proof.
- Technical lifecycle completion and saved-output verification do not establish business-quality acceptance. Existing MCP processes may need a connection refresh after a source upgrade.

## Public packaging and validation

- Product source matches accepted snapshot `279d92641f613b7d88b4f767f26e68359a6b24a1`; the public release commit is separately based on public main and excludes private local ancestry.
- Templates use generic paths; the notification recipient is empty and notifications remain opt-in. Public docs and the affected test fixture no longer contain the previous machine-specific values.
- New release content and assets are sanitized. Existing public history is retained without rewriting or purging it.
- Existing core/adapter/runtime tests are used. The inherited host-capacity test still expects 2 instead of the implementation's 10 and is reported as a known failure, not a passing test.

# Jarvis 0.1.9

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
