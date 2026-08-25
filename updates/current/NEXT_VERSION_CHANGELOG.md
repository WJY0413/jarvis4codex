# Jarvis 0.1.6

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
