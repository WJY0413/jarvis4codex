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
