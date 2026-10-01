# Controller surface and original acceptance

The retained MCP API baseline is 0.2.5. Existing create, Hold, Loop, close, read, resume, monitor, heartbeat, update and notification routes remain available. The dot adaptation also exposes contract checking, exact operation receipts, callback, unchanged relay, independent delivery readback, bounded capacity and owned dispatch recovery.

- `jarvis_contract_check` records actual registered contract/config/source observations; it is not a unit-test wrapper
- `jarvis_callback` and `jarvis_relay` require exact source/target identities and terminal readback; relay preserves source bytes
- `jarvis_notify` and `jarvis_read_delivery` distinguish submitted/outbox state from actual receiver delivery. A TEST inbox uses a separate receiver process and storage
- `jarvis_capacity` binds its full declaration and separate child identities; changed-input reuse or ambiguous duplicate dispatch is rejected
- `jarvis_dispatch` persists a real starting intent and exact owner before dispatch. Recovery of an orphaned intent remains `requires_readback` without replacement

Use one versioned public Jarvis surface for requested actions and readbacks. Keep source, deployment policy and packet frozen during a formal suite. Never mix results across different runs or claim an old passed case repairs a later failed run.

The unchanged original `agent-controller-live-test` SKILL, acceptance matrix and full P0–P7 process are the full-suite acceptance authority. Source inspection and business execution do not replace that standard. The recorded original-suite outcome and remaining P5 gap are in [VALIDATION.md](VALIDATION.md).
