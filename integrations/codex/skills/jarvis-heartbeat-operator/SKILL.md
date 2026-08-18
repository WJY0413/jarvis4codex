---
name: jarvis-heartbeat-operator
description: Control a Jarvis product heartbeat through the versioned jarvisctl/API surface. Use when creating, listing, inspecting, pausing, resuming, cancelling, or reviewing a heartbeat schedule and its run receipts.
---

# Jarvis Heartbeat Operator

Use only the product CLI/API. Do not edit runtime SQLite files or call a harness directly.

1. Read the schedule and adapter health.
2. Preview target, cadence, stop condition, expiry, and declared effects.
3. Apply the required confirmation gate.
4. Execute through `jarvisctl`.
5. Read back the schedule and receipt; report business, execution, and delivery status separately.
