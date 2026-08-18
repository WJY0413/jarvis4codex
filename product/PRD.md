# Jarvis v0.1.0 PRD

## Goal

Provide a harness-neutral, auditable runtime for heartbeats and monitors while preserving Codex as the first adapter.

## In scope

- Stable Python contracts for schedules, targets, receipts and monitor events.
- Harness adapter SDK and Codex App Server adapter boundary.
- Separate heartbeat scheduling, monitoring observations and control-plane policy.
- Contract tests and versioned product updates.

## Out of scope

- Migration or modification of the existing production Jarvis heartbeat service.
- Direct external messaging, production business actions, or autonomous code updates.
- A public multi-tenant SaaS control plane.

## Acceptance criteria

1. The engine depends on no Codex-specific fields or modules.
2. The Codex adapter is isolated under `adapters/codex_app_server`.
3. A mock adapter can satisfy the same contract tests.
4. Product source, runtime data and skills have separate ownership boundaries.
