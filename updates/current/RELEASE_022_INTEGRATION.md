# 0.2.2 source integration

## Provenance and decisions

- Public main baseline: `fbf564cd1a20cc5b31b88a65960f53f2ffe496c1`.
- Accepted rotation, scheduler-result and host-aware recovery source: `99f406c2799f11b9a32c797c47a6ac13bb070715`.
- Final-answer JSON intake source: `1c53a34974f89dcb61fabc6c821781bc203d9921`. This capability was absent from both integration parents and is intentionally included, not inferred from a release label.
- Portable examples and documentation reconciled against public `912368b7832db2af446130dd6c865addf8c13935`. Private feature-branch ancestry and operational receipts are excluded.
- Retain production notification behavior, existing-thread adoption, explicit worker capacity and idle-only capacity upgrades. Resolve the older forced minimum of ten in favor of the current explicit capacity contract. Existing Loop expectations for requested two/three workers remain unchanged.
- Existing capacity regressions now check explicit one, three, twenty and twenty-four workers; the two-loop shared-host fixture explicitly provisions twenty. No arbitrary twenty-worker ceiling is introduced.
- Final-answer intake keeps its exact bound receipt and hash checks fail-closed. Business payloads may remain unverified for review; an explicit runtime safety stop still wins. Rotation carries the intake mode into both initial and continuation prompts.

## Verification

- Before integration: 53 existing Hold recovery/write, holder and Loop tests passed.
- Affected remaining bridge, heartbeat, store and monitor modules: 94 tests passed.
- Integrated intake/native launcher/holder/provisioning/Loop/MCP/monitor check: 170 tests passed.
- Final cross-contract, host recovery, receipt-write, wiring, notification, adoption and sanitized heartbeat check: 145 tests passed. These runs overlap and must not be added as a unique-test total.
- Added one release-integration case for five-turn rotation followed by the final remaining item, with controller reload, intact intake contracts and no repeated item dispatch. Extended the existing intake monitor matrix with explicit runtime safety stop. Both are traceable to integrating the requested accepted capabilities.
- Existing legacy-state, exact recovery, repeated stop, concurrent healthy holder and failed receipt/cleanup tests remain in the canonical suite. No production state or installed test instance is used.
- A stopped non-recovery request is cancelled before dispatch. A recovered stopped request observes only the persisted exact turn and cannot continue; missing thread/turn identity does not authorize a new turn. Existing recovery tests assert no create/resume dispatch.
- `git diff --check` and the existing Hold Host CLI help passed. The tracked tree contains no known machine-specific account path or actual recipient from the replaced templates.

## Deployment boundary

This document records source verification only. It does not certify installation, a running service's source snapshot, current MCP connections, publication or actual release of any operational Hold. Those must be read back independently by the release controller. Healthy running work must not be restarted or replayed to activate this source.

## Attempt 2: release identity correction

- Independent review rejected attempt 1 for one P2: MCP initialization advertised 0.2.1 while package metadata declared 0.2.2. The separate independent related-core run passed 290 tests; it did not waive that release-identity defect.
- Ran the existing version-targeted test first: one passed, confirming it still encoded the stale version rather than checking release metadata.
- Changed the existing MCP version constant to 0.2.2. Updated the same canonical test to compare server identity with `pyproject.toml` and perform the actual SDK legacy initialization handshake, then read `client.session.server_info.version`.
- Targeted MCP module verification: 21 tests passed. The wider 290-test run was not repeated because the correction is limited to release metadata and its MCP assertion.
- Candidate remains pending independent acceptance (`self_accepted=false`). No installation, publication or operational runtime action was performed.
