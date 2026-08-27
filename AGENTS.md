# Jarvis Control Plane

- Treat this repository as the product source. Do not read or write production Jarvis runtime state from tests.
- Keep harness-specific calls inside `adapters/`; engine and monitoring code may consume only the SDK contract.
- Skills are control-plane integrations only. They must call versioned CLI/API surfaces and must not embed runtime implementation.
- Keep current release work in `updates/current/`; archive shipped notes under `updates/history/`.
- Require contract tests for every harness adapter and migration tests for every persisted-state change.
- Preserve the existing project structure. For bug fixes, modify existing modules and their existing tests first. Do not add, move, rename, or delete directories, packages, public entrypoints, CLI wrappers, deployment frameworks, or architectural layers unless Cooper explicitly approves that structural change. New focused tests within existing test areas are allowed.
