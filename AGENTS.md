# Jarvis Control Plane

- Treat this repository as the product source. Do not read or write production Jarvis runtime state from tests.
- Keep harness-specific calls inside `adapters/`; engine and monitoring code may consume only the SDK contract.
- Skills are control-plane integrations only. They must call versioned CLI/API surfaces and must not embed runtime implementation.
- Keep current release work in `updates/current/`; archive shipped notes under `updates/history/`.
- Require contract tests for every harness adapter and migration tests for every persisted-state change.
- Maintain one canonical test suite under `tests/`; do not create parallel suites or update-specific test collections. For a module change, run only its relevant core tests, including directly affected adapter contracts or persisted-state compatibility checks. Do not default to full-suite runs or repeatedly broaden testing without an explicit user request.
- Reuse existing tests first. Add or change a case only when traceable to an explicit requirement, an established contract, or a reproduced defect, and identify that basis. Do not invent requirements, hypothetical scenarios, or redundant tests to inflate coverage or test counts. Keep necessary cases in the same canonical suite.
- Run the existing relevant core tests before writing or modifying any tests. Do not write tests first and then run them as the validation workflow; do not use test-driven development by default. If existing coverage is missing, report the specific gap and its evidence before adding a justified case under the rule above. Never change expectations merely to make the implementation pass, or present newly authored passing tests as independent proof of correctness.
- Preserve the existing project structure. For bug fixes, modify existing modules and their existing tests first. Do not add, move, rename, or delete directories, packages, public entrypoints, CLI wrappers, deployment frameworks, or architectural layers unless Cooper explicitly approves that structural change. New focused tests within existing test areas are allowed.
