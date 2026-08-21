# Jarvis Runtime Migration Baseline

This repository now tracks an exact source-only import of the active Jarvis
runtime. The import excludes SQLite databases, configuration, contracts,
logs, credentials, and every other production state object.

## Imported source

- Source root: `C:\Users\22524\Documents\Chief of Staff\tools`
- Runtime modules: `packages/jarvis_runtime/`
- Existing tests: `tests/jarvis_runtime/`
- Verified source date: 2026-08-21

The imported files were SHA-256 matched to their source files before and after
the move into tracked paths. The existing Jarvis heartbeat and native launcher
unit-test files ran unchanged: 67 tests passed.

No production runtime files were moved, overwritten, or configured by this
migration.
