# Jarvis 0.3.0 rev10 — TEST prerelease

Jarvis coordinates bounded Codex work through Hold, Monitor and Loop. This TEST
snapshot adds native prediction/task continuation, a configurable physical-memory
admission gate and the reviewed Windows result-archive locking repair to rev9.
Package metadata remains `0.3.0`; the release tag is `v0.3.0-rev10-test`.

## Download and review

Download `Jarvis-0.3.0-rev10-test.zip`, `FILE_MANIFEST.json` and `SHA256SUMS.txt`
from the release assets. Verify the ZIP checksum before extracting it into a new
directory. The manifest records the exact bytes of every included file.

```powershell
gh release download v0.3.0-rev10-test --repo WJY0413/jarvis4codex --dir jarvis-download
Get-FileHash jarvis-download/Jarvis-0.3.0-rev10-test.zip -Algorithm SHA256
# Compare with SHA256SUMS.txt before extraction.
Expand-Archive jarvis-download/Jarvis-0.3.0-rev10-test.zip -DestinationPath jarvis-test-source
```

Source and canonical tests are under `source/`; 17 existing integration Skills
are under `skills/`. Python 3.11 or later is required. In an isolated environment,
`python -m pip install ./source` installs the declared Python package and
dependencies. It does not register Skills or MCP tools, configure a service,
copy authentication, migrate production state or restart Codex. Keep the full
extracted source to run the canonical tests and fixtures.

Experimental native prediction depends on an existing authorized login and the
supported native prediction interface. No authentication material is included.

## Memory admission switch

The optional launcher setting is a strict JSON boolean. Protection remains
enabled when the setting is absent. The normal configuration is:

```json
{
  "memory_admission_enabled": true,
  "memory_admission_max_used_percent": 95
}
```

Explicit `false` skips memory sampling and threshold checks at queue admission
and native dispatch. It does not disable capacity, identity, read-only or
cancellation checks. `true` restores the existing policy: reject used physical
memory at or above the threshold, and reject unknown measurements. Non-boolean
switch values fail closed. No machine-specific launcher configuration ships in
this package; one workstation's explicit opt-out is not the product default.
Updating files or JSON does not prove a resident Host loaded the new code.

## Verification and limits

The reviewed memory switch passed 64 directly relevant isolated checks, including
adapter and native-launcher contracts. The Windows result-archive repair passed
its scoped independent review and regression checks. Four inherited cancellation
fixture testcase failures remain recorded; this is not an all-tests-pass claim.

A preceding five-seat, five-turn live run produced 15 fully accepted results
across three healthy seats. One further correct output lacked complete owner
terminal evidence; nine results were not completed. Memory refusals at the then
active 95% threshold were correct policy behavior, not an overall Loop failure.
The distinct owner-terminal anomaly remains unresolved. All 25 results were not
accepted, and this release does not rerun or complete that acceptance.

`DISTRIBUTION_CHECK.json` records only shipping-layout/source-identity checks.
Use `source/tests/run_suite.py --help` for the canonical runner. Production
readiness, full live-model acceptance and high-concurrency capacity are not
certified by this prerelease. Publication does not activate a local service.

Credentials, local runtime state, raw operational receipts, logs, private chat
identifiers and customer/contact data are excluded. Two source/fixture metadata
values were sanitized; `SHARE_REPORT.md` identifies the boundaries. No new
license grant is introduced; existing ownership and third-party terms apply.
