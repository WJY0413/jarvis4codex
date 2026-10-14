# Jarvis 0.3.1 — TEST prerelease

Jarvis coordinates bounded Codex work through Hold, Monitor and Loop.
Version 0.3.1 rolls up the reviewed 0.3.0 revision series through rev10 under one
product version. It is not just a renamed asset: package metadata, MCP advertised
version and product build labels all report `0.3.1`. The release tag is `v0.3.1`.

## What is included

This snapshot inherits the reviewed revision work: managed Hold/Monitor/Loop
lifecycle and receipts; exact-turn result and terminal readback; soft final-answer
JSON capture marked for review, not business approval; bounded dispatch/task
continuation and close/cancellation handling; schema-backed result validation;
and the canonical source/fixture distribution. It also includes the reviewed
native prediction/task continuation, Windows result-archive sharing/binding-lock
repair, and configurable physical-memory admission policy.

This rollup does not add new business logic beyond rev10. Historical protocol
and schema identifiers are unchanged. It preserves existing evidence limits
rather than treating the new version number as completed production acceptance.

## Download and review

Download `Jarvis-0.3.1-test.zip`, `FILE_MANIFEST.json` and `SHA256SUMS.txt`
from the release assets. Verify the ZIP checksum before extracting it into a new
directory. The manifest records the exact bytes of every included file.

```powershell
gh release download v0.3.1 --repo WJY0413/jarvis4codex --dir jarvis-download
Get-FileHash jarvis-download/Jarvis-0.3.1-test.zip -Algorithm SHA256
# Compare with SHA256SUMS.txt before extraction.
Expand-Archive jarvis-download/Jarvis-0.3.1-test.zip -DestinationPath jarvis-test-source
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

The memory switch previously passed 64 directly relevant isolated checks,
including adapter and native-launcher contracts. The Windows result-archive
repair passed its scoped independent review and regression checks. This
version-only rollup does not claim to repeat that business-logic acceptance.

The directly affected version/distribution checks pass. A broader selected
seven-method baseline and final comparison retain six passes and one inherited
HTTP fixture error: its historical 17-tool assertion conflicts with the 19-tool
prediction/task surface already in rev10. That assertion was not changed for
this release. Four separate inherited cancellation fixture testcase failures
also remain recorded. This is not an all-tests-pass claim.

A preceding five-seat, five-turn live run produced 15 fully accepted results
across three healthy seats. One further correct output lacked complete owner
terminal evidence; nine results were not completed. Memory refusals at the then
active 95% threshold were correct policy behavior, not an overall Loop failure.
The distinct owner-terminal anomaly remains unresolved. All 25 results were not
accepted, and this release does not rerun or complete that acceptance.

`DISTRIBUTION_CHECK.json` records the scoped version/distribution result and its
limits. See `source/tests/README_SUITE.md` for the explicit source pin and exact
bounded command. Production readiness, full live-model acceptance and
high-concurrency capacity are not certified. Publication does not activate a
local service. Previous TEST releases and the latest stable release are retained.

Credentials, local runtime state, raw operational receipts, logs, private chat
identifiers and customer/contact data are excluded. The prior public-copy
redactions remain intact. No new license grant is introduced; existing ownership
and third-party terms apply.
