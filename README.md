# Jarvis 0.3.0 rev9 — TEST prerelease

This snapshot contains the Jarvis control plane source, 17 integration Skills,
the canonical tests and their fixtures. It is a TEST distribution, not a stable
production release. Package metadata remains `0.3.0`; the GitHub release tag is
`v0.3.0-rev9-test`. Prediction/task additions proposed for 3.1 are not included.

## Download and inspect

Download `Jarvis-0.3.0-rev9-test.zip`, `SHA256SUMS.txt`, and `FILE_MANIFEST.json`
from the release's Assets section. Verify the ZIP SHA256 before extracting it
to a new directory. Prefer this named asset over GitHub's generated source ZIP
when comparing the archive checksum. The manifest pins every included file.

```powershell
gh release download v0.3.0-rev9-test --repo WJY0413/jarvis4codex --dir jarvis-download
Get-FileHash jarvis-download/Jarvis-0.3.0-rev9-test.zip -Algorithm SHA256
# Compare with SHA256SUMS.txt before extraction.
Expand-Archive jarvis-download/Jarvis-0.3.0-rev9-test.zip -DestinationPath jarvis-test-source
```

Source is under `source/`; Skills are under `skills/`. Python 3.11 or later is
required. Install declared dependencies into a separate environment, following
`source/pyproject.toml`. In that environment `python -m pip install ./source`
installs the Python package. Skills/resources remain in the extracted archive;
pip does not register them with a client. Keep the full extracted source for
canonical tests and fixtures.

For isolated cloud setup, read `CLOUD_COLD_START_GUIDE_REV6.md` (its contents
describe rev9 despite the historical filename). Follow its original-environment
guard, explicit TEST paths and bounded read-only protocol procedure. Do not copy
authentication files or reuse production state. This archive does not change
client MCP registrations, install an autostart service, or restart Codex.

## Verification and limits

`VERIFICATION_RECEIPT_REV9.json` preserves historical results and attribution;
its `public_release=false` describes the original review stage. The current
publication is a prerelease and does not change full acceptance to complete.
Historical Windows reconciled evidence is 587 passed, 11 skipped, 1 partial and
1 not run; this is not a newly executed single full-suite result. Linux affected
checks cover 91 distinct methods, not a full-suite claim.

New distribution checks belong to `DISTRIBUTION_CHECK.json`. They verify source
identity and the shipping layout only; no live model acceptance is implied.
Use `source/tests/run_suite.py` as the canonical entry; its `--help` describes
explicit source, receipt, selectors and deadline arguments.

Real model-to-Schema-to-terminal-to-release/close acceptance on this source,
optional integration acceptance and claimed high-concurrency capacity remain
incomplete. The current local TEST HTTP activation is separately blocked by an
execution-tool policy rejection. A GitHub download cannot override that policy.

No new license grant is introduced by this distribution. Existing ownership
and third-party terms continue to apply.
