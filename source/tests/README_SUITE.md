# Canonical test runner — rev10 TEST

This is the single bundled test suite. Product metadata is `0.3.0`.
The runner retains its historical rev9 default identity; always pass the explicit
rev10 hash below. A rejected identity or selector means tests did not run.

## Repeat the bounded Windows distribution check

From the extracted directory containing `source/` and `skills/`, use Python
3.11 or later in an isolated environment with dependencies installed from
`source/pyproject.toml`. No cloud setup guide is included in this snapshot.

```powershell
$testSource = (Resolve-Path 'source').Path
$testRun = Join-Path $env:TEMP ('jarvis-rev10-dist-' + [guid]::NewGuid().ToString('N'))
$testPython = '<TEST_VENV>/Scripts/python.exe'
& $testPython -B source/tests/run_suite.py --source $testSource --expected-version 0.3.0 --expected-code-sha256 2005b02ba488a14675661dc1df03397e8aaaa141244057e8bfaa5350d73763fd --receipt-dir $testRun --select source::test_distribution_identity.DistributionIdentityE2E.test_unrelated_installed_dependency_does_not_change_product_identity --select source::test_027_registry_versions.CurrentRegistryVersions.test_distribution_and_advertised_mcp_version_are_same_prerelease --deadline 45
Get-Content (Join-Path $testRun 'report.md')
```

This Windows hash belongs to the public sanitized package. The identity algorithm
uses platform path separators; it is not a Linux identity pin. No new Linux run
is claimed by this publication. The file manifest is platform-independent.

Always use a fresh receipt directory outside `source/`. The runner does not
install dependencies, edit source, register MCP tools or start model work.
Selectors must match full discovered method identifiers, not module names.
Use `--help` for options and `--list` with the same explicit version/hash and
a fresh receipt directory to inspect selectors without executing methods.
Choose only checks relevant to a change; do not default to the full suite.

## Receipts and limits

The runner writes a receipt, report, plan, progress, worker log and case artifacts.
Keep these internal if they contain local paths. Exit 0 means only the selected
scope passed (or was listed); 1 records failures/errors, 2 incomplete evidence,
3 source rejection/runner error, 124 deadline and 130 cancellation.
A passing mock or isolated contract does not prove a live model or resident
process loaded this release. Windows private Job cleanup applies only to the
test's owned process tree, not other applications.

See the package README for current limitations, including the unresolved live
owner-terminal evidence gap and four inherited cancellation fixture testcase
failures. Historical suite comments and fixtures are retained, not presented as
new rev10 acceptance evidence. No test expectations were changed for publication.
