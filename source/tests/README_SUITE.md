# Canonical test runner — 0.3.1 TEST

This is the single bundled test suite. Product metadata is `0.3.1`.
The runner retains its historical rev9 default identity; always pass the explicit
0.3.1 hash below. A rejected identity or selector means tests did not run.

## Repeat the bounded Windows distribution check

From the extracted directory containing `source/` and `skills/`, use Python
3.11 or later in an isolated environment with dependencies installed from
`source/pyproject.toml`. No cloud setup guide is included in this snapshot.

```powershell
$testSource = (Resolve-Path 'source').Path
$testRun = Join-Path $env:TEMP ('jarvis-0.3.1-dist-' + [guid]::NewGuid().ToString('N'))
$testPython = '<TEST_VENV>/Scripts/python.exe'
& $testPython -B source/tests/run_suite.py --source $testSource --expected-version 0.3.1 --expected-code-sha256 df06d24745126729990864b041cd8e70d24b9f455935fb7e2ae2a2a7f7337cef --receipt-dir $testRun --select source::test_distribution_identity.DistributionIdentityE2E.test_unrelated_installed_dependency_does_not_change_product_identity --select source::test_027_registry_versions.CurrentRegistryVersions.test_distribution_and_advertised_mcp_version_are_same_prerelease --select source::test_027_registry_versions.CurrentRegistryVersions.test_all_product_builds_use_the_same_candidate_version --deadline 45
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
failures and the separate inherited HTTP fixture assertion of 17 tools against
the existing 19-tool surface. Historical suite comments and fixtures are retained, not presented as
new 0.3.1 acceptance evidence. No test expectations were changed for publication.
