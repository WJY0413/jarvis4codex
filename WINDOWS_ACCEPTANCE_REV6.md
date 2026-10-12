# Windows acceptance — rev6 isolated candidate

The filename and broad counts below preserve rev6 historical evidence. Rev7 corrected the stale `_release_claim` reader mock and one deterministic stopped-unknown close-report race; rev8 platform and sharing-denial repairs are described in the bundled `REV8_REPAIR_REPORT.md`. Those earlier results are not rev9 scores. Fresh rev9 evidence is indexed in `REV9_DELIVERY_REPAIR_REPORT.md`; historical unresolved timing and real acceptance gates are not silently closed.

For current checks, run from this extracted review root using an isolated venv with `source/pyproject.toml` dependencies. The canonical entry selects packaged tests and binds this exact source. A fresh receipt outside source is required; do not run the historical broad commands as a default validation step.

```powershell
$testSource = (Resolve-Path source).Path
$testReceipt = Join-Path $env:TEMP ('jarvis-rev9-windows-' + [guid]::NewGuid().ToString('N'))
& '<TEST_VENV>/Scripts/python.exe' -B source/tests/run_suite.py --source $testSource --expected-version 0.3.0 --expected-code-sha256 621c3db30dac2adcbcd71a05a72428350cc44f445cf6ca29792d6e2b51a6a226 --receipt-dir $testReceipt --profile core --deadline 90
```

The complete first pass ran **382 test methods**: 259 + 45 + 72 + 6. Method-level accounting is 363 successful methods, 14 skipped methods, and 5 non-successful methods. There were **4 failure records and 2 error records**; the adapter method emitted two separate failing subTest records, so records must not be subtracted as methods. The first `tests` run had 2 failures, 1 error, 14 skips; adapter contracts had 2 subTest failure records in 1 method; runtime had 1 error; monitor was 6/6. A second complete adapter-contract run was 45/45. This is not an all-green full run.

| Exact failing method | Classification and evidence | Action/limit |
| --- | --- | --- |
| `test_integrated_boundaries.IntegratedBoundaries.test_permission_unknown_growth_does_not_reap_original_owner_or_enable_recovery` | Linux/POSIX-specific fixture patches `os.kill` to simulate EPERM. On Windows `_pid_is_alive` uses `OpenProcess`; fake PID 987654 is truly absent, so the fixture's expected `True` fails. No evidence of production safety bypass. | Keep Windows actual PID probe; port the fixture to mock the Windows API if this cross-platform contract is required. Not changed here. |
| `test_secure_inbox_capability_fake.SecureInboxCapabilityFakeTests.test_supported_linux_flags_and_secure_open_are_not_weakened` | Asserts Linux `O_DIRECTORY`, `O_NOFOLLOW`, `O_CLOEXEC`, `dir_fd` secure-open behavior and `_SECURE_STORAGE_SUPPORTED=True`. Native Windows intentionally lacks that Linux capability. | Applicable on Linux only; do not disable secure-storage checks to make Windows green. |
| `test_legacy_cloud_contracts.JarvisHostIdentityContracts.test_linux_process_start_time_uses_utc_and_proc_ticks` | Linux `/proc` fixture patches `os.sysconf`, absent on native Windows; `AttributeError` occurs before product call. | Linux fixture/test boundary; Windows birth-time path is exercised by the real child first-create test. |
| `test_jarvis_task_hold_host.HostCapacityRegressionTest.test_stopped_hold_drains_after_concurrent_claim_owner_reader` | Old mock `reader_during_release(path)` does not accept the existing `_release_claim(..., expected_request=request)` keyword. The error occurs inside mock invocation, not in release logic. | Fixture maintenance remains; do not remove exact-request guard. |
| `test_hold_host_recovery.HoldHostRecoveryAdapterContractTest.test_host_owned_stop_quiesces_only_exact_holder_and_unknown_never_releases` | First full-directory run had two failing subTests: a `failed` vs expected `rejected` during concurrent cleanup, and late healthy-seat release. The exact method reran 1/1 and the full adapter directory reran 45/45. | Timing-sensitive result is unresolved, not relabeled as pass. Reproduce/diagnose before claiming full deterministic Windows acceptance. |

The 14 skips in `tests` were 4 Windows account `WinError 1314` symlink-creation cases in first-create negative checks and 10 previously classified platform-gated cases. Security predicates were not removed. The new Windows first-create group was **15 run, 0 failure, 0 error, 4 skip**; the same group on Linux was **15 run, 0 failure, 0 error, 1 Windows-only skip**. A Windows child with exit code **259** was independently read back as `strict_alive=False`; a live child passed exact non-dispatch proof and a read-only replay after exit. These use TEST state and no model calls, and do not prove the deployed Host path.

Focused current rerun (new receipt; retains the strict Linux recipe's Windows skip):

```powershell
& '<TEST_VENV>/Scripts/python.exe' -B source/tests/run_suite.py --source $testSource --expected-version 0.3.0 --expected-code-sha256 621c3db30dac2adcbcd71a05a72428350cc44f445cf6ca29792d6e2b51a6a226 --receipt-dir ($testReceipt + '-focused') --select 'source::test_cloud_coldstart.CloudColdstartTest.test_official_stdio_coldstart_is_bounded_and_isolated' --select 'source::test_hold_host_recovery.HoldHostRecoveryAdapterContractTest.test_host_owned_stop_quiesces_only_exact_holder_and_unknown_never_releases' --deadline 60
```

Neither the rev5 cloud Linux 375/0/5 result nor the older product live E2E is a rev6 result. No new contact research or production state was used.
