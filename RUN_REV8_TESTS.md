# Independent review commands

Filename retained from rev8; all commands below target this rev9 `0.3.0` candidate. Historical rev8 results remain in `REV8_REPAIR_REPORT.md` and are not new scores. Fresh rev9 receipts are indexed in `REV9_DELIVERY_REPAIR_REPORT.md`.

The complete product source, all 17 Skills with current 0.3.0 labels, migrated canonical fixtures and the
canonical test entry are bundled. Extract to a new directory, not a production
release or active checkout. Use a disposable Python >=3.11 venv with the declared
source/pyproject.toml dependencies. No credential copy or real model request is
needed. receipts must be fresh and outside source/.

Windows, from the extracted review root:

```powershell
$source = (Resolve-Path source).Path
$receipt = Join-Path $env:TEMP ('jarvis-rev9-' + [guid]::NewGuid().ToString('N'))
& '<TEST_VENV>/Scripts/python.exe' -B source/tests/run_suite.py --source $source --expected-version 0.3.0 --expected-code-sha256 621c3db30dac2adcbcd71a05a72428350cc44f445cf6ca29792d6e2b51a6a226 --receipt-dir $receipt --profile core --deadline 60
Get-Content (Join-Path $receipt 'report.md')
```

Linux, from the same extracted review root:

```sh
<TEST_VENV>/bin/python -B source/tests/run_suite.py --source "$PWD/source" --expected-version 0.3.0 --expected-code-sha256 016a38db534153a5d674b89b112af5e846963da59cc4a4cf0cb0221f37b32b12 --receipt-dir /tmp/jarvis-rev9-review-fresh --profile core --deadline 90
```

Use a new receipt path for each profile. entry verifies pin/selection/statistics
and owned timeout/cancellation cleanup; protocol verifies actual shared HTTP/SDK
clients, Schema isolation/replay/reconnect and unknown-close safety; resources
records resource measurements. core is representative, not complete acceptance.
all/acceptance are explicit broader runs and retain platform skips and missing
real evidence; do not call them green when the receipt says incomplete.

Focused repaired lifecycle cases: replace --profile core with repeated --select:

```text
source::test_hold_host_recovery.HoldHostRecoveryAdapterContractTest.test_transient_other_seat_read_denial_does_not_exit_healthy_holder
source::test_hold_host_recovery.HoldHostRecoveryAdapterContractTest.test_transient_generation_and_final_read_denial_preserves_real_cleanup
source::test_hold_host_recovery.HoldHostRecoveryAdapterContractTest.test_host_owned_stop_quiesces_only_exact_holder_and_unknown_never_releases
```

The bundled canonical README and cloud commands use this exact rev9 version and
platform hash. These fixed pins are not an automatic "accept current bytes" flag.
The rev6 acceptance/Windows/Skills counts and rev8 repair report are explicitly
historical. The cloud guide keeps its rev6 filename but its bounded read-only
recipe targets rev9; the expired Oct 5 model window launches no present task.
This package authorizes no service install/config switch or public release.
