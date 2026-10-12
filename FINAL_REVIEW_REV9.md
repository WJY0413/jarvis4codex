# Jarvis 0.3.0 rev9 — current local review receipt

This is the current receipt, not a public release or proof that all original
requirements have been accepted. Historical rev6–rev8 tables and the earlier
rev9 version-only stage remain attributed to their own source bytes. No model
turn, business redispatch, production 027 change or Codex restart occurred here.

## Current implementation

- Metadata and advertised MCP version: `0.3.0`; 51 product Python files.
- Windows code SHA256: `621c3db30dac2adcbcd71a05a72428350cc44f445cf6ca29792d6e2b51a6a226`.
- Linux/POSIX-key code SHA256: `016a38db534153a5d674b89b112af5e846963da59cc4a4cf0cb0221f37b32b12`.
- Original-generation reads and validated continuation keep exact owner,
  generation, dispatch, terminal and release gates. Missing authoritative
  recovery backend still returns `capability_missing`.
- Startup failures have one cleanup owner; initialize/Home mismatch do not
  double-close. Ordinary create does not promise idempotent replay; an unknown
  submission must be read back, not blindly resubmitted.

## Adjustable real-time machine-memory admission

The launcher configuration accepts one parameter:

```json
{"memory_admission_max_used_percent": 95}
```

Default is 95; finite numeric values in `(0,100]` are supported. This is a
deployment configuration field, not a per-request bypass or a claimed hot-reload
interface. At each new create/resume/growth and actual spawn/thread/turn dispatch,
sample current **whole-machine physical** total/available memory. Refuse when
`100 * (total - available) >= threshold * total`. Windows uses
`GlobalMemoryStatusEx`; Linux uses `MemTotal`/`MemAvailable` from `/proc/meminfo`.
Unknown samples or invalid policy refuse new execution with explicit evidence.
Read/status/close/cleanup remain allowed; internal read-only attach cannot send
new thread/turn writes. Existing healthy tasks are not killed. After pressure
drops, the next explicit request is checked afresh. Previously saved committed
receipts retain their existing replay contracts.

This is **admission protection**, not a running-process hard cap, memory-leak
proof, new background monitor or 20+30/99-seat load acceptance. It imposes no
fixed GB reservation. No real machine-filling experiment was performed.

## Actual verification, without merging duplicate counts

| Evidence | Actual result |
| --- | --- |
| Complete Windows run, final product hash | 600 selected; 599 methods ran: 586 passed, 0 failed, 1 fixture error, 11 skipped, 1 partial; 1 real-network/model case not run. Original receipt is preserved. |
| Correction of that error | `native.config=self.config` only in the existing succession fixture; its complete 22-method module passed on the same product hash, preserving commit-once/owner/recovery assertions. |
| Latest per-selector Windows evidence | 587 passed, 11 skipped, 1 partial, 1 not run; no unresolved failed/error methods. This is a reconciled evidence set, **not** a new single all-green run. |
| Linux affected run | 67 passed; 120-second deadline left 2 not started; cleanup confirmed. |
| Linux bounded follow-up | Those 2 plus the 22-method succession module passed, 24/24. Combined distinct affected methods: 91 passed; not a full Linux suite. |
| Independent source QA | Existing canonical 10/10 passed, including 7 admission methods; additional diagnostic probes are labeled synthetic, not a new parallel suite. |
| Writer final selections | Original legacy failures plus version readback 37/37; admission+recovery 29/29; succession 22/22; canonical entry 7/7. These overlap other runs and must not be summed. |
| Skills | 17 packaged; Skill Creator 17/17 format-valid. Corrected create wording revalidated independently. Format-valid is not 17 live integrations accepted. |

All completed runs confirm owned-process cleanup and no residual owned PIDs;
model calls were zero. Windows skips remain actual platform/privilege boundaries;
the partial method retains its subTest skips. No safety check was removed to
obtain passing results. Before product changes, existing affected checks and the
36 old failures were reproduced; the seven justified memory cases were recorded
all-red before implementation. The one late fixture error also has its preserved
failure receipt before the one-line fixture correction.

## Reproduction and package contents

`RUN_REV8_TESTS.md` and `source/tests/README_SUITE.md` retain their historical
filenames but contain current exact pins and `--expected-version 0.3.0` commands.
Use a fresh isolated receipt directory and the single `source/tests/run_suite.py`
entry; do not read production state. `core` is representative, not complete.
For the adjustable policy, select the seven existing `PhysicalAdmissionTests`
methods in `source/tests/test_host_memory_bounded_readback.py`; no parallel suite
or post-code unit-test collection is needed.

Full source, 17 Skills/resources, original six Schema fixtures and canonical
tests are included. `REV9_*_CHANGES.diff` files distinguish the metadata stage
from later product, fixture and documentation changes. The separately supplied
final per-file manifest and ZIP hash are authoritative for package bytes.
`VERIFICATION_RECEIPT_REV9.json` records receipt locations as **maintenance-only
provenance**, not external dependencies needed to run the extracted source.

The cold-start guide preserves original-environment guard ordering, SAFE_ENV
inheritance, TLS and bounded SDK/session deadlines. It requires an isolated
authorized officially logged-in Home for a live model run; no credentials are
included or copied. The old cloud/staging/live E2E evidence is not assigned to
these source bytes.

## Remaining release boundaries

- New-source real model → registered Schema → exact terminal → release → Loop
  status/close still needs the bounded isolated product test. The local installed
  TEST configuration has live creation disabled; this receipt does not enable it.
- A supported authoritative backend is required for original-generation
  recovery; absence is not silently bypassed.
- 20-local+30-cloud/99-seat real load and cross-environment claim authority are
  not proved by synthetic protocol/preparation tests. Do not redispatch the
  previously completed 50-company or mower queues.
- Optional local integrations need their own installed dependencies and
  permissions. No blanket live-Skill acceptance is claimed.
- TEST activation has a separate actual loaded-version receipt. Production
  migration/public publication and public privacy/license review are separate;
  historical diffs contain local maintenance paths. This is a local review pack.
