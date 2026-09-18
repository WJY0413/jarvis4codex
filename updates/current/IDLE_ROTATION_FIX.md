# Idle Host and independent rotation fix

Status: ready for independent QA; self-accepted: false.
Base: `ea1580852db88d17db549001adf8cadf18ba3f7f`.

## Defect and minimal change

Every idle Hold Host worker independently enumerated all Hold/Monitor directories
and parsed request/ack JSON before checking for a terminal result. Twenty workers
therefore multiplied both historical reads and health writes. Completed history
was repeatedly inspected, not newly dispatched.

The existing Host now has one coordinator and a bounded pending queue. Configured
worker concurrency is unchanged. Workers only inspect an assigned directory.
Both scanning paths skip result-present directories before JSON reads. Queue plus
in-flight tracking is bounded by worker capacity; no permanent terminal cache or
persisted-state migration is introduced. Removing a result for an explicitly
requeued request remains visible on the next scan. The existing ownership/claim,
exact terminal recovery, controlled-stop and claim-release checks are unchanged.
History is preserved. Directory metadata is still enumerated once per poll, so
this is not a claim of zero cost for arbitrarily large history.

Scheduler health previously reported `running` simply by constructing a service
and reading configuration/SQLite. A completed tick now records PID and timestamp.
A read-only health check returns `recent_tick`, `stale`, `unverified` (legacy
evidence), or `unobserved`, and does not rewrite health. `recent_tick` is evidence
of a recent completed tick, not proof that the PID is currently alive. The MCP
health operation retains its existing top-level `completed` contract and exposes
the scheduler evidence separately.

## Reproduction and comparative benchmark

All fixtures are temporary, synthetic and isolated. Twenty workers, one-second
poll, five-second stop timer; JSON reads count the Hold Host reader. CPU is process
CPU seconds, not percent of machine CPU. Windows contention made the old Host's
in-flight scans continue beyond the timer.

| Fixture | Source | JSON reads | Health writes | CPU seconds | Wall seconds |
| --- | --- | ---: | ---: | ---: | ---: |
| 590 Hold + 21 Monitor, all terminal | base | 24440 | 25 | 21.59375 | 8.42644 |
| identical all-terminal fixture | candidate | 0 | 5 | 0.46875 | 5.12627 |
| 605 terminal + 6 stopped/incomplete, plus scheduler and 383 terminal Loops | base Host | 24560 | 25 | 24.015625 | 9.49243 |
| identical mixed fixture and scheduler | candidate Host | 90 | 35 | 1.59375 | 5.20995 |

The scheduler fixture runs real `HeartbeatService.run_once()` and historical
`LoopStore.active_loop_ids()` each second, with no active business or notifier.
Raw mixed-fixture process CPU seconds fell about 93.4%; the completed-only
comparison fell about 97.8%. Because baseline shutdown was delayed, normalized
CPU seconds per wall second is the appropriate rate comparison: all-terminal
2.563 to 0.0914 (about 96.4% lower), mixed plus scheduler 2.530 to 0.3059 (about
87.9% lower). These are process CPU rates, not whole-machine percentages.
These are implementation measurements, not independent QA or deployed
machine measurements. Historical Loop scanning remains unchanged: the measured
combined cost does not justify an additional caching/state change in this fix.

## Validation and explicit coverage gaps closed

Before edits: 71 existing affected tests passed. After source changes, before new
cases: 79 existing tests passed including directly relevant MCP wiring.

Final command (set `PYTHONPATH=packages;packages/jarvis_runtime` on Windows):

```
python -m unittest tests.adapter_contract.test_hold_host_recovery tests.adapter_contract.test_hold_receipt_write tests.jarvis_runtime.test_jarvis_local_heartbeat tests.contract.test_jarvis_loop tests.adapter_contract.test_mcp_wiring
```

Result: 82 passed. Existing tests covered exact identity, stop/recovery, receipt
writes, rotation contracts and scheduler persistence, but did not reproduce
multiworker completed-history scans or connect a background scheduler to writer
release and new-thread dispatch. Three focused cases were added to those existing
canonical modules, based on the reported defects:

1. Synthetic completed history is not JSON-read while idle; twenty holders can
   be simultaneously active; a 21st request is subsequently consumed; new
   directories and same-directory explicit requeue remain visible; no duplicate
   dispatch and no history removal.
2. A background real scheduler and real function-runner invoke the real Loop
   controller against temporary receipts and a fake native-holder boundary.
   Run count grows while the first writer is still claimed, but a second thread
   is not dispatched. Scheduler/controller reload from the persisted SQLite and
   Loop state; after writer release a distinct second thread is dispatched and
   the bounded loop completes, without an external status call. Thus run-count
   growth alone is not used as proof of rotation.
3. Health reads distinguish no evidence, recent ticks, stale ticks, and legacy
   unverified evidence without manufacturing a current running claim.

Initial new-test runs exposed a Windows atomic-replacement readback race and
default-encoding mismatch in the fixture. The test readback retries that transient
permission condition and explicitly uses UTF-8; no production assertion was
weakened and no business execution was used for validation.

## Operational boundaries

This commit does not register/start a scheduler, restart a Host, send notifications,
modify business queues, activate cancelled jobs, install a release, or prove live
business rotation. Startup registration and fresh process/health/CPU verification
remain controller deployment work. The existing scheduler's unconditional
terminal-notification drain can deliver old pending events when enabled; this
must be reconciled under explicit user direction before activation.

Independent QA must reproduce the benchmark and safety contracts on the frozen
commit. An installation receipt must separately report actual process state,
startup/recovery configuration, CPU sampling and any limits on live-business proof.

## Attempt 2: independent QA health-publication correction

The first independent review rejected candidate
`ba7e63b1ee75e8683d1f1237679cf22a0bc0b57a`: its 82-test run had 81 passes
and one timing-sensitive failure. A normal scheduler health-file truncate/write
made the newly added reader return `unobserved` between otherwise valid recent
ticks. A focused rerun passing did not erase that failure. The independent probe
was rerun against the implementation worktree before this correction and again
reported `before=recent_tick`, `during=unobserved`, `after=recent_tick`.

Correction is confined to the existing heartbeat module: publish a complete JSON
tick through a unique same-directory temporary file and atomic replacement.
Replacement retries Windows `PermissionError` up to 20 attempts using the existing
receipt-writer backoff pattern (50 ms increasing to 250 ms). If publication still
fails, the prior complete health file survives and the failure is raised; temporary
cleanup does not mask the publication outcome. No adapter import or new schema.

Two focused cases in the existing heartbeat test module capture the reproduced
failure window and Windows sharing behavior. One pauses the temporary write and
requires the original `recent_tick` evidence to remain readable; the other verifies
transient replacement retry plus bounded persistent failure preserving previous
bytes and removing temporary files. The original background rotation assertion
remains unchanged.

Attempt-2 validation: `python -m unittest tests.jarvis_runtime.test_jarvis_local_heartbeat`
passed all 13 tests, including autonomous rotation/reload and both new atomic-write
checks. The full 82-test collection was not rerun. Previously independent Host
capacity/exception-cleanup, recovery contracts and 90.81% normalized synthetic
CPU-rate reduction remain separate prior evidence; the first QA failure remains
recorded above. Await a new independent health/integration review; no deployment
or self-acceptance is claimed.
