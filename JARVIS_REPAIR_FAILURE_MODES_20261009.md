# Bounded rev7 follow-up before editing

Authority: Cooper's 2026-10-09 request to continue repairs. Single writer, original
isolated `source-rev6` candidate, version 3.0.0a2, product hash
9f8efa97e57b6584d5863e589846b479e97c3b99a8631264f515208ee57fde8d.
Frozen rev7 archive, installed releases, dirty root products, production 027 and
Codex are protected. Existing worktrees are not interchangeable with this archive
snapshot; no worker is routed to an unrelated older Git commit. The earlier
user-approved archive-based isolation remains the authority, not an invented
Workpack prerequisite.

Existing relevant tests ran first. `JARVIS_REPAIR_RED_20261009/receipt.json`:
5 methods, 1 passed, 2 failures, 2 errors; clean owned worker exit. Separate
9-method reproduction also reproduced WinError 10106 in the Linux-only recipe.

Observed failure modes and intended corrections:

1. Permission-unknown fixture patches POSIX `os.kill`, but Windows implementation
   uses OpenProcess. A nonexistent synthetic PID returns false before that mock.
   Simulate the POSIX module capability locally; do not change real liveness,
   capability_missing, owner bytes, or release gates.
2. Linux birth fixture does not create the absent mocked `os.sysconf` on Windows.
   Supply the already-simulated Linux API. Preserve exact UTC/tick assertion.
3. Complete Linux secure-capability fake inherits Windows' absent POSIX flags and
   geteuid. Supply an explicit synthetic POSIX capability set; all storage calls
   remain mocks. Keep each missing-capability failure and original open flags.
   This is not proof of real secure Windows inbox support.
4. Healthy concurrent fake has a one-second timeout field, but the actual monitor
   calls wait_forever=True. The historical failure is not proved to be that timer.
   An unmodified 1.2-second delayed-readback probe passed all seven subscenarios;
   keep this fixture unchanged until the actual exit reason is obtained. Keep
   explicit terminal notifications/cleanup and exact-other-seat assertions.
5. Cloud recipe deliberately removes SYSTEMROOT under its strict Linux SAFE_ENV.
   WinError 10106 is reproduced and matches CPython issue 118234. Do not enlarge
   SAFE_ENV. Mark the Linux success scenario inapplicable on Windows and verify
   its pre-spawn guard and silent-process cleanup on Windows as before. Normal
   Windows protocol verification is a separate actual SDK TEST case, not this
   Linux recipe. Linux success needs a new Linux execution after fixture changes.

No new product safety behavior is inferred. Use canonical selectors and receipts;
report skipped cases explicitly. Resource samples/clean exit are measurements,
not a leak-free or capacity-99 acceptance claim. No paid model work or business
dispatch. TEST 18767 absent and its last log does not establish why it exited;
do not silently add an auto-restart or update-on-access mechanism.

## Confirmed additional defect before product edit

File-backed adapter reproduction `JARVIS_REPAIR_ALIAS_RED_20261009` injects one
PermissionError on a different seat's ack during real cancellation alias lookup.
The healthy holder closes its client before any terminal notification (assertion
`closed.is_set()` fails). Existing control_poll retries only request.json reads,
not the cancellation alias read. Minimal correction: include that observational
lookup in the same existing transient-read retry. No dispatch/interrupt/release
is sent on a denied read. Malformed JSON, identity mismatch and unknown terminal
must retain existing fail-closed behavior. Preserve original concurrent test and
its strict healthy-seat assertion. The historical failure itself still lacks an
exit traceback; this confirmed fault path is not proof of its unique cause.

## Canonical entry compatibility gap reproduced

Running the existing seven entry E2Es against the explicit repaired product hash
produced six failures: nested runs silently fell back to the rev7 default hash.
The entry also asserts a hard-coded 385-method source count, now 386 after the
justified new regression. Preserve default rev7 pin and source rejection; pass
the outer already-verified expected identity to nested runs and compare source
discovery against actual source methods, not a historical count. No weakening
of explicit wrong-hash/version, method/subTest, deadline or cleanup assertions.

Independent QA caught another exact request.json PermissionError during holder
terminal publication/final cleanup. `JARVIS_REPAIR_PUBLICATION_RED_20261009`
reproduces both possible sites separately before their product changes: 1 method,
2 failing subTests, with host_execution_error despite the synthetic exact final.
Retry only transient PermissionError for at most 0.2 seconds in metadata reads,
re-reading and revalidating the generation after success. Exhausted permission,
invalid JSON, wrong request_id, live owner and unknown terminal must still fail
closed. Never retry a yielded side effect or replay model dispatch. This is not
permission to transform a holder exception into an exit-confirmed flag.

Cold review package `JARVIS_REPAIR_COLD_PACK_RED_20261009` also proves that simply
colocating tests loses legacy selector origins. Preserve provenance with generated
bundled_legacy_files metadata, and discover suite E2Es exactly once. Keep one tests
directory and the original source/legacy/suite selector IDs; core must actually run.
