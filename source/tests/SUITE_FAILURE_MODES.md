# Required failures, recorded before runner/fixture implementation

Basis: Cooper's 2026-10-08 tests-only instruction; reuse the earlier exact-rev7
385-method and dirty-root 195-method receipts. No full baseline repetition.

1. Wrong/missing source, wrong version or hash, or imported root package must
   reject before any case. Save the rejection receipt; never fall back.
2. Discovery must include nested source tests and retained legacy contracts,
   exclude paid/live onboarding from execution, and deduplicate selectors.
3. Eight-block/profile/selector filtering must select actual methods. Missing
   required evidence must be not_run/incomplete, never passed or silent skip.
4. Count method outcomes independently from subTest failure/error/skip events;
   a method with skipped children is partial, not fully passed/fully unexecuted.
5. Preserve assertion failures/errors and their first tracebacks. A passing
   subset cannot imply product acceptance or erase inherited failures.
6. Deadline, cancellation, worker crash, or KeyboardInterrupt must still leave
   a receipt, progress and report. Unstarted cases remain not_run.
7. Tests may create descendants. Cleanup must target the owned process group
   or Windows Job (including venv launcher/interpreter), never shared services.
   Missing cleanup proof is incomplete. Sample idle/peak/end/repeat resources;
   no invented performance threshold or leak verdict.
8. Shared HTTP: real SDK clients must not cross-bind schema results; same
   schema/version replay preserves bytes; conflicts reject; disconnect/reconnect
   preserves server/state. Unknown Hold close must not fabricate remote release.
9. Resources, input/fixtures and logs must be under a fresh run directory. Reject
   output inside target source or an existing nonempty directory. No production
   state/config/auth, real model turns, notification sends or deployments.
10. The suite is root canonical tests/ with one internal entry; immutable target
    tests are reused by explicit source path. No product file or dirty test edits.
