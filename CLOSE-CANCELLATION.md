# Close cancellation development candidate

This is an isolated development candidate, not the validated dot.4 release and not a deployed update. It extends the existing `jarvis_close` API and existing `closed_unconfirmed` state. It does not add a force-kill API.

## Public behavior

Call `jarvis_close(hold_id=..., request_id=...)` or `jarvis_close(loop_id=..., request_id=...)` with the exact existing identity. Closing commits an immutable management cancellation before attempting compatible stop delivery or owner interruption. A cancelled Hold cannot be reused; resuming its native thread through another Hold alias is also rejected. A separately authorized replacement must use a new request, Hold and native thread. Close never creates the replacement itself.

| Outcome | Meaning |
| --- | --- |
| `closing` | Scheduling is cancelled; a proven current owner has a pending exact stop request |
| `closed_unconfirmed` | Management cancellation is durable; external execution or release is unconfirmed |
| `closed` | Actual terminal and Hold-release evidence both exist |
| `failed` | The requested identity is invalid or cancellation could not be persisted; no closure is claimed |

`jarvis_read(subject="hold", hold_id=...)` preserves the original execution `status` and exposes `management_status`, `scheduling_closed`, `management_closed`, the cancellation record, and the close report. `jarvis_loop(action="status", loop_id=...)` exposes the same distinction through its close report. A live child keeps its parent Loop in `closing`/`stopping`; cancellation never relabels a live owner as an unknown stopped execution. Readback of closing work may continue, but cannot dispatch another turn.

`jarvis_read(subject="capabilities")` advertises `jarvis_close.durable_management_cancellation` and `cancellation_version` only for an adapter implementing this version. Original `terminal_confirmed` and `hold_released` remain independent. Unknown execution is never capacity-release evidence or a successful business result.

## Durable boundary

Each exact Hold or Loop has one immutable `cancellation.json`. It records the identity, original request/state hash, close request and timestamp. It is atomically published and fsynced. The original request, owner claim, acknowledgement, error and result are retained. A compatible `stop_requested=true` may be added to the original request; its original pre-cancel hash remains in the cancellation record. No original failure is replaced with a fabricated terminal receipt.

The Host coordinator, claim path, recovery path, Holder continuation, and Loop acquisition check cancellation. Hold requests inherit parent Loop cancellation. Admission of a native resume records the known prior owner roots so a late close cannot be bypassed by a new Hold alias.

The final `turn/start` write has an owner-installed hook. It locks all relevant parent/Hold gates in a consistent order, verifies cancellation and request identity, and persists an exact dispatch commit before any pipe write. Close uses the same gate. A new command cannot commit after cancellation. A command committed before cancellation may still be in flight; its immutable record states that delivery is unconfirmed and is never automatically replayed. A prior commit is not reclassified as `not_sent` after cancellation.

Locks are kernel-held file locks, not decisions inferred from saved PIDs. A busy or failed cancellation write returns an error rather than claiming persistence. Malformed cancellation markers block dispatch. Legacy unescaped Loop identifiers are resolved against exact stored Loop/slot identity; ambiguous bindings fail closed.

## Actual interruption and old owners

An active Holder uses its own App Server client and exact runtime thread/turn identity to request interruption once. There is no OS process kill in this close path. External exact-stop delivery additionally checks a per-Host generation nonce; PID equality alone cannot establish ownership, and the delivery path never reaps a PID-based stale request lock.

Legacy unknown owners remain unconfirmed. Their claims are not deleted. On Linux, automatic orphan requeue based only on PID absence is disabled in this candidate; namespace/start-signature recovery is deliberately deferred. Management cancellation works without inventing that missing death evidence.

Already loaded older source does not gain these gates through an in-place file update. Normal supported deployment still uses one consistent source version for MCP and its Hosts. The close report records failure to deliver the compatible legacy stop flag. An old execution may remain unresolved; this candidate does not claim it was killed or consume its capacity as released.

## Integration and validation

The baseline is `Jarvis-dot-business-r1`, source hash `83d896939696867e8506038f0ecf6953d200e91c0bee1070cd91a540f2456291`. Apply the narrow patch when integrating; do not replace a newer source tree with this whole directory. In particular, retain `business-r2-nosubmit` changes to `contact_task.py`. That file is not part of this close patch. Resolve the `operations.py` build-label hunk explicitly against the chosen next-release version.

The development-only offline tests, kept outside this sanitized archive, exercise cancellation, stale PID locks, exact identity, same-thread aliases, Loop cleanup failures, live/unknown readback, and dispatch/cancel ordering using temporary state and fake native clients. They do not start Codex, call models, touch live state, or prove native interruption. The integrated dot package is Linux-only: Windows import currently fails before tests run. No Windows file-lock compatibility is claimed.

Formal acceptance remains the unchanged original `agent-controller-live-test` P0–P7 suite. The integrated dot.5 candidate completed that exact suite; see VALIDATION.md. Relevant review points are P0's actual version/capability contract; P1–P3 lifecycle and continuation; P4 cleanup integration; P5's real native `interrupted` evidence; P6 capacity and cancellation isolation; and P7 durable recovery without replacement dispatch. Local management cancellation cannot substitute for P5, release proof or any original dependency rule. The previous suite's unsuccessful P5 outcome is preserved.

No global configuration, credentials, production database, active source tree or GitHub release was changed by this development work.
