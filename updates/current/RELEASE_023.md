# Jarvis 0.2.3

## Fixes

- Reduce idle Hold Host work with one coordinator and a bounded pending queue,
  preserving configured worker concurrency. Completed Hold and Monitor results
  are skipped before request and acknowledgement JSON reads; history is retained.
- Report scheduler health from recorded tick evidence rather than configuration
  alone. A recent tick is not a claim that its process is still alive.
- Publish scheduler health atomically, with bounded retries for Windows file
  sharing conflicts, so concurrent readers retain the previous complete record.

## Verification and deployment

The underlying fix passed independent review, including isolated autonomous
rotation, ownership/recovery checks and comparative idle-load measurements.
Live business rotation after this release remains pending; isolated checks do not
constitute business acceptance.

This source package does not automatically register Windows startup tasks.
The local deployment's login and one-minute recovery triggers with `IgnoreNew`
are deployment configuration, not behavior installed by this package. Operators
must configure and verify their scheduler process separately. Existing cancelled
jobs are not reactivated by these changes.
