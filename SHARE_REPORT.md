# Share report: Jarvis rev10 TEST

This distribution makes the reviewed prediction/task, Windows result-lock and
memory-admission changes available for source review without bundling a local
installation or its state. It advances the TEST release line only. The previous
TEST assets and the stable release are retained.

Start with `README.md`, inspect `source/packages/`, then use the canonical suite
under `source/tests/` for explicitly selected checks. Skills are unchanged from
rev9. `DISTRIBUTION_CHECK.json` is a bounded packaging check, not model acceptance.

The public copy comes from the reviewed candidate, not the repository's dirty
working tree. A historical private requirement-chat identifier in dispatcher
metadata is blanked. A native-launcher fixture uses a clearly synthetic UUID.
Those redactions do not change admission or dispatch behavior. Existing public
author/operator wording and compatibility parsing are retained.

No local launcher, credentials, runtime databases, raw logs, customer records or
internal audit receipts are included. The product memory gate still defaults to
enabled. Explicit per-machine opt-out remains possible and requires newly loaded
code; this publication neither reloads nor restarts a resident process.

The known owner-terminal evidence gap and inherited cancellation fixture failures
remain visible in the README. Review this as a prerelease, not as production or
complete 5-by-5 acceptance. No license change is part of this publication.
