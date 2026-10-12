# 3.0.0a2 rev8 isolated repair review

Historical rev8 evidence copied into the rev9 delivery candidate. The version and product hashes in this report describe rev8 bytes only; rev9 `0.3.0` changes/results are in `REV9_DELIVERY_REPAIR_REPORT.md`. The original rev8 source/archive remain unchanged.

2026-10-09. Local candidate only, not installed, formally released or a complete
3.0 acceptance. Frozen rev7 and production 027 remain protected. No Codex restart,
Host restart, model call, company dispatch, notification or authentication change.

## Repairs and evidence

- Healthy Hold could exit when cancellation alias observation hit a transient
  PermissionError reading another seat's ack. A file-backed injected regression
  failed before the change. Observation now retries at the next control poll;
  no interrupt/release on that denied observation.
- Independent QA then caught a request.json sharing denial during terminal
  publication/final observation. Separate generation/final file-backed subTests
  both failed before correction. Metadata reads now re-read for at most 0.2s on
  PermissionError only. Generation/identity checks still run on the successful
  read; expired permissions and JSON/identity errors remain errors. The yielded
  body is never repeated, and a holder exception is not converted into exit proof.
- POSIX permission, process birth and secure-capability fixtures now simulate
  their actual platform instead of inheriting absent Windows APIs. Storage remains
  mocked; real Windows secure inbox still fails closed. The strict Linux cloud
  recipe is explicitly skipped on Windows, not made compatible by expanding
  SAFE_ENV. Its WinError 10106 reproduction matches the
  [CPython SYSTEMROOT issue](https://github.com/python/cpython/issues/118234).
- Canonical nested E2Es now preserve the explicitly verified caller pin, and
  source method counts follow the actual source. The cold package retains legacy
  selector provenance in bundled_legacy_files and discovers suite E2Es once.
  One tests directory; no second test system or Jarvis builder Skill.

## Final-source results

All results below bind the final product bytes; overlapping selections MUST NOT
be added to form a unique total. Method counts and subTest records stay separate.

| Selection | Methods | Passed | Fail/error | Skip |
|---|---:|---:|---:|---:|
| Relevant Windows modules | 88 | 87 | 0/0 | 1 strict Linux recipe |
| Relevant Linux selection | 9 | 9 | 0/0 | 0 |
| Independent repair QA | 4 | 4 | 0/0 | 0 |
| Packaged core entry | 17 | 17 | 0/0 | 0 |
| Packaged entry E2E | 7 | 7 | 0/0 | 0 |

The original rev8 HTTP/SDK protocol result was recorded separately from this
historical report. Rev9 protocol execution requires its own fresh receipt and
is indexed in the rev9 report. Rev8 Linux stdio initializes 17 tools, reads capabilities
and preflight, refuses original Noise/route conflicts, and reaps unresponsive
TEST children. All are zero-model checks, not the real model lifecycle chain.

Independent boundary probe: persistent read denial raises after 0.208s; malformed
and non-object JSON remain errors; retired generation never enters the body; body
PermissionError is raised after exactly one body call. Cleanup confirmed and no
owned process residuals in the final recorded selections.

Earlier red/incomplete attempts are retained outside this review package. The
first post-alias run had a new test that incorrectly expected a raw hold_released
key; it was corrected to check the existing public adapter contract, not by
changing product persistence. An initial WSL environment setup accidentally
created five venv objects in the workspace; exact creation identity was checked
and those objects were moved recoverably to updates/current/
JARVIS_WSL_SETUP_RECOVERY_20261009. No pre-existing files were removed. Subsequent
Linux probes use one disposable venv lifecycle; no OS package installation.

## Identity and limitations

Version 3.0.0a2; revision rev8 repair review.
Windows product hash: efbb2abcc82762caf888a5547f9ffe39121806ce97613c37418fe27f80652d4c.
Linux product hash: 9e4353da92ae91603d9582fa67083e14872a810d6fc08ba675d9c24d4be1baa9.
The platform hash differs because existing identity keys use platform path
separators, not because the source payload differs.

No full-suite-green claim. Real model -> Schema -> exact terminal -> release ->
close, authoritative original-generation recovery, sustained memory/admission
and advertised concurrency/capacity acceptance remain separate gates. Short TEST
resource samples and successful cleanup do not prove leak-free operation. The
historical intermittent healthy-seat failure has no unique original traceback;
these reproducible fault paths are verified repairs, not proof that every cause
of intermittency has been eliminated.

TEST port 18767 was not listening on readback; last existing log does not prove
why its service exited. No persistent service or automatic updater was added.
Production port 18766 remained listening and untouched. Do not deploy this review
package merely because selected tests pass.
