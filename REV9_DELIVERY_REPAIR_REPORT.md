# Rev9 version and package documentation repair handoff

2026-10-09. Status: `ready_for_independent_qa` for this bounded metadata/documentation stage only. This is not independent acceptance, a formal release, an installed product, or a final product-hash freeze. Parent-owned later product work must recompute identities, update command pins and rerun directly affected contracts. No model/company run, external message, production write, authentication/configuration change or Codex restart was performed by this stage.

## Baseline, scope and test ordering

The immutable rev8 archive SHA256 is `d5a7c1325eb79516e306583680d437db201ec1f1f5c2226691e75c21a108000f`; the original rev8 Windows product identity is `efbb2abcc82762caf888a5547f9ffe39121806ce97613c37418fe27f80652d4c`. The archive remains unchanged. The complete rev8 review tree was copied to a new `review-bundle-rev9`: 138 existing files, no removed files. All edits belong only to this new tree or new stage receipts outside it. Production 027, source-rev6, dirty root source/tests and older snapshots were not write targets.

Existing relevant core, entry, protocol, identity/guide and Windows version-boundary cases ran before any test edits. An explicit expected-version 0.3.0 on original rev8 correctly returned source_rejected/exit 3/zero methods. The first identity/guide command had an incorrectly named selector and was rejected before methods; that receipt is preserved and the correct selector selection subsequently passed. No new test was authored. The four fixed-version test consumers were migrated before product version declarations, based on the user's explicit target 0.3.0 and parent confirmation, not by changing behavioral expectations to make a failure disappear. Tool count, actions, schemas, isolation, lifecycle, cleanup and safety assertions were retained.

Package-relative references to the old rev7 ZIP, PACKAGE_RECEIPT_REV7.json, WINDOWS_FOLLOWUP_REV7.md, PRODUCT_E2E_PLAN_REV7.md, inputs/REQUIREMENTS-MATRIX.json, source-rev6, test-env and the repository rev7-source-only path were all absent in the rev8 archive. These broken operational dependencies were replaced with commands using this bundle's source/ and declared independent TEST venv. Historical provenance is explicitly labeled instead of treated as new evidence. No historical model authorization was renewed.

## Exact change set

Product declarations: source/pyproject.toml project.version; source/packages/jarvis_mcp/server.py advertised MCP version; source/packages/jarvis_control/{operations,file_task,contact_task}.py BUILD. Values now identify candidate metadata 0.3.0. No runtime semantics or architecture changed.

Direct consumers: source/tests/run_suite.py VERSION and fixed Windows hash; coverage_manifest.json target_version and accurate provenance description; only literal version expectations in test_027_registry_versions.py, test_suite_entry_e2e.py, test_suite_protocol_e2e.py and test_windows_secure_inbox_gate.py. Remaining assertions are byte-for-byte unchanged outside these literal edits.

Documentation: cloud recipe handshake 0.3.0 and actual platform pins; README/RUN/Windows commands relative to the extracted review root; explicit historical attribution in acceptance/Skills/rev8 reports. The rev6/8 filenames remain to preserve existing structure. No optional Skill was deleted.

Changed existing paths:

- `ACCEPTANCE_MATRIX_REV6.md`
- `CLOUD_COLD_START_GUIDE_REV6.md`
- `REV8_REPAIR_REPORT.md`
- `RUN_REV8_TESTS.md`
- `SKILLS_RELEASE_AUDIT_REV6.md`
- `WINDOWS_ACCEPTANCE_REV6.md`
- `source/packages/jarvis_control/contact_task.py`
- `source/packages/jarvis_control/file_task.py`
- `source/packages/jarvis_control/operations.py`
- `source/packages/jarvis_mcp/server.py`
- `source/pyproject.toml`
- `source/tests/README_SUITE.md`
- `source/tests/coverage_manifest.json`
- `source/tests/run_suite.py`
- `source/tests/test_027_registry_versions.py`
- `source/tests/test_suite_entry_e2e.py`
- `source/tests/test_suite_protocol_e2e.py`
- `source/tests/test_windows_secure_inbox_gate.py`

New package document: `REV9_DELIVERY_REPAIR_REPORT.md`. Complete old/new diff and complete per-file hash inventory are delivered outside the review tree as `REV9_DELIVERY_CHANGES.diff` and `REV9_DELIVERY_ASSET_MANIFEST.json`; they are stage handoff artifacts, not required runtime inputs.

## Version-only stage identities and historical verification

Version-only stage Windows product identity: `90f527bd10bfa9c9e057b0dff85619721365ae2a8a9c5ac4a73bfe0553f8c88a`.
Version-only stage POSIX-key/Linux product identity: `2b441cedf06479200b5f7912343e4c30d3db509d1785aab25162deab9aef6590`.
There are 51 product Python files. The Linux value was computed from the same file digests with POSIX identity keys; it is not by itself a Linux execution result. Independent Linux QA is parent-owned.

At the version-only stage, all 17 Skill directories and every Skills file matched the original rev8 copy by SHA256. Source tests, qa and six alpha7-schema evidence originals were retained at that stage; only version-consumer edits listed above differed in tests. These are historical stage observations, not the later A/B repair inventory. Product hash excludes docs/tests as defined by the unchanged source_identity contract, so per-file asset hashes are also supplied.

Windows selected-scope results: core 17/17; entry 7/7; protocol 3/3; identity/guide/Windows version selection 6/6. These selections overlap and MUST NOT be summed into a unique total. Each receipt confirms owned cleanup and no residual owned PIDs. Old expected-version 3.0.0a2 is rejected on rev9, exit 3, zero methods. The all --list command reports 593 selected selectors; this is discovery only, not a full suite run or 593 passed cases. The original rev8 broad Windows run belongs to the parent thread and is not recounted as rev9 evidence.

The canonical commands in RUN_REV8_TESTS.md and source/tests/README_SUITE.md use exact candidate/platform pins, never an automatic accept-current-bytes shortcut. Identity/guide selection uses these exact existing methods:

```text
source::test_027_registry_versions.CurrentRegistryVersions.test_distribution_and_advertised_mcp_version_are_same_prerelease
source::test_027_registry_versions.CurrentRegistryVersions.test_all_product_builds_use_the_same_candidate_version
source::test_distribution_identity.DistributionIdentityE2E.test_unrelated_installed_dependency_does_not_change_product_identity
source::test_cloud_coldstart.CloudColdstartTest.test_conflicting_original_environment_rejected_before_sdk_spawn
source::test_cloud_coldstart.CloudColdstartTest.test_unresponsive_test_process_times_out_and_is_reaped
source::test_windows_secure_inbox_gate.WindowsSecureInboxGateTests.test_linux_only_task_clis_import_but_refuse_windows_execution
```

The original receipts below are stage-local directories adjacent to the review bundle, each containing receipt.json/report.md/plan and artifacts. Their exact invoked commands are recorded in receipt.json; these paths are provenance, not a dependency for running the extracted package. No new ZIP was created and no old ZIP was overwritten.

| Receipt directory | Status | Exit | Methods | Passed | receipt.json SHA256 |
|---|---|---:|---:|---:|---|
| rev9-pre-core | passed_selected_scope | 0 | 17 | 17 | 19afef2f874da49903ff882f81b5f9a698f09042087312d361b0ebc021bfaeaa |
| rev9-pre-entry | passed_selected_scope | 0 | 7 | 7 | a54428219e93688c5bcad42a28d50d11941cb544d017581f739d424f6f903d35 |
| rev9-pre-protocol | passed_selected_scope | 0 | 3 | 3 | 0a23b62ff1406f8900bf5e2ba42f99545a4d4a1b37a53503cf6fa705a471cad0 |
| rev9-pre-identity-guide-valid | passed_selected_scope | 0 | 5 | 5 | 2f8728eda24ea341ccf15081ee7f0680917a4f9738661c9030aea3693e379ee6 |
| rev9-pre-windows-version | passed_selected_scope | 0 | 1 | 1 | 60847c877fe2060f7cf0b83a85cf762299fada422137057bdd4b40cfea4e9009 |
| rev9-pre-version-rejected | source_rejected | 3 | 0 | 0 | a7dbb26c0b2e5ce675115d7e3e4eba47d7ba80eab36fd7029bdfa351b7d73489 |
| rev9-pre-identity-guide | source_rejected | 3 | 0 | 0 | fac16d8b0ead634c620ec1975a1ecf69e9fc4a7264ca1e78c3cc0c56de83af27 |
| rev9-post-core | passed_selected_scope | 0 | 17 | 17 | 2c994cabc20c15ee8e1074048fbd8f87d40c80ec85c0acb5d7f34cea9d4b5f7c |
| rev9-post-entry | passed_selected_scope | 0 | 7 | 7 | 8e1c61c78684ec5a0e2a876860736bc9509f41234e5f7cf828af37359c2b7127 |
| rev9-post-protocol | passed_selected_scope | 0 | 3 | 3 | 9d8144d585ab97a3f5cb1f17d79d01742ade535f63bc89ffe5ec26af805818ab |
| rev9-post-identity-guide | passed_selected_scope | 0 | 6 | 6 | 217f38d17486594135e5dde525612db344759501294d0bce5c54b0791657641c |
| rev9-post-old-version-rejected | source_rejected | 3 | 0 | 0 | 1c9c8cc6257728de6ba2bc4ee6706027c92187866fffe57709313664bfcb93f0 |
| rev9-post-list | listed | 0 | 0 | 0 | fbc3fa2469f1b996da59ff6dd5517831b53e0d2cfd561753bcd33e3daf8e03cd |

## Remaining gates

Independent review, final packaging and cold-extracted verification remain parent-owned. Later product fixes or the newly requested live machine-memory admission behavior are outside this bounded metadata/documentation stage and require new product identities and affected verification. The inherited gaps in source/tests/coverage_manifest.json remain explicit: real model/Schema/terminal/release/close, authoritative original-generation recovery, advertised capacity/resource acceptance, global memory admission, Loop pause/resume, listener-fault recovery and optional local integrations. No selected green run proves these gaps closed. Optional integrations remain unsupported until configured with their own dependencies.

The strict cloud recipe remains Linux-only; Windows SYSTEMROOT was not added to SAFE_ENV. The expired Oct 5 model window is historical only. No independent acceptance or production/public release claim follows from the metadata 0.3.0.


## Subsequent A/B admission and legacy-contract repair

The version-only stage identities and results above remain historical; they are not attributed to the later product bytes. The subsequent authorized A/B work repairs 36 reproduced legacy methods without removing/skipping them, restores close-once startup cleanup, implements configurable real whole-machine admission, and updates 17 Skill version labels. Its exact backup, original/red and affected-core receipts, changed-file inventory and final source identity are indexed in `updates/current/JARVIS_DELIVERY_REV9_ADMISSION_FIX_20261009/HANDOFF.md` in the maintenance workspace. A final independent QA and applicable complete Windows run remain controller-owned; no model turn, production activation, packaging acceptance or public release follows from the local repair.
