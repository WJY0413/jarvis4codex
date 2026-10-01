# Jarvis dot 0.2.5+dot.5 — source release candidate

Jarvis dot is a Linux cloud adaptation of the Jarvis 0.2.5 control plane. It shares the existing core and is currently Linux-only. It is not an upstream OpenAI release. Windows users must continue using the original main/v0.2.5 distribution, not install this dot variant. The intended publishing target is a dedicated dot branch of [WJY0413/jarvis4codex](https://github.com/WJY0413/jarvis4codex), without rewriting the original main branch or its history.

## Observed capability

This exact candidate code completed one fresh, unchanged original Jarvistest P0–P7 run on 2026-10-01. P0–P4 and P6 completed; P5 read back the same active action as `interrupted`; P7 recovered a genuine persisted starting intent to the required `requires_readback` state without a replacement. All actions and readbacks used the public Jarvis API. There were 12 native model turns: 11 completed and one interrupted. See [VALIDATION.md](VALIDATION.md).

The new close implementation durably cancels future management and requests interruption only through a proven exact owner. Actual execution and resource release remain separate: an unknown owner can remain `closed_unconfirmed`. Closing never creates replacement work. See [CLOSE-CANCELLATION.md](CLOSE-CANCELLATION.md).

The earlier dot.4 functional lineage separately processed 100 historical company-research assignments across ten fixed threads. It yielded 83 real receiver seals and independent structural accepts, 16 receiver rejections and one no-submission source-error outcome. Those business executions were not run on dot.5; source review identified material quality issues, so they are not a send-ready prospect list. The historical original suite's P5 limitation is preserved separately from this new run.

## Components

- Existing Jarvis MCP control plane: create, Hold, bounded Loop, readback, continuation, callbacks, relay, delivery and conservative recovery
- Linux bootstrap: independent officially authenticated CODEX_HOME, clean process environment, config-relative paths and actual model/provider/policy readback
- `jarvis_control.contact_task`: one assigned company per invocation, stable typed form, real receiver result and exact native-turn evidence
- `jarvis_control.contact_campaign`: a specialized, independent 10×10 secondary-research allocation and work-item issuer; no production queue claims
- `jarvis_schema`: a thin versioned schema adapter (`1.0.0-contact-v25`), preserving the original V2.5 form and receiver contract
- `jarvis_control.file_task`: fixed caller-authorized file inputs/outputs through separate official file-service RPCs; not native model filesystem tools

## Install and configure

Use Python 3.11+ in an isolated virtual environment and an already-authorized official Codex CLI/App Server. The observed runtime was Codex `0.159.0-alpha.7`; experimental fields must be revalidated after a runtime upgrade.

```sh
python -m venv .venv
.venv/bin/python -m pip install .
```

Do not install or run this dot variant on Windows. A Windows Python 3.14 import check failed because the Linux TEST inbox receiver uses `os.O_DIRECTORY` at module import; zero Windows tests executed. Use the original main/v0.2.5 distribution on Windows. Installing the package does not log in, copy credentials, register an MCP service or start any model work.

Read [JARVIS_DOT.md](JARVIS_DOT.md), [CONTACT_TASK.md](CONTACT_TASK.md) and [JARVIS_CONTROLLER.md](JARVIS_CONTROLLER.md). Shipped examples keep `live_creation_enabled=false`. Review the project allowlist and independent home before enabling a specific deployment. `profile` is only a label, not a native Codex profile selector.

## Safety and limitations

- Model processes use the supported `CODEX_EXEC_SERVER_URL=none` capability restriction and verify empty environments, read-only sandbox and approval `never`
- Existing proxy/CA settings are preserved, while unrelated platform authentication and routing variables are excluded
- No protected host directory, sandbox policy or credential is modified
- Native model shell/file tools remain unavailable in this mode; controller-side fixed file/form operations do not restore those tools
- Unknown dispatch, receiver-write, authentication, permission or quota outcomes pause for reconciliation; they do not authorize replacement work
- Keep the owning process alive until native terminal evidence is recorded. Premature owner exit may leave external execution unconfirmed; durable cancellation does not prove exit or release capacity
- Release examples include no account, recipient, customer, database or runtime configuration

This is a sanitized **source candidate**, not a published or installed release. Packaged Python modules are byte-identical to the frozen code used by the new original suite. Apply it as an allowlisted overlay on a dedicated branch; retain the upstream license, tests and history. See [PACKAGING.md](PACKAGING.md) for rollback and exclusions.
