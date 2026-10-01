# Linux cloud deployment

This Jarvis dot package is Linux-only. Windows must use the separate original main/v0.2.5 distribution. Jarvis dot uses the existing control-plane core with an explicit Linux adapter. Use an independent CODEX_HOME that has already completed the official login flow; never copy another installation's authentication files or platform tokens.

## Bootstrap and paths

Set `linux_cloud=true`, `runtime_environments=[]`, `model_provider=openai`, sandbox `read-only` and approval policy `never`. The launcher itself constructs the clean child environment and sets the documented no-environment provider. It preserves current proxy/CA values without displaying them. Conflicting environment files, Noise routes, inherited alternate exec-server routes and custom gateway overrides fail closed.

Home, project and executable paths resolve relative to the configuration file. Windows drive, UNC and backslash paths on Linux require an explicit caller-verified mapping. Actual initialize `codexHome` must match the configured resolved home. The `profile` field is informational only.

For the observed contact profile, the model is `gpt-6-luna`, effort `low`, with the official catalog's Fast tier ID `priority`. Both catalog support and thread start/resume readback are required; a label or silent fallback is not proof. Public web search uses the official standalone extension in live mode. No persistent MCP registration or new authentication is added.

## Fixed-file workflow

```sh
python -m jarvis_control.file_task run --packet <packet.json>
python -m jarvis_control.file_task status --packet <packet.json>
python -m jarvis_control.file_task resume --packet <packet.json>
python -m jarvis_control.file_task reconcile --packet <packet.json>
```

The version-1 file packet permits two explicit steps with a fixed input hash, closed output schema, byte bounds and distinct new output directories. File-service `fs/readFile`/`fs/writeFile` calls run in separate zero-model processes. Model and file stages are serial and use the fixed-file instance lock. Symlinks, traversal, special files, overlapping paths and overwrites are refused. A lost write response is reconciled against the exact saved bytes; it is never blindly rewritten.

Service filesystem RPCs use the process's outer file permissions, not the model's read-only turn policy. Therefore controller path constraints are mandatory. These operations are not evidence that native shell, apply-patch or general model file tools work.

## Lifecycle discipline

Native execution identity and exact terminal evidence take precedence over acknowledgments or final text alone. A process must remain alive while it owns a turn. A cold observer's sparse status cannot resolve a conflicting active/dead-owner record by itself.

The dot.5 close path durably cancels management even when the owner is unknown. It interrupts only a proven current owner and reports closed only with terminal/release evidence; unresolved external execution remains closed_unconfirmed. No cancelled Hold or same-thread alias may resume. See CLOSE-CANCELLATION.md. Linux PID-only orphan requeue is disabled; unknown execution does not release capacity or authorize a replacement.

The contact adapter's narrow no-submission repair requires a known completed native turn, complete original evidence, no form/receiver invocation or uncertain write, an explicit parent disposition, an exact old-to-new build transition and authorization for the next different assigned company. It creates no synthetic submission receipt and does not revisit a refused source. It is not a general recovery exception.
