# Jarvis Control Plane 0.2.1

Jarvis provides durable task holding, bounded loops, monitoring and local heartbeat scheduling through MCP. The Codex App Server adapter owns native turns; Monitor decides whether a completed turn may continue, and Hold executes only the command bound to that Hold and turn.

The public MCP tools are `jarvis_create`, `jarvis_hold`, `jarvis_loop`, `jarvis_read`, `jarvis_resume`, `jarvis_monitor`, `jarvis_heartbeat` and `jarvis_notify`. Harness-specific operations remain in adapters behind the control-plane contracts.

## Included in 0.2.1

- Durable Hold ownership and recovery with a shared host capacity of 10 workers.
- Finite lanes with arbitrary positive batch sizes, exact candidate binding, last-batch handling, per-item saved output/receipt verification and optional Draft 2020-12 output schemas. Open-ended work can omit lanes.
- Native observer reads distinguish raw observer status from execution evidence: `read_source`, `execution_source` and `execution_status`. An interrupted observer is not proof that the owning Hold stopped; missing authoritative evidence remains unknown.
- Local heartbeat scheduling, terminal reconciliation and optional outbox notifications. Delivery is successful only with its delivery receipt; enqueueing is not delivery.

This release validates **saved JSON files and their receipts**. It does not automatically ingest JSON from a Worker's final answer. Runtime completion and technical output verification do not imply business-quality acceptance.

## Install the Python package

Requires Python 3.11 or newer. Native task operations additionally require a compatible Codex CLI/App Server, a configured local account/profile, and explicitly allowed project paths. Windows is the primary tested host environment.

From an extracted release or checkout:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install .
```

The package declares its MCP and JSON Schema dependencies in `pyproject.toml`. On other platforms, use the equivalent virtual-environment Python path. Installation alone does not register MCP, start background services, migrate existing runtime state or configure notification delivery.

## Configure an isolated instance

Keep credentials and runtime state outside source control. These are configuration **examples**; replace every example path/profile/project before running a native task. Use a new local configuration directory and a dedicated state directory.

Create `launcher.json` in that configuration directory:

```json
{
  "version": 1,
  "dispatcher_thread_id": "configure-your-controller-task",
  "codex_cli": "auto",
  "profile": "your-configured-profile",
  "expected_codex_home": "C:/example/codex-home",
  "live_creation_enabled": false,
  "allowed_projects": {"ExampleProject": "C:/example/project"}
}
```

Leave creation disabled until the instance's project allowlist and account/profile have been reviewed. Create `transport.json` beside it:

```json
{
  "db_path": "../state/bridge-heartbeats.sqlite",
  "health_path": "../state/bridge-health.json",
  "lock_path": "../state/bridge.lock",
  "native_task_launcher_config": "launcher.json",
  "heartbeat_contracts_dir": "../state/heartbeat-contracts",
  "desktop_recovery_enabled": false,
  "standard_bridge_package_root": "C:/example/jarvis-control-plane/packages"
}
```

`standard_bridge_package_root` must be an absolute path to the release's `packages` directory (or the installed directory containing `jarvis_codex_bridge`). Copy `templates/local-heartbeat.config.example.json` into the same configuration directory as `local-heartbeat.json`; its relative state paths resolve from the configuration location.

Register this command and arguments with your MCP client, using absolute paths for that client:

```text
<venv-python> -m jarvis_mcp.entrypoint --config <config-dir>/transport.json --launcher-config <config-dir>/launcher.json --state-dir <state-dir> --local-heartbeat-config <config-dir>/local-heartbeat.json
```

For long-running local scheduling, the existing host module uses the same configuration:

```text
<venv-python> -m adapters.codex_app_server.jarvis_local_heartbeat_host --config <config-dir>/transport.json --launcher-config <config-dir>/launcher.json --state-dir <state-dir> --local-heartbeat-config <config-dir>/local-heartbeat.json run-forever
```

Loop startup initializes or checks the normal-user Hold host. Run the instance under the account whose Codex profile and state it uses. Stop and drain existing work before changing a running instance's source snapshot. Refresh persistent MCP connections after an upgrade; new files do not replace code already loaded in an old client.

## Notifications

Omit `--notification-config` to leave the notification adapter unconfigured. `templates/feishu-notification.config.example.json` intentionally has an empty recipient and cannot load as an active sender. To enable it, explicitly configure your own recipient, Python executable and compatible dispatcher/outbox delivery service, then pass the filled configuration. No credentials, recipient IDs or running delivery bridge are distributed with this release. Leave notification requests disabled for isolated tests.

## Validate source changes

Use the existing contract and runtime test modules; no live Codex task or production database is required:

```powershell
$env:PYTHONPATH = 'packages;packages/jarvis_runtime'
.\.venv\Scripts\python.exe -m unittest tests.adapter_contract.test_hold_host_recovery tests.adapter_contract.test_task_provisioning_adapter tests.contract.test_codex_bridge tests.contract.test_jarvis_loop tests.contract.test_jarvis_mcp tests.jarvis_runtime.test_jarvis_heartbeat_service tests.jarvis_runtime.test_jarvis_task_hold_host tests.monitor.test_hold_turn_monitor
```

One inherited test, `test_loop_start_self_heals_the_host_before_creating_workers`, still expects host capacity 2 while this source uses capacity 10. Release validation reports that existing mismatch separately; it is not a passing test.

## Release provenance

The 40 product-source files (`packages/` and `pyproject.toml`) match accepted implementation snapshot `279d92641f613b7d88b4f767f26e68359a6b24a1`. The public release commit has a different identity: it is based on public main, with portable examples, a sanitized test fixture and updated public documentation. Private local development ancestry, installation receipts and business-task records are not imported.

The new release content and assets are sanitized. Existing public repository history is retained; this release does not rewrite or purge that history. See `updates/current/NEXT_VERSION_CHANGELOG.md` for release notes.
