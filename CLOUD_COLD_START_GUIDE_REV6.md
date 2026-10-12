# Jarvis 0.3.0 rev9 cloud cold start — isolated TEST only

The filename preserves the guide's rev6 origin; the commands and marked SDK recipe below target this rev9 bundle's `source/pyproject.toml` version `0.3.0`, 17 bundled `skills/*/SKILL.md`, `jarvis_mcp.entrypoint` and its real config loaders. This prepares a new TEST instance. No model turn, production install, global MCP registration or cloud30 queue change is authorized by this document. The earlier handoff attributed an independently logged-in cloud Home; that historical claim is not current official login readback. Verify and reuse the existing independent Home without copying credentials. No Home/auth files are included here.

## 1. Verify and isolate

Choose a new absolute Linux TEST root on the target host. Extract the exact rev9 review archive there, or copy this complete review directory. The extracted review root must directly contain `source/`, `skills/` and this guide. Record the archive SHA256 and compare it with the matching rev9 packaging receipt supplied at handoff; an older revision's receipt is invalid. Require 51 product Python files, 17 Skill directories, 6 `source/evidence/alpha7-schema/*.json` originals, canonical `source/tests`, and `source/pyproject.toml`. The exact rev9 product identity is Windows `621c3db30dac2adcbcd71a05a72428350cc44f445cf6ca29792d6e2b51a6a226`, Linux `016a38db534153a5d674b89b112af5e846963da59cc4a4cf0cb0221f37b32b12`; platform path separators explain the difference. Confirm runtime `jarvis_control.operations.source_identity().code_sha256` equals the Linux pin. Record exact config and dependency hashes. Never use a Windows drive/UNC path in a Linux config.

Create a venv **inside the new TEST root** (or use an existing isolated venv with the declared dependencies), then install only this extracted source:

```sh
python3 -m venv "<NEW_TEST_ROOT>/venv"
"<NEW_TEST_ROOT>/venv/bin/python" -m pip install -e "<NEW_TEST_ROOT>/source"
"<NEW_TEST_ROOT>/venv/bin/python" -c 'from jarvis_control.operations import source_identity; print(source_identity()["code_sha256"])'
```

The final line is the source identity check; MCP SDK initialize supplies the runtime version below. Do not install this candidate into the old 027 environment. If dependency installation or archive verification fails, stop and record the error; do not point the candidate at production state as a workaround.

## 2. Bind real config fields, not credentials

Place both JSON files under `<NEW_TEST_ROOT>/config/`. Create a separate `<NEW_TEST_ROOT>/workspace/` and `<NEW_TEST_ROOT>/state/`. Use absolute paths, resolve them, and reject any path that overlaps production 027/cloud30 state or an existing business workspace.

`launcher.json` (replace only the placeholders at runtime):

```json
{
  "dispatcher_thread_id": "<EXISTING_TEST_DISPATCHER_ID_FROM_OFFICIAL_THREAD_READ>",
  "codex_cli": "auto",
  "profile": "jarvis30-isolated-test",
  "linux_cloud": true,
  "expected_codex_home": "<EXISTING_INDEPENDENT_OFFICIALLY_LOGGED_IN_HOME>",
  "runtime_environments": [],
  "runtime_constraints": {"model_provider": "openai", "sandbox": "read-only", "approval_policy": "never"},
  "live_creation_enabled": false,
  "worker_capacity": 1,
  "memory_admission_max_used_percent": 95,
  "allowed_projects": {"TEST": "<NEW_TEST_ROOT>/workspace"}
}
```

`dispatcher_thread_id` must be obtained from official native thread list/read in that Home, not generated from a label. `codex_cli=auto` may be replaced by the already verified official SDK launcher command in that environment. `profile` is merely a local label. The project key is `TEST`; its path must already exist. `memory_admission_max_used_percent` defaults to 95; finite numeric (0,100] values adjust only new-action admission. Unknown samples/invalid policy refuse new admissions, not readback/close; no continuous hard cap or healthy-task termination is promised. `live_creation_enabled=false` intentionally prevents this cold-read stage from creating a task. The exact runtime Home/model/sandbox/approval and later actual thread/turn IDs require official API/Host readback, not config-file inference.

`heartbeat.json` (all paths must stay under the new TEST root):

```json
{
  "db_path": "<NEW_TEST_ROOT>/state/heartbeat.sqlite",
  "health_path": "<NEW_TEST_ROOT>/state/heartbeat-health.json",
  "lock_path": "<NEW_TEST_ROOT>/state/heartbeat.lock",
  "heartbeat_contracts_dir": "<NEW_TEST_ROOT>/state/contracts",
  "native_task_launcher_config": "<NEW_TEST_ROOT>/config/launcher.json",
  "standard_bridge_package_root": "<NEW_TEST_ROOT>/source/packages",
  "desktop_recovery_enabled": false,
  "max_concurrent_runs": 1
}
```

These explicit paths matter: omitted heartbeat path fields otherwise use defaults outside the TEST root. Do not set `--notification-config`, a Feishu receiver, or a Desktop recovery target for this cold start. The source's `HeartbeatConfig.load` validates `standard_bridge_package_root`, and `NativeTaskLauncherConfig` requires `dispatcher_thread_id`, nonempty `allowed_projects`, and the cloud runtime constraints.

## 3. Read-only official SDK handshake

Connect an official MCP SDK stdio client to this exact command; do **not** run it as a detached daemon or publish an HTTP listener:

```sh
"<NEW_TEST_ROOT>/venv/bin/python" -m jarvis_mcp.entrypoint \
  --config "<NEW_TEST_ROOT>/config/heartbeat.json" \
  --launcher-config "<NEW_TEST_ROOT>/config/launcher.json" \
  --state-dir "<NEW_TEST_ROOT>/state/mcp" --transport stdio
```

Use the following SDK recipe in one fresh, isolated Python process. Before constructing SDK parameters or spawning MCP, it loads this candidate's `NativeTaskLauncherConfig` and calls the original `child_environment(cfg)` against the **complete, unchanged `os.environ`**. A guard error stops immediately; do not clear an inherited Noise/exec route or retry with a sanitized parent. Only then forward existing `SAFE_ENV` entries, retaining their exact values, plus candidate-only `PYTHONPATH` and `PYTHONDONTWRITEBYTECODE`. Do not invent proxy/CA values, disable TLS, read/copy authentication, or print environment values.

MCP 2.x normally merges its own default inherited keys into `env`. The narrowly scoped patch below suppresses that fallback only in this single-purpose probe process, after the original guard; it never changes `os.environ`, the candidate guard, or installed SDK files. Do not run concurrent SDK sessions inside that process. The total async deadline is 45 seconds, with a shorter 15-second session read timeout. SDK context teardown may use its bounded shielded child cleanup after cancellation; retain the external 55-second observation ceiling as a backstop, not as the primary timeout. Cleanup is limited to the SDK-owned TEST process tree; never kill a shared Host.

<!-- coldstart-sdk-example -->
```python
import json
import os
from pathlib import Path
import sys
from unittest.mock import patch

import anyio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

TOTAL_DEADLINE_SECONDS = 45
READ_TIMEOUT_SECONDS = 15


async def coldstart(launcher_path, heartbeat_path, state_dir, packages):
    packages = Path(packages).resolve()
    sys.path[:0] = [str(packages), str(packages / "jarvis_runtime")]
    from jarvis_runtime.jarvis_native_task_launcher import NativeTaskLauncherConfig
    from jarvis_runtime.linux_cloud import SAFE_ENV, child_environment

    with anyio.fail_after(TOTAL_DEADLINE_SECONDS):
        cfg = NativeTaskLauncherConfig(Path(launcher_path))
        validated = child_environment(cfg)  # Full original os.environ; failure stops here.
        env = {key: validated[key] for key in SAFE_ENV if key in validated}
        env.update(PYTHONPATH=os.pathsep.join((str(packages), str(packages / "jarvis_runtime"))),
                   PYTHONDONTWRITEBYTECODE="1")
        parameters = StdioServerParameters(
            command=sys.executable,
            args=["-B", "-m", "jarvis_mcp.entrypoint", "--config", str(heartbeat_path),
                  "--launcher-config", str(launcher_path), "--state-dir", str(state_dir),
                  "--transport", "stdio"],
            env=env, cwd=str(packages),
        )
        with patch("mcp.client.stdio.get_default_environment", return_value={}):
            async with stdio_client(parameters) as (reader, writer):
                async with ClientSession(reader, writer,
                                         read_timeout_seconds=READ_TIMEOUT_SECONDS) as session:
                    initialized = await session.initialize()
                    tools = await session.list_tools()
                    names = {tool.name for tool in tools.tools}
                    if initialized.server_info.version != "0.3.0" or len(names) != 17:
                        raise RuntimeError("Candidate MCP identity/discovery mismatch")
                    if not {"jarvis_read", "jarvis_loop"}.issubset(names):
                        raise RuntimeError("Candidate read-only tools missing")
                    capabilities = await session.call_tool("jarvis_read", {"subject": "capabilities"})
                    preflight = await session.call_tool("jarvis_loop", {"action": "preflight"})
                    if capabilities.is_error or capabilities.structured_content["status"] != "completed":
                        raise RuntimeError("Candidate capabilities check failed")
                    if (preflight.is_error or preflight.structured_content["status"] != "completed"
                            or preflight.structured_content["data"]["allowed_projects"] != ["TEST"]
                            or preflight.structured_content["readback"]["verified"] is not True):
                        raise RuntimeError("Candidate preflight check failed")
    return {"transport": "official_sdk_stdio", "server_version": "0.3.0",
            "tool_count": 17, "capabilities_status": "completed", "preflight_verified": True,
            "allowed_projects": ["TEST"], "model_turns": 0, "sdk_context_closed": True}


if __name__ == "__main__":
    if len(sys.argv) != 5:
        raise SystemExit("Pass absolute TEST launcher.json, heartbeat.json, state/mcp, source/packages paths")
    print(json.dumps(anyio.run(coldstart, *sys.argv[1:]), sort_keys=True))
```
<!-- /coldstart-sdk-example -->

Save the example inside the new TEST root and execute it using that root's venv Python with the four absolute TEST paths described in its usage message. `initialize` must report `0.3.0`; `tools/list` must contain exactly 17 tools. Only `jarvis_read(subject="capabilities")` and `jarvis_loop(action="preflight")` are called. This proves import/wiring/stdio/public read-only calls, **not** official login readback, a native model turn, Schema intake, Host release, or release readiness. The receipt confirms context closure; independently retain TEST child-exit evidence. A timeout is a failed probe, never a readiness result.

The canonical test executes this exact marked guide example in a disposable directory. From the extracted review-bundle root, with its declared dependencies installed in the isolated venv, run:

```sh
"<NEW_TEST_ROOT>/venv/bin/python" -B -c 'import sys, unittest; from pathlib import Path; p=Path("source").resolve(); sys.path[:0]=[str(p/"packages"),str(p/"packages/jarvis_runtime"),str(p/"tests")]; r=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromName("test_cloud_coldstart")); sys.exit(not r.wasSuccessful())'
```

This command leaves the parent environment intact. The E2E tests verify inherited allowlist values in memory without printing them, check actual child environment keys, reject additive synthetic Noise/exec-route conflicts before spawn, and reap silent SDK-owned TEST children after independently injected read/total timeouts. Their shortened injected clocks do not change the example's 45/15-second settings. CPython may itself generate `LC_CTYPE` during POSIX locale coercion; it is not passed by the recipe. This strict Linux recipe is not a Windows compatibility promise: Windows Python may require `SYSTEMROOT` for asyncio, which is outside this candidate's `SAFE_ENV`. Keep that limitation visible; do not expand the allowlist to make Windows pass. A synthetic empty Home cannot stand in for the existing logged-in Home.

The bundled `CLOUD_E2E_PUBLIC_CONTRACT_REV4.md` contains current 0.3.0/rev9 parameter examples while retaining its historical filename; it is not a current model-run authorization. A later model E2E requires a separately approved finite plan, current official quota/Home/model readback and this candidate's revision/hash/nonce. A general paid turn does not cover original-generation recovery. Missing IDs, unknown terminal or `closed_unconfirmed` cannot justify further dispatch or fabricated closure.

The original rev6 model window ended at 2026-10-05 04:00 UTC and is expired. It authorizes no current model work. This rev9 guide permits only the bounded, isolated read-only checks described above; no automatic reset, recharge, public release or production configuration change follows from it.
