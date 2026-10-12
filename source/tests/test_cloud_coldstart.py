"""Real stdio E2E for the bundled guide; no credentials or model calls.

Basis: user's two fixes; COLDSTART_TWO_FIXES_FAILURE_MODES.md preceded these tests.
"""
from __future__ import annotations

from contextlib import contextmanager
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

import mcp.client.stdio as sdk_stdio
from mcp.shared.exceptions import MCPError
from adapters.codex_app_server.jarvis_hold_host_service import _pid_is_alive
from jarvis_runtime.linux_cloud import SAFE_ENV

SOURCE = Path(__file__).resolve().parents[1]
GUIDE = SOURCE.parent / "CLOUD_COLD_START_GUIDE_REV6.md"


def recipe():
    section = GUIDE.read_text(encoding="utf-8").split("<!-- coldstart-sdk-example -->", 1)[1]
    code = section.split("```python\n", 1)[1].split("\n```", 1)[0]
    namespace = {"__name__": "coldstart_guide_test"}
    exec(compile(code, str(GUIDE), "exec"), namespace)
    return namespace


@contextmanager
def test_root():
    with tempfile.TemporaryDirectory(prefix="jarvis30-coldstart-") as temporary:
        root = Path(temporary)
        home, project, state = (root / name for name in ("empty-home", "project", "state"))
        for path in (home, project, state):
            path.mkdir()
        launcher = root / "launcher.json"
        launcher.write_text(json.dumps({
            "dispatcher_thread_id": "TEST-existing-dispatcher",
            "profile": "jarvis30-coldstart-test", "linux_cloud": True,
            "expected_codex_home": str(home), "runtime_environments": [],
            "runtime_constraints": {"model_provider": "openai", "sandbox": "read-only",
                                    "approval_policy": "never"},
            "live_creation_enabled": False, "worker_capacity": 1,
            "allowed_projects": {"TEST": str(project)},
        }), encoding="utf-8")
        heartbeat = root / "heartbeat.json"
        heartbeat.write_text(json.dumps({
            "db_path": str(root / "heartbeat.sqlite"), "health_path": str(root / "health.json"),
            "lock_path": str(root / "heartbeat.lock"), "heartbeat_contracts_dir": str(root / "contracts"),
            "native_task_launcher_config": str(launcher),
            "standard_bridge_package_root": str(SOURCE / "packages"),
            "desktop_recovery_enabled": False, "max_concurrent_runs": 1,
        }), encoding="utf-8")
        yield root, (launcher, heartbeat, state, SOURCE / "packages")


def leaves(error):
    if isinstance(error, BaseExceptionGroup):
        return [leaf for child in error.exceptions for leaf in leaves(child)]
    return [error]


class CloudColdstartTest(unittest.IsolatedAsyncioTestCase):
    @unittest.skipUnless(os.name == 'posix',
        'Strict Linux SAFE_ENV recipe excludes Windows SYSTEMROOT; WinError 10106 reproduced. '
        'Windows SDK/HTTP and inbox gates are verified separately; do not expand cloud SAFE_ENV.')
    async def test_official_stdio_coldstart_is_bounded_and_isolated(self):
        ns = recipe()
        self.assertEqual(ns["TOTAL_DEADLINE_SECONDS"], 45)
        self.assertEqual(ns["READ_TIMEOUT_SECONDS"], 15)
        real_spawn = sdk_stdio._create_platform_compatible_process
        children = []
        with test_root() as (root, arguments):
            observed_keys = root / "test-child-environment-keys.json"
            async def observe_spawn(command, args, env, **kwargs):
                # Iterate real keys: Windows lookup is case-insensitive and must
                # not synthesize absent lowercase aliases of proxy variables.
                expected = {key: value for key, value in os.environ.items() if key in SAFE_ENV}
                expected.update(PYTHONPATH=os.pathsep.join((str(SOURCE / "packages"),
                                                          str(SOURCE / "packages/jarvis_runtime"))),
                                PYTHONDONTWRITEBYTECODE="1")
                # Boolean only: never expose env values in failure output.
                self.assertTrue(env == expected, "SDK spawn environment must match the candidate allowlist")
                self.assertEqual(args[:3], ["-B", "-m", "jarvis_mcp.entrypoint"])
                code = ("import json,os,runpy; from pathlib import Path; "
                        f"Path({str(observed_keys)!r}).write_text(json.dumps(dict(keys=sorted(os.environ),pid=os.getpid())),encoding='utf-8'); "
                        "runpy.run_module('jarvis_mcp.entrypoint',run_name='__main__')")
                process = await real_spawn(command=command, args=["-B", "-c", code, *args[3:]],
                                           env=env, **kwargs)
                children.append(process)
                return process
            # Add only a non-secret marker, preserving every original environment entry.
            with patch.dict(os.environ, {"COLDSTART_NON_ALLOWLIST_TEST": "synthetic-marker"}), \
                 patch.object(sdk_stdio, "_create_platform_compatible_process", observe_spawn):
                started = time.monotonic()
                result = await ns["coldstart"](*arguments)
            self.assertLess(time.monotonic() - started, 55)
            self.assertEqual(len(children), 1)
            self.assertIsNotNone(children[0].returncode, "SDK-owned TEST child must be reaped")
            self.assertEqual(children[0].returncode, 0)
            observed = json.loads(observed_keys.read_text(encoding="utf-8"))
            self.assertFalse(_pid_is_alive(observed["pid"]), "Actual TEST interpreter must have exited")
            keys = set(observed["keys"])
            # CPython may generate LC_CTYPE during POSIX locale coercion; it is not inherited.
            self.assertFalse(keys - SAFE_ENV - {"PYTHONPATH", "PYTHONDONTWRITEBYTECODE", "LC_CTYPE"})
            self.assertNotIn("COLDSTART_NON_ALLOWLIST_TEST", keys)
            self.assertEqual(result["tool_count"], 17)
            self.assertTrue(result["preflight_verified"])
            self.assertFalse(any((arguments[2] / name).exists()
                                 for name in ("task-holds", "task-monitors", "loops")))
            print("COLDSTART_RECEIPT " + json.dumps({**result,
                "allowlist_values_preserved": True, "non_allowlist_absent": True,
                "sdk_child_reaped": True, "sdk_child_exit_code": children[0].returncode,
                "deadline_seconds": 45, "read_timeout_seconds": 15,
            }, sort_keys=True))

    async def test_conflicting_original_environment_rejected_before_sdk_spawn(self):
        ns = recipe()
        with test_root() as (_, arguments):
            for injection, reason in (
                ({"CODEX_EXEC_SERVER_NOISE_COLDSTART_TEST": "synthetic-marker"}, "Noise routing"),
                ({"CODEX_EXEC_SERVER_URL": "synthetic-incompatible-route"}, "exec-server route"),
            ):
                with self.subTest(reason=reason), patch.dict(os.environ, injection), \
                     patch.object(sdk_stdio, "_create_platform_compatible_process") as spawn:
                    with self.assertRaisesRegex(ValueError, reason):
                        await ns["coldstart"](*arguments)
                    spawn.assert_not_called()
            print('COLDSTART_GUARD_RECEIPT {"noise_rejected_before_spawn":true,'
                  '"incompatible_route_rejected_before_spawn":true,"model_turns":0}')

    async def test_unresponsive_test_process_times_out_and_is_reaped(self):
        for mode, deadline, read_timeout in (("session_read", 8, 0.25), ("total_deadline", 0.5, 5)):
            with self.subTest(mode=mode), test_root() as (root, arguments):
                ns = recipe()
                # Compress clocks independently for fault injection; production stays 45/15.
                ns["TOTAL_DEADLINE_SECONDS"] = deadline
                ns["READ_TIMEOUT_SECONDS"] = read_timeout
                pid_file = root / "silent-test-child.pid"
                children = []
                real_spawn = sdk_stdio._create_platform_compatible_process
                async def silent_spawn(command, args, env, **kwargs):
                    code = ("import os,time; from pathlib import Path; "
                            f"Path({str(pid_file)!r}).write_text(str(os.getpid()),encoding='utf-8'); "
                            "time.sleep(300)")
                    process = await real_spawn(command=command, args=["-B", "-c", code], env=env, **kwargs)
                    children.append(process)
                    return process
                started = time.monotonic()
                with patch.object(sdk_stdio, "_create_platform_compatible_process", silent_spawn):
                    with self.assertRaises(Exception) as raised:
                        await ns["coldstart"](*arguments)
                elapsed = time.monotonic() - started
                errors = leaves(raised.exception)
                if mode == "session_read":
                    self.assertTrue(any(isinstance(e, MCPError) and e.error.code == -32001 for e in errors),
                                    "Expected SDK request timeout, not an unrelated failure")
                else:
                    self.assertTrue(any(isinstance(e, TimeoutError) for e in errors),
                                    "Expected enclosing total deadline")
                self.assertLess(elapsed, 12)
                self.assertEqual(len(children), 1)
                # Windows venv python.exe may launch a different interpreter PID.
                test_pid = int(pid_file.read_text(encoding="utf-8"))
                self.assertFalse(_pid_is_alive(test_pid), "Actual silent TEST interpreter survived cleanup")
                self.assertIsNotNone(children[0].returncode, "Silent SDK-owned TEST child survived cleanup")
                print("COLDSTART_TIMEOUT_RECEIPT " + json.dumps({
                    "mode": mode, "elapsed_seconds": round(elapsed, 3),
                    "sdk_owned_test_pid": children[0].pid, "sdk_child_reaped": True,
                    "test_interpreter_pid": test_pid, "test_interpreter_exited": True,
                    "model_turns": 0, "shared_host_touched": False,
                }, sort_keys=True))


if __name__ == "__main__":
    unittest.main()

