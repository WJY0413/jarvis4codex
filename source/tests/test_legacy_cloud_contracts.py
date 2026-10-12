"""Offline Jarvis cloud contract checks; never start services, workers, or models.

Run from cooper-cloud with:
    PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -B -m unittest discover \
        -s tests -p test_jarvis_cloud_contracts.py -v

The source checkout is preferred over the installed package. Runtime requests,
process health receipts, and Linux process metadata are mocked. Passing these
checks does not prove that the cloud App Server or a model can run.
"""

from __future__ import annotations

import ast
import asyncio
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]
PACKAGES = ROOT / "packages"
sys.dont_write_bytecode = True
sys.path[:0] = [str(PACKAGES), str(PACKAGES / "jarvis_runtime")]

from adapters.codex_app_server import jarvis_hold_host_service as host
from jarvis_control.loop import (
    LoopController,
    _cleanup_evidence_confirmed,
    _validate_start,
)
from jarvis_mcp.server import JarvisMcpServer, _tool_result
from jarvis_runtime.jarvis_native_task_launcher import AppServerClient


class OfflineTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.offline_guards = ExitStack()
        self.addCleanup(self.offline_guards.close)
        for target in (
            "subprocess.Popen",
            "socket.socket.connect",
            "socket.socket.connect_ex",
        ):
            self.offline_guards.enter_context(
                patch(target, side_effect=AssertionError(f"Live interface forbidden: {target}"))
            )


class JarvisRegistryContracts(OfflineTestCase):
    def registry(self):
        # Registration only: neither MCP transport runner is invoked.
        server = JarvisMcpServer(type("MutableControl", (), {})())
        return server.mcp._tool_manager.list_tools()

    def test_all_jarvis_python_sources_parse(self) -> None:
        sources = sorted(PACKAGES.rglob("*.py"))
        self.assertTrue(sources)
        for path in sources:
            with self.subTest(source=str(path.relative_to(PACKAGES))):
                ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))

    def test_exactly_seventeen_public_tools_register(self) -> None:
        tools = self.registry()
        self.assertEqual(len(tools), 17)
        self.assertEqual(
            {tool.name for tool in tools},
            {
                "jarvis_create", "jarvis_hold", "jarvis_loop", "jarvis_close",
                "jarvis_read", "jarvis_resume", "jarvis_monitor",
                "jarvis_heartbeat", "jarvis_update", "jarvis_notify",
                "jarvis_contract_check", "jarvis_receipt", "jarvis_callback",
                "jarvis_relay", "jarvis_read_delivery", "jarvis_capacity", "jarvis_dispatch",
            },
        )

    def test_loop_schema_preserves_four_actions_and_adds_two_schema_actions(self) -> None:
        tool = next(tool for tool in self.registry() if tool.name == "jarvis_loop")
        schema = tool.parameters  # Registered public input schema, without an event loop.
        actions = schema["properties"]["action"]["enum"]
        self.assertEqual(actions[:4], ["preflight", "start", "status", "stop"])
        self.assertEqual(actions[4:], ["schema_register", "schema_read"])
        for field in ("target_thread_count", "model", "reasoning_effort", "threads"):
            self.assertIn(field, schema["properties"])

    def test_result_preserves_wire_aliases_and_error_flag(self) -> None:
        receipt = {"status": "failed", "reason": "offline fixture", "data": {"sample": True}}
        result = _tool_result(receipt).model_dump(by_alias=True)
        self.assertEqual(result["structuredContent"], receipt)
        self.assertIs(result["isError"], True)
        self.assertEqual(result["content"][0]["type"], "text")


class JarvisLoopContracts(OfflineTestCase):
    def test_preflight_contract_requires_no_runtime(self) -> None:
        contract = LoopController.preflight_contract(allowed_projects=["CloudContactWorker"])
        self.assertEqual(contract["allowed_projects"], ["CloudContactWorker"])
        self.assertIn("expires_at", contract["start_contract"]["required"])
        self.assertIn("target_thread_count", contract["start_contract"]["required"])

    def test_ten_independent_single_seat_luna_low_contracts(self) -> None:
        expires = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        request_ids = set()
        for seat in range(1, 11):
            with self.subTest(seat=seat):
                value = _validate_start({
                    "request_id": f"offline-single-{seat}",
                    "project": "CloudContactWorker",
                    "prompt": "Offline validation only",
                    "title": "Offline validation",
                    "target_thread_count": 1,
                    "max_rounds": 1,
                    "expires_at": expires,
                    "model": "gpt-6-luna",
                    "reasoning_effort": "low",
                    "notifications": False,
                })
                request_ids.add(value["request_id"])
                self.assertEqual(value["target_thread_count"], 1)
                self.assertEqual(len(value["threads"]), 1)
                self.assertEqual(value["model"], "gpt-6-luna")
                self.assertEqual(value["reasoning_effort"], "low")
        self.assertEqual(len(request_ids), 10)

    def test_fake_transport_preserves_exact_model_and_effort(self) -> None:
        # Skip __init__, which discovers a real Codex executable.
        client = object.__new__(AppServerClient)
        client.start = Mock(return_value={})
        client.request = Mock(return_value={"turn": {"id": "offline-turn", "status": "inProgress"}})
        receipt = client.start_turn_async(
            "offline-thread", "Offline test",
            client_user_message_id="offline-request",
            model="gpt-6-luna", reasoning_effort="low",
        )
        client.request.assert_called_once_with("turn/start", {
            "threadId": "offline-thread",
            "clientUserMessageId": "offline-request",
            "input": [{"type": "text", "text": "Offline test"}],
            "model": "gpt-6-luna",
            "effort": "low",
        })
        self.assertEqual(receipt["thread_id"], "offline-thread")
        self.assertEqual(receipt["turn_id"], "offline-turn")
        self.assertEqual(receipt["model"], "gpt-6-luna")
        self.assertEqual(receipt["reasoning_effort"], "low")


class JarvisHostIdentityContracts(OfflineTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.now = datetime(2026, 9, 30, 3, 0, tzinfo=timezone.utc)
        self.state = ROOT / "offline-fixture-state-not-created"
        self.home = ROOT / "offline-fixture-home-not-created"
        self.birth = self.now - timedelta(seconds=2)
        self.health = {
            "status": "ready", "observed_at": self.now.isoformat(), "pid": 12345,
            "worker_capacity": 10, "host_started_at": self.now.isoformat(),
            "profile": "offline", "codex_home": str(self.home), "state_dir": str(self.state),
        }
        self.kwargs = {
            "now": lambda: self.now, "pid_alive": lambda _: True,
            "pid_started_at": lambda _: self.birth, "required_workers": 10,
        }

    def matches(self, *, profile="offline", **overrides) -> bool:
        with patch.object(host, "_read_json", return_value=dict(self.health)):
            return host._matching_health(
                self.state, profile, str(self.home), **{**self.kwargs, **overrides}
            )

    def test_linux_process_start_time_uses_utc_and_proc_ticks(self) -> None:
        # Fields after comm start at field 3; index 19 is field 22/starttime.
        fields = ["S"] + ["0"] * 18 + ["250"] + ["0"] * 5
        process_stat = "12345 (fixture name (with parentheses)) " + " ".join(fields)
        def read_text(path, *args, **kwargs):
            if path.as_posix() == "/proc/12345/stat":
                return process_stat
            if path.as_posix() == "/proc/stat":
                return "cpu 0 0 0\nbtime 1700000000\n"
            raise AssertionError(f"Unexpected filesystem read: {path}")
        with patch.object(host.sys, "platform", "linux"), \
                patch.object(Path, "read_text", autospec=True, side_effect=read_text), \
                patch.object(host.os, "sysconf", return_value=100, create=True):
            birth = host._pid_started_at(12345)
        self.assertEqual(birth, datetime.fromtimestamp(1700000002.5, tz=timezone.utc))

    def test_linux_missing_process_fails_closed(self) -> None:
        with patch.object(host.sys, "platform", "linux"), \
                patch.object(Path, "read_text", side_effect=FileNotFoundError):
            self.assertIsNone(host._pid_started_at(-1))

    def test_matching_identity_and_ten_worker_capacity_are_accepted(self) -> None:
        self.assertTrue(self.matches())

    def test_wrong_profile_is_rejected(self) -> None:
        self.assertFalse(self.matches(profile="wrong"))

    def test_insufficient_capacity_is_rejected(self) -> None:
        self.assertFalse(self.matches(required_workers=11))

    def test_reused_pid_with_newer_birth_is_rejected(self) -> None:
        self.assertFalse(self.matches(pid_started_at=lambda _: self.now + timedelta(seconds=10)))

    def test_missing_process_birth_is_rejected(self) -> None:
        self.assertFalse(self.matches(pid_started_at=lambda _: None))


class JarvisClosureContracts(OfflineTestCase):
    @staticmethod
    def state(*, terminal_confirmed: bool, hold_released: bool):
        return {"children": [{
            "hold_id": "offline-hold",
            "last_receipt": {"status": "completed", "data": {
                "status": "completed", "hold_released": hold_released,
                "terminal_confirmed": terminal_confirmed,
            }},
        }]}

    def test_closure_accepts_both_terminal_and_release_evidence(self) -> None:
        self.assertTrue(_cleanup_evidence_confirmed(self.state(terminal_confirmed=True, hold_released=True)))

    def test_closure_rejects_missing_terminal_evidence(self) -> None:
        self.assertFalse(_cleanup_evidence_confirmed(self.state(terminal_confirmed=False, hold_released=True)))

    def test_closure_rejects_missing_release_evidence(self) -> None:
        self.assertFalse(_cleanup_evidence_confirmed(self.state(terminal_confirmed=True, hold_released=False)))


if __name__ == "__main__":
    unittest.main()
