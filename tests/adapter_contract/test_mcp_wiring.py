from __future__ import annotations

import tempfile
import unittest
import json
from pathlib import Path

from adapters.codex_app_server.mcp_wiring import build_jarvis_control
from adapters.codex_app_server.jarvis_local_heartbeat_host import JarvisControlFunctionRunner
from jarvis_codex_bridge import StartedTurn, ThreadState, TurnState


class FakeTransport:
    name = "fake-mcp-wiring"

    def health(self):
        return {"status": "ok"}

    def read_thread(self, thread_id):
        return ThreadState(thread_id, "idle", (TurnState("turn-1", "completed"),))

    def resume_existing(self, request):
        return StartedTurn(request.thread_id, "turn-2", "completed")


class McpWiringContractTest(unittest.TestCase):
    def test_wiring_uses_the_supplied_existing_thread_transport(self):
        with tempfile.TemporaryDirectory() as temp:
            config_path = Path(temp) / "heartbeat.json"
            config_path.write_text(json.dumps({
                "db_path": str(Path(temp) / "heartbeat.sqlite"),
                "health_path": str(Path(temp) / "heartbeat-health.json"),
            }), encoding="utf-8")
            launcher_config = Path(temp) / "launcher.json"
            control = build_jarvis_control(
                config_path,
                Path(temp),
                launcher_config_path=launcher_config,
                transport_factory=lambda _: FakeTransport(),
            )
            receipt = control.read(subject="thread", task_id="thread-1")
        self.assertEqual(receipt["status"], "completed")
        self.assertEqual(receipt["target_thread_id"], "thread-1")
        self.assertEqual(control._provisioner._config_path, launcher_config)

    def test_wiring_exposes_the_local_heartbeat_control(self):
        with tempfile.TemporaryDirectory() as temp:
            config_path = Path(temp) / "heartbeat.json"
            config_path.write_text(json.dumps({
                "db_path": str(Path(temp) / "heartbeat.sqlite"),
                "health_path": str(Path(temp) / "heartbeat-health.json"),
            }), encoding="utf-8")
            control = build_jarvis_control(
                config_path,
                Path(temp),
                launcher_config_path=Path(temp) / "launcher.json",
                transport_factory=lambda _: FakeTransport(),
            )
            receipt = control.heartbeat(
                action="health", request_id="health-1", source_ref="test",
            )
        self.assertEqual(receipt["status"], "completed")
        self.assertIn("local_time", receipt["data"])
        self.assertIsNotNone(control._loop_controller)

    def test_wiring_keeps_notification_unsupported_without_an_adapter_config(self):
        with tempfile.TemporaryDirectory() as temp:
            config_path = Path(temp) / "heartbeat.json"
            config_path.write_text(json.dumps({"db_path": str(Path(temp) / "heartbeat.sqlite")}), encoding="utf-8")
            control = build_jarvis_control(
                config_path, Path(temp), launcher_config_path=Path(temp) / "launcher.json",
                transport_factory=lambda _: FakeTransport(),
            )
            receipt = control.notify(request_id="n-1", source_ref="test", message="hi")
        self.assertEqual(receipt["status"], "unsupported")

    def test_heartbeat_runner_routes_loop_tick_to_the_control_plane(self):
        class Control:
            def loop(self, **kwargs):
                self.kwargs = kwargs
                return {"status": "running"}

        control = Control()
        result = JarvisControlFunctionRunner(control)(
            "JarvisControl.loop_tick", {"loop_id": "loop-contract-1"},
            {"heartbeat_id": "heartbeat-loop-1", "run_number": 1},
        )
        self.assertEqual(result["status"], "running")
        self.assertEqual(control.kwargs, {"action": "tick", "loop_id": "loop-contract-1"})


if __name__ == "__main__":
    unittest.main()
