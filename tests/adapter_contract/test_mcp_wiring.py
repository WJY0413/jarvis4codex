from __future__ import annotations

import tempfile
import unittest
import json
from unittest.mock import patch
from pathlib import Path

from adapters.codex_app_server.mcp_wiring import build_jarvis_control
from adapters.codex_app_server.jarvis_local_heartbeat_host import JarvisControlFunctionRunner
from adapters.codex_app_server import jarvis_local_heartbeat_host
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
    def test_observer_read_facade_and_monitor_share_owner_terminal_evidence(self):
        import jarvis_codex_bridge as bridge_module
        import jarvis_heartbeat_service as runtime

        response = {"thread": {"id": "thread-1", "status": {"type": "notLoaded"},
                               "turns": [{"id": "turn-1", "status": "interrupted"}]}}

        class ObserverClient:
            def __init__(self, config): pass
            def start(self): pass
            def close(self): pass
            def request(self, method, params):
                self_test.assertEqual(method, "thread/read")
                self_test.assertEqual(params, {"threadId": "thread-1", "includeTurns": True})
                return response

        self_test = self
        transports = []

        def standard_transport(config, *, execution_reader):
            transport = runtime.StandardBridgeHeartbeatTransport.__new__(runtime.StandardBridgeHeartbeatTransport)
            transport.launcher_config = object()
            transport.bridge = bridge_module
            transport.execution_reader = execution_reader
            transports.append(transport)
            return transport

        with tempfile.TemporaryDirectory() as temp, patch.object(
            runtime, "AppServerClient", ObserverClient
        ), patch("adapters.codex_app_server.mcp_wiring._standard_transport", standard_transport):
            config = Path(temp) / "heartbeat.json"
            config.write_text(json.dumps({"db_path": str(Path(temp) / "heartbeat.sqlite")}))
            control = build_jarvis_control(config, Path(temp), launcher_config_path=Path(temp) / "unused.json")
            root = Path(temp) / "task-holds" / "hold-1"
            root.mkdir(parents=True)
            identity = {"hold_id": "hold-1", "request_id": "req-1", "thread_id": "thread-1", "turn_id": "turn-1"}
            (root / "request.json").write_text(json.dumps(identity))
            (root / "ack.json").write_text(json.dumps({**identity, "status": "holding"}))
            monitor = bridge_module.ThreadTerminalMonitor(transports[0], Path(temp) / "check-monitor.json")
            route = bridge_module.ReceiptRoute("thread-1", "parent")
            self.assertEqual(monitor.observe("m", route).state, "baseline_active")
            data = control.read(subject="thread", task_id="thread-1")["data"]
            self.assertEqual(data["status"], "notLoaded")
            self.assertEqual(data["turns"][-1]["status"], "interrupted")
            self.assertEqual(data["execution_status"], "unknown")
            self.assertEqual(data["read_source"], "native_observer")
            request = bridge_module.ResumeRequest("do-not-run", "thread-1", "continue", "test")
            refused = control._bridge.resume_existing(request)
            self.assertEqual(refused.status, "requires_readback")
            for status in ("completed", "failed", "cancelled", "blocked", "interrupted"):
                with self.subTest(status=status):
                    result = {**identity, "status": status, "terminal_confirmed": True,
                              "output_verification": {"status": "blocked" if status == "blocked" else "verified"}}
                    (root / "result.json").write_text(json.dumps(result))
                    observed = control.read(subject="thread", task_id="thread-1")
                    self.assertEqual(observed["data"]["execution_status"], status)
                    self.assertEqual(observed["data"]["execution_source"], "hold_terminal_result")
                    self.assertEqual(observed["data"]["turns"][-1]["status"], "interrupted")
                    self.assertFalse(observed["readback"]["verified"])
                    terminal = monitor.observe("m", route)
                    self.assertEqual(terminal.state, "terminal_changed")
                    self.assertEqual(terminal.reason, status)
                    self.assertEqual(monitor.observe("m", route).state, "no_change")

            # A request that explicitly names another thread cannot authorize this one.
            (root / "request.json").write_text(json.dumps({**identity, "thread_id": "different"}))
            mismatch = control.read(subject="thread", task_id="thread-1")["data"]
            self.assertEqual(mismatch["execution_status"], "unknown")
            self.assertEqual(control._bridge.resume_existing(request).status, "requires_readback")

            # Recovery requests may retain their initial turn ID across later Hold turns.
            (root / "request.json").write_text(json.dumps({**identity, "mode": "recover", "turn_id": "turn-initial"}))
            self.assertEqual(control.read(subject="thread", task_id="thread-1")["data"]["execution_status"], "interrupted")

            # A released historical Hold must not govern a later ordinary native turn.
            response["thread"]["turns"].append({"id": "turn-new", "status": "inProgress", "items": []})
            monitor.observe("m", route)
            response["thread"]["turns"][-1].update(status="completed", items=[
                {"type": "agentMessage", "phase": "final_answer", "text": "done"}])
            later = control.read(subject="thread", task_id="thread-1")["data"]
            self.assertEqual(later["execution_status"], "completed")
            self.assertEqual(later["execution_source"], "native_observer")
            self.assertEqual(monitor.observe("m", route).state, "terminal_changed")
            resumed = []
            def resume_stub(req):
                resumed.append(req.thread_id)
                return StartedTurn(req.thread_id, "turn-new", "completed")
            transports[0].resume_existing = resume_stub
            self.assertEqual(control._bridge.resume_existing(request).status, "completed")
            self.assertEqual(resumed, ["thread-1"])

    def test_heartbeat_cli_accepts_optional_notification_config(self):
        for notification_path in (None, Path("test-notification.json")):
            with self.subTest(notification_path=notification_path):
                argv = ["heartbeat-host", "--config", "transport.json",
                        "--local-heartbeat-config", "heartbeat.json",
                        "--launcher-config", "launcher.json", "--state-dir", "test-state"]
                if notification_path is not None:
                    argv += ["--notification-config", str(notification_path)]
                argv += ["health-check"]
                with patch("sys.argv", argv), \
                     patch.object(jarvis_local_heartbeat_host, "build_jarvis_control") as build, \
                     patch.object(jarvis_local_heartbeat_host, "LocalHeartbeatConfig"), \
                     patch.object(jarvis_local_heartbeat_host, "HeartbeatService") as service, \
                     patch("builtins.print"):
                    service.return_value.health.return_value = {"status": "ok"}
                    self.assertEqual(jarvis_local_heartbeat_host.main(), 0)
                build.assert_called_once_with(
                    Path("transport.json"), Path("test-state"),
                    launcher_config_path=Path("launcher.json"),
                    local_heartbeat_config_path=Path("heartbeat.json"),
                    notification_config_path=notification_path,
                )

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

    def test_heartbeat_host_runner_reconciles_terminal_holds_without_a_scheduled_tick(self):
        class Control:
            def loop(self, **kwargs):
                self.kwargs = kwargs
                return {"status": "completed"}

            def deliver_pending_hold_notifications(self, **_kwargs):
                return {"status": "completed", "readback": {"verified": True}}

        control = Control()
        result = JarvisControlFunctionRunner(control).reconcile_terminal_holds()
        self.assertEqual(result["status"], "completed")
        self.assertEqual(control.kwargs, {"action": "reconcile"})

    def test_heartbeat_host_runner_drains_pending_terminal_notifications(self):
        class Control:
            def loop(self, **kwargs):
                self.loop_kwargs = kwargs
                return {"status": "completed"}

            def deliver_pending_hold_notifications(self, **kwargs):
                self.delivery_kwargs = kwargs
                return {
                    "status": "completed",
                    "readback": {"verified": True},
                    "data": {"deliveries": [{"message_id": "om-1"}]},
                }

        control = Control()
        result = JarvisControlFunctionRunner(control).reconcile_terminal_holds()

        self.assertEqual(result["status"], "completed")
        self.assertEqual(control.loop_kwargs, {"action": "reconcile"})
        self.assertEqual(control.delivery_kwargs["source_ref"], "local-heartbeat:terminal-notification-drain")
        self.assertTrue(result["notification_delivery"]["readback"]["verified"])


if __name__ == "__main__":
    unittest.main()
