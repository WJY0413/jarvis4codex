"""Isolated Hold flow: real orchestration, explicit synthetic Codex transport."""
import json
import tempfile
from pathlib import Path
import unittest
import asyncio
from unittest.mock import patch
from contract_fake_inputs import host_config_fixture
from tests.jarvis_runtime.test_jarvis_task_hold_host import FakeClient, FakeConfig
from adapters.codex_app_server.jarvis_task_hold_host import hold_task


@host_config_fixture
class PredictionTaskFlow(unittest.TestCase):
    def test_unknown_prediction_helper_prevents_hold_terminal_confirmation(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            request, ack, result = [root / name for name in ("request.json", "ack.json", "result.json")]
            request.write_text(json.dumps({"mode": "resume", "thread_id": "thread-1",
                "request_id": "unknown-helper", "hold_id": "hold-unknown-helper", "prompt": "fixed",
                "max_turns": 2, "auto_continue": True,
                "prediction_options": {"mode": "prediction", "fixed_prompt": ""}}))
            predictions = [{"status": "completed", "prompt": "first"},
                {"status": "requires_readback", "cleanup_completed": False, "prediction_turn_id": "unknown-helper-turn"}]
            with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch("adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=FakeClient(None)), patch("adapters.codex_app_server.jarvis_task_hold_host.prepare_prediction_prompt", side_effect=predictions):
                self.assertEqual(hold_task(root / "launcher.json", request, ack, result), 0)
            self.assertFalse(json.loads(result.read_text())["terminal_confirmed"])
            self.assertEqual(json.loads((root / "prediction-2.json").read_text())["prediction"]["prediction_turn_id"], "unknown-helper-turn")

    def test_recovered_prediction_continuation_uses_native_dispatch_gate(self):
        import io
        import threading
        from types import SimpleNamespace
        from jarvis_native_task_launcher import AppServerClient
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            request, ack, result = [root / name for name in ("request.json", "ack.json", "result.json")]
            request.write_text(json.dumps({"mode": "recover", "thread_id": "thread-1", "turn_id": "old-turn",
                "request_id": "recovered-prediction", "hold_id": "hold-recovered-prediction",
                "prompt": "fixed", "max_turns": 2, "auto_continue": True,
                "prediction_options": {"mode": "prediction_plus_fixed", "fixed_prompt": "fixed"}}))
            client = FakeClient(None)
            admission = []
            def prepare(client, **kwargs):
                native = AppServerClient.__new__(AppServerClient)
                native.read_only = client.read_only
                native.process = SimpleNamespace(poll=lambda: None, stdin=io.StringIO())
                native._write_lock = threading.Lock()
                native._require_memory_admission = lambda: admission.append(True)
                native._write({"method": "turn/start", "params": {"threadId": "synthetic-prediction"}})
                return {"status": "completed", "request_id": kwargs["request_id"], "prompt": "next fixed"}
            with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch("adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=client), patch("adapters.codex_app_server.jarvis_task_hold_host.prepare_prediction_prompt", side_effect=prepare):
                self.assertEqual(hold_task(root / "launcher.json", request, ack, result), 0)
            self.assertEqual(admission, [True])
            self.assertEqual(json.loads(result.read_text())["status"], "turn_limit_reached")
            self.assertEqual(client.started_prompts, ["next fixed"])

    def test_public_mcp_to_persisted_resume_and_legacy_replay(self):
        from mcp import Client
        from jarvis_control import JarvisControl
        from jarvis_mcp import JarvisMcpServer
        from adapters.codex_app_server.task_provisioning_adapter import CodexAppServerTaskProvisioningAdapter
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = SimpleNamespace(live_creation_enabled=True, profile="offline", expected_codex_home=str(root/"empty-home"))
            adapter = CodexAppServerTaskProvisioningAdapter(root/"launcher.json", state_dir=root,
                config_loader=lambda _: config)
            server = JarvisMcpServer(JarvisControl(None, None, provisioner=adapter))
            async def run():
                async with Client(server.mcp) as client:
                    args = {"task_id":"thread-1", "request_id":"task-wire", "max_turns":3,
                            "fixed_prompt":"Only edit A", "stop_mode":"project", "completion_marker":"PROJECT_DONE"}
                    response = await client.call_tool("jarvis_task", args)
                    self.assertFalse(response.is_error)
                    saved = json.loads((root/"task-holds/hold-task-wire/request.json").read_text(encoding="utf-8"))
                    self.assertEqual(saved["prediction_options"]["completion_marker"], "PROJECT_DONE")
                    self.assertIn("Only edit A", saved["prediction_options"]["fixed_prompt"])
                    self.assertEqual(saved["max_turns"], 3)
                    before = (root/"task-holds/hold-task-wire/request.json").read_bytes()
                    await client.call_tool("jarvis_task", args)
                    self.assertEqual((root/"task-holds/hold-task-wire/request.json").read_bytes(), before)
                    await client.call_tool("jarvis_resume", {"task_id":"other", "request_id":"legacy", "prompt":"fixed"})
                    legacy = json.loads((root/"task-holds/hold-legacy/request.json").read_text(encoding="utf-8"))
                    self.assertNotIn("prediction_options", legacy)
                    self.assertEqual(legacy["prompt"], "fixed")
                    print("PREDICTION_MCP_PERSISTENCE_RECEIPT " + json.dumps({"task_mode":"project",
                        "budget":3, "same_request_replayed":True, "legacy_fields_unchanged":True, "production_state_used":False}))
            with patch("adapters.codex_app_server.task_provisioning_adapter.require_memory_admission"):
                asyncio.run(run())

    def test_each_turn_composes_new_prediction_and_stops_at_budget(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            request, ack, result = [root / name for name in ("request.json", "ack.json", "result.json")]
            request.write_text(json.dumps({"mode":"resume", "thread_id":"thread-1", "request_id":"predict-task", "hold_id":"hold-predict-task",
                "prompt":"Only edit A", "max_turns":2, "auto_continue":True,
                "prediction_options":{"mode":"prediction_plus_fixed", "fixed_prompt":"Only edit A"}}), encoding="utf-8")
            client = FakeClient(None)
            predictions = []
            def prepare(client, *, thread_id, request_id, options, receipt_dir, source_turn_id=None, **kwargs):
                predictions.append(source_turn_id)
                return {"status":"completed", "request_id":request_id, "prompt":"prediction %d\nOnly edit A" % len(predictions),
                        "source_turn_id":source_turn_id or "before", "suggestion":"prediction"}
            with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), \
                 patch("adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=client), \
                 patch("adapters.codex_app_server.jarvis_task_hold_host.prepare_prediction_prompt", side_effect=prepare):
                self.assertEqual(hold_task(root/"launcher.json", request, ack, result), 0)
            final = json.loads(result.read_text(encoding="utf-8"))
            self.assertEqual(final["status"], "turn_limit_reached")
            self.assertEqual(len(predictions), 2)
            self.assertEqual(client.resumed[1], "prediction 1\nOnly edit A")
            self.assertEqual(client.started_prompts, ["prediction 2\nOnly edit A"])
            receipts = [json.loads((root/name).read_text(encoding="utf-8"))
                        for name in ("prediction-first.json", "prediction-2.json")]
            for receipt in receipts:
                self.assertEqual(receipt["request_id"], "predict-task")
                self.assertEqual(receipt["hold_id"], "hold-predict-task")
                self.assertNotEqual(receipt["prediction"]["request_id"], receipt["request_id"])
                self.assertIn("Only edit A", receipt["prediction"]["prompt"])
            self.assertNotEqual(receipts[0]["prediction"]["request_id"], receipts[1]["prediction"]["request_id"])
            print("PREDICTION_TASK_ISOLATED_RECEIPT " + json.dumps({"status": final["status"],
                "prediction_calls":len(predictions), "task_turns":2, "native_transport":"synthetic", "production_state_used":False}))

