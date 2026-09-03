from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from adapters.codex_app_server.jarvis_hold_host_service import JarvisHoldHost
from adapters.codex_app_server.jarvis_task_hold_host import hold_task


class FakeConfig:
    pass


class RecoveryClient:
    def __init__(self, _config):
        self.created_requests: list[dict] = []
        self.resumed = False

    def create_task(self, request, **_kwargs):
        self.created_requests.append(request)
        return {"thread_id": "unexpected-thread", "turn_id": "unexpected-turn"}

    def resume_turn_async(self, *_args, **_kwargs):
        self.resumed = True
        return {"turn_id": "unexpected-turn"}

    def wait_for_turn_started(self, _thread_id, turn_id):
        return {"id": turn_id, "status": "inProgress"}

    def wait_for_turn_terminal(self, _thread_id, turn_id, **_kwargs):
        return {"id": turn_id, "status": "completed"}

    def wait_for_turn_readback(self, _thread_id, _turn_id):
        return "done"

    def close(self):
        pass


class HoldHostRecoveryAdapterContractTest(unittest.TestCase):
    def test_dead_claim_recovery_reuses_the_exact_persisted_turn(self):
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            launcher_config = state_dir / "launcher.json"
            launcher_config.write_text(json.dumps({
                "profile": "jarvis_test", "expected_codex_home": "C:/test/codex-home",
            }), encoding="utf-8")
            root = state_dir / "task-holds" / "hold-1"
            root.mkdir(parents=True)
            request_path, ack_path, result_path = root / "request.json", root / "ack.json", root / "result.json"
            request_path.write_text(json.dumps({
                "request_id": "hold-1", "mode": "create", "prompt": "work", "hold_id": "hold-1",
            }), encoding="utf-8")
            ack_path.write_text(json.dumps({
                "request_id": "hold-1", "status": "holding", "phase": "turn_holding",
                "thread_id": "thread-existing-1", "turn_id": "turn-existing-1",
            }), encoding="utf-8")
            claim_dir = root / ".user-host-claim"
            claim_dir.mkdir()
            (claim_dir / "owner.json").write_text(json.dumps({"pid": 1780}), encoding="utf-8")

            with patch("adapters.codex_app_server.jarvis_hold_host_service._pid_is_alive", return_value=False):
                self.assertTrue(JarvisHoldHost(state_dir=state_dir, launcher_config=launcher_config).run_once())

            client = RecoveryClient(None)
            with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=client,
            ):
                exit_code = hold_task(launcher_config, request_path, ack_path, result_path)
            recovered_request = json.loads(request_path.read_text(encoding="utf-8"))
            final = json.loads(result_path.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertEqual(recovered_request["mode"], "recover")
        self.assertEqual(recovered_request["thread_id"], "thread-existing-1")
        self.assertEqual(recovered_request["turn_id"], "turn-existing-1")
        self.assertEqual(client.created_requests, [])
        self.assertFalse(client.resumed)
        self.assertEqual(final["thread_id"], "thread-existing-1")
        self.assertEqual(final["turn_id"], "turn-existing-1")


if __name__ == "__main__":
    unittest.main()
