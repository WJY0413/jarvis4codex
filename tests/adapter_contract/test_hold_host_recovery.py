from __future__ import annotations

import json
import os
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
    def _dead_claim(self, temp, lock_owner=987654321):
        root = Path(temp) / "task-holds" / "a-dead"
        root.mkdir(parents=True)
        request = {"request_id": "dead-run", "hold_id": "a-dead", "mode": "create", "max_turns": 3}
        ack = {"status": "holding", "thread_id": "exact-thread", "turn_id": "exact-turn", "turn_count": 1}
        request_path, ack_path = root / "request.json", root / "ack.json"
        request_path.write_text(json.dumps(request), encoding="utf-8")
        ack_path.write_text(json.dumps(ack), encoding="utf-8")
        claim = root / ".user-host-claim"
        claim.mkdir()
        (claim / "owner.json").write_text(json.dumps({"pid": 987654321}), encoding="utf-8")
        lock = request_path.with_suffix(".lock")
        lock.write_text(json.dumps({"pid": lock_owner}), encoding="utf-8")
        os.utime(lock, (1, 1))
        return root, request_path, ack_path, request, ack

    def test_dead_request_lock_recovers_the_exact_turn_and_removes_claim_only_after_persistence(self):
        with tempfile.TemporaryDirectory() as temp:
            args = self._dead_claim(temp)
            with patch("adapters.codex_app_server.jarvis_hold_host_service._pid_is_alive", side_effect=lambda pid: pid == os.getpid()):
                self.assertTrue(JarvisHoldHost._requeue_dead_claim(*args))
            root, request_path, ack_path, _, _ = args
            saved = json.loads(request_path.read_text(encoding="utf-8"))
            self.assertEqual((saved["mode"], saved["thread_id"], saved["turn_id"]), ("recover", "exact-thread", "exact-turn"))
            self.assertEqual(json.loads(ack_path.read_text(encoding="utf-8"))["phase"], "queued_for_user_host")
            self.assertFalse((root / ".user-host-claim").exists())
            self.assertFalse(request_path.with_suffix(".lock").exists())

    def test_live_or_unreadable_request_lock_is_not_stolen_and_does_not_starve_other_holds(self):
        for lock_owner in (os.getpid(), None):
            with self.subTest(owner=lock_owner), tempfile.TemporaryDirectory() as temp:
                root, request_path, ack_path, _, _ = self._dead_claim(temp, lock_owner)
                before = [path.read_bytes() for path in (request_path, ack_path, request_path.with_suffix(".lock"))]
                other = root.parent / "z-other"
                other.mkdir()
                (other / "request.json").write_text(json.dumps({"request_id": "other"}), encoding="utf-8")
                (other / "ack.json").write_text(json.dumps({"status": "accepted", "phase": "queued_for_user_host"}), encoding="utf-8")
                def finish(_config, _request, _ack, result):
                    result.write_text(json.dumps({"status": "completed"}), encoding="utf-8")
                with patch("adapters.codex_app_server.jarvis_hold_host_service._pid_is_alive", side_effect=lambda pid: pid == os.getpid()), patch(
                    "adapters.codex_app_server.jarvis_hold_host_service.hold_task", side_effect=finish,
                ):
                    self.assertTrue(JarvisHoldHost(state_dir=Path(temp), launcher_config=Path(temp) / "unused").run_once())
                self.assertTrue((other / "result.json").exists())
                self.assertTrue((root / ".user-host-claim" / "owner.json").exists())
                self.assertEqual(before, [path.read_bytes() for path in (request_path, ack_path, request_path.with_suffix(".lock"))])

    def test_request_or_ack_write_failure_keeps_claim_and_recovery_can_retry(self):
        from adapters.codex_app_server.jarvis_hold_host_service import _write_json
        for failed_name in ("request.json", "ack.json"):
            with self.subTest(file=failed_name), tempfile.TemporaryDirectory() as temp:
                args = self._dead_claim(temp)
                root, request_path, ack_path, _, _ = args
                def fail(path, value):
                    self.assertTrue((root / ".user-host-claim" / "owner.json").exists())
                    if path.name == failed_name:
                        raise PermissionError("isolated persistence failure")
                    _write_json(path, value)
                with patch("adapters.codex_app_server.jarvis_hold_host_service._pid_is_alive", side_effect=lambda pid: pid == os.getpid()):
                    with patch("adapters.codex_app_server.jarvis_hold_host_service._write_json", side_effect=fail):
                        self.assertFalse(JarvisHoldHost._requeue_dead_claim(*args))
                    self.assertTrue((root / ".user-host-claim" / "owner.json").exists())
                    current_request = json.loads(request_path.read_text(encoding="utf-8"))
                    current_ack = json.loads(ack_path.read_text(encoding="utf-8"))
                    self.assertTrue(JarvisHoldHost._requeue_dead_claim(root, request_path, ack_path, current_request, current_ack))
                self.assertFalse((root / ".user-host-claim").exists())

    def test_recovery_preserves_stop_and_waits_for_the_owned_turn_without_new_dispatch(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "task-holds" / "hold-1"
            root.mkdir(parents=True)
            request_path, ack_path, result_path = (root / name for name in ("request.json", "ack.json", "result.json"))
            request = {"request_id": "run-1", "hold_id": "hold-1", "mode": "create", "auto_continue": True,
                       "max_turns": 3, "stop_requested": True}
            ack = {"status": "holding", "thread_id": "exact-thread", "turn_id": "exact-turn", "turn_count": 1}
            request_path.write_text(json.dumps(request), encoding="utf-8")
            ack_path.write_text(json.dumps(ack), encoding="utf-8")
            claim = root / ".user-host-claim"
            claim.mkdir()
            (claim / "owner.json").write_text(json.dumps({"pid": 1780}), encoding="utf-8")
            with patch("adapters.codex_app_server.jarvis_hold_host_service._pid_is_alive", return_value=False):
                self.assertTrue(JarvisHoldHost._requeue_dead_claim(root, request_path, ack_path, request, ack))
            recovered = json.loads(request_path.read_text(encoding="utf-8"))
            self.assertTrue(recovered["stop_requested"])
            self.assertEqual(recovered["mode"], "recover")
            client = RecoveryClient(None)
            with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=client,
            ):
                self.assertEqual(hold_task(Path(temp) / "unused.json", request_path, ack_path, result_path), 0)
            final = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertEqual(final["status"], "cancelled")
            self.assertEqual(final["turn_id"], "exact-turn")
            self.assertEqual(client.created_requests, [])
            self.assertFalse(client.resumed)

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
