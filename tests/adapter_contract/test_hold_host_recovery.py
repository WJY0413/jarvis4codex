from __future__ import annotations

import json
import os
import queue
import threading
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from adapters.codex_app_server.jarvis_hold_host_service import JarvisHoldHost
from adapters.codex_app_server.jarvis_task_hold_host import hold_task


class FakeConfig:
    pass


class RecoveryClient:
    def __init__(self, _config):
        self.created_requests: list[dict] = []
        self.resumed = False

    def start(self):
        pass

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
    def test_counter_cannot_override_binding_and_exact_completed_repeat_is_idempotent(self):
        from adapters.codex_app_server.jarvis_task_hold_host import _write_json
        for terminal in (False, True):
            with self.subTest(terminal=terminal), tempfile.TemporaryDirectory() as temp:
                host, root, _ = self._exception_result(temp)
                identity = {"request_id": "req", "hold_id": "hold-error", "thread_id": "thread-exact", "turn_id": "different"}
                request = {**identity, "mode": "recover", "turn_id": "first", "initial_turn_count": 1, "max_turns": 2 if terminal else 1}
                ack = {**identity, "turn_count": 2, "status": "holding", "phase": "turn_holding"}
                if terminal:
                    request.update(host_stop=identity, stop_requested=True)
                    ack.update(status="interrupted", phase="host_stop_reconciled", host_stop=True,
                               terminal_confirmed=True, holder_exit_confirmed=True, holder_client_exit_confirmed=True,
                               owner_terminal_readback={"thread_id": "thread-exact", "turn_id": "different", "status": "interrupted"})
                    _write_json(root / "result.json", ack)
                    host._release_claim(root)
                else:
                    (root / "result.json").unlink()
                _write_json(root / "request.json", request)
                _write_json(root / "ack.json", ack)
                _write_json(host.health_path, {"pid": 99999999, "capabilities": ["exact_hold_stop_v1"], "active_hold_ids": ["hold-error"]})
                before = {str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()}
                with patch("adapters.codex_app_server.jarvis_hold_host_service._matching_health", return_value=True):
                    response = host.request_host_stop("hold-error", "thread-exact", "different")
                self.assertEqual(response["status"], "interrupted" if terminal else "rejected")
                self.assertEqual(response["hold_released"], terminal)
                self.assertEqual(before, {str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()})

    def test_recover_continuation_persists_actual_new_turn_binding(self):
        from adapters.codex_app_server.jarvis_task_hold_host import _write_json
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            request, ack, result = (root / name for name in ("request.json", "ack.json", "result.json"))
            _write_json(request, {"request_id": "r", "hold_id": "h", "mode": "recover", "thread_id": "exact-thread",
                                  "turn_id": "first", "auto_continue": True, "max_turns": 2})
            client = RecoveryClient(None)
            client.start_turn_async = lambda *args, **kwargs: {"turn_id": "actual-next-turn"}
            with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=client):
                self.assertEqual(hold_task(root / "unused", request, ack, result), 0)
            self.assertEqual(json.loads(request.read_text())["turn_id"], "actual-next-turn")
            self.assertEqual(json.loads(result.read_text())["turn_id"], "actual-next-turn")

    def test_controlled_result_write_failure_cannot_be_promoted_by_external_interrupted(self):
        from adapters.codex_app_server.jarvis_task_hold_host import _write_json
        from adapters.codex_app_server.task_provisioning_adapter import CodexAppServerTaskProvisioningAdapter
        from jarvis_runtime.jarvis_native_task_launcher import AppServerClient
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp)
            root = state / "task-holds" / "h"
            request = {"request_id": "r", "hold_id": "h", "mode": "recover", "thread_id": "t", "turn_id": "u", "stop_requested": True}
            _write_json(root / "request.json", request)
            _write_json(root / "ack.json", {"request_id": "r", "hold_id": "h", "status": "accepted", "phase": "queued_for_user_host"})
            client = Mock()
            client.config = type("Config", (), {"turn_completion_timeout_seconds": 1, "poll_seconds": .01})()
            client.notifications = queue.Queue()
            client.wait_for_turn_terminal = lambda *args, **kwargs: AppServerClient.wait_for_turn_terminal(client, *args, **kwargs)
            client.request.return_value = {"thread": {"id": "t", "turns": [{"id": "u", "status": "inProgress"}]}}
            client.process.poll.return_value = 0
            def denied(path, value):
                if path.name == "result.json":
                    raise PermissionError("exhausted holder result replace")
                _write_json(path, value)
            host = JarvisHoldHost(state_dir=state, launcher_config=state / "unused")
            with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=client
            ), patch("adapters.codex_app_server.jarvis_task_hold_host._write_json", side_effect=denied):
                self.assertTrue(host.run_once())
            adapter = CodexAppServerTaskProvisioningAdapter("unused", state_dir=state, config_loader=lambda _: FakeConfig())
            before = adapter.hold_status("h")
            claim_retained = (root / ".user-host-claim").exists()
            self.assertTrue(before.get("host_stop"), "fallback must retain control provenance")
            client.request.return_value["thread"]["turns"][0]["status"] = "interrupted"
            with patch("adapters.codex_app_server.jarvis_hold_host_service.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                "adapters.codex_app_server.jarvis_hold_host_service.AppServerClient", return_value=client
            ):
                reconciled = host.reconcile_hold("h")
            after = adapter.hold_status("h")
            self.assertFalse(before["terminal_confirmed"])
            self.assertFalse(after["terminal_confirmed"], "external interrupted must never promote failed controlled persistence")
            self.assertFalse(after["hold_released"])
            self.assertEqual(reconciled["status"], "requires_readback")
            self.assertTrue(claim_retained)
            self.assertTrue((root / ".user-host-claim").exists())
            legacy_fallback = {key: value for key, value in json.loads((root / "result.json").read_text()).items()
                               if key not in {"host_stop", "execution_evidence", "holder_exit_confirmed", "holder_client_exit_confirmed"}}
            _write_json(root / "result.json", legacy_fallback)
            self.assertEqual(host.reconcile_hold("h")["status"], "requires_readback")
            self.assertFalse(adapter.hold_status("h")["hold_released"])

    def test_recover_request_ack_conflict_rejected_without_mutation(self):
        from adapters.codex_app_server.jarvis_task_hold_host import _write_json
        with tempfile.TemporaryDirectory() as temp:
            host, root, _ = self._exception_result(temp)
            (root / "result.json").unlink()
            request = json.loads((root / "request.json").read_text())
            _write_json(root / "request.json", {**request, "mode": "recover", "turn_id": "actual-turn"})
            ack = json.loads((root / "ack.json").read_text())
            ack.update(thread_id="other-thread", turn_id="other-turn")
            _write_json(root / "ack.json", ack)
            _write_json(host.health_path, {"pid": 99999999, "capabilities": ["exact_hold_stop_v1"], "active_hold_ids": ["hold-error"]})
            paths = [root / "request.json", root / "ack.json", root / ".user-host-claim" / "owner.json"]
            before = [path.read_bytes() for path in paths]
            with patch("adapters.codex_app_server.jarvis_hold_host_service._matching_health", return_value=True):
                result = host.request_host_stop("hold-error", "other-thread", "other-turn")
            self.assertEqual(result["status"], "rejected")
            self.assertEqual(before, [path.read_bytes() for path in paths])
            # Create/resume hints are historical, but a counter alone must never
            # override a recover binding (QA counter-only-binding-bypass).
            for mode in ("create", "resume", "recover"):
                _write_json(root / "request.json", {**request, "mode": mode, "turn_id": "actual-turn"})
                current_ack = {**ack, "status": "holding", "phase": "turn_holding", "turn_count": 2}
                if mode == "recover":
                    current_ack["thread_id"] = request["thread_id"]
                _write_json(root / "ack.json", current_ack)
                with patch("adapters.codex_app_server.jarvis_hold_host_service._matching_health", return_value=True):
                    accepted = host.request_host_stop("hold-error", current_ack["thread_id"], current_ack["turn_id"])
                self.assertEqual(accepted["status"], "rejected" if mode == "recover" else "stop_requested")

    def test_host_owned_stop_quiesces_only_exact_holder_and_unknown_never_releases(self):
        from adapters.codex_app_server.task_provisioning_adapter import CodexAppServerTaskProvisioningAdapter
        from jarvis_runtime.jarvis_native_task_launcher import AppServerClient
        from adapters.codex_app_server.jarvis_task_hold_host import _write_json
        for mode, read_status, cleanup_failure in (("recover", "interrupted", False), ("recover", "completed", False),
                                                   ("recover", "inProgress", False),
                                                   ("recover", "completed", True), ("recover", "interrupted", "client"),
                                                   ("recover", "interrupted", "legacy"), ("create", "interrupted", False)):
            with self.subTest(mode=mode, read_status=read_status, cleanup_failure=cleanup_failure), tempfile.TemporaryDirectory() as temp:
                state = Path(temp)
                config_path = state / "launcher.json"
                _write_json(config_path, {"profile": "isolated", "expected_codex_home": str(state)})
                host = JarvisHoldHost(state_dir=state, launcher_config=config_path)
                clients = []
                class Client:
                    wait_for_turn_terminal = AppServerClient.wait_for_turn_terminal
                    def __init__(self, config):
                        self.config = type("Config", (), {"poll_seconds": .01, "turn_completion_timeout_seconds": 1})()
                        self.notifications = queue.Queue()
                        self.process = Mock()
                        self.process.poll.return_value = None
                        self.calls = []
                        self.started = threading.Event()
                        clients.append(self)
                    def start(self):
                        self.started.set()
                    def create_task(self, request, **kwargs):
                        self.start()
                        return {"thread_id": "target-thread", "turn_id": "target-turn"}
                    def request(self, method, params):
                        self.calls.append((method, params))
                        if method == "turn/interrupt":
                            self.notifications.put({"method": "turn/completed", "params": {
                                "threadId": params["threadId"], "turn": {"id": params["turnId"], "status": "interrupted"}}})
                        return {"thread": {"id": "target-thread", "turns": [{"id": "target-turn", "status": read_status}]}}
                    def wait_for_turn_readback(self, *args):
                        return "done"
                    def close(self):
                        if cleanup_failure != "client":
                            self.process.poll.return_value = 0
                target = state / "task-holds" / "a-target"
                healthy = state / "task-holds" / "z-healthy"
                for root, thread_id, turn_id in ((target, "target-thread", "target-turn"), (healthy, "healthy-thread", "healthy-turn")):
                    _write_json(root / "request.json", {"hold_id": root.name, "request_id": root.name,
                        "mode": mode if root == target else "recover", "thread_id": thread_id, "turn_id": turn_id})
                    _write_json(root / "ack.json", {"hold_id": root.name, "request_id": root.name,
                        "status": "accepted", "phase": "queued_for_user_host"})
                adapter = CodexAppServerTaskProvisioningAdapter("unused", state_dir=state, config_loader=lambda _: FakeConfig())
                release = host._release_claim
                def cleanup(root):
                    if root == target and cleanup_failure is True:
                        raise PermissionError("injected cleanup denial")
                    return release(root)
                with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                    "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", Client
                ), patch("adapters.codex_app_server.jarvis_hold_host_service._matching_health", return_value=True), patch.object(host, "_release_claim", side_effect=cleanup):
                    workers = [threading.Thread(target=host.run_once) for _ in range(2)]
                    def phase(path):
                        try:
                            return json.loads(path.read_text()).get("phase")
                        except PermissionError:
                            return None  # Windows may deny a reader during atomic replacement.
                    try:
                        workers[0].start()
                        # Use the host's published active/holding state, not thread liveness, as readiness.
                        import time
                        deadline = time.monotonic() + 3
                        while phase(target / "ack.json") != "turn_holding":
                            self.assertLess(time.monotonic(), deadline)
                            time.sleep(.01)
                        workers[1].start()
                        while len(clients) < 2 or phase(healthy / "ack.json") != "turn_holding":
                            self.assertLess(time.monotonic(), deadline)
                            time.sleep(.01)
                        original = (healthy / "request.json").read_bytes()
                        health = json.loads(host.health_path.read_text())
                        _write_json(host.health_path, {key: value for key, value in health.items() if key != "capabilities"})
                        self.assertEqual(host.request_host_stop("a-target", "target-thread", "target-turn")["status"], "rejected")
                        _write_json(host.health_path, health)
                        rejected = host.request_host_stop("a-target", "wrong-thread", "target-turn")
                        self.assertEqual(rejected["status"], "rejected")
                        self.assertNotIn("host_stop", json.loads((target / "request.json").read_text()))
                        if cleanup_failure == "legacy":
                            adapter.request_hold_stop("a-target")  # Legacy optional-field request.
                        else:
                            accepted = host.request_host_stop("a-target", "target-thread", "target-turn")
                            self.assertEqual(accepted["status"], "stop_requested")
                            self.assertFalse(accepted["hold_released"])
                            host.request_host_stop("a-target", "target-thread", "target-turn")
                        workers[0].join(3)
                        self.assertFalse(workers[0].is_alive())
                        receipt = adapter.hold_status("a-target")
                        self.assertTrue(receipt["holder_exit_confirmed"])
                        self.assertEqual(receipt["holder_client_exit_confirmed"], cleanup_failure != "client")
                        terminal = mode == "create" or read_status in {"completed", "interrupted"}
                        confirmed = terminal and cleanup_failure != "client"
                        self.assertEqual(receipt["hold_released"], confirmed and cleanup_failure is not True)
                        self.assertEqual(receipt["terminal_confirmed"], confirmed)
                        self.assertEqual(json.loads((target / "ack.json").read_text()), json.loads((target / "result.json").read_text()))
                        self.assertEqual(len(clients[0].calls), 1)
                        self.assertTrue(workers[1].is_alive())
                        self.assertEqual(original, (healthy / "request.json").read_bytes())
                        if not confirmed:
                            self.assertEqual(host.reconcile_hold("a-target")["status"], "requires_readback")
                            self.assertTrue((target / ".user-host-claim").exists())
                        print("HOST_AWARE_TEST_RECEIPT", json.dumps({"environment": "isolated_temp_TEST", "mode": mode,
                            "read_status": read_status, "failure": cleanup_failure, "hold_id": "a-target",
                            "thread_id": "target-thread", "turn_id": "target-turn",
                            "holder_exit_confirmed": receipt["holder_exit_confirmed"],
                            "client_exit_confirmed": receipt["holder_client_exit_confirmed"],
                            "terminal_confirmed": receipt["terminal_confirmed"], "hold_released": receipt["hold_released"],
                            "production_recovery": "not_attempted"}))
                        clients[1].notifications.put({"method": "turn/completed", "params": {
                            "threadId": "healthy-thread", "turn": {"id": "healthy-turn", "status": "completed"}}})
                        workers[1].join(3)
                        self.assertFalse(workers[1].is_alive())
                        self.assertTrue(adapter.hold_status("z-healthy")["hold_released"])
                    finally:
                        for client in clients:
                            for thread_id, turn_id in (("target-thread", "target-turn"), ("healthy-thread", "healthy-turn")):
                                client.notifications.put({"method": "turn/completed", "params": {
                                    "threadId": thread_id, "turn": {"id": turn_id, "status": "interrupted"}}})
                        for worker in workers:
                            if worker.ident is not None:
                                worker.join(3)

    def _exception_result(self, temp, *, identity=True):
        from adapters.codex_app_server.jarvis_task_hold_host import _write_json
        state = Path(temp)
        launcher = state / "launcher.json"
        _write_json(launcher, {"profile": "test", "expected_codex_home": str(state / "home")})
        root = state / "task-holds" / "hold-error"
        root.mkdir(parents=True)
        request = {"hold_id": "hold-error", "request_id": "req", "mode": "resume", "thread_id": "thread-exact"}
        result = {"request_id": "req", "hold_id": "hold-error", "status": "failed", "phase": "host_execution_error",
                  "terminal_confirmed": False, "reason": "ack denied", "observed_at": "2026-09-16T06:30:00+00:00"}
        if identity:
            result.update(thread_id="thread-exact", turn_id="turn-exact")
        for name, value in (("request.json", request), ("ack.json", result), ("result.json", result)):
            _write_json(root / name, value)
        _write_json(root / ".user-host-claim" / "owner.json", {"pid": 99999999})
        client = Mock()
        client.request.return_value = {"thread": {"id": "thread-exact", "turns": [{"id": "turn-exact", "status": "completed"}]}}
        return JarvisHoldHost(state_dir=state, launcher_config=launcher), root, client

    def test_single_hold_reconciliation_of_existing_result_is_idempotent_and_never_dispatches(self):
        from adapters.codex_app_server.task_provisioning_adapter import CodexAppServerTaskProvisioningAdapter
        with tempfile.TemporaryDirectory() as temp:
            host, root, client = self._exception_result(temp)
            other = root.parent / "healthy"
            other.mkdir()
            (other / "request.json").write_text('{"hold_id":"healthy","request_id":"other"}', encoding="utf-8")
            (other / "ack.json").write_text('{"status":"holding"}', encoding="utf-8")
            before = [(path.name, path.read_bytes()) for path in other.iterdir()]
            adapter = CodexAppServerTaskProvisioningAdapter("unused", state_dir=Path(temp), config_loader=lambda _: FakeConfig())
            adapter.request_hold_stop("hold-error")
            stopped = (root / "request.json").read_bytes()
            with patch("adapters.codex_app_server.jarvis_hold_host_service._pid_is_alive", side_effect=lambda pid: pid == os.getpid()), patch(
                "adapters.codex_app_server.jarvis_hold_host_service.NativeTaskLauncherConfig", return_value=FakeConfig()
            ), patch("adapters.codex_app_server.jarvis_hold_host_service.AppServerClient", return_value=client):
                receipt = host.reconcile_hold("hold-error")
                self.assertTrue(receipt["hold_released"])
                self.assertTrue(receipt["terminal_confirmed"])
                self.assertEqual(receipt["status"], "failed")
                self.assertEqual(receipt["reason"], "ack denied")
                retried = host.reconcile_hold("hold-error")
                self.assertTrue(retried["hold_released"])
                self.assertNotIn("cleanup_error", retried)
                adapter.request_hold_stop("hold-error")
                self.assertEqual(stopped, (root / "request.json").read_bytes())
            client.request.assert_called_once_with("thread/read", {"threadId": "thread-exact", "includeTurns": True})
            client.create_task.assert_not_called()
            client.resume_turn_async.assert_not_called()
            self.assertEqual(before, [(path.name, path.read_bytes()) for path in other.iterdir()])
            self.assertTrue(adapter.hold_status("hold-error")["hold_released"])

    def test_host_preserves_terminal_result_and_cleanup_failure_does_not_stop_other_seat(self):
        from adapters.codex_app_server.jarvis_task_hold_host import _write_json
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp)
            host = JarvisHoldHost(state_dir=state, launcher_config=state / "unused")
            roots = [state / "task-holds" / name for name in ("a-error", "z-healthy")]
            for root in roots:
                _write_json(root / "request.json", {"request_id": root.name, "hold_id": root.name})
                _write_json(root / "ack.json", {"request_id": root.name, "status": "accepted", "phase": "queued_for_user_host"})
            def finish(_config, request_path, ack, result):
                request = json.loads(request_path.read_text())
                _write_json(result, {**request, "status": "completed", "terminal_confirmed": True,
                                     "thread_id": "t", "turn_id": "u", "phase": "turn_terminal"})
                if request_path.parent == roots[0]:
                    raise PermissionError("ack replace denied after terminal result")
            release = host._release_claim
            def cleanup(root):
                if root == roots[0]:
                    raise PermissionError("claim cleanup denied")
                release(root)
            with patch("adapters.codex_app_server.jarvis_hold_host_service.hold_task", side_effect=finish), patch.object(
                host, "_release_claim", side_effect=cleanup
            ):
                self.assertTrue(host.run_once())
                self.assertTrue(host.run_once())
            failed = json.loads((roots[0] / "result.json").read_text())
            self.assertEqual(failed["status"], "completed")
            self.assertTrue(failed["terminal_confirmed"])
            self.assertEqual(failed["turn_id"], "u")
            self.assertIn("cleanup denied", failed["cleanup_error"])
            self.assertTrue((roots[1] / "result.json").exists())
            self.assertEqual(json.loads(host.health_path.read_text())["active_count"], 0)

    def test_live_shared_owner_only_allows_inactive_hold_with_fresh_matching_health(self):
        from adapters.codex_app_server.jarvis_task_hold_host import _write_json
        with tempfile.TemporaryDirectory() as temp:
            host, root, client = self._exception_result(temp)
            _write_json(host.health_path, {"pid": 99999999, "active_hold_ids": ["healthy"],
                                          "observed_at": "2026-09-16T06:31:00+00:00"})
            with patch("adapters.codex_app_server.jarvis_hold_host_service._pid_is_alive", return_value=True), patch(
                "adapters.codex_app_server.jarvis_hold_host_service._matching_health", return_value=True
            ), patch("adapters.codex_app_server.jarvis_hold_host_service.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                "adapters.codex_app_server.jarvis_hold_host_service.AppServerClient", return_value=client
            ):
                self.assertTrue(host.reconcile_hold("hold-error")["hold_released"])

    def test_cleanup_failure_is_observable_and_retryable_even_after_owner_removed(self):
        with tempfile.TemporaryDirectory() as temp:
            host, root, client = self._exception_result(temp)
            def partial_cleanup(_root):
                (root / ".user-host-claim" / "owner.json").unlink()
                raise PermissionError("injected claim rmdir denied")
            with patch("adapters.codex_app_server.jarvis_hold_host_service._pid_is_alive", side_effect=lambda pid: pid == os.getpid()), patch(
                "adapters.codex_app_server.jarvis_hold_host_service.NativeTaskLauncherConfig", return_value=FakeConfig()
            ), patch("adapters.codex_app_server.jarvis_hold_host_service.AppServerClient", return_value=client):
                with patch.object(host, "_release_claim", side_effect=partial_cleanup):
                    receipt = host.reconcile_hold("hold-error")
                self.assertFalse(receipt["hold_released"])
                self.assertTrue(receipt["terminal_confirmed"])
                self.assertIn("denied", receipt["cleanup_error"])
                retried = host.reconcile_hold("hold-error")
                self.assertTrue(retried["hold_released"])
                self.assertNotIn("cleanup_error", retried)

    def test_old_missing_identity_and_nonterminal_or_wrong_readback_remain_unknown(self):
        for case in ("missing_identity", "pending_continuation", "running", "wrong_thread", "wrong_turn", "request_mismatch", "owner_active"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temp:
                host, root, client = self._exception_result(temp, identity=case != "missing_identity")
                if case == "running":
                    client.request.return_value["thread"]["turns"][0]["status"] = "inProgress"
                elif case == "wrong_thread":
                    client.request.return_value["thread"]["id"] = "other"
                elif case == "wrong_turn":
                    client.request.return_value["thread"]["turns"][0]["id"] = "latest-not-owned"
                elif case == "request_mismatch":
                    result = json.loads((root / "result.json").read_text())
                    result["request_id"] = "old-request"
                    (root / "result.json").write_text(json.dumps(result))
                elif case == "pending_continuation":
                    result = json.loads((root / "result.json").read_text())
                    result["turn_id"] = ""
                    (root / "result.json").write_text(json.dumps(result))
                    request = json.loads((root / "request.json").read_text())
                    request.update(mode="recover", turn_id="turn-exact")
                    (root / "request.json").write_text(json.dumps(request))
                before = [(path.name, path.read_bytes()) for path in root.iterdir() if path.is_file()]
                with patch("adapters.codex_app_server.jarvis_hold_host_service._pid_is_alive", side_effect=lambda pid: case == "owner_active" or pid == os.getpid()), patch(
                    "adapters.codex_app_server.jarvis_hold_host_service.NativeTaskLauncherConfig", return_value=FakeConfig()
                ), patch("adapters.codex_app_server.jarvis_hold_host_service.AppServerClient", return_value=client):
                    receipt = host.reconcile_hold("hold-error")
                self.assertEqual(receipt["status"], "requires_readback")
                self.assertFalse(receipt["hold_released"])
                self.assertFalse(receipt["terminal_confirmed"])
                self.assertTrue((root / ".user-host-claim").exists())
                self.assertEqual(before, [(path.name, path.read_bytes()) for path in root.iterdir() if path.is_file()])

    def test_temp_cleanup_denial_cannot_mask_committed_receipt(self):
        from adapters.codex_app_server.jarvis_task_hold_host import _write_json
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp) / "ack.json"
            with patch.object(Path, "unlink", side_effect=PermissionError("temporary cleanup denied")):
                _write_json(target, {"test": 3})
            self.assertEqual(json.loads(target.read_text()), {"test": 3})

    def test_ack_failure_after_turn_started_preserves_exact_identity_in_result(self):
        from adapters.codex_app_server.jarvis_task_hold_host import _write_json
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            request, ack, result = (root / name for name in ("request.json", "ack.json", "result.json"))
            _write_json(request, {"request_id": "r", "hold_id": "h", "mode": "resume", "thread_id": "exact-thread"})
            client = RecoveryClient(None)
            def started(*args, **kwargs):
                kwargs["on_phase"]("turn_started", {"thread_id": "exact-thread", "turn_id": "exact-turn"})
                return {"turn_id": "exact-turn"}
            client.resume_turn_async = started
            def denied(path, value):
                if path == ack and value.get("phase") == "turn_started":
                    raise PermissionError("[WinError 5] injected ack replace denied")
                _write_json(path, value)
            with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=client
            ), patch("adapters.codex_app_server.jarvis_task_hold_host._write_json", side_effect=denied):
                try:
                    hold_task(root / "unused", request, ack, result)
                except PermissionError:
                    pass
            saved = json.loads(result.read_text(encoding="utf-8"))
            self.assertEqual((saved.get("thread_id"), saved.get("turn_id")), ("exact-thread", "exact-turn"))
            self.assertFalse(saved["terminal_confirmed"])

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
