from __future__ import annotations

import json
import io
import os
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from unittest.mock import ANY

from adapters.codex_app_server.jarvis_hold_host_service import JarvisHoldHost, _pid_is_alive, ensure_hold_host, initialize_user_host, main
from jarvis_native_task_launcher import HostContextRequiredError
from adapters.codex_app_server.jarvis_task_hold_host import hold_task


class FakeConfig:
    pass


class FakeClient:
    def __init__(self, _config):
        self.started_prompts: list[str] = []
        self.started_turns: list[dict] = []
        self.created_requests: list[dict] = []
        self.waited: list[str] = []
        self.closed = False

    def create_task(self, request, **_kwargs):
        self.created_requests.append(request)
        return {"thread_id": "thread-1", "turn_id": "turn-1", "model": "gpt-test"}

    def wait_for_turn_started(self, _thread_id, turn_id):
        return {"id": turn_id, "status": "inProgress"}

    def wait_for_turn_terminal(self, _thread_id, turn_id, **_kwargs):
        self.waited.append(turn_id)
        return {"id": turn_id, "status": "completed"}

    def wait_for_turn_readback(self, _thread_id, _turn_id):
        return "done"

    def start_turn_async(self, _thread_id, prompt, **_kwargs):
        self.started_prompts.append(prompt)
        self.started_turns.append(_kwargs)
        return {"turn_id": "turn-2", "model": "gpt-test"}

    def resume_turn_async(self, thread_id, prompt, **kwargs):
        self.resumed = (thread_id, prompt, kwargs)
        return {"turn_id": "turn-resumed-1", "model": "gpt-test"}

    def close(self):
        self.closed = True


class TerminalGateClient(FakeClient):
    def __init__(self, config):
        super().__init__(config)
        self.terminal_waiting = threading.Event()
        self.allow_terminal = threading.Event()

    def wait_for_turn_terminal(self, _thread_id, turn_id, **_kwargs):
        self.terminal_waiting.set()
        if not self.allow_terminal.wait(timeout=1):
            raise AssertionError("test did not release turn/completed")
        return {"id": turn_id, "status": "completed"}


class EmptyReadbackClient(FakeClient):
    def wait_for_turn_readback(self, _thread_id, _turn_id):
        return ""


class HostContextFailureClient(FakeClient):
    def create_task(self, _request, *, on_phase, **_kwargs):
        on_phase("app_server_initializing", {})
        raise HostContextRequiredError("JARVIS_HOST_CONTEXT_REQUIRED: test failure")


class TaskMonitorHostTest(unittest.TestCase):
    def test_reports_holding_before_the_exact_turn_reaches_a_terminal_state(self):
        client = TerminalGateClient(None)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            request = root / "request.json"
            ack = root / "ack.json"
            result = root / "result.json"
            request.write_text(json.dumps({
                "request_id": "create-startup-1", "prompt": "hello", "max_turns": 1,
            }), encoding="utf-8")
            with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=client
            ):
                worker = threading.Thread(
                    target=hold_task, args=(Path("launcher.json"), request, ack, result), daemon=True
                )
                worker.start()
                self.assertTrue(client.terminal_waiting.wait(timeout=1))
                holding = json.loads(ack.read_text(encoding="utf-8"))
                self.assertEqual(holding["status"], "holding")
                self.assertEqual(holding["phase"], "turn_holding")
                self.assertIn("pid", holding)
                client.allow_terminal.set()
                worker.join(timeout=1)

            self.assertFalse(worker.is_alive())
            self.assertEqual(json.loads(result.read_text(encoding="utf-8"))["status"], "turn_limit_reached")

    def test_stops_at_max_turns_after_resuming_with_the_short_default_prompt(self):
        client = FakeClient(None)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            request = root / "request.json"
            ack = root / "ack.json"
            result = root / "result.json"
            request.write_text(json.dumps({
                "request_id": "create-1",
                "prompt": "hello",
                "max_turns": 2,
                "auto_continue": True,
                "continue_prompt": "继续",
                "input_binding": {"candidate_ids": [7]},
            }), encoding="utf-8")
            with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=client
            ):
                exit_code = hold_task(Path("launcher.json"), request, ack, result)

            final = json.loads(result.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertEqual(client.started_prompts, ["继续"])
        self.assertEqual(client.started_turns, [{"client_user_message_id": "monitor:hold-create-1:turn-1:1", "model": None, "reasoning_effort": None, "input_binding": {"candidate_ids": [7]}, "on_phase": ANY}])
        self.assertEqual(client.waited, ["turn-1", "turn-2"])
        self.assertEqual(final["status"], "turn_limit_reached")
        self.assertEqual(final["turn_count"], 2)
        self.assertEqual(final["session_turn_count"], 2)
        self.assertEqual(final["total_turn_count"], 2)
        self.assertTrue(client.closed)

    def test_holder_advances_a_private_lane_one_candidate_per_monitor_continue(self):
        client = FakeClient(None)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            request, ack, result = root / "request.json", root / "ack.json", root / "result.json"
            request.write_text(json.dumps({
                "request_id": "lane-continue", "prompt": "hello", "max_turns": 2,
                "auto_continue": True, "continue_prompt": "继续",
                "input_binding": {
                    "candidate_ids": [7, 9], "database_path": "C:/collection.sqlite",
                    "output_boundary": "C:/outputs", "lane_identity": "worker-1", "lane_item_count": 2,
                },
            }), encoding="utf-8")
            with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=client
            ):
                exit_code = hold_task(Path("launcher.json"), request, ack, result)

        self.assertEqual(exit_code, 0)
        self.assertEqual(client.created_requests[0]["input_binding"]["candidate_ids"], [7])
        self.assertEqual(client.started_turns[0]["input_binding"]["candidate_ids"], [9])
        self.assertEqual(client.started_turns[0]["input_binding"]["lane_identity"], "worker-1")

    def test_empty_turn_readback_does_not_block_holder_continuation(self):
        client = EmptyReadbackClient(None)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            request, ack, result = root / "request.json", root / "ack.json", root / "result.json"
            request.write_text(json.dumps({
                "request_id": "empty-readback", "prompt": "hello", "max_turns": 2,
                "auto_continue": True, "continue_prompt": "继续",
            }), encoding="utf-8")
            with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=client
            ):
                exit_code = hold_task(Path("launcher.json"), request, ack, result)

            final = json.loads(result.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertEqual(client.started_prompts, ["继续"])
        self.assertEqual(final["status"], "turn_limit_reached")
        self.assertEqual(final["final_message"], "")

    def test_reports_a_stable_host_context_error_with_the_last_phase(self):
        client = HostContextFailureClient(None)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            request = root / "request.json"
            ack = root / "ack.json"
            result = root / "result.json"
            request.write_text(json.dumps({"request_id": "create-host-error", "prompt": "hello"}), encoding="utf-8")
            with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=client
            ):
                exit_code = hold_task(Path("launcher.json"), request, ack, result)

            failure = json.loads(result.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 1)
        self.assertEqual(failure["error_code"], "JARVIS_HOST_CONTEXT_REQUIRED")
        self.assertEqual(failure["phase"], "app_server_initializing")

    def test_resume_passes_the_persisted_thread_and_prompt_to_the_client_recovery_path(self):
        client = FakeClient(None)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            request = root / "request.json"
            ack = root / "ack.json"
            result = root / "result.json"
            request.write_text(json.dumps({
                "mode": "resume",
                "request_id": "resume-1",
                "thread_id": "thread-created-1",
                "prompt": "继续",
                "max_turns": 1,
                "input_binding": {"candidate_ids": [7]},
            }), encoding="utf-8")
            with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=client
            ):
                exit_code = hold_task(Path("launcher.json"), request, ack, result)

        self.assertEqual(exit_code, 0)
        self.assertEqual(client.resumed[0], "thread-created-1")
        self.assertEqual(client.resumed[1], "继续")
        self.assertEqual(client.resumed[2]["client_user_message_id"], "resume-1")
        self.assertEqual(client.resumed[2]["input_binding"], {"candidate_ids": [7]})

    def test_normal_user_host_claims_a_queued_request(self):
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            launcher_config = state_dir / "launcher.json"
            launcher_config.write_text(json.dumps({
                "profile": "jarvis_test",
                "expected_codex_home": "C:/test/codex-home",
            }), encoding="utf-8")
            root = state_dir / "task-monitors" / "monitor-create-1"
            root.mkdir(parents=True)
            (root / "request.json").write_text(json.dumps({"request_id": "create-1"}), encoding="utf-8")
            (root / "ack.json").write_text(json.dumps({
                "request_id": "create-1", "status": "accepted", "phase": "queued_for_user_host",
            }), encoding="utf-8")

            def fake_hold(_config, _request, _ack, result):
                result.write_text(json.dumps({"status": "completed"}), encoding="utf-8")
                return 0

            with patch("adapters.codex_app_server.jarvis_hold_host_service.hold_task", side_effect=fake_hold) as runner:
                handled = JarvisHoldHost(
                    state_dir=state_dir, launcher_config=launcher_config
                ).run_once()

            health = json.loads((state_dir / "hold-host.json").read_text(encoding="utf-8"))

        self.assertTrue(handled)
        runner.assert_called_once()
        self.assertEqual(health["status"], "ready")
        self.assertEqual(health["profile"], "jarvis_test")
        self.assertEqual(health["codex_home"], "C:/test/codex-home")
        self.assertEqual(health["state_dir"], str(state_dir.resolve()))

    def test_second_host_cannot_claim_the_same_accepted_hold(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "task-holds" / "hold-1"
            root.mkdir(parents=True)
            ack_path = root / "ack.json"
            ack = {"request_id": "hold-1", "status": "accepted", "phase": "queued_for_user_host"}
            ack_path.write_text(json.dumps(ack), encoding="utf-8")

            self.assertTrue(JarvisHoldHost._claim(root, ack_path, ack))
            self.assertFalse(JarvisHoldHost._claim(root, ack_path, ack))
            JarvisHoldHost._release_claim(root)
            self.assertTrue(JarvisHoldHost._claim(root, ack_path, ack))

    def test_ensure_hold_host_reuses_a_matching_fresh_host(self):
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            launcher_config = state_dir / "launcher.json"
            launcher_config.write_text(json.dumps({
                "profile": "jarvis_test",
                "expected_codex_home": "C:/test/codex-home",
            }), encoding="utf-8")
            (state_dir / "hold-host.json").write_text(json.dumps({
                "status": "ready", "pid": 1780,
                "observed_at": "2026-09-01T00:00:00+00:00",
                "profile": "jarvis_test", "codex_home": "C:/test/codex-home",
                "state_dir": str(state_dir.resolve()),
            }), encoding="utf-8")
            start_host = Mock()

            receipt = ensure_hold_host(
                state_dir=state_dir, launcher_config=launcher_config,
                start_host=start_host, now=lambda: "2026-09-01T00:00:10+00:00", pid_alive=lambda _: True,
            )

        self.assertEqual(receipt, {"status": "ready", "phase": "already_running"})
        start_host.assert_not_called()

    def test_ensure_hold_host_starts_once_and_waits_for_matching_health(self):
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            launcher_config = state_dir / "launcher.json"
            launcher_config.write_text(json.dumps({
                "profile": "jarvis_test",
                "expected_codex_home": "C:/test/codex-home",
            }), encoding="utf-8")
            (state_dir / "hold-host.json").write_text(json.dumps({
                "status": "ready", "pid": 1780,
                "observed_at": "2026-08-31T00:00:00+00:00",
                "profile": "jarvis_test", "codex_home": "C:/test/codex-home",
                "state_dir": str(state_dir.resolve()),
            }), encoding="utf-8")

            def start_host():
                (state_dir / "hold-host.json").write_text(json.dumps({
                    "status": "ready", "pid": 1781,
                    "observed_at": "2026-09-01T00:00:10+00:00",
                    "profile": "jarvis_test", "codex_home": "C:/test/codex-home",
                    "state_dir": str(state_dir.resolve()),
                }), encoding="utf-8")

            receipt = ensure_hold_host(
                state_dir=state_dir, launcher_config=launcher_config,
                start_host=start_host, wait_seconds=1, poll_seconds=0,
                now=lambda: "2026-09-01T00:00:10+00:00", pid_alive=lambda _: True,
            )

        self.assertEqual(receipt, {"status": "ready", "phase": "started"})

    def test_initialize_user_host_creates_the_test_state_roots_before_starting(self):
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp) / "new-state"
            launcher_config = Path(temp) / "launcher.json"
            launcher_config.write_text(json.dumps({
                "profile": "jarvis_test",
                "expected_codex_home": "C:/test/codex-home",
            }), encoding="utf-8")

            def start_host():
                (state_dir / "hold-host.json").write_text(json.dumps({
                    "status": "ready", "pid": 1781,
                    "observed_at": "2026-09-01T00:00:10+00:00",
                    "profile": "jarvis_test", "codex_home": "C:/test/codex-home",
                    "state_dir": str(state_dir.resolve()),
                }), encoding="utf-8")

            receipt = initialize_user_host(
                state_dir=state_dir, launcher_config=launcher_config,
                start_host=start_host, poll_seconds=0,
                now=lambda: "2026-09-01T00:00:10+00:00", pid_alive=lambda _: True,
            )

            roots_exist = (state_dir / "task-holds").is_dir() and (state_dir / "task-monitors").is_dir()

        self.assertEqual(receipt, {"status": "ready", "phase": "started"})
        self.assertTrue(roots_exist)

    def test_initialize_user_host_restarts_when_a_fresh_health_pid_is_dead(self):
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            launcher_config = state_dir / "launcher.json"
            launcher_config.write_text(json.dumps({
                "profile": "jarvis_test", "expected_codex_home": "C:/test/codex-home",
            }), encoding="utf-8")
            (state_dir / "hold-host.json").write_text(json.dumps({
                "status": "ready", "pid": 1780,
                "observed_at": "2026-09-01T00:00:00+00:00",
                "profile": "jarvis_test", "codex_home": "C:/test/codex-home",
                "state_dir": str(state_dir.resolve()),
            }), encoding="utf-8")

            def start_host():
                (state_dir / "hold-host.json").write_text(json.dumps({
                    "status": "ready", "pid": 1781,
                    "observed_at": "2026-09-01T00:00:10+00:00",
                    "profile": "jarvis_test", "codex_home": "C:/test/codex-home",
                    "state_dir": str(state_dir.resolve()),
                }), encoding="utf-8")

            receipt = initialize_user_host(
                state_dir=state_dir, launcher_config=launcher_config, start_host=start_host,
                poll_seconds=0, now=lambda: "2026-09-01T00:00:10+00:00",
                pid_alive=lambda pid: pid == 1781,
            )

        self.assertEqual(receipt, {"status": "ready", "phase": "started"})

    def test_pid_liveness_accepts_the_current_process_and_rejects_an_unknown_pid(self):
        self.assertTrue(_pid_is_alive(os.getpid()))
        self.assertFalse(_pid_is_alive(999999))

    def test_ensure_hold_host_does_not_start_a_second_host_while_bootstrap_is_locked(self):
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            launcher_config = state_dir / "launcher.json"
            launcher_config.write_text(json.dumps({
                "profile": "jarvis_test",
                "expected_codex_home": "C:/test/codex-home",
            }), encoding="utf-8")
            (state_dir / "hold-host-bootstrap.lock").mkdir()
            start_host = Mock()

            receipt = ensure_hold_host(
                state_dir=state_dir, launcher_config=launcher_config,
                start_host=start_host, now=lambda: "2026-09-01T00:00:10+00:00",
            )

        self.assertEqual(receipt, {"status": "blocked", "reason": "HoldHost bootstrap is already in progress"})
        start_host.assert_not_called()

    def test_ensure_running_cli_returns_the_bootstrap_readback(self):
        with patch.object(sys, "argv", [
            "jarvis_hold_host_service", "--initialize-user-host", "--state-dir", "C:/test/state",
            "--launcher-config", "C:/test/launcher.json",
        ]), patch(
            "adapters.codex_app_server.jarvis_hold_host_service.initialize_user_host",
            return_value={"status": "ready", "phase": "started"},
        ) as ensure, patch("sys.stdout", new_callable=io.StringIO) as stdout:
            exit_code = main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(json.loads(stdout.getvalue()), {"status": "ready", "phase": "started"})
        ensure.assert_called_once()


if __name__ == "__main__":
    unittest.main()
