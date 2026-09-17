from __future__ import annotations

import json
import io
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from unittest.mock import ANY

from adapters.codex_app_server.jarvis_hold_host_service import JarvisHoldHost, _pid_is_alive, _start_hold_host, ensure_hold_host, initialize_user_host, main
from adapters.codex_app_server.task_provisioning_adapter import read_turn_history
from jarvis_native_task_launcher import HostContextRequiredError
from adapters.codex_app_server.jarvis_task_hold_host import hold_task


class FakeConfig:
    pass


class ReliabilityRegressionTest(unittest.TestCase):
    def test_review_is_durable_before_next_turn_and_does_not_leak_into_unknown_turn(self):
        # Actual missing-receipt failures must progress, but an unknown following execution must not.
        for history_failure in (False, True):
            with self.subTest(history_failure=history_failure), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                request, ack, result = (root / name for name in ("request.json", "ack.json", "result.json"))
                request.write_text(json.dumps({"request_id": "review-test", "hold_id": "review-hold",
                    "prompt": "test", "auto_continue": True, "max_turns": 3,
                    "input_binding": {"candidate_ids": [7, 9, 11], "lane_item_count": 3,
                        "output_boundary": str(root), "result_verification": {
                            "receipt_paths": {str(i): f"{i}.json" for i in (7, 9, 11)},
                            "terminal_statuses": ["completed"]}}}), encoding="utf-8")
                client = FakeClient(None)
                def start_next(thread, prompt, **kwargs):
                    rows = read_turn_history(root / "turn-history.sqlite", hold_id="review-hold")
                    self.assertEqual(len(rows), 1)
                    self.assertEqual(rows[0]["status"], "failed")
                    self.assertEqual(rows[0]["output_verification"]["candidate_id"], 7)
                    client.started_turns.append(kwargs)
                    return {"turn_id": "turn-2"}
                client.start_turn_async = start_next
                client.wait_for_turn_terminal = lambda thread, turn, **kwargs: {
                    "status": "completed" if turn == "turn-1" else "unknown"}
                from contextlib import nullcontext
                failure = patch("adapters.codex_app_server.jarvis_task_hold_host.append_terminal_turn_history",
                    side_effect=OSError("history unavailable")) if history_failure else nullcontext()
                with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                    "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=client), failure:
                    self.assertEqual(hold_task(root / "unused", request, ack, result), 1 if history_failure else 0)
                final = json.loads(result.read_text(encoding="utf-8"))
                self.assertEqual(len(client.started_turns), 0 if history_failure else 1)
                if not history_failure:
                    self.assertFalse(final["terminal_confirmed"])
                    self.assertEqual(final["output_verification"], {"status": "not_checked"})
                    self.assertEqual(final["scheduler_failed_items"], 1)
                    self.assertEqual(len(read_turn_history(root / "turn-history.sqlite", hold_id="review-hold")), 1)

    def test_same_hold_resume_cannot_skip_an_unverified_bound_candidate(self):
        from adapters.codex_app_server.task_provisioning_adapter import CodexAppServerTaskProvisioningAdapter
        from jarvis_control.provisioning import TaskMonitorResumeRequest
        for case in ("missing", "omitted_binding", "forged_summary", "valid", "schema_invalid_output", "schema_forged_summary", "schema_resume_omit", "schema_resume_downgrade"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                hold_root = root / "task-holds" / "hold-test"
                hold_root.mkdir(parents=True)
                binding = {"candidate_ids": [7, 9], "lane_item_count": 2, "output_boundary": str(root),
                           "result_verification": {"receipt_paths": {"7": "7.receipt.json", "9": "9.receipt.json"},
                                                   "terminal_statuses": ["completed"]}}
                request, ack, result = (hold_root / name for name in ("request.json", "ack.json", "result.json"))
                request.write_text(json.dumps({"request_id": "old-run", "hold_id": "hold-test", "mode": "resume",
                    "thread_id": "thread-1", "prompt": "test", "max_turns": 1, "input_binding": binding}), encoding="utf-8")
                if case.startswith("schema_"):
                    binding["result_verification"]["output_schema"] = {"properties": {"score": {"type": "integer"}}, "required": ["score"]}
                    saved = json.loads(request.read_text(encoding="utf-8")); saved["input_binding"] = binding
                    request.write_text(json.dumps(saved), encoding="utf-8")
                if case == "valid" or case.startswith("schema_"):
                    data = {"request_id": "old-run", "candidate_id": 7, "turn_number": 1, "status": "completed"}
                    if case.startswith("schema_"):
                        data["score"] = "bad" if case in {"schema_invalid_output", "schema_forged_summary"} else 1
                    (root / "7.output.json").write_text(json.dumps(data), encoding="utf-8")
                    (root / "7.receipt.json").write_text(json.dumps({**data, "output_path": "7.output.json"}), encoding="utf-8")
                with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                    "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=FakeClient(None),
                ):
                    self.assertEqual(hold_task(root / "unused", request, ack, result), 0)
                before = json.loads(result.read_text(encoding="utf-8"))
                # New contract: known completed execution consumes the item even if business output needs review.
                self.assertEqual(before["status"], "turn_limit_reached")
                if case in {"forged_summary", "schema_forged_summary"}:
                    before["output_verification"] = {"status": "verified"}
                    result.write_text(json.dumps(before), encoding="utf-8")
                snapshot = [path.read_bytes() for path in (request, ack, result)]
                adapter = CodexAppServerTaskProvisioningAdapter("unused", state_dir=root, config_loader=lambda _: FakeConfig())
                incoming = None if case in {"omitted_binding", "schema_resume_omit", "schema_forged_summary"} else json.loads(json.dumps(binding))
                if case == "schema_resume_downgrade":
                    incoming["result_verification"].pop("output_schema")
                resumed = adapter.resume_with_monitor(TaskMonitorResumeRequest(
                    request_id="new-run", task_id="thread-1", prompt="resume", source_ref="test", hold_id="hold-test",
                    input_binding=incoming))
                if case in {"valid", "schema_resume_omit"}:
                    if case == "schema_resume_omit":
                        self.assertEqual(json.loads(request.read_text(encoding="utf-8"))["input_binding"], binding)
                    self.assertEqual(resumed.status, "accepted")
                    self.assertEqual(resumed.total_turn_count, 2)
                else:
                    self.assertEqual(resumed.status, "failed")
                    self.assertIn("binding cannot change" if case == "schema_resume_downgrade" else "current candidate is unverified", resumed.reason)
                    self.assertEqual(snapshot, [path.read_bytes() for path in (request, ack, result)])
                    self.assertFalse((hold_root / "history").exists())
                    self.assertEqual(before["total_turn_count"], 1)

    @unittest.skipUnless(os.name == "nt", "Windows detached Host contract")
    def test_windows_detached_host_uses_space_paths_and_drains_a_stopped_test_hold(self):
        import signal
        from adapters.codex_app_server.jarvis_hold_host_service import _pid_started_at, _start_hold_host
        with tempfile.TemporaryDirectory(prefix="Jarvis Loop Test ' $ ") as temp:
            root = Path(temp)
            launcher = root / "launcher config.json"
            launcher.write_text(json.dumps({"profile": "jarvis_offline_test", "expected_codex_home": str(root / "empty home")}), encoding="utf-8")
            hold_root = root / "task-holds" / "hold-offline"
            hold_root.mkdir(parents=True)
            (hold_root / "request.json").write_text(json.dumps({
                "request_id": "offline", "hold_id": "hold-offline", "stop_requested": True,
            }), encoding="utf-8")
            (hold_root / "ack.json").write_text(json.dumps({"status": "accepted", "phase": "queued_for_user_host"}), encoding="utf-8")
            health = None
            process_identity = None
            launchers = []
            def start(**kwargs):
                process = _start_hold_host(**kwargs)
                launchers.append(process)
                return process
            try:
                with patch("adapters.codex_app_server.jarvis_hold_host_service._start_hold_host", side_effect=start):
                    receipt = initialize_user_host(state_dir=root, launcher_config=launcher, workers=3, poll_seconds=0.25, wait_seconds=10)
                self.assertEqual(receipt["status"], "ready")
                health = json.loads((root / "hold-host.json").read_text(encoding="utf-8"))
                process_identity = _pid_started_at(health["pid"])
                self.assertEqual(Path(health["state_dir"]), root.resolve())
                self.assertEqual(health["profile"], "jarvis_offline_test")
                self.assertEqual(health["worker_capacity"], 3)
                deadline = time.monotonic() + 5
                while not (hold_root / "result.json").exists() or (hold_root / ".user-host-claim").exists():
                    if time.monotonic() >= deadline:
                        self.fail("isolated stopped Hold did not drain: " + json.dumps({
                            name: (hold_root / name).read_text(encoding="utf-8") if (hold_root / name).is_file() else None
                            for name in ("request.json", "ack.json", "result.json", ".user-host-claim/owner.json")
                        }, ensure_ascii=False))
                    time.sleep(0.05)
                final = json.loads((hold_root / "result.json").read_text(encoding="utf-8"))
                self.assertEqual(final["status"], "cancelled")
                self.assertEqual(final["total_turn_count"], 0)
                self.assertTrue(final["terminal_confirmed"])
                self.assertFalse((root / "turn-history.sqlite").exists())
            finally:
                # This test created the only process allowed to use this fresh temporary state root.
                if health is None and (root / "hold-host.json").exists():
                    health = json.loads((root / "hold-host.json").read_text(encoding="utf-8"))
                    process_identity = _pid_started_at(health["pid"])
                if health is not None and Path(health["state_dir"]) == root.resolve() and process_identity is not None:
                    self.assertEqual(_pid_started_at(health["pid"]), process_identity)
                    os.kill(health["pid"], signal.SIGTERM)
                    deadline = time.monotonic() + 5
                    while _pid_is_alive(health["pid"]) and time.monotonic() < deadline:
                        time.sleep(0.05)
                    self.assertFalse(_pid_is_alive(health["pid"]))
                for process in launchers:
                    self.assertEqual(process.wait(timeout=5), 0)
            print("ISOLATED_HOST_RECEIPT " + json.dumps({"host_start": receipt, "host_health": health,
                  "hold_result": final, "claim_released": True, "test_process_exit_confirmed": True,
                  "app_server_turns_started": 0, "production_published": False}))

    def test_saved_output_gate_covers_wrong_missing_old_and_last_candidate(self):
        cases = ["valid", "allowed_exception", "wrong_receipt", "wrong_output", "old_request",
                 "old_turn", "missing_receipt", "missing_output", "outside_receipt", "outside_output",
                 "nonterminal", "malformed", "last_missing", "schema_valid", "schema_first_bad", "schema_last_bad", "schema_invalid"]
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                output_root = root / "outputs"
                output_root.mkdir()
                request, ack, result = (root / name for name in ("request.json", "ack.json", "result.json"))
                contract = {"receipt_paths": {"7": "7.receipt.json", "9": "9.receipt.json"},
                            "terminal_statuses": ["completed", "excluded"]}
                if case.startswith("schema_"):
                    contract["output_schema"] = {"properties": {"score": {"type": "integer"}}, "required": ["score"]}
                    if case == "schema_invalid":
                        contract["output_schema"] = {"type": "invalid"}
                request_data = {"request_id": "gate-test", "hold_id": "gate-hold", "prompt": "test",
                                "auto_continue": True, "max_turns": 2,
                                "input_binding": {"candidate_ids": [7, 9], "lane_item_count": 2,
                                                  "output_boundary": str(output_root), "result_verification": contract}}
                if case == "outside_receipt":
                    contract["receipt_paths"]["7"] = str(root / "outside.json")
                request.write_text(json.dumps(request_data), encoding="utf-8")
                client = FakeClient(None)

                def readback(_thread_id, turn_id):
                    turn_number = 1 if turn_id == "turn-1" else 2
                    candidate = 7 if turn_number == 1 else 9
                    data = {"request_id": "gate-test", "candidate_id": candidate, "turn_number": turn_number,
                            "status": "excluded" if case == "allowed_exception" else "completed"}
                    if case.startswith("schema_"):
                        data["score"] = "bad" if case == "schema_first_bad" or (case == "schema_last_bad" and turn_number == 2) else 3
                    receipt = {**data, "output_path": f"{candidate}.output.json"}
                    if case == "wrong_receipt":
                        receipt["candidate_id"] = 100
                    if case == "wrong_output":
                        data["candidate_id"] = 100
                    if case == "old_request":
                        data["request_id"] = receipt["request_id"] = "old-run"
                    if case == "old_turn":
                        data["turn_number"] = receipt["turn_number"] = 999
                    if case == "nonterminal":
                        data["status"] = receipt["status"] = "running"
                    if case == "outside_output":
                        receipt["output_path"] = str(root / "outside.json")
                    artifact = output_root / f"{candidate}.output.json"
                    if case != "missing_output":
                        artifact.write_text(json.dumps(data), encoding="utf-8")
                    (root / "outside.json").write_text(json.dumps(data), encoding="utf-8")
                    if case != "missing_receipt" and not (case == "last_missing" and turn_number == 2):
                        (output_root / f"{candidate}.receipt.json").write_text(
                            "broken" if case == "malformed" else json.dumps(receipt), encoding="utf-8")
                    return "done"

                client.wait_for_turn_readback = readback
                with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                    "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=client,
                ):
                    self.assertEqual(hold_task(root / "launcher.json", request, ack, result), 0)
                final = json.loads(result.read_text(encoding="utf-8"))
                valid = case in {"valid", "allowed_exception", "schema_valid"}
                hard = case in {"wrong_receipt", "wrong_output", "outside_receipt", "outside_output"}
                self.assertEqual(final["status"], "blocked" if hard else "turn_limit_reached")
                self.assertEqual(len(client.started_turns), 0 if hard else 1)
                self.assertEqual(final["total_turn_count"], 1 if hard else 2)
                self.assertEqual(final["output_verification"]["status"], "verified" if valid else "blocked" if hard else "review")
                if not valid and not hard:
                    history = read_turn_history(root / "turn-history.sqlite", hold_id="gate-hold")
                    self.assertTrue(any(row["status"] == "failed" for row in history))
                    self.assertTrue(final["scheduler_failed_items"])
                self.assertEqual(json.loads(request.read_text(encoding="utf-8")), request_data)
                if case in {"schema_first_bad", "schema_last_bad"}:
                    self.assertIn("/score", final["output_verification"]["reason"])
                if valid:
                    if case == "schema_valid":
                        self.assertEqual(client.started_turns[0]["input_binding"]["result_verification"]["output_schema"], contract["output_schema"])
                    binding = client.started_turns[0]["input_binding"]
                    self.assertEqual(binding["candidate_ids"], [9])
                    self.assertEqual(binding["result_verification"]["request_id"], "gate-test")
                    self.assertEqual(binding["result_verification"]["turn_number"], 2)
                    self.assertNotIn("receipt_paths", binding["result_verification"])

    def test_stopped_queued_hold_never_constructs_a_client(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            request, ack, result = (root / name for name in ("request.json", "ack.json", "result.json"))
            request.write_text(json.dumps({"request_id": "stopped", "stop_requested": True}), encoding="utf-8")
            with patch("adapters.codex_app_server.jarvis_task_hold_host.AppServerClient") as client:
                self.assertEqual(hold_task(root / "unused.json", request, ack, result), 0)
            client.assert_not_called()
            final = json.loads(result.read_text(encoding="utf-8"))
            self.assertEqual(final["status"], "cancelled")
            self.assertTrue(final["terminal_confirmed"])
            self.assertEqual(final["total_turn_count"], 0)

    def test_stop_arriving_after_monitor_continue_is_checked_before_dispatch(self):
        from jarvis_monitor import HoldTurnMonitor
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            request, ack, result = (root / name for name in ("request.json", "ack.json", "result.json"))
            payload = {"request_id": "race-stop", "max_turns": 2, "auto_continue": True}
            request.write_text(json.dumps(payload), encoding="utf-8")
            class StopAfterDecision(HoldTurnMonitor):
                def observe(self, client, observation):
                    decision = super().observe(client, observation)
                    request.write_text(json.dumps({**payload, "stop_requested": True}), encoding="utf-8")
                    return decision
            client = FakeClient(None)
            with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=client,
            ):
                self.assertEqual(hold_task(root / "unused.json", request, ack, result, monitor=StopAfterDecision()), 0)
            self.assertEqual(client.started_turns, [])
            self.assertEqual(json.loads(result.read_text(encoding="utf-8"))["status"], "cancelled")
            self.assertEqual(read_turn_history(root / "turn-history.sqlite", hold_id="hold-race-stop")[0]["status"], "cancelled")

    def test_atomic_receipt_retries_and_preserves_existing_receipt_on_persistent_failure(self):
        from adapters.codex_app_server.jarvis_task_hold_host import _write_json
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "receipt.json"
            original_replace = Path.replace
            attempts = []
            def transient(source, target):
                attempts.append(source)
                if len(attempts) < 3:
                    raise PermissionError("test transient reader")
                return original_replace(source, target)
            with patch.object(Path, "replace", transient), patch("adapters.codex_app_server.jarvis_task_hold_host.time.sleep"):
                _write_json(path, {"status": "completed"})
            self.assertEqual(len(attempts), 3)
            with patch.object(Path, "replace", side_effect=PermissionError("test persistent reader")) as replace, patch(
                "adapters.codex_app_server.jarvis_task_hold_host.time.sleep"
            ):
                with self.assertRaises(PermissionError):
                    _write_json(path, {"status": "failed"})
                self.assertEqual(replace.call_count, 20)
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"status": "completed"})
            self.assertEqual(list(Path(temp).glob("*.tmp")), [])

    def test_concurrent_receipt_writers_use_distinct_temporary_files(self):
        from adapters.codex_app_server.jarvis_task_hold_host import _write_json
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp) / "receipt.json"
            barrier = threading.Barrier(2)
            sources, failures = [], []
            original_replace = Path.replace
            def replace(source, destination):
                if source not in sources:
                    sources.append(source)
                    barrier.wait(timeout=2)
                return original_replace(source, destination)
            def write(number):
                try:
                    _write_json(target, {"writer": number})
                except Exception as exc:
                    failures.append(str(exc))
            with patch.object(Path, "replace", replace):
                workers = [threading.Thread(target=write, args=(number,)) for number in range(2)]
                for worker in workers:
                    worker.start()
                for worker in workers:
                    worker.join(timeout=3)
            self.assertFalse(failures)
            self.assertEqual(len(set(sources)), 2)
            self.assertIn(json.loads(target.read_text(encoding="utf-8"))["writer"], [0, 1])

    def test_host_exception_has_failure_receipt_and_releases_claim(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            hold_root = root / "task-holds" / "hold-test"
            hold_root.mkdir(parents=True)
            (hold_root / "request.json").write_text(json.dumps({"request_id": "test", "hold_id": "hold-test"}), encoding="utf-8")
            (hold_root / "ack.json").write_text(json.dumps({"status": "accepted", "phase": "queued_for_user_host"}), encoding="utf-8")
            with patch("adapters.codex_app_server.jarvis_hold_host_service.hold_task", side_effect=RuntimeError("test host error")):
                host = JarvisHoldHost(state_dir=root, launcher_config=root / "unused.json")
                self.assertTrue(host.run_once())
            final = json.loads((hold_root / "result.json").read_text(encoding="utf-8"))
            self.assertEqual(final["status"], "failed")
            self.assertEqual(final["phase"], "host_execution_error")
            self.assertFalse(final["terminal_confirmed"])
            self.assertFalse((hold_root / ".user-host-claim").exists())
            self.assertEqual(json.loads((root / "hold-host.json").read_text(encoding="utf-8"))["active_count"], 0)

    def test_last_candidate_missing_saved_output_is_reviewed(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            request, ack, result = (root / name for name in ("request.json", "ack.json", "result.json"))
            request.write_text(json.dumps({
                "request_id": "verified", "prompt": "test", "max_turns": 1,
                "input_binding": {"candidate_ids": [7], "lane_item_count": 1,
                    "output_boundary": str(root), "result_verification": {
                        "receipt_paths": {"7": str(root / "missing.json")}, "terminal_statuses": ["completed"],
                    }},
            }), encoding="utf-8")
            client = FakeClient(None)
            with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=client,
            ):
                self.assertEqual(hold_task(root / "launcher.json", request, ack, result), 0)
            final = json.loads(result.read_text(encoding="utf-8"))
            self.assertEqual(final["status"], "turn_limit_reached")
            self.assertEqual(final["output_verification"]["scheduler_outcome"], "failed")
            self.assertEqual(client.started_prompts, [])


class FakeClient:
    def start(self):
        pass

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

            history = read_turn_history(root / "turn-history.sqlite", hold_id="hold-lane-continue")
            final = json.loads(result.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertEqual(client.created_requests[0]["input_binding"]["candidate_ids"], [7])
        self.assertEqual(final["output_verification"]["status"], "legacy_unverified")
        self.assertEqual(client.started_turns[0]["input_binding"]["candidate_ids"], [9])
        self.assertEqual(client.started_turns[0]["input_binding"]["lane_identity"], "worker-1")
        self.assertEqual([row["turn_id"] for row in history], ["turn-1", "turn-2"])
        self.assertEqual([row["candidate_id"] for row in history], [7, 9])
        self.assertEqual({row["task_id"] for row in history}, {"hold-lane-continue"})
        self.assertTrue(all(row["has_final_answer"] for row in history))

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

    def test_history_write_failure_stops_before_the_next_turn(self):
        client = FakeClient(None)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            request, ack, result = root / "request.json", root / "ack.json", root / "result.json"
            request.write_text(json.dumps({
                "request_id": "history-failure", "prompt": "hello", "max_turns": 2,
                "auto_continue": True, "continue_prompt": "继续",
            }), encoding="utf-8")
            with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=client
            ), patch(
                "adapters.codex_app_server.jarvis_task_hold_host.append_terminal_turn_history", side_effect=OSError("disk full")
            ):
                exit_code = hold_task(Path("launcher.json"), request, ack, result)
            failure = json.loads(result.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 1)
        self.assertEqual(client.started_prompts, [])
        self.assertEqual(failure["phase"], "history_recording")
        self.assertIn("disk full", failure["reason"])

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

    def test_recovery_attaches_the_persisted_turn_without_creating_or_resuming(self):
        client = FakeClient(None)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            request, ack, result = root / "request.json", root / "ack.json", root / "result.json"
            request.write_text(json.dumps({
                "mode": "recover", "request_id": "recover-1", "thread_id": "thread-existing-1",
                "turn_id": "turn-existing-1", "prompt": "continue", "max_turns": 1,
            }), encoding="utf-8")
            with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=client
            ):
                exit_code = hold_task(Path("launcher.json"), request, ack, result)

        self.assertEqual(exit_code, 0)
        self.assertEqual(client.created_requests, [])
        self.assertFalse(hasattr(client, "resumed"))
        self.assertEqual(client.waited, ["turn-existing-1"])

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

    def test_running_host_refreshes_holding_health_while_a_task_is_blocked(self):
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            launcher_config = state_dir / "launcher.json"
            launcher_config.write_text(json.dumps({
                "profile": "jarvis_test", "expected_codex_home": "C:/test/codex-home",
            }), encoding="utf-8")
            root = state_dir / "task-holds" / "hold-1"
            root.mkdir(parents=True)
            (root / "request.json").write_text(json.dumps({"request_id": "hold-1"}), encoding="utf-8")
            (root / "ack.json").write_text(json.dumps({
                "request_id": "hold-1", "status": "accepted", "phase": "queued_for_user_host",
            }), encoding="utf-8")
            entered, release, stop = threading.Event(), threading.Event(), threading.Event()

            def blocking_hold(_config, _request, _ack, result):
                entered.set()
                release.wait(timeout=2)
                result.write_text(json.dumps({"status": "completed"}), encoding="utf-8")
                return 0

            host = JarvisHoldHost(state_dir=state_dir, launcher_config=launcher_config, workers=1)
            with patch("adapters.codex_app_server.jarvis_hold_host_service.hold_task", side_effect=blocking_hold):
                runner = threading.Thread(
                    target=host.run_forever,
                    kwargs={"poll_seconds": 0.25, "stop_event": stop},
                    daemon=True,
                )
                runner.start()
                self.assertTrue(entered.wait(timeout=1))
                first = json.loads((state_dir / "hold-host.json").read_text(encoding="utf-8"))["observed_at"]
                time.sleep(0.35)
                second_health = json.loads((state_dir / "hold-host.json").read_text(encoding="utf-8"))
                release.set()
                stop.set()
                runner.join(timeout=1)

        self.assertEqual(second_health["status"], "holding")
        self.assertNotEqual(second_health["observed_at"], first)

    def test_second_host_cannot_claim_the_same_accepted_hold(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "task-holds" / "hold-1"
            root.mkdir(parents=True)
            ack_path = root / "ack.json"
            ack = {"request_id": "hold-1", "status": "accepted", "phase": "queued_for_user_host"}
            ack_path.write_text(json.dumps(ack), encoding="utf-8")

            (root / "request.json").write_text(json.dumps({"request_id": "hold-1"}))
            self.assertTrue(JarvisHoldHost._claim(root, ack_path, ack))
            self.assertFalse(JarvisHoldHost._claim(root, ack_path, ack))
            JarvisHoldHost._release_claim(root)
            ack_path.write_text(json.dumps(ack))
            self.assertTrue(JarvisHoldHost._claim(root, ack_path, ack))
            JarvisHoldHost._release_claim(root)

    def test_new_host_requeues_a_dead_claim_as_existing_turn_recovery(self):
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            launcher_config = state_dir / "launcher.json"
            launcher_config.write_text(json.dumps({
                "profile": "jarvis_test", "expected_codex_home": "C:/test/codex-home",
            }), encoding="utf-8")
            root = state_dir / "task-holds" / "hold-1"
            root.mkdir(parents=True)
            (root / "request.json").write_text(json.dumps({
                "request_id": "hold-1", "mode": "create", "prompt": "work", "hold_id": "hold-1",
            }), encoding="utf-8")
            (root / "ack.json").write_text(json.dumps({
                "request_id": "hold-1", "status": "holding", "phase": "turn_holding",
                "thread_id": "thread-existing-1", "turn_id": "turn-existing-1",
                "turn_count": 2, "total_turn_count": 5, "max_turns": 9,
            }), encoding="utf-8")
            claim = root / ".user-host-claim"
            claim.mkdir()
            (claim / "owner.json").write_text(json.dumps({"pid": 1780}), encoding="utf-8")
            host = JarvisHoldHost(state_dir=state_dir, launcher_config=launcher_config)

            with patch("adapters.codex_app_server.jarvis_hold_host_service._pid_is_alive", return_value=False):
                self.assertTrue(host.run_once())

            recovered_request = json.loads((root / "request.json").read_text(encoding="utf-8"))
            recovered_ack = json.loads((root / "ack.json").read_text(encoding="utf-8"))

        self.assertEqual(recovered_request["mode"], "recover")
        self.assertEqual(recovered_request["thread_id"], "thread-existing-1")
        self.assertEqual(recovered_request["turn_id"], "turn-existing-1")
        self.assertEqual(recovered_ack["status"], "accepted")
        self.assertEqual(recovered_ack["phase"], "queued_for_user_host")

    def test_new_host_marks_a_dead_claim_without_a_durable_turn_identity_as_unrecoverable(self):
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            launcher_config = state_dir / "launcher.json"
            launcher_config.write_text(json.dumps({
                "profile": "jarvis_test", "expected_codex_home": "C:/test/codex-home",
            }), encoding="utf-8")
            root = state_dir / "task-holds" / "hold-1"
            root.mkdir(parents=True)
            (root / "request.json").write_text(json.dumps({
                "request_id": "hold-1", "mode": "create", "prompt": "work", "hold_id": "hold-1",
            }), encoding="utf-8")
            (root / "ack.json").write_text(json.dumps({
                "request_id": "hold-1", "status": "accepted", "phase": "queued_for_user_host",
            }), encoding="utf-8")
            claim = root / ".user-host-claim"
            claim.mkdir()
            (claim / "owner.json").write_text(json.dumps({"pid": 1780}), encoding="utf-8")
            host = JarvisHoldHost(state_dir=state_dir, launcher_config=launcher_config)

            with patch("adapters.codex_app_server.jarvis_hold_host_service._pid_is_alive", return_value=False):
                self.assertTrue(host.run_once())

            recovered_request = json.loads((root / "request.json").read_text(encoding="utf-8"))
            recovered_ack = json.loads((root / "ack.json").read_text(encoding="utf-8"))

        self.assertEqual(recovered_request["mode"], "create")
        self.assertEqual(recovered_ack["status"], "failed")
        self.assertEqual(recovered_ack["phase"], "recovery_failed")
        self.assertIn("no durable thread identity", recovered_ack["reason"])

    def test_new_host_marks_a_dead_thread_starting_claim_without_identity_as_unrecoverable(self):
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            launcher_config = state_dir / "launcher.json"
            launcher_config.write_text(json.dumps({
                "profile": "jarvis_test", "expected_codex_home": "C:/test/codex-home",
            }), encoding="utf-8")
            root = state_dir / "task-holds" / "hold-1"
            root.mkdir(parents=True)
            (root / "request.json").write_text(json.dumps({
                "request_id": "hold-1", "mode": "create", "prompt": "work", "hold_id": "hold-1",
            }), encoding="utf-8")
            (root / "ack.json").write_text(json.dumps({
                "request_id": "hold-1", "status": "accepted", "phase": "thread_starting",
            }), encoding="utf-8")
            claim = root / ".user-host-claim"
            claim.mkdir()
            (claim / "owner.json").write_text(json.dumps({"pid": 1780}), encoding="utf-8")
            host = JarvisHoldHost(state_dir=state_dir, launcher_config=launcher_config)

            with patch("adapters.codex_app_server.jarvis_hold_host_service._pid_is_alive", return_value=False):
                self.assertTrue(host.run_once())

            recovered_ack = json.loads((root / "ack.json").read_text(encoding="utf-8"))

        self.assertEqual(recovered_ack["status"], "failed")
        self.assertEqual(recovered_ack["phase"], "recovery_failed")
        self.assertIn("no durable thread identity", recovered_ack["reason"])

    def test_initialize_user_host_restarts_an_idle_matching_host_for_more_capacity(self):
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            launcher_config = state_dir / "launcher.json"
            launcher_config.write_text(json.dumps({
                "profile": "jarvis_test", "expected_codex_home": "C:/test/codex-home",
            }), encoding="utf-8")
            (state_dir / "hold-host.json").write_text(json.dumps({
                "status": "ready", "pid": 1780, "worker_capacity": 1, "active_count": 0,
                "observed_at": "2026-09-01T00:00:00+00:00",
                "host_started_at": "2026-09-01T00:00:00+00:00",
                "profile": "jarvis_test", "codex_home": "C:/test/codex-home",
                "state_dir": str(state_dir.resolve()),
            }), encoding="utf-8")
            live_pids = {1780: True, 1781: True}
            stopped: list[int] = []

            def stop_host(pid: int):
                stopped.append(pid)
                live_pids[pid] = False

            def start_host():
                (state_dir / "hold-host.json").write_text(json.dumps({
                    "status": "ready", "pid": 1781, "worker_capacity": 3,
                    "active_count": 0,
                    "observed_at": "2026-09-01T00:00:10+00:00",
                    "host_started_at": "2026-09-01T00:00:00+00:00",
                    "profile": "jarvis_test", "codex_home": "C:/test/codex-home",
                    "state_dir": str(state_dir.resolve()),
                }), encoding="utf-8")

            with patch("adapters.codex_app_server.jarvis_hold_host_service._stop_hold_host", side_effect=stop_host):
                receipt = initialize_user_host(
                    state_dir=state_dir, launcher_config=launcher_config, workers=3,
                    start_host=start_host,
                    now=lambda: "2026-09-01T00:00:10+00:00", pid_alive=lambda pid: live_pids.get(pid, False),
                    pid_started_at=lambda _: "2026-08-31T23:59:59+00:00",
                )

        self.assertEqual(receipt, {"status": "ready", "phase": "capacity_upgraded"})
        self.assertEqual(stopped, [1780])

    def test_initialize_user_host_does_not_replace_an_active_host_for_more_capacity(self):
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            launcher_config = state_dir / "launcher.json"
            launcher_config.write_text(json.dumps({
                "profile": "jarvis_test", "expected_codex_home": "C:/test/codex-home",
            }), encoding="utf-8")
            (state_dir / "hold-host.json").write_text(json.dumps({
                "status": "holding", "pid": 1780, "worker_capacity": 1, "active_count": 1,
                "observed_at": "2026-09-01T00:00:00+00:00",
                "host_started_at": "2026-09-01T00:00:00+00:00",
                "profile": "jarvis_test", "codex_home": "C:/test/codex-home",
                "state_dir": str(state_dir.resolve()),
            }), encoding="utf-8")
            start_host = Mock()
            receipt = initialize_user_host(
                state_dir=state_dir, launcher_config=launcher_config, workers=3,
                start_host=start_host,
                now=lambda: "2026-09-01T00:00:10+00:00", pid_alive=lambda _: True,
                pid_started_at=lambda _: "2026-08-31T23:59:59+00:00",
            )

        self.assertEqual(receipt, {"status": "blocked", "reason": "HoldHost capacity upgrade requires an idle Host"})
        start_host.assert_not_called()

    def test_ensure_hold_host_reuses_a_matching_fresh_host(self):
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            launcher_config = state_dir / "launcher.json"
            launcher_config.write_text(json.dumps({
                "profile": "jarvis_test",
                "expected_codex_home": "C:/test/codex-home",
            }), encoding="utf-8")
            (state_dir / "hold-host.json").write_text(json.dumps({
                "status": "ready", "pid": 1780, "worker_capacity": 5, "active_count": 0,
                "observed_at": "2026-09-01T00:00:00+00:00",
                "host_started_at": "2026-09-01T00:00:00+00:00",
                "profile": "jarvis_test", "codex_home": "C:/test/codex-home",
                "state_dir": str(state_dir.resolve()),
            }), encoding="utf-8")
            start_host = Mock()

            receipt = ensure_hold_host(
                state_dir=state_dir, launcher_config=launcher_config,
                start_host=start_host, now=lambda: "2026-09-01T00:00:10+00:00", pid_alive=lambda _: True,
                pid_started_at=lambda _: "2026-08-31T23:59:59+00:00",
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
                "host_started_at": "2026-08-31T00:00:00+00:00",
                "profile": "jarvis_test", "codex_home": "C:/test/codex-home",
                "state_dir": str(state_dir.resolve()),
            }), encoding="utf-8")

            def start_host():
                (state_dir / "hold-host.json").write_text(json.dumps({
                    "status": "ready", "pid": 1781, "worker_capacity": 5, "active_count": 0,
                    "observed_at": "2026-09-01T00:00:10+00:00",
                    "host_started_at": "2026-09-01T00:00:00+00:00",
                    "profile": "jarvis_test", "codex_home": "C:/test/codex-home",
                    "state_dir": str(state_dir.resolve()),
                }), encoding="utf-8")

            receipt = ensure_hold_host(
                state_dir=state_dir, launcher_config=launcher_config,
                start_host=start_host, wait_seconds=1, poll_seconds=0,
                now=lambda: "2026-09-01T00:00:10+00:00", pid_alive=lambda _: True,
                pid_started_at=lambda _: "2026-08-31T23:59:59+00:00",
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
                    "status": "ready", "pid": 1781, "worker_capacity": 5, "active_count": 0,
                    "observed_at": "2026-09-01T00:00:10+00:00",
                    "host_started_at": "2026-09-01T00:00:00+00:00",
                    "profile": "jarvis_test", "codex_home": "C:/test/codex-home",
                    "state_dir": str(state_dir.resolve()),
                }), encoding="utf-8")

            receipt = initialize_user_host(
                state_dir=state_dir, launcher_config=launcher_config,
                start_host=start_host, poll_seconds=0,
                now=lambda: "2026-09-01T00:00:10+00:00", pid_alive=lambda _: True,
                pid_started_at=lambda _: "2026-08-31T23:59:59+00:00",
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
                "host_started_at": "2026-09-01T00:00:00+00:00",
                "profile": "jarvis_test", "codex_home": "C:/test/codex-home",
                "state_dir": str(state_dir.resolve()),
            }), encoding="utf-8")

            def start_host():
                (state_dir / "hold-host.json").write_text(json.dumps({
                    "status": "ready", "pid": 1781, "worker_capacity": 5, "active_count": 0,
                    "observed_at": "2026-09-01T00:00:10+00:00",
                    "host_started_at": "2026-09-01T00:00:00+00:00",
                    "profile": "jarvis_test", "codex_home": "C:/test/codex-home",
                    "state_dir": str(state_dir.resolve()),
                }), encoding="utf-8")

            receipt = initialize_user_host(
                state_dir=state_dir, launcher_config=launcher_config, start_host=start_host,
                poll_seconds=0, now=lambda: "2026-09-01T00:00:10+00:00",
                pid_alive=lambda pid: pid == 1781,
                pid_started_at=lambda _: "2026-08-31T23:59:59+00:00",
            )

        self.assertEqual(receipt, {"status": "ready", "phase": "started"})

    def test_initialize_user_host_restarts_when_pid_has_been_reused(self):
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            launcher_config = state_dir / "launcher.json"
            launcher_config.write_text(json.dumps({
                "profile": "jarvis_test", "expected_codex_home": "C:/test/codex-home",
            }), encoding="utf-8")
            (state_dir / "hold-host.json").write_text(json.dumps({
                "status": "ready", "pid": 1780,
                "observed_at": "2026-09-01T00:00:10+00:00",
                "host_started_at": "2026-09-01T00:00:00+00:00",
                "profile": "jarvis_test", "codex_home": "C:/test/codex-home",
                "state_dir": str(state_dir.resolve()),
            }), encoding="utf-8")

            def start_host():
                (state_dir / "hold-host.json").write_text(json.dumps({
                    "status": "ready", "pid": 1781, "worker_capacity": 5, "active_count": 0,
                    "observed_at": "2026-09-01T00:00:10+00:00",
                    "host_started_at": "2026-09-01T00:00:00+00:00",
                    "profile": "jarvis_test", "codex_home": "C:/test/codex-home",
                    "state_dir": str(state_dir.resolve()),
                }), encoding="utf-8")

            receipt = initialize_user_host(
                state_dir=state_dir, launcher_config=launcher_config, start_host=start_host,
                poll_seconds=0, now=lambda: "2026-09-01T00:00:10+00:00",
                pid_alive=lambda _: True,
                pid_started_at=lambda pid: "2026-09-01T00:01:00+00:00" if pid == 1780 else "2026-08-31T23:59:59+00:00",
            )

        self.assertEqual(receipt, {"status": "ready", "phase": "started"})

    def test_pid_liveness_accepts_the_current_process_and_rejects_an_unknown_pid(self):
        self.assertTrue(_pid_is_alive(os.getpid()))
        self.assertFalse(_pid_is_alive(999999))

    def test_pid_liveness_rejects_an_exited_process_with_an_open_handle(self):
        child = subprocess.Popen([sys.executable, "-c", "pass"])
        child.wait(timeout=5)
        self.assertFalse(_pid_is_alive(child.pid))

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
        self.assertEqual(ensure.call_args.kwargs["workers"], 5)

    def test_self_healed_host_uses_windows_start_process(self):
        with tempfile.TemporaryDirectory() as temp, patch(
            "adapters.codex_app_server.jarvis_hold_host_service.os.name", "nt"
        ), patch(
            "adapters.codex_app_server.jarvis_hold_host_service.subprocess.Popen"
        ) as popen:
            _start_hold_host(
                state_dir=Path(temp), launcher_config=Path(temp) / "launcher.json",
                workers=3, poll_seconds=1,
            )

        self.assertEqual(popen.call_args.args[0][0].lower(), "powershell.exe")




class HostCapacityRegressionTest(unittest.TestCase):
    def test_stale_scanner_cannot_execute_a_completed_hold_twice(self):
        import adapters.codex_app_server.jarvis_hold_host_service as host_module
        with tempfile.TemporaryDirectory(prefix="jarvis-qa-duplicate-") as temp:
            root = Path(temp)
            hold_root = root / "task-holds" / "hold-1"
            hold_root.mkdir(parents=True)
            request, ack = hold_root / "request.json", hold_root / "ack.json"
            request.write_text(json.dumps({
                "request_id": "same-request", "hold_id": "hold-1", "prompt": "fake only",
            }), encoding="utf-8")
            ack.write_text(json.dumps({"status": "accepted", "phase": "queued_for_user_host"}), encoding="utf-8")
            host = host_module.JarvisHoldHost(state_dir=root, launcher_config=root / "unused", workers=1)
            barrier = threading.Barrier(2)
            first_finished = threading.Event()
            calls, errors = [], []
            original_claim = host_module.JarvisHoldHost._claim

            def claim(*claim_args):
                # Both scanners have already passed the request/ack/result checks.
                barrier.wait(timeout=3)
                if threading.current_thread().name == "second":
                    assert first_finished.wait(timeout=3), "First scanner did not finish"
                return original_claim(*claim_args)

            def fake_hold(config, req, ack_path, result_path):
                calls.append(threading.current_thread().name)
                host_module._write_json(result_path, {
                    "status": "completed", "request_id": "same-request", "execution": len(calls),
                })
                host_module._write_json(ack_path, {"status": "completed"})

            def run():
                try:
                    host.run_once()
                except Exception as exc:
                    errors.append(repr(exc))
                finally:
                    if threading.current_thread().name == "first":
                        first_finished.set()

            with patch.object(host_module.JarvisHoldHost, "_claim", side_effect=claim), patch.object(
                host_module, "hold_task", side_effect=fake_hold,
            ):
                threads = [threading.Thread(target=run, name=name) for name in ("first", "second")]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(timeout=5)
            print(json.dumps({
                "source": host_module.__file__, "requested_workers": 1, "actual_workers": host.workers,
                "executions": calls, "errors": errors,
                "claim_remaining": (hold_root / ".user-host-claim").exists(),
            }), flush=True)
            assert all(not thread.is_alive() for thread in threads), "QA scanner thread did not finish"
            assert not errors, errors
            self.assertEqual(json.loads((hold_root / "ack.json").read_text()), {"status": "completed"})
            self.assertEqual(json.loads((hold_root / "result.json").read_text())["execution"], 1)
            assert len(calls) == 1, (
                "A queued Hold must execute once even if another scanner has a stale pre-claim snapshot"
            )

    def test_claim_rechecks_request_identity_and_preserves_stop(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            request = {"request_id": "old", "hold_id": "hold"}
            ack = {"request_id": "new", "status": "accepted", "phase": "queued_for_user_host"}
            (root / "request.json").write_text(json.dumps({**request, "request_id": "new"}))
            (root / "ack.json").write_text(json.dumps(ack))
            before = (root / "ack.json").read_bytes()
            self.assertFalse(JarvisHoldHost._claim(root, root / "ack.json", ack, request))
            self.assertEqual((root / "ack.json").read_bytes(), before)
            self.assertFalse((root / ".user-host-claim").exists())
            request["request_id"] = "new"
            stopped = {**request, "stop_requested": True}
            (root / "request.json").write_text(json.dumps(stopped))
            self.assertTrue(JarvisHoldHost._claim(root, root / "ack.json", ack, request))
            try:
                self.assertEqual(json.loads((root / "request.json").read_text()), stopped)
                self.assertEqual(hold_task(root / "unused", root / "request.json", root / "ack.json", root / "result.json"), 0)
                self.assertEqual(json.loads((root / "result.json").read_text())["phase"], "stopped_before_dispatch")
            finally:
                JarvisHoldHost._release_claim(root)

    @unittest.skipUnless(os.name == "nt", "Windows reader/delete sharing contract")
    def test_stopped_hold_drains_after_concurrent_claim_owner_reader(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            hold_root = root / "task-holds" / "stopped"
            hold_root.mkdir(parents=True)
            (hold_root / "request.json").write_text(json.dumps({"request_id": "stopped", "stop_requested": True}))
            (hold_root / "ack.json").write_text(json.dumps({"status": "accepted", "phase": "queued_for_user_host"}))
            original = JarvisHoldHost._release_claim
            timers = []
            def reader_during_release(path):
                # A real Windows file handle, as held by another scanner reading owner.json.
                reader = (path / ".user-host-claim" / "owner.json").open("r")
                timer = threading.Timer(.15, reader.close)
                timers.append(timer)
                timer.start()
                original(path)
            try:
                with patch.object(JarvisHoldHost, "_release_claim", side_effect=reader_during_release):
                    self.assertTrue(JarvisHoldHost(state_dir=root, launcher_config=root / "unused").run_once())
            finally:
                for timer in timers: timer.join()
            self.assertFalse((hold_root / ".user-host-claim").exists())
            result = json.loads((hold_root / "result.json").read_text())
            self.assertEqual(result["status"], "cancelled")
            self.assertEqual(result["total_turn_count"], 0)
            self.assertTrue(result["terminal_confirmed"])

    def test_explicit_host_capacity_applies_to_initializer_constructor_and_process_entry(self):
        from adapters.codex_app_server.jarvis_hold_host_service import _start_hold_host
        for requested, expected in ((1, 1), (3, 3), (20, 20), (24, 24)):
            with self.subTest(requested=requested), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                launcher = root / "launcher.json"
                launcher.write_text(json.dumps({"profile": "capacity-test", "expected_codex_home": str(root)}), encoding="utf-8")
                def start(**kwargs):
                    self.assertEqual(kwargs["workers"], expected)
                    host = JarvisHoldHost(state_dir=root, launcher_config=launcher, workers=kwargs["workers"])
                    host._write_health("ready")
                with patch("adapters.codex_app_server.jarvis_hold_host_service._start_hold_host", side_effect=start) as starter:
                    receipt = initialize_user_host(state_dir=root, launcher_config=launcher, workers=requested,
                        wait_seconds=0, pid_alive=lambda _: True, pid_started_at=lambda _: "2000-01-01T00:00:00+00:00")
                self.assertEqual(receipt["status"], "ready")
                starter.assert_called_once()
                self.assertEqual(json.loads((root / "hold-host.json").read_text())["worker_capacity"], expected)
                self.assertEqual(list((root / "task-holds").iterdir()), [])
                self.assertEqual(JarvisHoldHost(state_dir=root, launcher_config=launcher, workers=requested).workers, expected)
                with patch("adapters.codex_app_server.jarvis_hold_host_service.subprocess.Popen") as popen:
                    _start_hold_host(state_dir=root, launcher_config=launcher, workers=requested, poll_seconds=1)
                command = " ".join(popen.call_args.args[0])
                self.assertIn("--workers " + str(expected), command)

    def test_two_loops_share_explicit_twenty_slot_host_without_extra_tasks(self):
        from jarvis_control.loop import LoopController, LoopStore
        from jarvis_control.provisioning import TaskProvisionRequest
        from adapters.codex_app_server.task_provisioning_adapter import CodexAppServerTaskProvisioningAdapter
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            launcher = root / "launcher.json"
            launcher.write_text(json.dumps({"profile": "capacity-test", "expected_codex_home": str(root)}), encoding="utf-8")
            config = type("Config", (), {"profile": "capacity-test", "expected_codex_home": str(root),
                "resolve_project": lambda _, project: (project, str(root))})()
            adapter = CodexAppServerTaskProvisioningAdapter(launcher, state_dir=root, config_loader=lambda _: config,
                pid_alive=lambda _: True, pid_started_at=lambda _: "2000-01-01T00:00:00+00:00")
            class Runtime:
                def hold(self, **kwargs):
                    receipt = adapter.provision(TaskProvisionRequest(**{k: v for k, v in kwargs.items() if k != "task_id"}))
                    return {"status": receipt.status, "data": {"hold_id": receipt.hold_id}}
                def monitor(self, **kwargs):
                    return {"status": "completed", "data": adapter.hold_status(kwargs["hold_id"])}
                def heartbeat(self, **kwargs):
                    return {"status": "active" if kwargs["action"] == "create" else "cancelled"}
                def stop_hold(self, hold_id):
                    return adapter.request_hold_stop(hold_id)
            release = threading.Event()
            stop = threading.Event()
            first_three = threading.Event()
            all_five = threading.Event()
            clients = []
            host_threads = []
            count_lock = threading.Lock()
            class Client(FakeClient):
                def create_task(self, request, **kwargs):
                    super().create_task(request, **kwargs)
                    with count_lock:
                        clients.append(self)
                        number = len(clients)
                        if number == 3: first_three.set()
                        if number == 5: all_five.set()
                    return {"thread_id": f"thread-{number}", "turn_id": f"turn-{number}"}
                def wait_for_turn_readback(self, *_):
                    if not release.wait(10): raise RuntimeError("test release timed out")
                    return "done"
            def start(**kwargs):
                self.assertEqual(kwargs["workers"], 20)
                host = JarvisHoldHost(state_dir=root, launcher_config=launcher, workers=kwargs["workers"])
                thread = threading.Thread(target=host.run_forever, kwargs={"poll_seconds": .01, "stop_event": stop})
                host_threads.append(thread)
                thread.start()
            controller = LoopController(LoopStore(root / "loops"))
            runtime = Runtime()
            with patch("adapters.codex_app_server.jarvis_hold_host_service._start_hold_host", side_effect=start) as starter, patch(
                "adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", side_effect=Client):
                try:
                    self.assertEqual(adapter.ensure_hold_host_ready(required_workers=20)["status"], "ready")
                    self.assertEqual(clients, [])
                    first = controller.start(runtime, request_id="first", project="test", title="first", prompt="test",
                        target_thread_count=3, max_rounds=1, expires_at="2099-01-01T00:00:00+00:00")
                    self.assertTrue(first_three.wait(5))
                    self.assertEqual(adapter.ensure_hold_host_ready(required_workers=2), {"status": "ready", "phase": "already_running"})
                    second = controller.start(runtime, request_id="second", project="test", title="second", prompt="test",
                        target_thread_count=2, max_rounds=1, expires_at="2099-01-01T00:00:00+00:00")
                    self.assertTrue(all_five.wait(5))
                    starter.assert_called_once()
                    health = json.loads((root / "hold-host.json").read_text())
                    self.assertEqual(health["worker_capacity"], 20)
                    self.assertEqual(health["active_count"], 5)
                    self.assertEqual(len(clients), 5)
                    self.assertEqual(len(list((root / "task-holds").glob("*/request.json"))), 5)
                    release.set()
                    deadline = time.monotonic() + 5
                    while len(list((root / "task-holds").glob("*/result.json"))) != 5:
                        if time.monotonic() > deadline: self.fail("five requested Holds did not finish")
                        time.sleep(.02)
                    for result in (first, second):
                        self.assertEqual(controller.tick(runtime, loop_id=result.loop_id).status, "completed")
                    self.assertEqual(len(clients), 5)
                finally:
                    release.set()
                    stop.set()
                    for thread in host_threads:
                        thread.join(5)
                        self.assertFalse(thread.is_alive())

class BatchLaneRegressionTest(unittest.TestCase):
    def test_resumed_batch_runs_through_tail_and_rejects_exhausted_lane(self):
        from adapters.codex_app_server.task_provisioning_adapter import CodexAppServerTaskProvisioningAdapter
        from jarvis_control.provisioning import TaskProvisionRequest, TaskMonitorResumeRequest
        with tempfile.TemporaryDirectory(prefix="batch-qa-resume-") as temp:
            root = Path(temp)
            class Config:
                def resolve_project(self, project):
                    return project, str(root)
            adapter = CodexAppServerTaskProvisioningAdapter(root / "unused", state_dir=root,
                                                          config_loader=lambda _: Config())
            ids = [11, 22, 33, 44, 55]
            binding = {"candidate_ids": ids, "batch_size": 2, "lane_item_count": len(ids),
                       "database_path": str(root / "unused.sqlite"), "output_boundary": str(root),
                       "result_verification": {"receipt_paths": {str(i): f"{i}.receipt.json" for i in ids},
                                               "terminal_statuses": ["completed"]}}
            created = adapter.provision(TaskProvisionRequest(
                request_id="first-run", project="fake", title="test", prompt="test", source_ref="qa",
                max_turns=3, auto_continue=True, input_binding=binding))
            assert created.status == "accepted"
            paths = adapter._existing_paths(created.hold_id)
            observed = []

            class Client(FakeClient):
                def __init__(self, stop_after_first=False):
                    super().__init__(None)
                    self.stop_after_first = stop_after_first

                def create_task(self, request, **kwargs):
                    self.binding = request["input_binding"]
                    return super().create_task(request, **kwargs)

                def resume_turn_async(self, thread_id, prompt, **kwargs):
                    self.binding = kwargs["input_binding"]
                    return super().resume_turn_async(thread_id, prompt, **kwargs)

                def start_turn_async(self, thread_id, prompt, **kwargs):
                    self.binding = kwargs["input_binding"]
                    super().start_turn_async(thread_id, prompt, **kwargs)
                    return {"turn_id": f"tail-{self.binding['result_verification']['turn_number']}"}

                def wait_for_turn_readback(self, *_):
                    contract = self.binding["result_verification"]
                    observed.append(list(self.binding["candidate_ids"]))
                    for candidate in self.binding["candidate_ids"]:
                        data = {"candidate_id": candidate, "request_id": contract["request_id"],
                                "turn_number": contract["turn_number"], "status": "completed"}
                        (root / f"{candidate}.output.json").write_text(json.dumps(data), encoding="utf-8")
                        (root / contract["receipt_paths"][str(candidate)]).write_text(
                            json.dumps({**data, "output_path": f"{candidate}.output.json"}), encoding="utf-8")
                    if self.stop_after_first:
                        adapter.request_hold_stop(created.hold_id)
                    return "done"

            def run(client):
                with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), \
                     patch("adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=client):
                    return hold_task(root / "unused", paths["request"], paths["ack"], paths["result"])

            assert run(Client(stop_after_first=True)) == 0
            first = adapter.hold_status(created.hold_id)
            assert first["status"] == "cancelled" and first["output_verification"]["status"] == "verified"
            resumed = adapter.resume_with_monitor(TaskMonitorResumeRequest(
                request_id="resume-run", task_id="thread-1", prompt="continue", source_ref="qa",
                hold_id=created.hold_id, auto_continue=True))
            assert resumed.status == "accepted"
            queued = json.loads(paths["request"].read_text(encoding="utf-8"))
            holder_exit = run(Client())
            final = adapter.hold_status(created.hold_id)
            assert holder_exit == 0 and final["status"] in {"completed", "turn_limit_reached"}, \
                "BUG: verified resumed tail attempts an out-of-range batch instead of completing"
            assert final["total_turn_count"] == 3 and final["hold_released"] is True
            assert observed == [[11, 22], [33, 44], [55]]
            assert queued["max_turns"] == 2
            assert final["terminal_confirmed"] is True
            saved = {name: paths[name].read_bytes() for name in ("request", "ack", "result")}
            exhausted = adapter.resume_with_monitor(TaskMonitorResumeRequest(
                request_id="exhausted", task_id="thread-1", prompt="continue", source_ref="qa",
                hold_id=created.hold_id, auto_continue=True))
            assert exhausted.status == "failed"
            assert "no candidates" in exhausted.reason
            assert saved == {name: paths[name].read_bytes() for name in saved}

    def test_generic_binding_batch_size_is_not_a_lane(self):
        with tempfile.TemporaryDirectory(prefix="jarvis-qa-generic-") as temp:
            root = Path(temp)
            request, ack, result = (root / name for name in ("request.json", "ack.json", "result.json"))
            binding = {"batch_size": 2, "topic": "generic data"}
            request.write_text(json.dumps({
                "request_id": "generic-hold", "prompt": "generic work", "max_turns": 1,
                "input_binding": binding,
            }), encoding="utf-8")
            client = FakeClient(None)
            with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=client,
            ):
                code = hold_task(root / "unused", request, ack, result)
            final = json.loads(result.read_text(encoding="utf-8"))
            print("GENERIC_QA " + json.dumps({
                "hold_exit": code, "status": final["status"], "phase": final["phase"],
                "reason": final.get("reason"), "delivered_binding": client.created_requests[0]["input_binding"],
            }), flush=True)
            self.assertEqual(client.created_requests[0]["input_binding"], binding)
            self.assertEqual(code, 0, "Generic non-lane binding must remain passthrough")
            self.assertEqual(final["status"], "turn_limit_reached")

    def _run_batch(self, count, size, *, lanes=1, case="valid", with_schema=True, rotation=None):
        from jarvis_control import LoopController, LoopStore, TaskProvisionRequest
        from jarvis_control.provisioning import TaskMonitorResumeRequest
        from adapters.codex_app_server.task_provisioning_adapter import CodexAppServerTaskProvisioningAdapter
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            adapter = CodexAppServerTaskProvisioningAdapter("unused", state_dir=root, config_loader=lambda _: type(
                "Config", (), {"resolve_project": lambda _, project: (project, str(root))})())
            beats = []
            class Runtime:
                def hold(self, **kwargs):
                    options = {key: value for key, value in kwargs.items() if key != "task_id"}
                    receipt = adapter.provision(TaskProvisionRequest(**options))
                    return {"status": receipt.status, "data": {"hold_id": receipt.hold_id}}
                def monitor(self, **kwargs):
                    return {"status": "completed", "data": adapter.hold_status(kwargs["hold_id"])}
                def heartbeat(self, **kwargs):
                    beats.append(kwargs)
                    return {"status": "active" if kwargs["action"] == "create" else "cancelled"}
                def stop_hold(self, hold_id):
                    return adapter.request_hold_stop(hold_id)
            runtime = Runtime()
            controller = LoopController(LoopStore(root / "loops"))
            rounds = (count + size - 1) // size
            threads = []
            all_ids = []
            for lane in range(lanes):
                ids = [101 + 3 * (lane * count + n) for n in range(count)]
                all_ids.extend(ids)
                boundary = root / f"output-{lane}"
                boundary.mkdir()
                threads.append({"slot": f"worker:{lane}" if rotation else f"worker-{lane}", "lane": {
                    "candidate_ids": ids, "batch_size": size, "database_path": str(root / "synthetic.sqlite"),
                    "output_boundary": str(boundary), "result_verification": {
                        "receipt_paths": {str(i): f"{i}.receipt.json" for i in ids},
                        "terminal_statuses": ["completed", "failed", "FAILED", "PARTIAL"],
                        "output_schema": {"properties": {"score": {"type": "integer"}}, "required": ["score"]},
                    }}})
            if not with_schema:
                for spec in threads: spec["lane"]["result_verification"].pop("output_schema")
            started = controller.start(runtime, request_id="batch:test" if rotation else "batch-test", project="synthetic", title="test", prompt="test",
                turns_per_thread=rotation, continue_prompt="continue this batch" if rotation else None,
                target_thread_count=lanes, max_rounds=rounds, expires_at="2099-01-01T00:00:00+00:00", threads=threads)
            self.assertEqual(started.status, "running")
            seen = []
            pending = list(zip(started.data["children"], threads))
            while pending:
                child, spec = pending.pop(0)
                offset = child.get("completed_rounds", 0)
                budget = min(rotation, rounds - offset) if rotation else rounds
                paths = adapter._existing_paths(child["hold_id"])
                saved = json.loads(paths["request"].read_text(encoding="utf-8"))
                self.assertEqual(saved["max_turns"], budget)
                if rotation:
                    self.assertTrue(saved["prompt"].endswith("任务：test"))
                    self.assertTrue(saved["continue_prompt"].endswith("任务：continue this batch"))
                    for prompt in (saved["prompt"], saved["continue_prompt"]):
                        self.assertIn("单独输出 JARVIS_RUN_STATUS: blocked", prompt)
                        self.assertIn("普通字段、计数、枚举或业务结果失败使用 failed/review", prompt)
                client = FakeClient(None)
                def emit(_thread_id, turn_id):
                    turn = 1 if not client.started_turns else len(client.started_turns) + 1
                    binding = client.created_requests[0]["input_binding"] if turn == 1 else client.started_turns[-1]["input_binding"]
                    ids = binding["candidate_ids"]
                    self.assertEqual(ids, spec["lane"]["candidate_ids"][(offset+turn-1)*size:(offset+turn)*size])
                    contract = binding["result_verification"]
                    if size > 1:
                        self.assertEqual(set(contract["receipt_paths"]), {str(i) for i in ids})
                    seen.extend(ids)
                    for index, candidate in enumerate(ids):
                        data = {"candidate_id": candidate, "request_id": saved["request_id"], "turn_number": turn,
                                "status": "completed", "score": 3}
                        bad = index == len(ids)-1 and (turn == rounds if case == "last_schema" else turn == 1)
                        if bad and case in {"first_schema", "last_schema"}: data["score"] = "bad"
                        if bad and case in {"partial", "upper_failed", "upper_partial"}:
                            data["status"] = {"partial": "failed", "upper_failed": "FAILED", "upper_partial": "PARTIAL"}[case]
                        if bad and case == "wrong": data["candidate_id"] = 999999
                        if bad and case == "duplicate": data["candidate_id"] = ids[0]
                        if bad and case == "old_turn": data["turn_number"] = 77
                        if bad and case == "old_request": data["request_id"] = "previous-run"
                        boundary = Path(binding["output_boundary"])
                        (boundary / f"{candidate}.output.json").write_text(json.dumps(data), encoding="utf-8")
                        if not (bad and case == "missing"):
                            (boundary / f"{candidate}.receipt.json").write_text(json.dumps({**data,
                                "output_path": f"{candidate}.output.json"}), encoding="utf-8")
                    if case == "stop" and turn == 1: controller.stop(runtime, loop_id=started.loop_id)
                    return "all done (text is not trusted)"
                client.wait_for_turn_readback = emit
                def next_turn(thread, prompt, **kwargs):
                    if rotation:
                        self.assertTrue(prompt.endswith("任务：continue this batch"))
                    client.started_turns.append(kwargs)
                    return {"turn_id": f"turn-{len(client.started_turns)+1}"}
                client.start_turn_async = next_turn
                with patch("adapters.codex_app_server.jarvis_task_hold_host.NativeTaskLauncherConfig", return_value=FakeConfig()), patch(
                    "adapters.codex_app_server.jarvis_task_hold_host.AppServerClient", return_value=client):
                    self.assertEqual(hold_task(root / "unused", paths["request"], paths["ack"], paths["result"]), 0)
                final = adapter.hold_status(child["hold_id"])
                hard = case in {"wrong", "duplicate"}
                expected = "blocked" if hard else "cancelled" if case == "stop" else "turn_limit_reached"
                self.assertEqual(final["status"], expected)
                self.assertEqual(final["total_turn_count"], 1 if hard or case == "stop" else budget)
                history = adapter.read_turn_history(hold_id=child["hold_id"])
                if size > 1:
                    self.assertEqual(history[-1]["candidate_ids"], final["output_verification"]["candidate_ids"])
                    self.assertEqual(history[-1]["output_verification"], final["output_verification"])
                    self.assertIsNone(history[-1]["candidate_id"])
                if case == "stop":
                    snapshot = json.loads(paths["request"].read_text(encoding="utf-8"))["input_binding"]
                    resumed = adapter.resume_with_monitor(TaskMonitorResumeRequest(request_id="resume-batch", task_id="thread-1",
                        prompt="resume", source_ref="test", hold_id=child["hold_id"]))
                    self.assertEqual(resumed.status, "accepted")
                    queued = json.loads(paths["request"].read_text(encoding="utf-8"))
                    self.assertEqual(queued["input_binding"], snapshot)
                    from adapters.codex_app_server.jarvis_task_hold_host import _turn_input_binding
                    self.assertEqual(_turn_input_binding(queued, 2)["candidate_ids"], spec["lane"]["candidate_ids"][size:2*size])
                    # Restore only isolated fixture to finish the already-stopped Loop readback.
                    paths["request"].write_text(json.dumps(saved), encoding="utf-8")
                    paths["result"].write_text(json.dumps(final), encoding="utf-8")
                if rotation:
                    updated = controller.tick(runtime, loop_id=started.loop_id)
                    replacement = next(c for c in updated.data["children"] if c["slot"] == child["slot"])
                    if replacement["hold_id"] != child["hold_id"]:
                        pending.insert(0, (replacement, spec))
                if case not in {"valid", "stop"} and rotation is None:
                    before = paths["request"].read_bytes()
                    # Even a forged summary cannot skip an invalid member on explicit resume.
                    forged = json.loads(paths["result"].read_text(encoding="utf-8"))
                    forged["output_verification"] = {"status": "verified"}
                    paths["result"].write_text(json.dumps(forged), encoding="utf-8")
                    resumed = adapter.resume_with_monitor(TaskMonitorResumeRequest(request_id="skip", task_id="thread-1",
                        prompt="skip", source_ref="test", hold_id=child["hold_id"]))
                    self.assertEqual(resumed.status, "failed")
                    self.assertEqual(paths["request"].read_bytes(), before)
                    paths["result"].write_text(json.dumps(final), encoding="utf-8")
            result = controller.tick(runtime, loop_id=started.loop_id)
            self.assertEqual(result.status, "blocked" if case in {"wrong", "duplicate"} else "stopped" if case == "stop" else "completed")
            if case not in {"valid", "stop", "wrong", "duplicate"}:
                self.assertEqual(result.data["business_status"], "review")
                self.assertEqual(seen, all_ids)
            if case == "valid":
                self.assertEqual(seen, all_ids)
                self.assertEqual(len(set(seen)), count * lanes)
                self.assertEqual(beats[-1]["action"], "cancel")
                history = adapter.read_turn_history(task_id=started.loop_id)
                self.assertEqual(len(history), rounds * lanes)
                self.assertEqual(sorted(candidate for row in history
                    for candidate in (row["candidate_ids"] if size > 1 else [row["candidate_id"]])), sorted(all_ids))
            return {"lanes": lanes, "count_per_lane": count, "batch_size": size, "rounds": rounds,
                    "seen_count": len(seen), "loop_status": result.status}

    def test_three_lanes_twenty_by_four_and_arbitrary_tail_sizes(self):
        evidence = [self._run_batch(80, 20, lanes=3)]
        for count, size in ((7, 3), (3, 20), (1, 7), (5, 1), (23, 6)):
            evidence.append(self._run_batch(count, size))
        evidence.append(self._run_batch(7, 3, with_schema=False))
        print("BATCH_ISOLATED_FUNCTIONAL_RECEIPT " + json.dumps(evidence))

    def test_rotation_keeps_lane_ids_and_delivers_distinct_continuation_to_holder(self):
        for count, size, quota in ((5, 1, 1), (7, 2, 2)):
            with self.subTest(batch_size=size, turns_per_thread=quota):
                self._run_batch(count, size, lanes=2, rotation=quota)

    def test_soft_failures_continue_across_five_turn_rotation_and_other_seat(self):
        for case in ("missing", "old_request", "first_schema"):
            with self.subTest(case=case):
                self._run_batch(7, 1, lanes=2, rotation=5, case=case)

    def test_batch_missing_wrong_duplicate_partial_and_schema_fail_closed(self):
        for case in ("missing", "wrong", "duplicate", "partial", "upper_failed", "upper_partial", "old_turn", "first_schema", "last_schema", "stop"):
            with self.subTest(case=case): self._run_batch(5, 2, case=case)
        self._run_batch(5, 2, case="stop", with_schema=False)

if __name__ == "__main__":
    unittest.main()
