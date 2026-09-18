from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from jarvis_local_heartbeat import HeartbeatService, JarvisControlHeartbeat, LocalHeartbeatConfig, LocalHeartbeatStore


class LocalHeartbeatTest(unittest.TestCase):
    def test_health_remains_readable_while_next_tick_is_being_written(self):
        # Independent QA reproduced an empty health file during a normal write.
        service = HeartbeatService(self.config, store=self.store)
        service.run_once()
        old_tick = self.config.health_path.read_bytes()
        entered, release = threading.Event(), threading.Event()
        original = Path.write_text
        errors = []
        def paused_write(path, data, *args, **kwargs):
            if path.parent == self.config.health_path.parent and path.name.endswith(".tmp"):
                with path.open("w", encoding="utf-8") as stream:
                    entered.set()
                    if not release.wait(5):
                        raise AssertionError("test publication release timeout")
                    return stream.write(data)
            return original(path, data, *args, **kwargs)
        def write_tick():
            try:
                service.run_once()
            except Exception as exc:
                errors.append(exc)
        with patch.object(Path, "write_text", paused_write):
            writer = threading.Thread(target=write_tick)
            writer.start()
            try:
                self.assertTrue(entered.wait(3))
                self.assertEqual(self.config.health_path.read_bytes(), old_tick)
                self.assertEqual(service.health()["status"], "recent_tick")
            finally:
                release.set()
                writer.join(5)
        self.assertFalse(writer.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(service.health()["status"], "recent_tick")
        self.assertEqual(list(self.config.health_path.parent.glob("*.tmp")), [])

    def test_health_atomic_replace_retries_and_preserves_previous_tick_on_failure(self):
        service = HeartbeatService(self.config, store=self.store)
        service.run_once()
        replace = Path.replace
        calls = []
        def reader_lock_then_replace(source, target):
            calls.append(target)
            if len(calls) == 1:
                raise PermissionError("Windows reader sharing violation")
            return replace(source, target)
        with patch.object(Path, "replace", reader_lock_then_replace), patch("jarvis_local_heartbeat.time.sleep"):
            service.run_once()
        self.assertEqual(len(calls), 2)
        self.assertEqual(service.health()["status"], "recent_tick")
        previous = self.config.health_path.read_bytes()
        with patch.object(Path, "replace", side_effect=PermissionError("persistently locked")) as attempts, patch("jarvis_local_heartbeat.time.sleep"):
            with self.assertRaises(PermissionError):
                service.run_once()
        self.assertEqual(attempts.call_count, 20)
        self.assertEqual(self.config.health_path.read_bytes(), previous)
        self.assertEqual(list(self.config.health_path.parent.glob("*.tmp")), [])

    def test_health_reports_tick_evidence_not_configuration_as_running(self):
        service = HeartbeatService(self.config, store=self.store)
        self.assertEqual(service.health()["status"], "unobserved")
        self.assertFalse(self.config.health_path.exists())
        service.run_once()
        self.assertEqual(service.health()["status"], "recent_tick")
        self.assertEqual(service.health()["last_tick"]["pid"], os.getpid())
        saved = json.loads(self.config.health_path.read_text(encoding="utf-8"))
        saved["observed_at"] = "2000-01-01T00:00:00+00:00"
        self.config.health_path.write_text(json.dumps(saved), encoding="utf-8")
        before = self.config.health_path.read_bytes()
        self.assertEqual(HeartbeatService(self.config).health()["status"], "stale")
        control = JarvisControlHeartbeat(self.config_path)
        receipt = control.invoke_heartbeat("health", "heartbeat.health", "test", {})
        self.assertEqual(receipt["status"], "completed")
        self.assertEqual(receipt["scheduler"]["status"], "stale")
        self.assertEqual(before, self.config.health_path.read_bytes())
        self.config.health_path.write_text(json.dumps({"status": "running", "utc_time": datetime.now(timezone.utc).isoformat()}))
        self.assertEqual(service.health()["status"], "unverified")

    def test_background_scheduler_rotates_after_writer_release_and_restarts_from_disk(self):
        # User requirement: independently scheduled tick, not an external status
        # query, must launch a new thread only after the previous writer exits.
        from dataclasses import replace
        from adapters.codex_app_server.jarvis_local_heartbeat_host import JarvisControlFunctionRunner
        from adapters.codex_app_server.jarvis_hold_host_service import JarvisHoldHost
        from adapters.codex_app_server.jarvis_task_hold_host import _write_json
        from jarvis_control import LoopController, LoopStore
        root = Path(self.temp.name)
        config = replace(self.config, min_interval_seconds=1)
        scheduler_store = LocalHeartbeatStore(config)
        loop_store = LoopStore(root / "loops")
        controller = LoopController(loop_store)
        writer_release, result_ready = threading.Event(), threading.Event()
        holder_stop, scheduler_stop = threading.Event(), threading.Event()
        dispatches, errors = [], []

        class Runtime:
            def hold(_self, **kwargs):
                for previous in dispatches:
                    if (previous / ".user-host-claim").exists():
                        errors.append("new dispatch before old writer release")
                hold_root = root / "task-holds" / str(len(dispatches) + 1)
                hold_root.mkdir(parents=True)
                thread_id = f"fake-thread-{len(dispatches) + 1}"
                dispatches.append(hold_root)
                request = {"request_id": kwargs["request_id"], "hold_id": kwargs["hold_id"], "thread_id": thread_id}
                _write_json(hold_root / "request.json", request)
                _write_json(hold_root / "ack.json", {**request, "status": "accepted", "phase": "queued_for_user_host"})
                return {"status": "holding", "target_thread_id": thread_id, "data": {"monitor_id": kwargs["hold_id"]}}

            def monitor(_self, **kwargs):
                for hold_root in dispatches:
                    request = json.loads((hold_root / "request.json").read_text())
                    if request["hold_id"] == kwargs["hold_id"]:
                        result = hold_root / "result.json"
                        receipt = json.loads(result.read_text()) if result.exists() else {"status": "holding"}
                        return {"status": "completed", "data": {**request, **receipt,
                            "hold_released": result.exists() and not (hold_root / ".user-host-claim").exists()}}
                raise AssertionError("unexpected hold")

            def heartbeat(_self, **kwargs):
                if kwargs["action"] == "create":
                    scheduler_store.create(kwargs["options"])
                    return {"status": "active"}
                scheduler_store.cancel(kwargs["heartbeat_id"])
                return {"status": "cancelled"}

            def stop_hold(_self, _hold_id):
                return {"status": "stop_requested"}

        runtime = Runtime()

        class Control:
            def loop(_self, *, action, loop_id):
                if action != "tick":
                    raise AssertionError("test cannot drive an external status call")
                result = controller.tick(runtime, loop_id=loop_id)
                return {"status": result.status, "data": result.data}

        def holder(_config, request, ack, result):
            payload = json.loads(request.read_text())
            _write_json(result, {**payload, "status": "turn_limit_reached", "terminal_confirmed": True,
                                 "session_turn_count": 1})
            if request.parent == dispatches[0]:
                result_ready.set()
                if not writer_release.wait(10):
                    errors.append("writer release timeout")

        def schedule(service, stop):
            try:
                while not stop.is_set():
                    service.run_once()
                    stop.wait(.05)
            except Exception as exc:
                errors.append(repr(exc))

        def await_condition(condition, timeout=6):
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                try:
                    if condition():
                        return True
                except PermissionError:
                    pass  # Windows readback can briefly race atomic replacement.
                time.sleep(.02)
            return False

        started = controller.start(runtime, request_id="background-rotation", project="test", title="test",
            prompt="isolated fake native holder", turns_per_thread=1, max_rounds=2, target_thread_count=1,
            interval_seconds=1, expires_at="2099-01-01T00:00:00+00:00")
        self.assertEqual(started.status, "running")
        heartbeat_id = started.data["heartbeat_id"]
        host = JarvisHoldHost(state_dir=root, launcher_config=root / "unused", workers=20)
        service = HeartbeatService(config, function_runner=JarvisControlFunctionRunner(Control()))
        host_thread = threading.Thread(target=host.run_forever, kwargs={"poll_seconds": .25, "stop_event": holder_stop})
        scheduler_thread = threading.Thread(target=schedule, args=(service, scheduler_stop))
        with patch("adapters.codex_app_server.jarvis_hold_host_service.hold_task", side_effect=holder):
            host_thread.start()
            scheduler_thread.start()
            try:
                self.assertTrue(result_ready.wait(3))
                self.assertTrue(await_condition(lambda: scheduler_store.get(heartbeat_id)["run_count"] >= 2))
                self.assertEqual(len(dispatches), 1, "terminal result alone cannot release a writer")
                scheduler_stop.set()
                scheduler_thread.join(3)
                prior_count = scheduler_store.get(heartbeat_id)["run_count"]
                controller = LoopController(LoopStore(root / "loops"))
                service = HeartbeatService(config, function_runner=JarvisControlFunctionRunner(Control()))
                scheduler_stop = threading.Event()
                scheduler_thread = threading.Thread(target=schedule, args=(service, scheduler_stop))
                writer_release.set()
                scheduler_thread.start()
                self.assertTrue(await_condition(lambda: loop_store.load(started.loop_id)["status"] == "completed"))
                self.assertGreater(scheduler_store.get(heartbeat_id)["run_count"], prior_count)
                self.assertEqual(len(dispatches), 2)
                self.assertEqual(service.health()["status"], "recent_tick")
                self.assertEqual([json.loads((path / "request.json").read_text())["thread_id"] for path in dispatches],
                                 ["fake-thread-1", "fake-thread-2"])
            finally:
                writer_release.set()
                scheduler_stop.set()
                holder_stop.set()
                scheduler_thread.join(5)
                host_thread.join(5)
        self.assertEqual(errors, [])
        self.assertFalse(host_thread.is_alive())
        self.assertFalse(scheduler_thread.is_alive())

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.config_path = root / "heartbeat.json"
        self.config_path.write_text(json.dumps({
            "db_path": str(root / "heartbeats.sqlite"),
            "health_path": str(root / "health.json"),
            "poll_seconds": 1,
            "min_interval_seconds": 10,
            "max_interval_seconds": 3600,
        }), encoding="utf-8")
        self.config = LocalHeartbeatConfig.load(self.config_path)
        self.store = LocalHeartbeatStore(self.config)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def request(self) -> dict[str, object]:
        return {
            "heartbeat_id": "local-counter-v1",
            "name": "Local counter",
            "function": "JarvisControl.monitor",
            "arguments": {"monitor_id": "monitor-1"},
            "interval_seconds": 30,
            "start_immediately": True,
            "max_runs": 1,
            "expires_at": "2099-01-01T00:00:00+00:00",
            "source_event_key": "test-local-counter",
            "confirmation_evidence": "Cooper confirmed the local counter test.",
        }

    def test_counter_calls_bound_function_once_then_completes_without_prompt(self) -> None:
        heartbeat = self.store.create(self.request())["heartbeat"]
        calls: list[tuple[str, dict[str, object], dict[str, object]]] = []

        service = HeartbeatService(
            self.config,
            store=self.store,
            function_runner=lambda function, arguments, context: calls.append((function, dict(arguments), dict(context))) or {"status": "completed"},
        )
        result = service.run_once()

        self.assertEqual(calls[0][:2], ("JarvisControl.monitor", {"monitor_id": "monitor-1"}))
        self.assertEqual(calls[0][2]["run_number"], 1)
        self.assertEqual(result["results"][0]["outcome"], "function_completed")
        self.assertIn("local_time", result["health"])
        stored = self.store.get(str(heartbeat["heartbeat_id"]))
        self.assertEqual(stored["run_count"], 1)
        self.assertEqual(stored["status"], "COMPLETED")

    def test_control_health_returns_computer_clock_and_never_requires_codex(self) -> None:
        control = JarvisControlHeartbeat(self.config_path)
        receipt = control.invoke_heartbeat("health-1", "heartbeat.health", "test", {})

        self.assertEqual(receipt["status"], "completed")
        self.assertIn("local_time", receipt)
        self.assertIn("utc_time", receipt)
        self.assertEqual(receipt["active_count"], 0)

    def test_prompt_is_rejected_from_the_new_heartbeat_contract(self) -> None:
        invalid = {**self.request(), "prompt": "continue"}
        with self.assertRaisesRegex(ValueError, "prompt"):
            self.store.create(invalid)

    def test_loop_tick_is_an_allowed_scheduler_function(self) -> None:
        request = {**self.request(), "function": "JarvisControl.loop_tick", "arguments": {"loop_id": "loop-1"}}
        heartbeat = self.store.create(request)["heartbeat"]
        self.assertEqual(heartbeat["function_name"], "JarvisControl.loop_tick")

    def test_loop_transitions_do_not_disable_reconciliation(self) -> None:
        heartbeat = self.store.create({**self.request(), "function": "JarvisControl.loop_tick", "max_runs": 20})["heartbeat"]
        for status in ["acquiring"] * 3 + ["stopping", "finalizing"]:
            result = self.store.record(heartbeat, {"status": status})
            self.assertEqual(result["outcome"], "function_completed")
        stored = self.store.get(str(heartbeat["heartbeat_id"]))
        self.assertEqual(stored["status"], "ACTIVE")
        self.assertEqual(stored["failure_count"], 0)
        self.assertEqual(stored["run_count"], 5)

    def test_non_loop_transition_is_still_a_failure(self) -> None:
        heartbeat = self.store.create(self.request())["heartbeat"]
        for _ in range(3):
            self.store.record(heartbeat, {"status": "acquiring"})
        self.assertEqual(self.store.get(str(heartbeat["heartbeat_id"]))["status"], "FAILED")

    def test_cancel_confirms_existing_terminal_state_without_rewriting_it(self) -> None:
        for terminal in ("FAILED", "COMPLETED", "CANCELLED", "RETIRED"):
            with self.subTest(terminal=terminal):
                heartbeat = self.store.create({**self.request(), "heartbeat_id": terminal})["heartbeat"]
                with self.store._session() as db:
                    db.execute("UPDATE jarvis_local_heartbeats SET status=?,next_run_epoch=NULL WHERE heartbeat_id=?", (terminal, terminal))
                control = JarvisControlHeartbeat(self.config_path)
                receipt = control.invoke_heartbeat("cancel", "heartbeat.cancel", "test", {"heartbeat_id": terminal})
                self.assertEqual(receipt["status"], "cancelled" if terminal == "CANCELLED" else "not_required")
                self.assertEqual(receipt["heartbeat"]["status"], terminal)
                self.assertIsNone(receipt["heartbeat"]["next_run_epoch"])
                self.assertEqual(self.store.cancel(terminal), receipt["heartbeat"])

    def test_cancel_missing_or_inconsistently_scheduled_terminal_is_not_success(self) -> None:
        with self.assertRaisesRegex(ValueError, "heartbeat not found"):
            self.store.cancel("missing")
        heartbeat = self.store.create(self.request())["heartbeat"]
        with self.store._session() as db:
            db.execute("UPDATE jarvis_local_heartbeats SET status='FAILED'")
        with self.assertRaisesRegex(ValueError, "not confirmed inactive"):
            self.store.cancel(str(heartbeat["heartbeat_id"]))

    def test_inflight_result_preserves_cancelled_state_at_run_limit(self) -> None:
        heartbeat = self.store.create(self.request())["heartbeat"]
        self.store.cancel(str(heartbeat["heartbeat_id"]))
        self.store.record(heartbeat, {"status": "completed"})
        stored = self.store.get(str(heartbeat["heartbeat_id"]))
        self.assertEqual(stored["status"], "CANCELLED")
        self.assertIsNone(stored["next_run_epoch"])


if __name__ == "__main__":
    unittest.main()
