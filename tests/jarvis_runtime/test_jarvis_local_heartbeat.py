from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest

from jarvis_local_heartbeat import HeartbeatService, JarvisControlHeartbeat, LocalHeartbeatConfig, LocalHeartbeatStore


class LocalHeartbeatTest(unittest.TestCase):
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
