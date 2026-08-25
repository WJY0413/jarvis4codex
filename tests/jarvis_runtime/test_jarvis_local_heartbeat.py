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


if __name__ == "__main__":
    unittest.main()
