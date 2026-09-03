from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from adapters.codex_app_server.task_provisioning_adapter import (
    CodexAppServerTaskProvisioningAdapter,
    append_terminal_turn_history,
)
from jarvis_control import TaskProvisionRequest
from jarvis_control.provisioning import TaskMonitorResumeRequest


class FakeConfig:
    profile = "jarvis_test"
    expected_codex_home = "C:/test/codex-home"

    def resolve_project(self, project: str):
        self.project = project
        return project, "C:/test/project"


class TaskProvisioningAdapterContractTest(unittest.TestCase):
    def test_hold_host_health_accepts_a_fresh_holding_identity_with_a_dead_pid(self):
        config = FakeConfig()
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            config_path = state_dir / "launcher.json"
            (state_dir / "hold-host.json").write_text(json.dumps({
                "status": "holding",
                "pid": 1780,
                "active_count": 3,
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "profile": config.profile,
                "codex_home": config.expected_codex_home,
                "state_dir": str(state_dir.resolve()),
            }), encoding="utf-8")
            adapter = CodexAppServerTaskProvisioningAdapter(
                config_path, state_dir=state_dir, config_loader=lambda _: config
            )

            health = adapter.hold_host_health()

        self.assertEqual(health, {"status": "ready"})

    def test_hold_host_health_blocks_a_stale_identity(self):
        config = FakeConfig()
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            (state_dir / "hold-host.json").write_text(json.dumps({
                "status": "ready",
                "pid": 1780,
                "observed_at": (datetime.now(timezone.utc) - timedelta(seconds=31)).isoformat(),
                "profile": config.profile,
                "codex_home": config.expected_codex_home,
                "state_dir": str(state_dir.resolve()),
            }), encoding="utf-8")
            adapter = CodexAppServerTaskProvisioningAdapter(
                state_dir / "launcher.json", state_dir=state_dir, config_loader=lambda _: config
            )

            health = adapter.hold_host_health()

        self.assertEqual(health["status"], "host_not_ready")
        self.assertEqual(health["reason"], "HoldHost observed_at is stale")

    def test_hold_host_health_blocks_a_profile_mismatch(self):
        config = FakeConfig()
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            (state_dir / "hold-host.json").write_text(json.dumps({
                "status": "ready",
                "pid": 1780,
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "profile": "other",
                "codex_home": config.expected_codex_home,
                "state_dir": str(state_dir.resolve()),
            }), encoding="utf-8")
            adapter = CodexAppServerTaskProvisioningAdapter(
                state_dir / "launcher.json", state_dir=state_dir, config_loader=lambda _: config
            )

            health = adapter.hold_host_health()

        self.assertEqual(health["status"], "host_not_ready")
        self.assertEqual(health["reason"], "HoldHost profile does not match")

    def test_create_queues_for_the_user_host_without_starting_an_app_server(self):
        config = FakeConfig()
        with tempfile.TemporaryDirectory() as temp:
            adapter = CodexAppServerTaskProvisioningAdapter(
                "unused.json", state_dir=Path(temp), config_loader=lambda _: config
            )
            receipt = adapter.provision(TaskProvisionRequest(
                request_id="create-1", project="Jarvis4codex", title="TEST Worker",
                prompt="hello", source_ref="mcp:test", max_turns=2,
            ))
            state = adapter.hold_status("hold-create-1")

        self.assertEqual(receipt.status, "accepted")
        self.assertEqual(receipt.phase, "queued_for_user_host")
        self.assertEqual(receipt.hold_id, "hold-create-1")
        self.assertEqual(receipt.monitor_id, "hold-create-1")
        self.assertEqual(state["status"], "accepted")
        self.assertEqual(state["phase"], "queued_for_user_host")
        self.assertEqual(config.project, "Jarvis4codex")

    def test_existing_active_hold_cannot_be_resumed_a_second_time(self):
        config = FakeConfig()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "task-holds" / "hold-create-1"
            root.mkdir(parents=True)
            (root / "ack.json").write_text(json.dumps({
                "status": "holding", "turn_count": 1, "total_turn_count": 4, "max_turns": 3,
            }), encoding="utf-8")
            adapter = CodexAppServerTaskProvisioningAdapter(
                "unused.json", state_dir=Path(temp), config_loader=lambda _: config
            )
            receipt = adapter.resume_with_monitor(TaskMonitorResumeRequest(
                request_id="resume-2", task_id="thread-created-1", prompt="继续",
                source_ref="mcp:test", hold_id="hold-create-1",
            ))

        self.assertEqual(receipt.status, "failed")
        self.assertIn("already owns", receipt.reason or "")

    def test_resume_archives_the_previous_terminal_receipt_before_queueing(self):
        config = FakeConfig()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "task-holds" / "hold-create-1"
            root.mkdir(parents=True)
            (root / "result.json").write_text(json.dumps({
                "request_id": "create-1", "status": "turn_limit_reached",
                "turn_count": 2, "total_turn_count": 2, "max_turns": 2,
            }), encoding="utf-8")
            adapter = CodexAppServerTaskProvisioningAdapter(
                "unused.json", state_dir=Path(temp), config_loader=lambda _: config
            )
            receipt = adapter.resume_with_monitor(TaskMonitorResumeRequest(
                request_id="resume-2", task_id="thread-created-1", prompt="继续",
                source_ref="mcp:test", hold_id="hold-create-1",
            ))
            queued = adapter.hold_status("hold-create-1")
            archived = root / "history" / "create-1.result.json"
            archived_exists = archived.is_file()

        self.assertEqual(receipt.status, "accepted")
        self.assertEqual(receipt.total_turn_count, 3)
        self.assertEqual(queued["request_id"], "resume-2")
        self.assertTrue(archived_exists)

    def test_status_reads_a_legacy_task_monitor_directory_without_migration(self):
        config = FakeConfig()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "task-monitors" / "monitor-legacy-1"
            root.mkdir(parents=True)
            (root / "result.json").write_text(json.dumps({
                "request_id": "legacy-1", "status": "completed",
            }), encoding="utf-8")
            adapter = CodexAppServerTaskProvisioningAdapter(
                "unused.json", state_dir=Path(temp), config_loader=lambda _: config
            )
            state = adapter.hold_status("monitor-legacy-1")

        self.assertEqual(state["request_id"], "legacy-1")

    def test_read_turn_history_filters_the_existing_sqlite_history(self):
        config = FakeConfig()
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            append_terminal_turn_history(
                state_dir / "turn-history.sqlite", task_id="loop-1", hold_id="loop-1:worker-1",
                request_id="loop-1:worker-1:acquire", thread_id="thread-1", turn_id="turn-1",
                turn_number=1, candidate_id=7, status="holding", final_answer="first",
                completed_at="2026-09-01T00:00:00+00:00",
            )
            append_terminal_turn_history(
                state_dir / "turn-history.sqlite", task_id="loop-1", hold_id="loop-1:worker-1",
                request_id="loop-1:worker-1:acquire", thread_id="thread-1", turn_id="turn-2",
                turn_number=2, candidate_id=9, status="turn_limit_reached", final_answer="second",
                completed_at="2026-09-01T00:01:00+00:00",
            )
            adapter = CodexAppServerTaskProvisioningAdapter(
                "unused.json", state_dir=state_dir, config_loader=lambda _: config
            )
            rows = adapter.read_turn_history(task_id="loop-1", turn_id="turn-2")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["candidate_id"], 9)
        self.assertEqual(rows[0]["final_answer"], "second")

    def test_pending_hold_notification_holds_returns_only_undelivered_events(self):
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            pending = state_dir / "task-holds" / "hold-pending"
            delivered = state_dir / "task-holds" / "hold-delivered"
            unverified = state_dir / "task-holds" / "hold-unverified"
            for root, hold_id in (
                (pending, "hold:pending"),
                (delivered, "hold:delivered"),
                (unverified, "hold:unverified"),
            ):
                root.mkdir(parents=True)
                (root / "request.json").write_text(json.dumps({"hold_id": hold_id}), encoding="utf-8")
                (root / "monitor-events.jsonl").write_text(json.dumps({"event_id": "terminal:turn-1"}) + "\n", encoding="utf-8")
            (delivered / "monitor-notification-deliveries.jsonl").write_text(
                json.dumps({"event_id": "terminal:turn-1", "delivery_status": "delivered", "message_id": "om-1"}) + "\n",
                encoding="utf-8",
            )
            adapter = CodexAppServerTaskProvisioningAdapter(
                "unused.json", state_dir=state_dir, config_loader=lambda _: FakeConfig()
            )

            hold_ids = adapter.pending_hold_notification_holds()

        self.assertEqual(hold_ids, ["hold:pending", "hold:unverified"])

    def test_pending_terminal_discovery_skips_milestones_and_records_legacy_delivery_in_place(self):
        with tempfile.TemporaryDirectory() as temp:
            state_dir = Path(temp)
            root = state_dir / "task-monitors" / "legacy-monitor"
            root.mkdir(parents=True)
            (root / "request.json").write_text(json.dumps({"hold_id": "legacy:monitor"}), encoding="utf-8")
            (root / "monitor-events.jsonl").write_text(
                "\n".join((
                    json.dumps({"event_id": "milestone:turn-1", "event_type": "milestone"}),
                    json.dumps({"event_id": "terminal:turn-1", "event_type": "terminal"}),
                )) + "\n",
                encoding="utf-8",
            )
            adapter = CodexAppServerTaskProvisioningAdapter(
                "unused.json", state_dir=state_dir, config_loader=lambda _: FakeConfig()
            )

            hold_ids = adapter.pending_hold_notification_holds(event_type="terminal")
            adapter.record_hold_notification_delivery(
                "legacy:monitor", "terminal:turn-1",
                {"delivery_status": "delivered", "message_id": "om-legacy"},
            )

            self.assertEqual(hold_ids, ["legacy:monitor"])
            self.assertTrue((root / "monitor-notification-deliveries.jsonl").is_file())
            self.assertFalse((state_dir / "task-holds" / "legacy_monitor").exists())


if __name__ == "__main__":
    unittest.main()
