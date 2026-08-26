from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from adapters.codex_app_server.task_provisioning_adapter import CodexAppServerTaskProvisioningAdapter
from jarvis_control import TaskProvisionRequest
from jarvis_control.provisioning import TaskMonitorResumeRequest


class FakeConfig:
    def resolve_project(self, project: str):
        self.project = project
        return project, "C:/test/project"


class TaskProvisioningAdapterContractTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
