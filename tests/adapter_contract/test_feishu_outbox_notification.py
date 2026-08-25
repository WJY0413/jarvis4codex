from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from adapters.feishu_outbox_notification import (
    FeishuOutboxNotificationConfig,
    FeishuOutboxNotificationPort,
)


class FeishuOutboxNotificationContractTest(unittest.TestCase):
    def test_notification_requires_exact_delivered_readback(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = FeishuOutboxNotificationConfig(
                python_executable=Path("python"), dispatcher_store_script=Path("store.py"),
                dispatcher_root=root, recipient="user:cooper",
            )
            calls: list[list[str]] = []

            def runner(command, **_kwargs):
                calls.append(command)
                if "enqueue-outbox" in command:
                    (root / "delivery_log.jsonl").write_text(json.dumps({
                        "outbox_id": "outbox-1", "delivery_status": "delivered", "message_id": "om-1",
                    }) + "\n", encoding="utf-8")
                    return subprocess.CompletedProcess(command, 0, json.dumps({
                        "ok": True, "result": {"record": {"outbox_id": "outbox-1"}},
                    }), "")
                raise AssertionError("the notification adapter must not launch a second bridge")

            receipt = FeishuOutboxNotificationPort(config, runner=runner).notify(
                request_id="notify-1", source_ref="test", message="hi",
            )
        self.assertEqual(receipt["status"], "completed")
        self.assertEqual(receipt["message_id"], "om-1")
        self.assertEqual(len(calls), 1)
        self.assertIn("enqueue-outbox", calls[0])

    def test_missing_message_id_is_not_reported_as_delivered(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = FeishuOutboxNotificationConfig(
                python_executable=Path("python"), dispatcher_store_script=Path("store.py"),
                dispatcher_root=root, recipient="user:cooper",
            )

            def runner(command, **_kwargs):
                if "enqueue-outbox" in command:
                    (root / "delivery_log.jsonl").write_text(json.dumps({
                        "outbox_id": "outbox-2", "delivery_status": "unknown", "message_id": None,
                    }) + "\n", encoding="utf-8")
                    return subprocess.CompletedProcess(command, 0, json.dumps({
                        "ok": True, "result": {"record": {"outbox_id": "outbox-2"}},
                    }), "")
                raise AssertionError("the notification adapter must not launch a second bridge")

            receipt = FeishuOutboxNotificationPort(config, runner=runner).notify(
                request_id="notify-2", source_ref="test", message="hi",
            )
        self.assertEqual(receipt["status"], "failed")
        self.assertEqual(receipt["delivery_status"], "unknown")
