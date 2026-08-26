from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from jarvis_runtime.coo_dispatcher_store import DispatcherStore


class DispatcherOutboxTest(unittest.TestCase):
    def test_allows_five_consecutive_identical_messages_then_blocks_the_sixth(self):
        with tempfile.TemporaryDirectory() as temp:
            store = DispatcherStore(Path(temp))
            store.bootstrap("thread-1")
            request = {
                "task_id": "task-1",
                "source_thread_id": "thread-1",
                "status": "notification",
                "recipient": "user:cooper",
                "content": "same message",
            }
            queued = [store.enqueue_outbox(request) for _ in range(5)]
            blocked = store.enqueue_outbox(request)

        self.assertTrue(all(item["queued"] for item in queued))
        self.assertTrue(blocked["duplicate"])
        self.assertEqual(blocked["reason"], "consecutive_identical_message_limit")


if __name__ == "__main__":
    unittest.main()
