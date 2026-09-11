from __future__ import annotations

import tempfile
import json
import os
import threading
import time
import unittest
from pathlib import Path

from jarvis_runtime.coo_dispatcher_store import DispatcherStore, ProcessLock


class DispatcherOutboxTest(unittest.TestCase):
    def test_concurrent_dead_owner_reclaim_never_removes_a_new_live_lock(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "request.lock"
            path.write_text(json.dumps({"pid": 987654321}), encoding="utf-8")
            entered, failures, active = [], [], []
            barrier = threading.Barrier(3)
            def run(number):
                try:
                    barrier.wait(timeout=2)
                    with ProcessLock(path, timeout_seconds=2, owner_alive=lambda pid: pid == os.getpid()):
                        self.assertFalse(active)
                        active.append(number)
                        entered.append(number)
                        time.sleep(0.03)
                        active.remove(number)
                except Exception as exc:
                    failures.append(repr(exc))
            threads = [threading.Thread(target=run, args=(number,)) for number in range(3)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=3)
            self.assertEqual(failures, [])
            self.assertEqual(sorted(entered), [0, 1, 2])
            self.assertFalse(path.exists())

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
