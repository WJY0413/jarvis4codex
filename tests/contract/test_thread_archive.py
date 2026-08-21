from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from jarvis_codex_bridge import SQLiteThreadArchive, ThreadState, TurnState


class SQLiteThreadArchiveContractTest(unittest.TestCase):
    def test_store_closes_connection_when_future_schema_is_rejected(self):
        state = ThreadState("thread-future-schema", "idle", ())
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "thread-library.sqlite"
            connection = sqlite3.connect(path)
            try:
                connection.execute("PRAGMA user_version = 2")
                connection.commit()
            finally:
                connection.close()

            real_connect = sqlite3.connect
            tracked_connections = []

            class TrackingConnection:
                def __init__(self, connection):
                    self.connection = connection
                    self.closed = False

                def close(self):
                    self.closed = True
                    self.connection.close()

                def __getattr__(self, name):
                    return getattr(self.connection, name)

            def tracked_connect(*args, **kwargs):
                connection = TrackingConnection(real_connect(*args, **kwargs))
                tracked_connections.append(connection)
                return connection

            archive = SQLiteThreadArchive(path)
            with patch("jarvis_codex_bridge.archive.sqlite3.connect", side_effect=tracked_connect):
                with self.assertRaisesRegex(RuntimeError, "schema is newer"):
                    archive.store(state)

            try:
                self.assertTrue(tracked_connections[0].closed)
            finally:
                for connection in tracked_connections:
                    if not connection.closed:
                        connection.close()

    def test_rebuilds_a_complete_chinese_thread_after_reopen(self):
        state = ThreadState(
            "thread-zh",
            "idle",
            (
                TurnState(
                    "turn-1",
                    "completed",
                    (
                        {"type": "userMessage", "text": "请整理这段中文对话", "locale": "zh-CN"},
                        {
                            "type": "agentMessage",
                            "phase": "final_answer",
                            "text": "已按原始顺序保存。",
                        },
                    ),
                ),
            ),
        )
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "thread-library.sqlite"
            SQLiteThreadArchive(path).store(state)
            restored = SQLiteThreadArchive(path).rebuild_thread("thread-zh")

        self.assertEqual(restored, state)


if __name__ == "__main__":
    unittest.main()
