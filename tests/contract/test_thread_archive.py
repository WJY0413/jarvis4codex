from pathlib import Path
import tempfile
import unittest

from jarvis_codex_bridge import SQLiteThreadArchive, ThreadState, TurnState


class SQLiteThreadArchiveContractTest(unittest.TestCase):
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
