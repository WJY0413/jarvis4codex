from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from adapters.codex_app_server.mcp_wiring import build_jarvis_control
from jarvis_codex_bridge import StartedTurn, ThreadState, TurnState


class FakeTransport:
    name = "fake-mcp-wiring"

    def health(self):
        return {"status": "ok"}

    def read_thread(self, thread_id):
        return ThreadState(thread_id, "idle", (TurnState("turn-1", "completed"),))

    def resume_existing(self, request):
        return StartedTurn(request.thread_id, "turn-2", "completed")


class McpWiringContractTest(unittest.TestCase):
    def test_wiring_uses_the_supplied_existing_thread_transport(self):
        with tempfile.TemporaryDirectory() as temp:
            launcher_config = Path(temp) / "launcher.json"
            control = build_jarvis_control(
                Path("heartbeat.json"),
                Path(temp),
                launcher_config_path=launcher_config,
                transport_factory=lambda _: FakeTransport(),
            )
            receipt = control.read(subject="thread", task_id="thread-1")
        self.assertEqual(receipt["status"], "completed")
        self.assertEqual(receipt["target_thread_id"], "thread-1")
        self.assertEqual(control._provisioner._config_path, launcher_config)


if __name__ == "__main__":
    unittest.main()
