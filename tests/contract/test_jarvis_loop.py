from __future__ import annotations

import ast
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

from jarvis_control import JarvisControl, LoopController, LoopStore


class FakeRuntime:
    def __init__(self) -> None:
        self.hold_calls: list[dict] = []
        self.heartbeat_calls: list[dict] = []
        self.states = {"hold-loop-contract-1:worker-1": {"status": "holding", "thread_id": "thread-1"}}

    def hold(self, **kwargs):
        self.hold_calls.append(kwargs)
        hold_id = kwargs["hold_id"]
        self.states.setdefault(hold_id, {"status": "holding", "thread_id": kwargs.get("task_id") or "thread-1"})
        return {"status": "holding", "target_thread_id": self.states[hold_id]["thread_id"], "data": {"monitor_id": hold_id}}

    def monitor(self, **kwargs):
        return {"status": "completed", "data": dict(self.states[kwargs["hold_id"]])}

    def heartbeat(self, **kwargs):
        self.heartbeat_calls.append(kwargs)
        return {"status": "active" if kwargs["action"] == "create" else "completed"}


class JarvisLoopContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.runtime = FakeRuntime()
        self.controller = LoopController(LoopStore(Path(self.temp.name) / "loops"))

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_loop_observes_hold_then_resumes_only_after_terminal_readback(self):
        started = self.controller.start(
            self.runtime, request_id="contract-1", project="Jarvis4codex", title="Worker",
            prompt="work", target_thread_count=1, max_rounds=2, max_turns=3,
            expires_at="2099-01-01T00:00:00+00:00",
        )
        self.assertEqual(started.status, "running")
        self.assertEqual(self.runtime.heartbeat_calls[0]["options"]["function"], "JarvisControl.loop_tick")
        self.assertFalse(self.runtime.heartbeat_calls[0]["options"]["start_immediately"])
        hold_id = started.data["children"][0]["hold_id"]
        self.runtime.states[hold_id] = {"status": "completed", "thread_id": "thread-1"}

        resumed = self.controller.tick(self.runtime, loop_id=started.loop_id)

        self.assertEqual(resumed.status, "running")
        self.assertEqual(len(self.runtime.hold_calls), 2)
        self.assertEqual(self.runtime.hold_calls[1]["task_id"], "thread-1")
        self.assertEqual(self.runtime.hold_calls[1]["prompt"], "继续")

        self.runtime.states[hold_id] = {"status": "completed", "thread_id": "thread-1"}
        completed = self.controller.tick(self.runtime, loop_id=started.loop_id)
        self.assertEqual(completed.status, "completed")
        self.assertEqual(self.runtime.heartbeat_calls[-1]["action"], "cancel")

    def test_control_reports_loop_unavailable_without_a_configured_controller(self):
        control = JarvisControl(object(), object())
        receipt = control.loop(action="start", request_id="no-loop")
        self.assertEqual(receipt["tool"], "jarvis_loop")
        self.assertEqual(receipt["status"], "unsupported")

    def test_invalid_existing_state_is_blocked_without_replacing_it(self):
        loop_id = "loop-corrupt"
        path = Path(self.temp.name) / "loops" / loop_id / "state.json"
        path.parent.mkdir(parents=True)
        path.write_text("{}", encoding="utf-8")

        result = self.controller.start(
            self.runtime, request_id="corrupt", project="Jarvis4codex", title="Worker",
            prompt="work", target_thread_count=1, max_rounds=1,
            expires_at="2099-01-01T00:00:00+00:00",
        )

        self.assertEqual(result.status, "blocked")
        self.assertEqual(path.read_text(encoding="utf-8"), "{}")

    def test_mcp_source_registers_loop_without_starting_or_connecting_to_mcp(self):
        server_path = Path(__file__).parents[2] / "packages" / "jarvis_mcp" / "server.py"
        tree = ast.parse(server_path.read_text(encoding="utf-8"))
        functions = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == "jarvis_loop"]
        self.assertEqual(len(functions), 1)
        decorators = functions[0].decorator_list
        names = [keyword.value.value for decorator in decorators if isinstance(decorator, ast.Call)
                 for keyword in decorator.keywords if keyword.arg == "name" and isinstance(keyword.value, ast.Constant)]
        self.assertEqual(names, ["jarvis_loop"])


if __name__ == "__main__":
    unittest.main()
