from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import unittest
from pathlib import Path

from mcp import Client

from jarvis_codex_bridge import (
    ExistingThreadBridge,
    JarvisCapabilityPort,
    JsonlReceiptJournal,
    StartedTurn,
    ThreadState,
    ThreadTerminalMonitor,
    TurnState,
)
from jarvis_control import JarvisControl
from jarvis_mcp import JarvisMcpServer


class SharedHttpMcpContractTest(unittest.TestCase):
    """Real HTTP acceptance; synthetic control never accesses runtime state."""

    def test_shared_process_clients_security_and_cleanup(self):
        import http.client

        with socket.socket() as reserve:
            reserve.bind(("127.0.0.1", 0))
            port = reserve.getsockname()[1]
        script = '''
import os, sys, time
from jarvis_mcp import JarvisMcpServer
class Control:
    def __init__(self):
        self.active = 0
        self.maximum = 0
    def read(self, **kwargs):
        self.active += 1
        self.maximum = max(self.maximum, self.active)
        time.sleep(0.03)
        self.active -= 1
        return {"status": "ok", "pid": os.getpid(), "maximum": self.maximum}
    jarvis_close = read
JarvisMcpServer(Control()).run_http(port=int(sys.argv[1]))
'''
        # Windows venv python is a launcher; use its base interpreter with the
        # venv packages so the owned PID is the actual HTTP service PID.
        bootstrap = f"import site; site.addsitedir({str(Path(sys.prefix) / 'Lib' / 'site-packages')!r});\n"
        executable = getattr(sys, "_base_executable", sys.executable) if os.name == "nt" else sys.executable
        command = [executable, "-c", bootstrap + script, str(port)]
        url = f"http://127.0.0.1:{port}/mcp"
        def request(method="GET", headers=None):
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
            try:
                connection.request(method, "/mcp", body="{}" if method == "POST" else None,
                                   headers=headers or {})
                response = connection.getresponse()
                response.read()
                return response.status
            finally:
                connection.close()
        receipt = {"transport": "streamable-http", "client_count": 3}
        with tempfile.TemporaryFile(mode="w+") as log:
            process = subprocess.Popen(command, stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 15
                while True:
                    if process.poll() is not None:
                        log.seek(0)
                        self.fail(log.read())
                    try:
                        request()
                        break
                    except OSError:
                        if time.monotonic() > deadline:
                            self.fail("HTTP startup timeout")
                        time.sleep(0.1)

                async def exercise():
                    async with Client(url) as first, Client(url) as second:
                        names = [tool.name for tool in (await first.list_tools()).tools]
                        self.assertIn("jarvis_read", names)
                        self.assertEqual(len(names), 10)
                        self.assertIn("jarvis_close", names)
                        results = await asyncio.gather(*[
                            client.call_tool(tool, arguments)
                            for client, tool, arguments in (
                                (first, "jarvis_read", {"subject": "capabilities"}),
                                (second, "jarvis_close", {"hold_id": "synthetic-hold"}),
                            )
                        ])
                        async with Client(url) as third:
                            results.append(await third.call_tool("jarvis_read", {"subject": "capabilities"}))
                        results.append(await second.call_tool("jarvis_read", {"subject": "capabilities"}))
                        return [result.structured_content for result in results]

                results = asyncio.run(exercise())
                self.assertEqual({result["pid"] for result in results}, {process.pid})
                self.assertEqual({result["maximum"] for result in results}, {1})
                self.assertIsNone(process.poll())
                bad_host = request("POST", {"host": "evil.example"})
                bad_origin = request("POST", {"origin": "https://evil.example"})
                self.assertIn(bad_host, (400, 403, 421))
                self.assertIn(bad_origin, (400, 403, 421))
                duplicate = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=15)
                self.assertNotEqual(duplicate.returncode, 0)
                self.assertIsNone(process.poll())
                receipt.update(pid=process.pid, readbacks=results, disconnect_survived=True,
                               unsafe_host=bad_host, unsafe_origin=bad_origin,
                               duplicate_listener_exit=duplicate.returncode)
            finally:
                process.terminate()
                process.wait(timeout=10)
                receipt["process_cleaned_up"] = process.poll() is not None
        target = os.environ.get("JARVIS_HTTP_E2E_RECEIPT")
        if target:
            Path(target).write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")


class FakeTransport:
    name = "fake-codex"

    def __init__(self) -> None:
        self.state = ThreadState("thread-1", "idle", (TurnState("turn-1", "completed"),))
        self.prompts: list[str] = []

    def health(self):
        return {"adapter": self.name, "status": "ok"}

    def read_thread(self, thread_id):
        self.assert_thread(thread_id)
        return self.state

    def resume_existing(self, request):
        self.assert_thread(request.thread_id)
        self.prompts.append(request.prompt)
        self.state = ThreadState(
            request.thread_id,
            "idle",
            (*self.state.turns, TurnState("turn-2", "completed", (
                {"type": "agentMessage", "phase": "final_answer", "text": "continued"},
            ))),
        )
        return StartedTurn(request.thread_id, "turn-2", "completed")

    @staticmethod
    def assert_thread(thread_id):
        if thread_id != "thread-1":
            raise AssertionError(thread_id)


class FakeHeartbeat:
    def __init__(self) -> None:
        self.calls = []

    def invoke_heartbeat(self, request_id, capability, source_ref, arguments):
        self.calls.append((request_id, capability, source_ref, dict(arguments)))
        return {"status": "active", "heartbeat_id": arguments.get("heartbeat_id")}


class JarvisMcpContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.transport = FakeTransport()
        bridge = ExistingThreadBridge(
            self.transport, JsonlReceiptJournal(Path(self.temp.name) / "receipts.jsonl")
        )
        self.heartbeat = FakeHeartbeat()
        capability_port = JarvisCapabilityPort(
            bridge,
            ThreadTerminalMonitor(self.transport, Path(self.temp.name) / "monitor.json"),
            self.heartbeat,
        )
        self.bridge = bridge
        self.capability_port = capability_port
        self.server = JarvisMcpServer(JarvisControl(capability_port, bridge))

    def tearDown(self) -> None:
        self.temp.cleanup()

    def list_tools(self):
        async def run():
            async with Client(self.server.mcp) as client:
                return await client.list_tools()
        return asyncio.run(run())

    def test_initialize_reports_the_current_mcp_version(self):
        metadata = tomllib.loads((Path(__file__).resolve().parents[2] / "pyproject.toml").read_text(encoding="utf-8"))
        expected_version = metadata["project"]["version"]
        self.assertEqual(expected_version, "0.2.5")
        self.assertEqual(self.server.mcp.version, expected_version)

        async def initialize():
            async with Client(self.server.mcp, mode="legacy") as client:
                return client.session.server_info.version

        self.assertEqual(asyncio.run(initialize()), expected_version)

    def test_loop_mcp_preserves_nested_output_schema(self):
        from unittest.mock import Mock
        control = Mock()
        control.loop.return_value = {"status": "invalid_request", "reason": "capture only"}
        self.server = JarvisMcpServer(control)
        schema = {"$defs": {"score": {"type": "integer"}},
                  "properties": {"score": {"$ref": "#/$defs/score"}}, "required": ["score"]}
        threads = [{"slot": "one", "lane": {"result_verification": {"mode": "final_answer_json", "output_schema": schema}}}]
        self.call("jarvis_loop", {"action": "start", "threads": threads})
        self.assertEqual(control.loop.call_args.kwargs["threads"], threads)
        tool = next(tool for tool in self.list_tools().tools if tool.name == "jarvis_loop")
        self.assertIn("output_schema", tool.input_schema["properties"]["threads"]["description"])
        lane = tool.input_schema["properties"]["threads"]["anyOf"][0]["items"]["properties"]["lane"]
        self.assertEqual(lane["properties"]["batch_size"], {"type": "integer", "minimum": 1, "default": 1})
        self.assertEqual(lane["properties"]["result_verification"]["properties"]["mode"]["enum"],
                         ["file_receipt", "final_answer_json"])

    def call(self, name, arguments):
        async def run():
            async with Client(self.server.mcp) as client:
                return await client.call_tool(name, arguments)
        return asyncio.run(run())

    def test_lists_the_public_jarvis_tools_with_sdk_generated_schema(self):
        result = self.list_tools()
        self.assertEqual(
            [tool.name for tool in result.tools],
            [
                "jarvis_create",
                "jarvis_hold",
                "jarvis_loop",
                "jarvis_close",
                "jarvis_read",
                "jarvis_resume",
                "jarvis_monitor",
                "jarvis_heartbeat",
                "jarvis_update",
                "jarvis_notify",
            ],
        )
        self.assertTrue(all(tool.input_schema["type"] == "object" for tool in result.tools))
        read_tool = next(tool for tool in result.tools if tool.name == "jarvis_read")
        self.assertTrue(read_tool.annotations.read_only_hint)
        loop_tool = next(tool for tool in result.tools if tool.name == "jarvis_loop")
        self.assertTrue({"business_skill", "controller_skill"}.issubset(loop_tool.input_schema["properties"]))
        self.assertIn("preflight", loop_tool.input_schema["properties"]["action"]["enum"])
        self.assertEqual(loop_tool.input_schema["properties"]["prompt"]["description"], "Required when action=start.")
        self.assertTrue({"continue_prompt", "turns_per_thread"}.issubset(loop_tool.input_schema["properties"]))
        self.assertIn("Optional business Skill", loop_tool.input_schema["properties"]["business_skill"]["description"])
        self.assertIn("action=start requires a prompt", loop_tool.description)

    def test_close_mcp_drains_queued_hold_or_loop_without_turn_id(self):
        from jarvis_control import LoopController, LoopStore
        from adapters.codex_app_server.task_provisioning_adapter import CodexAppServerTaskProvisioningAdapter
        from adapters.codex_app_server.jarvis_task_hold_host import hold_task
        for target in ("hold", "loop"):
            with self.subTest(target=target):
                root = Path(self.temp.name) / target
                hold_root = root / "task-holds" / "close-test"
                hold_root.mkdir(parents=True)
                request, ack, result = (hold_root / name for name in ("request.json", "ack.json", "result.json"))
                request.write_text(json.dumps({"request_id": "close-test", "hold_id": "close-test"}), encoding="utf-8")
                ack.write_text(json.dumps({"status": "accepted", "hold_id": "close-test"}), encoding="utf-8")
                store = LoopStore(root / "loops")
                store.create("close-loop", {"schema": "jarvis-loop-state/v1", "loop_id": "close-loop",
                    "status": "running", "heartbeat_id": None,
                    "children": [{"slot": "one", "hold_id": "close-test"}]})
                adapter = CodexAppServerTaskProvisioningAdapter(root / "unused", state_dir=root)
                self.server = JarvisMcpServer(JarvisControl(self.capability_port, self.bridge,
                    provisioner=adapter, loop_controller=LoopController(store)))
                args = {"hold_id": "close-test"} if target == "hold" else {"loop_id": "close-loop"}
                pending = self.call("jarvis_close", args).structured_content
                self.assertEqual(pending["status"], "closing")
                close_path = hold_root / "close.json"
                self.assertEqual(json.loads(close_path.read_text())["report_status"], "pending")
                if target == "loop":
                    self.assertEqual(store.load("close-loop")["close"]["report_status"], "pending")
                self.assertFalse(pending["readback"]["terminal"])
                self.assertTrue(json.loads(request.read_text())["stop_requested"])
                self.assertFalse(result.exists())
                self.assertEqual(hold_task(root / "unused", request, ack, result), 0)
                self.assertEqual(json.loads(close_path.read_text())["report_status"], "completed")
                if target == "loop":
                    self.assertEqual(store.load("close-loop")["close"]["report_status"], "completed")
                finished = self.call("jarvis_close", args).structured_content
                self.assertEqual(finished["status"], "closed")
                close_report = json.loads(close_path.read_text())
                self.assertEqual(close_report["report_status"], "completed")
                self.assertEqual(close_report["execution_state"], "terminal")
                self.assertTrue(close_report["scheduling_closed"])
                if target == "loop":
                    self.assertEqual(store.load("close-loop")["close"]["report_status"], "completed")
                self.assertTrue(finished["readback"]["terminal"])
                self.assertEqual(finished["tool"], "jarvis_close")
                snapshots = [path.read_bytes() for path in (request, ack, result)]
                self.assertEqual(self.call("jarvis_close", args).structured_content["status"], "closed")
                self.assertEqual(snapshots, [path.read_bytes() for path in (request, ack, result)])
                self.assertEqual(close_report, json.loads(close_path.read_text()))
                self.assertEqual(json.loads(result.read_text())["phase"], "stopped_before_dispatch")
                self.assertEqual(self.transport.prompts, [])
                print("JARVIS_CLOSE_E2E_RECEIPT " + json.dumps({"target": target,
                    "initial": pending["status"], "final": finished["status"], "turns_started": 0,
                    "repeat_idempotent": True, "production_state_used": False}))

    def test_close_reports_persistence_failure_and_cancels_unclaimed_queue_without_host(self):
        from unittest.mock import patch
        from adapters.codex_app_server.task_provisioning_adapter import CodexAppServerTaskProvisioningAdapter
        root = Path(self.temp.name)
        target = root / "task-holds" / "offline-queued"
        target.mkdir(parents=True)
        request = target / "request.json"
        request.write_text(json.dumps({"hold_id": "offline-queued", "request_id": "queued-v1"}))
        (target / "ack.json").write_text(json.dumps({"hold_id": "offline-queued", "request_id": "queued-v1",
            "status": "accepted", "phase": "queued_for_user_host"}))
        adapter = CodexAppServerTaskProvisioningAdapter(root / "unused", state_dir=root)
        self.server = JarvisMcpServer(JarvisControl(self.capability_port, self.bridge, provisioner=adapter))
        before = request.read_bytes()
        with patch("adapters.codex_app_server.task_provisioning_adapter._write_json", side_effect=PermissionError("disk denied")):
            failed = self.call("jarvis_close", {"hold_id": "offline-queued"}).structured_content
        self.assertEqual(failed["status"], "failed")
        self.assertIn("disk denied", failed["reason"])
        self.assertEqual(request.read_bytes(), before)
        self.assertFalse((target / "result.json").exists())
        # Crash window: intent persisted but request stop was not written.
        from adapters.codex_app_server.task_provisioning_adapter import _write_json
        def fail_latch(path, value):
            if path == request:
                raise PermissionError("stop latch denied")
            _write_json(path, value)
        with patch("adapters.codex_app_server.task_provisioning_adapter._write_json", side_effect=fail_latch):
            failed = self.call("jarvis_close", {"hold_id": "offline-queued"}).structured_content
        self.assertEqual(failed["status"], "failed")
        self.assertTrue((target / "close.json").exists())
        self.assertEqual(request.read_bytes(), before)
        from adapters.codex_app_server.jarvis_task_hold_host import hold_task
        self.assertEqual(hold_task(root / "unused", request, target / "ack.json", target / "result.json"), 0)
        receipt = self.call("jarvis_close", {"hold_id": "offline-queued"}).structured_content
        self.assertEqual(receipt["status"], "closed")
        report = json.loads((target / "close.json").read_text())
        self.assertEqual(report["request_id"], "queued-v1")
        self.assertEqual(report["report_status"], "completed")
        self.assertEqual(json.loads((target / "result.json").read_text())["phase"], "stopped_before_dispatch")
        # A reused Hold directory must not inherit the prior attempt's closure.
        request.write_text(json.dumps({"hold_id": "offline-queued", "request_id": "queued-v2"}))
        from adapters.codex_app_server.task_provisioning_adapter import refresh_close_report
        refresh_close_report(target, {"request_id": "queued-v1", "terminal_confirmed": True, "hold_released": True})
        self.assertEqual(report, json.loads((target / "close.json").read_text()))
        print("JARVIS_CLOSE_DURABLE_RECEIPT " + json.dumps({"queued_without_host": "closed",
            "failed_write_no_effects": True, "stale_owner_report_ignored": True}))

    def test_close_qa_binding_recovery_and_failure_receipts(self):
        from types import SimpleNamespace
        from jarvis_control import LoopController, LoopStore
        from adapters.codex_app_server.task_provisioning_adapter import CodexAppServerTaskProvisioningAdapter
        root = Path(self.temp.name)
        adapter = CodexAppServerTaskProvisioningAdapter(root / "unused", state_dir=root)
        for case in ("recover", "stale"):
            with self.subTest(case=case):
                target = root / "task-holds" / case
                target.mkdir(parents=True)
                request = {"hold_id": case, "request_id": "new"}
                if case == "recover":
                    request.update(mode="recover", thread_id="existing-thread", turn_id="existing-active-turn")
                (target / "request.json").write_text(json.dumps(request))
                (target / "ack.json").write_text(json.dumps({"hold_id": case, "request_id": "new",
                    "status": "accepted", "phase": "queued_for_user_host"}))
                if case == "stale":
                    (target / "result.json").write_text(json.dumps({"hold_id": case, "request_id": "old",
                        "status": "completed", "terminal_confirmed": True}))
                before = {p.name: p.read_bytes() for p in target.glob("*.json")}
                self.server = JarvisMcpServer(JarvisControl(self.capability_port, self.bridge, provisioner=adapter))
                receipt = self.call("jarvis_close", {"hold_id": case}).structured_content
                report = json.loads((target / "close.json").read_text())
                self.assertFalse(report["terminal_confirmed"])
                self.assertFalse(report["hold_released"])
                if case == "recover":
                    self.assertEqual(receipt["status"], "closed_unconfirmed")
                    self.assertEqual(report["turn_id"], "existing-active-turn")
                    self.assertTrue(report["external_execution_unresolved"])
                else:
                    self.assertEqual(receipt["status"], "failed")
                    self.assertFalse(receipt["readback"]["verified"])
                    self.assertFalse(adapter.hold_status(case)["hold_released"])
                    for name, content in before.items():
                        self.assertEqual((target / name).read_bytes(), content)
                    store = LoopStore(root / "loops")
                    store.create("stale-loop", {"schema": "jarvis-loop-state/v1", "loop_id": "stale-loop",
                        "status": "running", "heartbeat_id": None, "children": [{"slot": "one", "hold_id": case}]})
                    self.server = JarvisMcpServer(JarvisControl(self.capability_port, self.bridge,
                        provisioner=adapter, loop_controller=LoopController(store)))
                    loop_receipt = self.call("jarvis_close", {"loop_id": "stale-loop"}).structured_content
                    self.assertEqual(loop_receipt["status"], "failed")
                    self.assertEqual(store.load("stale-loop")["close"]["report_status"], "failed")
        for failure in ("failed", "unsupported"):
            for loop in (False, True):
                with self.subTest(failure=failure, loop=loop):
                    store = LoopStore(root / f"loops-{failure}")
                    if loop:
                        store.create("l", {"schema": "jarvis-loop-state/v1", "loop_id": "l", "status": "running",
                            "heartbeat_id": None, "children": [{"slot": "one", "hold_id": "h"}]})
                    provisioner = SimpleNamespace(close_hold=lambda *a, **k: {"status": failure, "reason": "durable intent unavailable"})
                    self.server = JarvisMcpServer(JarvisControl(self.capability_port, self.bridge,
                        provisioner=provisioner, loop_controller=LoopController(store)))
                    receipt = self.call("jarvis_close", {"loop_id": "l"} if loop else {"hold_id": "h"}).structured_content
                    self.assertEqual(receipt["status"], "failed" if loop else failure)
                    self.assertFalse(receipt["readback"]["verified"])
                    self.assertFalse(receipt["readback"].get("terminal", False))
                    self.assertIn("durable intent unavailable", receipt["reason"])
                    if loop:
                        state = store.load("l")
                        self.assertEqual(state["cleanup"]["status"], "pending")
                        self.assertEqual(state["close"]["report_status"], "failed")
        print("JARVIS_CLOSE_QA_ATTEMPT2 " + json.dumps({"recover": "closed_unconfirmed",
            "stale_result": "failed_preserved", "child_failures": "explicit"}))

    def test_close_rejects_ambiguous_targets_and_never_confirms_unknown_execution(self):
        from jarvis_control import LoopController, LoopStore
        from adapters.codex_app_server.task_provisioning_adapter import CodexAppServerTaskProvisioningAdapter
        root = Path(self.temp.name)
        hold_root = root / "task-holds" / "unknown-test"
        hold_root.mkdir(parents=True)
        request, result = (hold_root / name for name in ("request.json", "result.json"))
        request.write_text(json.dumps({"request_id": "unknown", "hold_id": "unknown-test"}), encoding="utf-8")
        result.write_text(json.dumps({"status": "failed", "terminal_confirmed": False, "turn_id": ""}), encoding="utf-8")
        adapter = CodexAppServerTaskProvisioningAdapter(root / "unused", state_dir=root)
        self.server = JarvisMcpServer(JarvisControl(self.capability_port, self.bridge, provisioner=adapter))
        before = request.read_bytes()
        for args in ({}, {"hold_id": "unknown-test", "loop_id": "other"}):
            self.assertEqual(self.call("jarvis_close", args).structured_content["status"], "invalid_request")
            self.assertEqual(request.read_bytes(), before)
        self.assertEqual(self.call("jarvis_close", {"hold_id": "missing"}).structured_content["status"], "failed")
        pending = self.call("jarvis_close", {"hold_id": "unknown-test"}).structured_content
        self.assertEqual(pending["status"], "closing")
        self.assertFalse(pending["readback"]["terminal"])
        self.assertFalse(pending["data"]["hold_released"])
        self.assertFalse(json.loads(result.read_text())["terminal_confirmed"])

        # QA P1: a legacy startup failure lacks terminal_confirmed entirely.
        legacy = {"status": "failed", "turn_id": "", "reason": "thread/resume already has an active writer"}
        result.write_text(json.dumps(legacy), encoding="utf-8")
        original = result.read_bytes()
        store = LoopStore(root / "loops")
        store.create("unknown-loop", {"schema": "jarvis-loop-state/v1", "loop_id": "unknown-loop",
            "status": "running", "heartbeat_id": None, "children": [{"slot": "one", "hold_id": "unknown-test"}]})
        self.server = JarvisMcpServer(JarvisControl(self.capability_port, self.bridge,
            provisioner=adapter, loop_controller=LoopController(store)))
        for args in ({"hold_id": "unknown-test"}, {"loop_id": "unknown-loop"}):
            pending = self.call("jarvis_close", args).structured_content
            self.assertEqual(pending["status"], "closing")
            self.assertFalse(pending["readback"]["terminal"])
            self.assertEqual(result.read_bytes(), original)
        state = store.load("unknown-loop")
        self.assertEqual(state["cleanup"]["status"], "pending")
        self.assertFalse(state["children"][0]["hold_released"])
        self.assertFalse(adapter.hold_status("unknown-test")["hold_released"])
        # Persisted output of the rejected version must not remain falsely closed.
        legacy_state = store.load("unknown-loop")
        legacy_state["status"] = "stopped"
        legacy_state["cleanup"]["status"] = "completed"
        legacy_state["children"][0]["hold_released"] = True
        store.save("unknown-loop", legacy_state)
        self.assertNotIn("unknown-loop", store.active_loop_ids())
        pending = self.call("jarvis_close", {"loop_id": "unknown-loop"}).structured_content
        self.assertEqual(pending["status"], "closing")
        self.assertFalse(pending["readback"]["terminal"])
        self.assertEqual(store.load("unknown-loop")["cleanup"]["status"], "pending")
        self.assertFalse(store.load("unknown-loop")["children"][0]["hold_released"])
        self.assertEqual(result.read_bytes(), original)
        print("JARVIS_CLOSE_E2E_RECEIPT " + json.dumps({"case": "legacy_no_turn_unknown",
            "hold_close": "closing", "loop_close": "closing", "cleanup": "pending",
            "hold_released": False, "history_bytes_preserved": True}))

    def test_resume_without_a_monitor_owned_adapter_reports_unsupported(self):
        result = self.call(
            "jarvis_resume",
            {"task_id": "thread-1", "prompt": "continue exactly once", "request_id": "resume-1"},
        )
        self.assertTrue(result.is_error)
        receipt = result.structured_content
        self.assertEqual(receipt["tool"], "jarvis_resume")
        self.assertEqual(receipt["status"], "unsupported")
        self.assertEqual(self.transport.prompts, [])

    def test_resume_always_starts_a_monitor_owned_run(self):
        class Provisioner:
            def resume_with_monitor(self, request):
                from jarvis_control import TaskProvisionReceipt
                from jarvis_control.provisioning import observed_now
                self.request = request
                return TaskProvisionReceipt(
                    request_id=request.request_id, status="holding", observed_at=observed_now(),
                    thread_id=request.task_id, turn_id="turn-2", monitor_id="monitor-resume-1",
                    turn_count=1, max_turns=2,
                )

        provisioner = Provisioner()
        server = JarvisMcpServer(JarvisControl(self.capability_port, self.bridge, provisioner))

        async def run():
            async with Client(server.mcp) as client:
                return await client.call_tool("jarvis_resume", {
                    "task_id": "thread-1", "prompt": "继续", "request_id": "resume-1",
                    "hold_with_monitor": False, "max_turns": 2,
                })

        result = asyncio.run(run())
        self.assertFalse(result.is_error)
        self.assertEqual(result.structured_content["status"], "holding")
        self.assertEqual(provisioner.request.max_turns, 2)

    def test_invalid_resume_request_returns_a_structured_error_receipt(self):
        result = self.call(
            "jarvis_resume",
            {"task_id": "thread-1", "prompt": "", "request_id": "resume-empty"},
        )
        self.assertTrue(result.is_error)
        receipt = result.structured_content
        self.assertEqual(receipt["status"], "invalid_request")
        self.assertIn("prompt", receipt["reason"])

    def test_read_capabilities_is_a_read_only_tool_result(self):
        result = self.call("jarvis_read", {"subject": "capabilities"})
        receipt = result.structured_content
        self.assertFalse(result.is_error)
        self.assertEqual(receipt["status"], "completed")
        self.assertFalse(receipt["data"]["jarvis_resume"]["available"])
        self.assertTrue(receipt["data"]["jarvis_resume"]["requires_monitor"])
        self.assertFalse(receipt["data"]["jarvis_create"]["available"])

    def test_read_history_uses_the_managed_hold_history_adapter(self):
        class Provisioner:
            def read_turn_history(self, **filters):
                self.filters = filters
                return [{"task_id": "loop-1", "turn_id": "turn-1", "final_answer": "done"}]

        provisioner = Provisioner()
        server = JarvisMcpServer(JarvisControl(self.capability_port, self.bridge, provisioner))

        async def run():
            async with Client(server.mcp) as client:
                return await client.call_tool(
                    "jarvis_read", {"subject": "history", "task_id": "loop-1", "turn_id": "turn-1"}
                )

        result = asyncio.run(run())
        self.assertFalse(result.is_error)
        self.assertEqual(result.structured_content["data"]["turns"][0]["final_answer"], "done")
        self.assertEqual(provisioner.filters["turn_id"], "turn-1")

    def test_monitor_observe_returns_its_observation_receipt(self):
        result = self.call(
            "jarvis_monitor",
            {
                "action": "observe",
                "request_id": "monitor-1",
                "monitor_id": "monitor-1",
                "observed_task_id": "thread-1",
                "receipt_task_id": "thread-1",
            },
        )
        receipt = result.structured_content
        self.assertFalse(result.is_error)
        self.assertEqual(receipt["status"], "baseline_terminal")
        self.assertEqual(receipt["tool"], "jarvis_monitor")

    def test_heartbeat_delegates_to_the_existing_heartbeat_port(self):
        result = self.call(
            "jarvis_heartbeat",
            {"action": "create", "heartbeat_id": "daily-check", "request_id": "heartbeat-1"},
        )
        receipt = result.structured_content
        self.assertFalse(result.is_error)
        self.assertEqual(receipt["status"], "active")
        self.assertEqual(self.heartbeat.calls[0][1], "heartbeat.create")

    def test_heartbeat_without_a_scheduler_reports_unsupported(self):
        bridge = ExistingThreadBridge(
            self.transport, JsonlReceiptJournal(Path(self.temp.name) / "no-heartbeat.jsonl")
        )
        no_scheduler = JarvisMcpServer(JarvisControl(
            JarvisCapabilityPort(
                bridge,
                ThreadTerminalMonitor(self.transport, Path(self.temp.name) / "no-heartbeat-monitor.json"),
            ),
            bridge,
        ))

        async def run():
            async with Client(no_scheduler.mcp) as client:
                return await client.call_tool(
                    "jarvis_heartbeat", {"action": "health", "request_id": "no-scheduler"}
                )

        result = asyncio.run(run())
        self.assertTrue(result.is_error)
        self.assertEqual(result.structured_content["status"], "unsupported")

    def test_create_and_notify_report_unavailable_adapters_without_false_success(self):
        create = self.call("jarvis_create", {
            "project": "Jarvis4codex", "title": "test", "prompt": "test", "request_id": "create-unsupported"
        })
        notify = self.call("jarvis_notify", {"message": "internal test"})
        self.assertTrue(create.is_error)
        self.assertTrue(notify.is_error)
        self.assertEqual(create.structured_content["status"], "unsupported")
        self.assertEqual(notify.structured_content["status"], "unsupported")

    def test_hold_is_the_public_managed_lifecycle_entry(self):
        class Provisioner:
            def provision(self, request):
                from jarvis_control import TaskProvisionReceipt
                from jarvis_control.provisioning import observed_now
                self.request = request
                return TaskProvisionReceipt(
                    request_id=request.request_id, status="accepted", observed_at=observed_now(),
                    hold_id=request.hold_id or "hold-create-1", monitor_id=request.hold_id or "hold-create-1",
                )

        provisioner = Provisioner()
        server = JarvisMcpServer(JarvisControl(self.capability_port, self.bridge, provisioner))

        async def run():
            async with Client(server.mcp) as client:
                return await client.call_tool("jarvis_hold", {
                    "project": "Jarvis4codex", "title": "TEST Worker", "prompt": "hello",
                    "request_id": "hold-1", "hold_id": "hold-contract-1",
                    "auto_continue": True, "notifications": {"milestones": [2], "terminal": True},
                })

        result = asyncio.run(run())
        self.assertFalse(result.is_error)
        self.assertEqual(result.structured_content["tool"], "jarvis_hold")
        self.assertEqual(provisioner.request.hold_id, "hold-contract-1")
        self.assertTrue(provisioner.request.auto_continue)
        self.assertEqual(provisioner.request.notifications, {"milestones": [2], "terminal": True})

    def test_create_delegates_to_a_configured_provisioner_and_returns_exact_identity(self):
        class Provisioner:
            def provision(self, request):
                from jarvis_control import TaskProvisionReceipt
                from jarvis_control.provisioning import observed_now
                self.request = request
                return TaskProvisionReceipt(
                    request_id=request.request_id,
                    status="holding",
                    observed_at=observed_now(),
                    thread_id="thread-created-1",
                    turn_id="turn-created-1",
                    monitor_id="monitor-create-1",
                    turn_count=1,
                    max_turns=2,
                )

        provisioner = Provisioner()
        server = JarvisMcpServer(JarvisControl(self.capability_port, self.bridge, provisioner))

        async def run():
            async with Client(server.mcp) as client:
                return await client.call_tool("jarvis_create", {
                    "project": "Jarvis4codex",
                    "title": "TEST Worker",
                    "prompt": "hello",
                    "request_id": "create-1",
                    "max_turns": 2,
                })

        result = asyncio.run(run())
        self.assertFalse(result.is_error)
        self.assertEqual(result.structured_content["status"], "holding")
        self.assertEqual(result.structured_content["target_thread_id"], "thread-created-1")
        self.assertEqual(provisioner.request.title, "TEST Worker")
        self.assertEqual(provisioner.request.max_turns, 2)

    def test_monitor_status_reads_the_registered_turn_counter(self):
        class Provisioner:
            def monitor_status(self, monitor_id):
                return {"monitor_id": monitor_id, "status": "running", "turn_count": 1, "max_turns": 2}

        server = JarvisMcpServer(JarvisControl(self.capability_port, self.bridge, Provisioner()))

        async def run():
            async with Client(server.mcp) as client:
                return await client.call_tool("jarvis_monitor", {
                    "action": "status",
                    "request_id": "monitor-status-1",
                    "monitor_id": "monitor-create-1",
                })

        result = asyncio.run(run())
        self.assertFalse(result.is_error)
        self.assertEqual(result.structured_content["data"]["turn_count"], 1)
        self.assertEqual(result.structured_content["data"]["max_turns"], 2)

    def test_monitor_status_verifies_an_exact_terminal_hold_readback(self):
        class Provisioner:
            def hold_status(self, hold_id):
                return {
                    "hold_id": hold_id, "status": "turn_limit_reached",
                    "thread_id": "thread-1", "turn_id": "turn-1", "final_message": "done",
                }

        server = JarvisMcpServer(JarvisControl(self.capability_port, self.bridge, Provisioner()))

        async def run():
            async with Client(server.mcp) as client:
                return await client.call_tool("jarvis_monitor", {
                    "action": "status", "request_id": "terminal-status-1",
                    "hold_id": "hold-terminal-1",
                })

        result = asyncio.run(run())
        self.assertFalse(result.is_error)
        self.assertEqual(result.structured_content["status"], "completed")
        self.assertTrue(result.structured_content["readback"]["verified"])
        self.assertTrue(result.structured_content["readback"]["terminal"])

    def test_monitor_delivers_pending_hold_notifications_with_saved_readback(self):
        class Provisioner:
            def __init__(self):
                self.recorded = []

            def pending_hold_notifications(self, hold_id):
                self.hold_id = hold_id
                return [{
                    "event_id": "terminal:turn-1",
                    "message": "JARVIS_HOLD_TERMINAL_V1 hold-1 completed",
                }]

            def record_hold_notification_delivery(self, hold_id, event_id, delivery):
                self.recorded.append((hold_id, event_id, delivery))

        class Notifier:
            def notify(self, **kwargs):
                self.kwargs = kwargs
                return {"delivery_status": "delivered", "message_id": "feishu-1"}

        provisioner = Provisioner()
        notifier = Notifier()
        control = JarvisControl(self.capability_port, self.bridge, provisioner, notifier)
        receipt = control.monitor(
            action="deliver_hold_notifications", request_id="notify-1", source_ref="mcp:test",
            hold_id="hold-1",
        )

        self.assertEqual(receipt["status"], "completed")
        self.assertTrue(receipt["readback"]["verified"])
        self.assertEqual(provisioner.recorded[0][0:2], ("hold-1", "terminal:turn-1"))
        self.assertEqual(notifier.kwargs["request_id"], "notify-1:terminal:turn-1")

    def test_terminal_notification_drain_discovers_and_delivers_pending_holds(self):
        class Provisioner:
            def __init__(self):
                self.recorded = []

            def pending_hold_notification_holds(self, *, event_type=None):
                self.event_type = event_type
                return ["hold-1"]

            def pending_hold_notifications(self, hold_id):
                self.hold_id = hold_id
                return [{
                    "event_id": "terminal:turn-1",
                    "event_type": "terminal",
                    "message": "JARVIS_HOLD_TERMINAL_V1 hold-1 completed",
                }]

            def record_hold_notification_delivery(self, hold_id, event_id, delivery):
                self.recorded.append((hold_id, event_id, delivery))

        class Notifier:
            def notify(self, **kwargs):
                self.kwargs = kwargs
                return {"delivery_status": "delivered", "message_id": "feishu-1"}

        provisioner = Provisioner()
        notifier = Notifier()
        control = JarvisControl(self.capability_port, self.bridge, provisioner, notifier)
        receipt = control.deliver_pending_hold_notifications(
            request_id="drain-1", source_ref="local-heartbeat:test",
        )

        self.assertEqual(receipt["status"], "completed")
        self.assertTrue(receipt["readback"]["verified"])
        self.assertEqual(provisioner.event_type, "terminal")
        self.assertEqual(provisioner.hold_id, "hold-1")
        self.assertEqual(provisioner.recorded[0][0:2], ("hold-1", "terminal:turn-1"))
        self.assertEqual(notifier.kwargs["request_id"], "drain-1:hold-1:terminal:turn-1")

    def test_terminal_notification_delivery_holds_the_per_hold_lock_through_receipt(self):
        class Lock:
            def __init__(self):
                self.active = False
                self.entered = 0
                self.exited = 0

            def __enter__(self):
                self.active = True
                self.entered += 1
                return self

            def __exit__(self, *_args):
                self.active = False
                self.exited += 1

        class Provisioner:
            def __init__(self):
                self.lock = Lock()
                self.recorded = []

            def pending_hold_notifications(self, _hold_id):
                return [{"event_id": "terminal:turn-1", "message": "done"}]

            def hold_notification_delivery_lock(self, _hold_id):
                return self.lock

            def record_hold_notification_delivery(self, hold_id, event_id, delivery):
                self.assert_lock_active()
                self.recorded.append((hold_id, event_id, delivery))

            def assert_lock_active(self):
                if not self.lock.active:
                    raise AssertionError("receipt was recorded outside the per-Hold lock")

        class Notifier:
            def __init__(self, lock):
                self._lock = lock

            def notify(self, **_kwargs):
                if not self._lock.active:
                    raise AssertionError("notifier ran outside the per-Hold lock")
                return {"delivery_status": "delivered", "message_id": "feishu-1"}

        provisioner = Provisioner()
        control = JarvisControl(
            self.capability_port, self.bridge, provisioner, Notifier(provisioner.lock),
        )
        receipt = control.monitor(
            action="deliver_hold_notifications", request_id="locked-notify",
            source_ref="mcp:test", hold_id="hold-1",
        )

        self.assertEqual(receipt["status"], "completed")
        self.assertEqual((provisioner.lock.entered, provisioner.lock.exited), (1, 1))
        self.assertEqual(len(provisioner.recorded), 1)

    def test_overlapping_terminal_notification_deliveries_enqueue_once(self):
        class Provisioner:
            def __init__(self):
                self.lock = threading.Lock()
                self.delivered = False

            def pending_hold_notifications(self, _hold_id):
                return [] if self.delivered else [{"event_id": "terminal:turn-1", "message": "done"}]

            def hold_notification_delivery_lock(self, _hold_id):
                return self.lock

            def record_hold_notification_delivery(self, _hold_id, _event_id, _delivery):
                self.delivered = True

        class Notifier:
            def __init__(self):
                self.calls = 0

            def notify(self, **_kwargs):
                self.calls += 1
                time.sleep(0.05)
                return {"delivery_status": "delivered", "message_id": "feishu-1"}

        provisioner = Provisioner()
        notifier = Notifier()
        control = JarvisControl(self.capability_port, self.bridge, provisioner, notifier)
        start = threading.Barrier(3)
        receipts: list[dict] = []

        def deliver(request_id: str) -> None:
            start.wait()
            receipts.append(control.monitor(
                action="deliver_hold_notifications", request_id=request_id,
                source_ref="mcp:test", hold_id="hold-1",
            ))

        workers = [
            threading.Thread(target=deliver, args=("concurrent-1",)),
            threading.Thread(target=deliver, args=("concurrent-2",)),
        ]
        for worker in workers:
            worker.start()
        start.wait()
        for worker in workers:
            worker.join(timeout=2)

        self.assertFalse(any(worker.is_alive() for worker in workers))
        self.assertEqual(notifier.calls, 1)
        self.assertEqual([receipt["status"] for receipt in receipts], ["completed", "completed"])

    def test_terminal_notification_drain_continues_after_a_bridge_failure(self):
        class Provisioner:
            def __init__(self):
                self.recorded = []

            def pending_hold_notification_holds(self, *, event_type=None):
                self.event_type = event_type
                return ["hold-1", "hold-2"]

            def pending_hold_notifications(self, hold_id):
                return [{"event_id": f"terminal:{hold_id}", "event_type": "terminal", "message": hold_id}]

            def record_hold_notification_delivery(self, hold_id, event_id, delivery):
                self.recorded.append((hold_id, event_id, delivery))

        class Notifier:
            def __init__(self):
                self.calls = 0

            def notify(self, **_kwargs):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("bridge unavailable")
                return {"delivery_status": "delivered", "message_id": "feishu-2"}

        provisioner = Provisioner()
        notifier = Notifier()
        control = JarvisControl(self.capability_port, self.bridge, provisioner, notifier)
        receipt = control.deliver_pending_hold_notifications(
            request_id="drain-failure", source_ref="local-heartbeat:test",
        )

        self.assertEqual(receipt["status"], "requires_readback")
        self.assertEqual(provisioner.event_type, "terminal")
        self.assertEqual([record[0] for record in provisioner.recorded], ["hold-1", "hold-2"])
        self.assertEqual(provisioner.recorded[0][2]["delivery_status"], "failed")
        self.assertEqual(provisioner.recorded[1][2]["message_id"], "feishu-2")


if __name__ == "__main__":
    unittest.main()
