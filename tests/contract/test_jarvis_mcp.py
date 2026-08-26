from __future__ import annotations

import asyncio
import tempfile
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

    def call(self, name, arguments):
        async def run():
            async with Client(self.server.mcp) as client:
                return await client.call_tool(name, arguments)
        return asyncio.run(run())

    def test_lists_the_eight_public_jarvis_tools_with_sdk_generated_schema(self):
        result = self.list_tools()
        self.assertEqual(
            [tool.name for tool in result.tools],
            [
                "jarvis_create",
                "jarvis_hold",
                "jarvis_loop",
                "jarvis_read",
                "jarvis_resume",
                "jarvis_monitor",
                "jarvis_heartbeat",
                "jarvis_notify",
            ],
        )
        self.assertTrue(all(tool.input_schema["type"] == "object" for tool in result.tools))
        read_tool = next(tool for tool in result.tools if tool.name == "jarvis_read")
        self.assertTrue(read_tool.annotations.read_only_hint)

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


if __name__ == "__main__":
    unittest.main()
