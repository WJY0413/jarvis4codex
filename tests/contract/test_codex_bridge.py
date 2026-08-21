from pathlib import Path
import tempfile
import unittest

from jarvis_codex_bridge import (
    CapabilityRequest,
    ContinuousMonitorSpec,
    ContinuousTerminalContinuationMonitor,
    ExistingThreadBridge,
    JarvisCapabilityPort,
    JsonlReceiptJournal,
    ReceiptRoute,
    ResumeRequest,
    StartedTurn,
    TerminalContinuationRule,
    ThreadState,
    ThreadTerminalMonitor,
    TurnState,
)


class FakeTransport:
    name = "fake-codex"

    def __init__(self, state: ThreadState):
        self.state = state
        self.calls = 0
        self.prompts = []

    def health(self):
        return {"adapter": self.name, "status": "ok"}

    def read_thread(self, thread_id):
        assert thread_id == self.state.thread_id
        return self.state

    def resume_existing(self, request):
        self.calls += 1
        self.prompts.append(request.prompt)
        turn_id = f"turn-{self.calls + 1}"
        self.state = ThreadState(
            request.thread_id,
            "idle",
            (
                *self.state.turns,
                TurnState(
                    turn_id,
                    "completed",
                    ({"type": "agentMessage", "phase": "final_answer", "text": "continued"},),
                ),
            ),
        )
        return StartedTurn(request.thread_id, turn_id, "completed")


class FakeHeartbeatControl:
    def __init__(self):
        self.calls = []

    def invoke_heartbeat(self, request_id, capability, source_ref, arguments):
        self.calls.append((request_id, capability, source_ref, dict(arguments)))
        return {"status": "active", "heartbeat_id": arguments.get("heartbeat_id")}


class CodexBridgeContractTest(unittest.TestCase):
    def test_terminal_then_one_idempotent_same_thread_resume(self):
        state = ThreadState("thread-1", "idle", (TurnState("turn-1", "completed"),))
        transport = FakeTransport(state)
        with tempfile.TemporaryDirectory() as temp:
            bridge = ExistingThreadBridge(transport, JsonlReceiptJournal(Path(temp) / "receipts.jsonl"))
            receipt = bridge.resume_existing(ResumeRequest("package-1", "thread-1", "继续", "test"))
            self.assertEqual(receipt.status, "completed")
            self.assertEqual(receipt.thread_id, "thread-1")
            self.assertEqual(receipt.turn_id, "turn-2")
            self.assertEqual(receipt.output, "continued")
            replay = bridge.resume_existing(ResumeRequest("package-1", "thread-1", "继续", "test"))
            self.assertTrue(replay.replayed)
            self.assertEqual(transport.calls, 1)

    def test_active_thread_is_not_contacted(self):
        transport = FakeTransport(ThreadState("thread-1", "running", (TurnState("turn-1", "inProgress"),)))
        with tempfile.TemporaryDirectory() as temp:
            bridge = ExistingThreadBridge(transport, JsonlReceiptJournal(Path(temp) / "receipts.jsonl"))
            receipt = bridge.resume_existing(ResumeRequest("package-2", "thread-1", "继续", "test"))
            self.assertEqual(receipt.status, "busy")
            self.assertEqual(transport.calls, 0)

    def test_monitor_baselines_existing_terminal_then_stays_quiet(self):
        transport = FakeTransport(ThreadState("thread-1", "idle", (TurnState("turn-1", "completed"),)))
        with tempfile.TemporaryDirectory() as temp:
            monitor = ThreadTerminalMonitor(transport, Path(temp) / "monitor.json")
            route = ReceiptRoute("thread-1", "receipt-thread-9")
            first = monitor.observe("monitor-1", route)
            second = monitor.observe("monitor-1", route)
            self.assertEqual(first.state, "baseline_terminal")
            self.assertEqual(second.state, "no_change")
            self.assertEqual(first.receipt_target_thread_id, "receipt-thread-9")
            delivery = monitor.receipt_delivery_request(first, source_ref="test")
            self.assertEqual(delivery.thread_id, "receipt-thread-9")
            self.assertIn('"thread_id": "thread-1"', delivery.prompt)

    def test_monitor_emits_terminal_changed_only_after_a_new_turn(self):
        transport = FakeTransport(ThreadState("thread-1", "running", (TurnState("turn-1", "inProgress"),)))
        with tempfile.TemporaryDirectory() as temp:
            monitor = ThreadTerminalMonitor(transport, Path(temp) / "monitor.json")
            route = ReceiptRoute("thread-1", "receipt-thread-9")
            self.assertEqual(monitor.observe("monitor-1", route).state, "baseline_active")
            transport.state = ThreadState("thread-1", "idle", (TurnState("turn-1", "completed"),))
            terminal = monitor.observe("monitor-1", route)
            self.assertEqual(terminal.state, "terminal_changed")
            self.assertEqual(monitor.observe("monitor-1", route).state, "no_change")

    def test_terminal_rule_uses_explicit_resume_target_and_prompt(self):
        transport = FakeTransport(ThreadState("thread-1", "running", (TurnState("turn-1", "inProgress"),)))
        with tempfile.TemporaryDirectory() as temp:
            monitor = ThreadTerminalMonitor(transport, Path(temp) / "monitor.json")
            route = ReceiptRoute("thread-1", "receipt-thread-9")
            monitor.observe("monitor-1", route)
            transport.state = ThreadState("thread-1", "idle", (TurnState("turn-1", "completed"),))
            receipt = monitor.observe("monitor-1", route)
            rule = TerminalContinuationRule(
                monitor_id="monitor-1",
                observed_thread_id="thread-1",
                resume_target_thread_id="worker-thread-7",
                prompt="继续",
                source_ref="heartbeat-1",
            )
            request = rule.resume_request(receipt)
            self.assertEqual(request.thread_id, "worker-thread-7")
            self.assertEqual(request.prompt, "继续")
            self.assertEqual(request.request_id, f"terminal-continue:monitor-1:{receipt.fingerprint[:32]}")

    def test_continuous_monitor_is_separate_from_heartbeat_and_runs_ordered_prompts(self):
        transport = FakeTransport(
            ThreadState("thread-1", "idle", (TurnState("turn-0", "completed"),))
        )
        with tempfile.TemporaryDirectory() as temp:
            monitor = ThreadTerminalMonitor(transport, Path(temp) / "monitor.json")
            bridge = ExistingThreadBridge(transport, JsonlReceiptJournal(Path(temp) / "receipts.jsonl"))

            def after_baseline(receipt):
                if receipt.state == "baseline_terminal":
                    transport.state = ThreadState(
                        "thread-1", "idle", (TurnState("turn-1", "completed"),)
                    )

            result = ContinuousTerminalContinuationMonitor(
                monitor,
                bridge,
                ContinuousMonitorSpec(
                    monitor_id="continuous-monitor-1",
                    route=ReceiptRoute("thread-1", "parent-thread"),
                    resume_target_thread_id="thread-1",
                    continuation_prompts=("test", "测试完成"),
                    source_ref="contract-test",
                    interval_seconds=1,
                ),
            ).run(on_observation=after_baseline)
            self.assertEqual(result.status, "completed")
            self.assertEqual(transport.calls, 2)
            self.assertEqual(
                transport.prompts,
                ["test", "测试完成"],
            )
            self.assertEqual(result.observations[0].state, "baseline_terminal")
            self.assertTrue(all(
                receipt.state == "terminal_changed" for receipt in result.observations[1:]
            ))

    def test_continuous_monitor_rejects_lifetime_over_24_hours(self):
        with self.assertRaisesRegex(ValueError, "24 hours"):
            ContinuousMonitorSpec(
                monitor_id="too-long",
                route=ReceiptRoute("thread-1", "parent-thread"),
                resume_target_thread_id="thread-1",
                continuation_prompts=("test",),
                source_ref="contract-test",
                max_duration_seconds=24 * 60 * 60 + 1,
            )

    def test_continuous_monitor_repeats_completed_cycles_with_custom_prompts(self):
        transport = FakeTransport(
            ThreadState("thread-1", "idle", (TurnState("turn-0", "completed"),))
        )
        with tempfile.TemporaryDirectory() as temp:
            monitor = ThreadTerminalMonitor(transport, Path(temp) / "monitor.json")
            bridge = ExistingThreadBridge(transport, JsonlReceiptJournal(Path(temp) / "receipts.jsonl"))

            def after_baseline(receipt):
                if receipt.state == "baseline_terminal":
                    transport.state = ThreadState(
                        "thread-1", "idle", (TurnState("turn-1", "completed"),)
                    )

            result = ContinuousTerminalContinuationMonitor(
                monitor,
                bridge,
                ContinuousMonitorSpec(
                    monitor_id="persistent-monitor-1",
                    route=ReceiptRoute("thread-1", "parent-thread"),
                    resume_target_thread_id="thread-1",
                    continuation_prompts=("continue",),
                    continuation_prompt_provider=lambda cycle, _: f"next-{cycle}",
                    max_continuations=3,
                    source_ref="contract-test",
                    interval_seconds=1,
                ),
            ).run(on_observation=after_baseline)

            self.assertEqual(result.status, "completed")
            self.assertEqual(transport.calls, 3)
            self.assertEqual(transport.prompts, ["next-1", "next-2", "next-3"])

    def test_continuous_monitor_does_not_continue_an_interrupted_turn(self):
        transport = FakeTransport(
            ThreadState("thread-1", "running", (TurnState("turn-1", "inProgress"),))
        )
        with tempfile.TemporaryDirectory() as temp:
            monitor = ThreadTerminalMonitor(transport, Path(temp) / "monitor.json")
            bridge = ExistingThreadBridge(transport, JsonlReceiptJournal(Path(temp) / "receipts.jsonl"))

            def interrupt_after_baseline(receipt):
                if receipt.state == "baseline_active":
                    transport.state = ThreadState(
                        "thread-1", "idle", (TurnState("turn-1", "interrupted"),)
                    )

            result = ContinuousTerminalContinuationMonitor(
                monitor,
                bridge,
                ContinuousMonitorSpec(
                    monitor_id="interrupted-monitor-1",
                    route=ReceiptRoute("thread-1", "parent-thread"),
                    resume_target_thread_id="thread-1",
                    continuation_prompts=("continue",),
                    source_ref="contract-test",
                    interval_seconds=1,
                ),
            ).run(on_observation=interrupt_after_baseline)

            self.assertEqual(result.status, "requires_readback")
            self.assertIn("interrupted", result.reason)
            self.assertEqual(transport.prompts, [])

    def test_capability_port_resumes_an_explicit_thread_with_a_custom_prompt(self):
        transport = FakeTransport(ThreadState("thread-1", "idle", (TurnState("turn-1", "completed"),)))
        with tempfile.TemporaryDirectory() as temp:
            bridge = ExistingThreadBridge(transport, JsonlReceiptJournal(Path(temp) / "receipts.jsonl"))
            port = JarvisCapabilityPort(
                bridge,
                ThreadTerminalMonitor(transport, Path(temp) / "monitor.json"),
            )
            receipt = port.invoke(CapabilityRequest(
                request_id="resume-1",
                capability="resume.existing",
                source_ref="contract-test",
                arguments={"thread_id": "thread-1", "prompt": "custom test prompt"},
            ))
            self.assertEqual(receipt.status, "completed")
            self.assertEqual(receipt.target_thread_id, "thread-1")
            self.assertEqual(receipt.turn_id, "turn-2")
            self.assertEqual(transport.calls, 1)

    def test_capability_port_composes_monitor_and_resume_once(self):
        transport = FakeTransport(ThreadState("source-thread", "idle", (TurnState("turn-1", "completed"),)))
        with tempfile.TemporaryDirectory() as temp:
            bridge = ExistingThreadBridge(transport, JsonlReceiptJournal(Path(temp) / "receipts.jsonl"))
            port = JarvisCapabilityPort(
                bridge,
                ThreadTerminalMonitor(transport, Path(temp) / "monitor.json"),
            )
            request = CapabilityRequest(
                request_id="compose-1",
                capability="monitor.terminal_resume",
                source_ref="contract-test",
                arguments={
                    "monitor_id": "monitor-1",
                    "observed_thread_id": "source-thread",
                    "receipt_target_thread_id": "parent-thread",
                    "resume_target_thread_id": "source-thread",
                    "prompt": "继续",
                },
            )
            baseline = port.invoke(request)
            self.assertEqual(baseline.status, "baseline_terminal")
            self.assertEqual(transport.calls, 0)
            transport.state = ThreadState("source-thread", "running", (TurnState("turn-2", "inProgress"),))
            self.assertEqual(port.invoke(CapabilityRequest(
                request_id="compose-active",
                capability="monitor.terminal_resume",
                source_ref="contract-test",
                arguments=request.arguments,
            )).status, "active_or_unknown_changed")
            transport.state = ThreadState("source-thread", "idle", (TurnState("turn-2", "completed"),))
            receipt = port.invoke(CapabilityRequest(
                request_id="compose-1",
                capability="monitor.terminal_resume",
                source_ref="contract-test",
                arguments=request.arguments,
            ))
            self.assertEqual(receipt.status, "completed")
            self.assertEqual(receipt.target_thread_id, "source-thread")
            self.assertIn("monitor", receipt.data)
            self.assertEqual(transport.calls, 1)

    def test_capability_port_rejects_missing_resume_prompt(self):
        with self.assertRaisesRegex(ValueError, "prompt"):
            CapabilityRequest(
                request_id="invalid-1",
                capability="resume.existing",
                source_ref="contract-test",
                arguments={"thread_id": "thread-1"},
            )

    def test_capability_port_delegates_heartbeat_control_without_hostbridge(self):
        transport = FakeTransport(ThreadState("thread-1", "idle"))
        heartbeat = FakeHeartbeatControl()
        with tempfile.TemporaryDirectory() as temp:
            port = JarvisCapabilityPort(
                ExistingThreadBridge(transport, JsonlReceiptJournal(Path(temp) / "receipts.jsonl")),
                ThreadTerminalMonitor(transport, Path(temp) / "monitor.json"),
                heartbeat,
            )
            receipt = port.invoke(CapabilityRequest(
                request_id="heartbeat-1",
                capability="heartbeat.create",
                source_ref="contract-test",
                arguments={"heartbeat_id": "test-heartbeat"},
            ))
            self.assertEqual(receipt.status, "active")
            self.assertEqual(heartbeat.calls[0][1], "heartbeat.create")
            self.assertEqual(heartbeat.calls[0][3]["heartbeat_id"], "test-heartbeat")


if __name__ == "__main__":
    unittest.main()
