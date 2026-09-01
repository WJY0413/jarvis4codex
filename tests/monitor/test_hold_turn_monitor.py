from __future__ import annotations

import unittest

from jarvis_monitor import HoldTurnMonitor, HoldTurnRequest, NotificationPolicy


class FakeHeldTurnClient:
    def __init__(self, status: str = "completed", content: str = "final answer") -> None:
        self.status = status
        self.content = content
        self.terminal_calls: list[tuple[str, str]] = []
        self.readback_calls: list[tuple[str, str]] = []

    def wait_for_turn_terminal(self, thread_id: str, turn_id: str, **_kwargs):
        self.terminal_calls.append((thread_id, turn_id))
        return {"id": turn_id, "status": self.status}

    def wait_for_turn_readback(self, thread_id: str, turn_id: str):
        self.readback_calls.append((thread_id, turn_id))
        return self.content


class HoldTurnMonitorTest(unittest.TestCase):
    def request(self, **overrides) -> HoldTurnRequest:
        values = {
            "hold_id": "hold-1",
            "thread_id": "thread-1",
            "turn_id": "turn-1",
            "turn_count": 1,
            "max_turns": 2,
            "continuation_enabled": True,
            "continue_prompt": "继续",
            "notification_policy": NotificationPolicy(),
        }
        values.update(overrides)
        return HoldTurnRequest(**values)

    def test_completed_nonempty_turn_emits_a_bound_continue_command(self):
        client = FakeHeldTurnClient()
        decision = HoldTurnMonitor().observe(client, self.request(
            notification_policy=NotificationPolicy(milestone_turns=(1,)),
        ))

        self.assertEqual(client.terminal_calls, [("thread-1", "turn-1")])
        self.assertEqual(client.readback_calls, [("thread-1", "turn-1")])
        self.assertEqual(decision.action, "CONTINUE")
        self.assertEqual(decision.command_id, "monitor:hold-1:turn-1:1")
        self.assertEqual(decision.expected_turn_id, "turn-1")
        self.assertEqual(decision.continue_prompt, "继续")
        self.assertEqual(decision.notification_events[0].event_type, "milestone")

    def test_empty_completed_turn_continues_without_a_terminal_event(self):
        client = FakeHeldTurnClient(content="  ")
        decision = HoldTurnMonitor().observe(client, self.request(
            notification_policy=NotificationPolicy(terminal=True),
        ))

        self.assertEqual(decision.action, "CONTINUE")
        self.assertEqual(decision.result_status, "holding")
        self.assertEqual(decision.reason, "completed_under_budget")
        self.assertEqual(decision.notification_events, ())

    def test_non_completed_terminal_does_not_read_content_or_continue(self):
        client = FakeHeldTurnClient(status="failed")
        decision = HoldTurnMonitor().observe(client, self.request(
            notification_policy=NotificationPolicy(terminal=True),
        ))

        self.assertEqual(client.readback_calls, [])
        self.assertEqual(decision.action, "STOP")
        self.assertEqual(decision.result_status, "failed")
        self.assertEqual(decision.reason, "non_completed_terminal")


if __name__ == "__main__":
    unittest.main()
