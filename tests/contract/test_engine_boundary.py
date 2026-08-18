from datetime import datetime, timezone
import unittest

from jarvis_contracts import ExecutionReceipt, ScheduleSpec, TargetRef
from jarvis_engine import HeartbeatEngine


class FakeAdapter:
    name = "fake"

    def preflight(self, target):
        assert target.harness == self.name

    def trigger(self, target, envelope_ref):
        return ExecutionReceipt(
            adapter=self.name,
            target=target,
            execution_id="run-1",
            status="accepted",
            observed_at=datetime.now(timezone.utc),
            evidence_ref=envelope_ref,
        )


class EngineBoundaryTest(unittest.TestCase):
    def test_engine_uses_only_harness_contract(self):
        schedule = ScheduleSpec("schedule-1", TargetRef("fake", "agent-1"), 60, "manual")
        receipt = HeartbeatEngine().trigger_once(schedule, FakeAdapter(), "contract.json")
        self.assertEqual(receipt.execution_id, "run-1")
