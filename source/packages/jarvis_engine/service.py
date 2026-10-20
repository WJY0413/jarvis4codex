from jarvis_contracts import ExecutionReceipt, ScheduleSpec
from jarvis_harness_sdk import HarnessAdapter


class HeartbeatEngine:
    """Schedule policy only; all harness effects go through an adapter."""

    def trigger_once(
        self, schedule: ScheduleSpec, adapter: HarnessAdapter, envelope_ref: str
    ) -> ExecutionReceipt:
        adapter.preflight(schedule.target)
        return adapter.trigger(schedule.target, envelope_ref)
