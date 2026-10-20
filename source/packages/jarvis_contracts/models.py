from dataclasses import dataclass
from datetime import datetime
from typing import Literal


RunStatus = Literal["accepted", "running", "completed", "failed", "unknown"]


@dataclass(frozen=True)
class TargetRef:
    harness: str
    locator: str


@dataclass(frozen=True)
class ScheduleSpec:
    schedule_id: str
    target: TargetRef
    interval_seconds: int
    stop_condition: str


@dataclass(frozen=True)
class ExecutionReceipt:
    adapter: str
    target: TargetRef
    execution_id: str
    status: RunStatus
    observed_at: datetime
    evidence_ref: str | None = None


@dataclass(frozen=True)
class MonitorEvent:
    monitor_id: str
    subject: str
    event_type: str
    observed_at: datetime
    evidence_ref: str
