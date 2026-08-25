"""Stable provisioning seam used by JarvisControl callers such as MCP."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Literal, Protocol


ProvisionStatus = Literal["accepted", "holding", "running", "completed", "failed", "requires_readback", "partial"]


@dataclass(frozen=True)
class TaskProvisionRequest:
    request_id: str
    project: str
    title: str
    prompt: str
    source_ref: str
    model: str | None = None
    reasoning_effort: str | None = None
    max_turns: int = 1
    auto_continue: bool = False
    continue_prompt: str = "继续"

    def __post_init__(self) -> None:
        required = {
            "request_id": self.request_id,
            "project": self.project,
            "title": self.title,
            "prompt": self.prompt,
            "source_ref": self.source_ref,
        }
        missing = [name for name, value in required.items() if not value.strip()]
        if missing:
            raise ValueError("required provision fields: " + ", ".join(missing))
        if not isinstance(self.max_turns, int) or self.max_turns < 1:
            raise ValueError("max_turns must be a positive integer")
        if not self.continue_prompt.strip():
            raise ValueError("continue_prompt is required")


@dataclass(frozen=True)
class TaskMonitorResumeRequest:
    request_id: str
    task_id: str
    prompt: str
    source_ref: str
    monitor_id: str | None = None
    max_turns: int = 1
    model: str | None = None
    reasoning_effort: str | None = None

    def __post_init__(self) -> None:
        required = {
            "request_id": self.request_id,
            "task_id": self.task_id,
            "prompt": self.prompt,
            "source_ref": self.source_ref,
        }
        missing = [name for name, value in required.items() if not value.strip()]
        if missing:
            raise ValueError("required monitor-resume fields: " + ", ".join(missing))
        if not isinstance(self.max_turns, int) or self.max_turns < 1:
            raise ValueError("max_turns must be a positive integer")


@dataclass(frozen=True)
class TaskProvisionReceipt:
    request_id: str
    status: ProvisionStatus
    observed_at: datetime
    thread_id: str | None = None
    turn_id: str | None = None
    output: str | None = None
    reason: str | None = None
    error_code: str | None = None
    phase: str | None = None
    monitor_id: str | None = None
    turn_count: int | None = None
    total_turn_count: int | None = None
    max_turns: int | None = None

    def as_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["observed_at"] = self.observed_at.isoformat()
        return value


class TaskProvisioningPort(Protocol):
    """Create one durable thread, start its first turn, and return exact identity."""

    def provision(self, request: TaskProvisionRequest) -> TaskProvisionReceipt: ...


def observed_now() -> datetime:
    return datetime.now(timezone.utc)
