"""Stable provisioning seam used by JarvisControl callers such as MCP."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Literal, Mapping, Protocol


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
    hold_id: str | None = None
    notifications: Mapping[str, Any] | None = None
    input_binding: Mapping[str, Any] | None = None

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
        if self.hold_id is not None and not self.hold_id.strip():
            raise ValueError("hold_id cannot be blank")
        notifications = dict(self.notifications or {})
        if self.input_binding is not None and not isinstance(self.input_binding, Mapping):
            raise ValueError("input_binding must be an object")
        milestones = notifications.get("milestones") or []
        if not isinstance(milestones, list):
            raise ValueError("notifications.milestones must be a list")
        try:
            if any(int(turn) < 1 for turn in milestones):
                raise ValueError("notifications.milestones must be positive")
        except (TypeError, ValueError) as exc:
            raise ValueError("notifications.milestones must contain positive integers") from exc
        if "terminal" in notifications and not isinstance(notifications["terminal"], bool):
            raise ValueError("notifications.terminal must be a boolean")


@dataclass(frozen=True)
class TaskMonitorResumeRequest:
    request_id: str
    task_id: str
    prompt: str
    source_ref: str
    monitor_id: str | None = None
    hold_id: str | None = None
    max_turns: int = 1
    model: str | None = None
    reasoning_effort: str | None = None
    auto_continue: bool = False
    continue_prompt: str = "继续"
    notifications: Mapping[str, Any] | None = None
    input_binding: Mapping[str, Any] | None = None

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
        if self.hold_id is not None and not self.hold_id.strip():
            raise ValueError("hold_id cannot be blank")
        if not self.continue_prompt.strip():
            raise ValueError("continue_prompt is required")
        notifications = dict(self.notifications or {})
        if self.input_binding is not None and not isinstance(self.input_binding, Mapping):
            raise ValueError("input_binding must be an object")
        milestones = notifications.get("milestones") or []
        if not isinstance(milestones, list):
            raise ValueError("notifications.milestones must be a list")
        try:
            if any(int(turn) < 1 for turn in milestones):
                raise ValueError("notifications.milestones must be positive")
        except (TypeError, ValueError) as exc:
            raise ValueError("notifications.milestones must contain positive integers") from exc
        if "terminal" in notifications and not isinstance(notifications["terminal"], bool):
            raise ValueError("notifications.terminal must be a boolean")


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
    hold_id: str | None = None
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

    def ensure_hold_host_ready(self, *, required_workers: int) -> dict[str, str]: ...


def observed_now() -> datetime:
    return datetime.now(timezone.utc)
