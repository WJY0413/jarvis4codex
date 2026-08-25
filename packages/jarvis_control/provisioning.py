"""Stable provisioning seam used by JarvisControl callers such as MCP."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Literal, Protocol


ProvisionStatus = Literal["completed", "failed", "requires_readback", "partial"]


@dataclass(frozen=True)
class TaskProvisionRequest:
    request_id: str
    project: str
    title: str
    prompt: str
    source_ref: str
    model: str | None = None
    reasoning_effort: str | None = None

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


@dataclass(frozen=True)
class TaskProvisionReceipt:
    request_id: str
    status: ProvisionStatus
    observed_at: datetime
    thread_id: str | None = None
    turn_id: str | None = None
    output: str | None = None
    reason: str | None = None

    def as_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["observed_at"] = self.observed_at.isoformat()
        return value


class TaskProvisioningPort(Protocol):
    """Create one durable thread, run its first turn, and return exact evidence."""

    def provision(self, request: TaskProvisionRequest) -> TaskProvisionReceipt: ...


def observed_now() -> datetime:
    return datetime.now(timezone.utc)
