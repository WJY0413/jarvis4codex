"""Stable contracts for resuming and monitoring existing Codex threads only."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Literal


BridgeStatus = Literal[
    "starting", "completed", "busy", "failed", "interrupted", "requires_readback", "duplicate"
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class ResumeRequest:
    """An idempotent request to add one turn to one existing thread."""

    request_id: str
    thread_id: str
    prompt: str
    source_ref: str
    model: str | None = None
    reasoning_effort: str | None = None

    def __post_init__(self) -> None:
        if not self.request_id.strip():
            raise ValueError("request_id is required")
        if not self.thread_id.strip():
            raise ValueError("thread_id is required")
        if not self.prompt.strip():
            raise ValueError("prompt is required")


@dataclass(frozen=True)
class TurnState:
    turn_id: str
    status: str
    items: tuple[dict[str, Any], ...] = ()
    error: str | None = None
    execution_status: str | None = None
    execution_source: str = "transport"

    @property
    def effective_status(self) -> str:
        """Execution evidence can override, but never rewrite, native status."""
        return self.execution_status if self.execution_status is not None else self.status


@dataclass(frozen=True)
class ThreadState:
    thread_id: str
    status: str
    turns: tuple[TurnState, ...] = ()
    read_source: str = "transport"
    execution_status: str | None = None
    execution_source: str = "transport"
    execution_evidence: dict[str, Any] | None = None
    remote_activity: dict[str, Any] | None = None

    @property
    def _activity_override(self) -> str | None:
        # Idle is absence evidence, never delivery or physical closure proof.
        if self.remote_activity is None:
            return None
        if self.remote_activity.get("thread_id") != self.thread_id:
            return "unknown"
        activity = self.remote_activity.get("status")
        if activity == "active":
            return "inProgress"
        if (activity != "idle" or self.remote_activity.get("no_active_turns") is not True
                or self.remote_activity.get("scan_complete") is not True):
            return "unknown"
        latest_id = self.turns[-1].turn_id if self.turns else None
        if self.remote_activity.get("latest_turn_id") != latest_id:
            return "unknown"  # Newer stopped/empty snapshots cannot inherit old delivery.
        if (self.turns and self.remote_activity.get("latest_turn_status") != self.turns[-1].status):
            return "unknown"  # Same-turn conflicting snapshots cannot confirm delivery.
        return None

    @property
    def effective_status(self) -> str:
        # Fresh remote activity cannot be erased by an old owner terminal record.
        override = self._activity_override
        if override is not None:
            return override
        if self.execution_status is not None:
            return self.execution_status
        return self.turns[-1].effective_status if self.turns else self.status

    @property
    def effective_source(self) -> str:
        if self._activity_override is not None:
            return "native_activity"
        return self.execution_source


@dataclass(frozen=True)
class StartedTurn:
    thread_id: str
    turn_id: str
    status: str


@dataclass(frozen=True)
class BridgeReceipt:
    request_id: str
    thread_id: str
    status: BridgeStatus
    observed_at: datetime
    source_ref: str
    turn_id: str | None = None
    output: str | None = None
    reason: str | None = None
    replayed: bool = False
    request_digest: str | None = None
    owner: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["observed_at"] = self.observed_at.isoformat()
        return value


@dataclass(frozen=True)
class MonitorReceipt:
    monitor_id: str
    thread_id: str
    receipt_target_thread_id: str
    state: str
    observed_at: datetime
    fingerprint: str
    reason: str | None = None

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["observed_at"] = self.observed_at.isoformat()
        return value


@dataclass(frozen=True)
class ReceiptRoute:
    """Exact mapping from a monitored thread to its designated receipt thread."""

    observed_thread_id: str
    receipt_target_thread_id: str

    def __post_init__(self) -> None:
        if not self.observed_thread_id.strip():
            raise ValueError("observed_thread_id is required")
        if not self.receipt_target_thread_id.strip():
            raise ValueError("receipt_target_thread_id is required")

    def matches(self, thread_id: str) -> bool:
        return self.observed_thread_id == thread_id


@dataclass(frozen=True)
class TerminalContinuationRule:
    """Compose a terminal monitor event with one configurable resume request."""

    monitor_id: str
    observed_thread_id: str
    resume_target_thread_id: str
    prompt: str
    source_ref: str
    model: str | None = None
    reasoning_effort: str | None = None

    def __post_init__(self) -> None:
        required = {
            "monitor_id": self.monitor_id,
            "observed_thread_id": self.observed_thread_id,
            "resume_target_thread_id": self.resume_target_thread_id,
            "prompt": self.prompt,
            "source_ref": self.source_ref,
        }
        missing = [name for name, value in required.items() if not value.strip()]
        if missing:
            raise ValueError("required terminal continuation fields: " + ", ".join(missing))

    def resume_request(self, receipt: MonitorReceipt) -> ResumeRequest:
        if receipt.monitor_id != self.monitor_id:
            raise ValueError("monitor receipt does not belong to this continuation rule")
        if receipt.thread_id != self.observed_thread_id:
            raise ValueError("monitor receipt thread does not match continuation rule")
        if receipt.state != "terminal_changed":
            raise ValueError("only a changed terminal receipt can trigger continuation")
        return ResumeRequest(
            request_id=(
                f"terminal-continue:{self.monitor_id}:{receipt.fingerprint[:32]}"
            ),
            thread_id=self.resume_target_thread_id,
            prompt=self.prompt,
            source_ref=self.source_ref,
            model=self.model,
            reasoning_effort=self.reasoning_effort,
        )
