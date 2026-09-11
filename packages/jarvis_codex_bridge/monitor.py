"""Read-only terminal monitor; scheduling remains the caller's responsibility."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Sequence

from .contracts import (
    BridgeReceipt,
    MonitorReceipt,
    ReceiptRoute,
    ResumeRequest,
    TerminalContinuationRule,
    ThreadState,
    utc_now,
)
from .service import ExistingThreadBridge
from .transport import ExistingThreadTransport


_TERMINAL = {"completed", "failed", "interrupted", "cancelled", "canceled", "blocked"}
DEFAULT_MONITOR_INTERVAL_SECONDS = 3
DEFAULT_MONITOR_MAX_DURATION_SECONDS = 24 * 60 * 60


class ThreadTerminalMonitor:
    """Reports a new terminal transition, never a pre-existing terminal snapshot."""

    def __init__(self, transport: ExistingThreadTransport, state_path: Path):
        self.transport = transport
        self.state_path = state_path

    @staticmethod
    def _fingerprint(state: ThreadState) -> str:
        latest = state.turns[-1] if state.turns else None
        value = {
            "thread_id": state.thread_id,
            "thread_status": state.status,
            "turn_id": latest.turn_id if latest else None,
            "turn_status": state.effective_status if latest else None,
        }
        raw = json.dumps(value, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def observe(self, monitor_id: str, route: ReceiptRoute) -> MonitorReceipt:
        thread_id = route.observed_thread_id
        state = self.transport.read_thread(thread_id)
        if state.thread_id != thread_id:
            raise RuntimeError("transport readback thread id does not match the receipt route")
        latest = state.turns[-1] if state.turns else None
        status = state.effective_status.strip().lower()
        fingerprint = self._fingerprint(state)
        previous = {}
        if self.state_path.exists():
            previous = json.loads(self.state_path.read_text(encoding="utf-8-sig"))
        if not previous:
            # A monitor can be attached to an already completed/idle task.
            # That state is a baseline only: no receipt consumer may treat it
            # as a newly completed unit of work.
            event = "baseline_terminal" if status in _TERMINAL else "baseline_active"
        elif previous.get("fingerprint") == fingerprint:
            event = "no_change"
        elif status in _TERMINAL:
            event = "terminal_changed"
        else:
            event = "active_or_unknown_changed"
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(
            json.dumps(
                {"version": 2, "fingerprint": fingerprint, "event": event},
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        return MonitorReceipt(
            monitor_id=monitor_id,
            thread_id=thread_id,
            receipt_target_thread_id=route.receipt_target_thread_id,
            state=event,
            observed_at=utc_now(),
            fingerprint=fingerprint,
            reason=status,
        )

    @staticmethod
    def receipt_delivery_request(receipt: MonitorReceipt, *, source_ref: str) -> ResumeRequest:
        """Build, but do not execute, the exact receipt delivery turn.

        The scheduler/controller owns execution.  This keeps receipt routing
        explicit and makes an observed-thread mismatch impossible to conceal.
        """
        prompt = "JARVIS_THREAD_MONITOR_RECEIPT_V1\n" + json.dumps(
            receipt.as_dict(), ensure_ascii=False, sort_keys=True
        )
        return ResumeRequest(
            request_id=f"monitor-receipt:{receipt.monitor_id}:{receipt.fingerprint}",
            thread_id=receipt.receipt_target_thread_id,
            prompt=prompt,
            source_ref=source_ref,
        )


@dataclass(frozen=True)
class ContinuousMonitorSpec:
    """One independent live Monitor session, never a Heartbeat schedule.

    A Monitor is a tight state observer.  It refreshes every three seconds by
    default and has a hard 24-hour maximum lifetime.  It ends sooner when all
    configured terminal continuations have completed or the caller cancels it.
    """

    monitor_id: str
    route: ReceiptRoute
    resume_target_thread_id: str
    continuation_prompts: tuple[str, ...]
    source_ref: str
    interval_seconds: int = DEFAULT_MONITOR_INTERVAL_SECONDS
    max_duration_seconds: int = DEFAULT_MONITOR_MAX_DURATION_SECONDS
    model: str | None = None
    reasoning_effort: str | None = None
    repeat_last_prompt: bool = False
    max_continuations: int | None = None
    continuation_prompt_provider: Callable[[int, MonitorReceipt], str | None] | None = None

    def __post_init__(self) -> None:
        if not self.monitor_id.strip() or not self.source_ref.strip():
            raise ValueError("monitor_id and source_ref are required")
        if not self.resume_target_thread_id.strip():
            raise ValueError("resume_target_thread_id is required")
        if not self.continuation_prompts or any(
            not prompt.strip() for prompt in self.continuation_prompts
        ):
            raise ValueError("at least one nonempty continuation prompt is required")
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        if not 1 <= self.max_duration_seconds <= DEFAULT_MONITOR_MAX_DURATION_SECONDS:
            raise ValueError("a single monitor must expire within 24 hours")
        if self.max_continuations is not None and self.max_continuations <= 0:
            raise ValueError("max_continuations must be positive when supplied")


@dataclass(frozen=True)
class ContinuousMonitorResult:
    monitor_id: str
    status: str
    started_at: datetime
    completed_at: datetime
    observations: tuple[MonitorReceipt, ...]
    continuation_receipts: tuple[BridgeReceipt, ...]
    reason: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "monitor_id": self.monitor_id,
            "status": self.status,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat(),
            "observations": [item.as_dict() for item in self.observations],
            "continuation_receipts": [
                item.as_dict() for item in self.continuation_receipts
            ],
            "reason": self.reason,
        }


class ContinuousTerminalContinuationMonitor:
    """Continuously observe one thread and advance only new completed turns.

    This is deliberately separate from Heartbeat.  It does not schedule a
    periodic report or a global health probe; it is a bounded, three-second
    state observer for one existing Codex thread. A changed ``interrupted`` or
    ``failed`` state is never treated as permission to continue.
    """

    def __init__(
        self,
        monitor: ThreadTerminalMonitor,
        bridge: ExistingThreadBridge,
        spec: ContinuousMonitorSpec,
    ) -> None:
        self.monitor = monitor
        self.bridge = bridge
        self.spec = spec

    def run(
        self,
        *,
        on_observation: Callable[[MonitorReceipt], None] | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> ContinuousMonitorResult:
        started_at = utc_now()
        deadline = time.monotonic() + self.spec.max_duration_seconds
        observations: list[MonitorReceipt] = []
        continuations: list[BridgeReceipt] = []

        while True:
            if cancelled and cancelled():
                return self._result(
                    "cancelled", started_at, observations, continuations, "caller cancelled"
                )
            if time.monotonic() >= deadline:
                return self._result(
                    "expired", started_at, observations, continuations,
                    "monitor reached its 24-hour bounded lifetime",
                )

            receipt = self.monitor.observe(self.spec.monitor_id, self.spec.route)
            observations.append(receipt)
            if on_observation:
                on_observation(receipt)

            if receipt.state == "terminal_changed" and receipt.reason != "completed":
                return self._result(
                    "requires_readback", started_at, observations, continuations,
                    f"observed terminal state {receipt.reason}; no continuation issued",
                )

            if receipt.state == "terminal_changed" and receipt.reason == "completed":
                prompt = self._next_prompt(len(continuations) + 1, receipt)
                if prompt is None:
                    return self._result(
                        "completed", started_at, observations, continuations,
                        "no next continuation prompt is configured",
                    )
                rule = TerminalContinuationRule(
                    monitor_id=self.spec.monitor_id,
                    observed_thread_id=self.spec.route.observed_thread_id,
                    resume_target_thread_id=self.spec.resume_target_thread_id,
                    prompt=prompt,
                    source_ref=self.spec.source_ref,
                    model=self.spec.model,
                    reasoning_effort=self.spec.reasoning_effort,
                )
                continuation = self.bridge.resume_existing(rule.resume_request(receipt))
                continuations.append(continuation)
                if continuation.status != "completed":
                    return self._result(
                        "requires_readback", started_at, observations, continuations,
                        f"continuation {len(continuations)} returned {continuation.status}",
                    )
                if self._continuation_budget_consumed(len(continuations)):
                    return self._result(
                        "completed", started_at, observations, continuations,
                        "configured continuation budget consumed",
                    )

            time.sleep(self.spec.interval_seconds)

    def _next_prompt(self, cycle_number: int, receipt: MonitorReceipt) -> str | None:
        provider = self.spec.continuation_prompt_provider
        if provider is not None:
            prompt = provider(cycle_number, receipt)
            if prompt is None:
                return None
            if not isinstance(prompt, str) or not prompt.strip():
                raise ValueError("continuation_prompt_provider must return a nonempty prompt or None")
            return prompt
        prompt_index = cycle_number - 1
        if prompt_index < len(self.spec.continuation_prompts):
            return self.spec.continuation_prompts[prompt_index]
        if self.spec.repeat_last_prompt:
            return self.spec.continuation_prompts[-1]
        return None

    def _continuation_budget_consumed(self, continuation_count: int) -> bool:
        if self.spec.max_continuations is not None:
            return continuation_count >= self.spec.max_continuations
        return (
            self.spec.continuation_prompt_provider is None
            and not self.spec.repeat_last_prompt
            and continuation_count >= len(self.spec.continuation_prompts)
        )

    def _result(
        self,
        status: str,
        started_at: datetime,
        observations: list[MonitorReceipt],
        continuations: list[BridgeReceipt],
        reason: str | None = None,
    ) -> ContinuousMonitorResult:
        return ContinuousMonitorResult(
            monitor_id=self.spec.monitor_id,
            status=status,
            started_at=started_at,
            completed_at=utc_now(),
            observations=tuple(observations),
            continuation_receipts=tuple(continuations),
            reason=reason,
        )
