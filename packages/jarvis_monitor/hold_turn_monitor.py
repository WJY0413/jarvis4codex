"""Monitor-owned decisions for a single Jarvis Hold turn."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class HeldTurnClient(Protocol):
    def wait_for_turn_terminal(
        self, thread_id: str, turn_id: str, **kwargs: Any
    ) -> dict[str, object]: ...

    def wait_for_turn_readback(self, thread_id: str, turn_id: str) -> str: ...


@dataclass(frozen=True)
class NotificationPolicy:
    milestone_turns: tuple[int, ...] = ()
    terminal: bool = False

    def __post_init__(self) -> None:
        if any(not isinstance(turn, int) or turn < 1 for turn in self.milestone_turns):
            raise ValueError("milestone_turns must contain positive integers")


@dataclass(frozen=True)
class HoldTurnRequest:
    hold_id: str
    thread_id: str
    turn_id: str
    turn_count: int
    max_turns: int
    continuation_enabled: bool
    continue_prompt: str
    notification_policy: NotificationPolicy


@dataclass(frozen=True)
class NotificationEvent:
    event_id: str
    event_type: str
    hold_id: str
    thread_id: str
    turn_id: str
    turn_count: int
    status: str
    message: str


@dataclass(frozen=True)
class HoldTurnDecision:
    action: str
    command_id: str
    hold_id: str
    expected_turn_id: str
    result_status: str
    final_message: str
    reason: str
    continue_prompt: str | None
    notification_events: tuple[NotificationEvent, ...]


class HoldTurnMonitor:
    """Read one exact turn and emit the sole command its owning Hold may execute."""

    def observe(self, client: HeldTurnClient, request: HoldTurnRequest) -> HoldTurnDecision:
        terminal = client.wait_for_turn_terminal(
            request.thread_id, request.turn_id, wait_forever=True
        )
        terminal_status = str(terminal.get("status") or "unknown")
        final_message = ""
        if terminal_status == "completed":
            final_message = str(
                client.wait_for_turn_readback(request.thread_id, request.turn_id) or ""
            )
        command_id = f"monitor:{request.hold_id}:{request.turn_id}:{request.turn_count}"
        if terminal_status != "completed":
            return self._stop(request, command_id, terminal_status, final_message, "non_completed_terminal")
        if _worker_reported_blocked(final_message):
            return self._stop(request, command_id, "blocked", final_message, "worker_reported_blocked")
        if request.turn_count >= request.max_turns:
            return self._stop(request, command_id, "turn_limit_reached", final_message, "turn_budget_consumed")
        if not request.continuation_enabled:
            return self._stop(request, command_id, "completed", final_message, "continuation_disabled")
        return HoldTurnDecision(
            action="CONTINUE",
            command_id=command_id,
            hold_id=request.hold_id,
            expected_turn_id=request.turn_id,
            result_status="holding",
            final_message=final_message,
            reason="completed_under_budget",
            continue_prompt=request.continue_prompt,
            notification_events=self._milestone_events(request, "completed"),
        )

    def _stop(
        self,
        request: HoldTurnRequest,
        command_id: str,
        result_status: str,
        final_message: str,
        reason: str,
    ) -> HoldTurnDecision:
        events = list(self._milestone_events(request, result_status))
        if request.notification_policy.terminal:
            events.append(NotificationEvent(
                event_id=f"terminal:{request.turn_id}",
                event_type="terminal",
                hold_id=request.hold_id,
                thread_id=request.thread_id,
                turn_id=request.turn_id,
                turn_count=request.turn_count,
                status=result_status,
                message=f"JARVIS_HOLD_TERMINAL_V1 {request.hold_id} {result_status}",
            ))
        return HoldTurnDecision(
            action="STOP",
            command_id=command_id,
            hold_id=request.hold_id,
            expected_turn_id=request.turn_id,
            result_status=result_status,
            final_message=final_message,
            reason=reason,
            continue_prompt=None,
            notification_events=tuple(events),
        )

    @staticmethod
    def _milestone_events(
        request: HoldTurnRequest, status: str
    ) -> tuple[NotificationEvent, ...]:
        if request.turn_count not in request.notification_policy.milestone_turns:
            return ()
        return (NotificationEvent(
            event_id=f"milestone:{request.turn_id}",
            event_type="milestone",
            hold_id=request.hold_id,
            thread_id=request.thread_id,
            turn_id=request.turn_id,
            turn_count=request.turn_count,
            status=status,
            message=f"JARVIS_HOLD_MILESTONE_V1 {request.hold_id} turn={request.turn_count}",
        ),)


def _worker_reported_blocked(final_message: str) -> bool:
    prefixes = ("jarvis_run_status: blocked", "blocked:", "blocked：")
    return any(line.strip().casefold().startswith(prefixes) for line in final_message.splitlines())
