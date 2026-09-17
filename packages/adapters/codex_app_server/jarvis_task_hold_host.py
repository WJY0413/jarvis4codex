"""Independent holder for one Jarvis App Server task turn.

This process is intentionally detached from the MCP stdio process. It writes
an ownership acknowledgement immediately after the exact turn starts, then
holds the App Server client until the turn has reached a terminal state.
"""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from jarvis_monitor import HoldTurnMonitor, HoldTurnRequest, NotificationPolicy
from jarvis_runtime.jarvis_native_task_launcher import AppServerClient, NativeTaskLauncherConfig
from adapters.codex_app_server.task_provisioning_adapter import append_terminal_turn_history, read_turn_history, verify_candidate_output, _pid_is_alive
from jarvis_runtime.coo_dispatcher_store import ProcessLock
from jarvis_control.provisioning import lane_batch_ids, lane_batch_size


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class _HolderStopUnknown(RuntimeError):
    """Owner is exiting, but this is not evidence that the model turn ended."""


class _RecoveredTurnTerminal(RuntimeError):
    """Exact terminal readback; release still requires the holder's own exit."""

    def __init__(self, status: str) -> None:
        super().__init__("Recovering holder read exact terminal; own exit confirmation pending")
        self.status = status


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
        for attempt in range(20):
            try:
                temporary.replace(path)
                break
            except PermissionError:
                if attempt == 19:
                    raise
                time.sleep(min(0.05 * (attempt + 1), 0.25))
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except PermissionError:
            pass  # A temp cleanup denial must not mask the replace outcome.


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError("task-monitor request must be a JSON object")
    return value


def _notification_policy(value: object) -> NotificationPolicy:
    raw = value if isinstance(value, dict) else {}
    milestones = raw.get("milestones") or []
    if not isinstance(milestones, list):
        raise RuntimeError("notifications.milestones must be a list")
    try:
        normalized = tuple(sorted({int(item) for item in milestones}))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("notifications.milestones must contain integers") from exc
    terminal = raw.get("terminal", False)
    if not isinstance(terminal, bool):
        raise RuntimeError("notifications.terminal must be a boolean")
    return NotificationPolicy(
        milestone_turns=normalized,
        terminal=terminal,
    )


def _append_monitor_events(path: Path, decision: object) -> None:
    events = getattr(decision, "notification_events", ())
    if not events:
        return
    with path.open("a", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps({
                "event_id": event.event_id,
                "event_type": event.event_type,
                "hold_id": event.hold_id,
                "thread_id": event.thread_id,
                "turn_id": event.turn_id,
                "turn_count": event.turn_count,
                "status": event.status,
                "message": event.message,
                "observed_at": _now(),
            }, ensure_ascii=False) + "\n")


def _validate_monitor_command(decision: object, *, hold_id: str, turn_id: str) -> None:
    if getattr(decision, "hold_id", None) != hold_id:
        raise RuntimeError("monitor command hold_id does not match active hold")
    if getattr(decision, "expected_turn_id", None) != turn_id:
        raise RuntimeError("monitor command expected_turn_id does not match active turn")
    if not str(getattr(decision, "command_id", "")).strip():
        raise RuntimeError("monitor command_id is required")


def _turn_input_binding(request: dict[str, Any], total_turn_count: int) -> dict[str, Any]:
    """Keep a full lane private to Holder while exposing only the current slice per Worker turn."""
    binding = dict(request.get("input_binding") or {})
    if "lane_item_count" not in binding:
        return binding
    binding["candidate_ids"] = lane_batch_ids(binding, total_turn_count)
    contract = binding.get("result_verification")
    if isinstance(contract, dict):
        candidate = binding["candidate_ids"][0]
        binding["result_verification"] = {
            **({"output_schema": contract["output_schema"]} if "output_schema" in contract else {}),
            **({"receipt_path": contract["receipt_paths"][str(candidate)]} if lane_batch_size(binding) == 1 else
               {"receipt_paths": {str(value): contract["receipt_paths"][str(value)] for value in binding["candidate_ids"]}}),
            "terminal_statuses": contract["terminal_statuses"],
            "request_id": request["request_id"], "turn_number": total_turn_count,
        }
    return binding


def _task_id(request: dict[str, Any], hold_id: str) -> str:
    source = request.get("source_ref") or ""
    if source.startswith("jarvis_loop_task:"):
        return source.removeprefix("jarvis_loop_task:")
    return hold_id.rsplit(":", 1)[0] if hold_id.startswith("loop-") and ":" in hold_id else hold_id


def _candidate_id(request: dict[str, Any], total_turn_count: int) -> int | None:
    binding = request.get("input_binding") or {}
    if "lane_item_count" in binding and lane_batch_size(binding) > 1:
        return None
    value = _turn_input_binding(request, total_turn_count).get("candidate_ids")
    return value[0] if isinstance(value, list) and len(value) == 1 and isinstance(value[0], int) else None


def hold_task(
    launcher_config_path: Path,
    request_path: Path,
    ack_path: Path,
    result_path: Path,
    *,
    monitor: HoldTurnMonitor | None = None,
) -> int:
    request = _read_json(request_path)
    request_id = str(request.get("request_id") or "").strip()
    max_turns = max(int(request.get("max_turns") or 1), 1)
    auto_continue = bool(request.get("auto_continue", False))
    continue_prompt = str(request.get("continue_prompt") or "继续").strip() or "继续"
    hold_id = str(request.get("hold_id") or request.get("monitor_id") or f"hold-{request_id}")
    initial_turn_count = max(int(request.get("initial_turn_count") or 1), 1)
    initial_total_turn_count = max(int(request.get("initial_total_turn_count") or initial_turn_count), 1)
    client: AppServerClient | None = None
    phase = "hold_started"
    terminal_confirmed = request.get("mode") != "recover"
    thread_id = str(request.get("thread_id") or "").strip()
    turn_id = str(request.get("turn_id") or "").strip() if request.get("mode") == "recover" else ""
    turn_count, total_turn_count = initial_turn_count, initial_total_turn_count
    verification: dict[str, Any] = {"status": "not_checked"}
    scheduler_failed_items = 0
    interrupt_at: float | None = None
    host_stop_selected = False

    def control_poll() -> None:
        nonlocal interrupt_at, host_stop_selected
        try:
            current = _read_json(request_path)
        except PermissionError:
            return  # Retry observation on the next bounded poll; do not stop healthy work.
        control = current.get("host_stop")
        if control is None:
            if not (current.get("mode") == "recover" and current.get("stop_requested") is True):
                return  # Preserve the ordinary stop-after-current-turn contract.
            control = {key: current.get(key) for key in ("request_id", "hold_id", "thread_id", "turn_id")}
        expected = {"request_id": request_id, "hold_id": hold_id, "thread_id": thread_id, "turn_id": turn_id}
        if (current.get("request_id") != request_id or not isinstance(control, dict)
                or any(control.get(key) != value for key, value in expected.items())):
            raise _HolderStopUnknown("Host stop identity conflict; no execution interrupt sent")
        host_stop_selected = True
        if request.get("mode") == "recover":
            readback = client.request("thread/read", {"threadId": thread_id, "includeTurns": True})
            thread = readback.get("thread") or {}
            turns = thread.get("turns") or []
            exact = [turn for turn in turns if isinstance(turn, dict) and turn.get("id") == turn_id]
            if (thread.get("id") == thread_id and len(exact) == 1
                    and exact[0].get("status") in {"completed", "failed", "interrupted", "cancelled", "canceled"}
                    and not any(isinstance(turn, dict) and turn.get("status") == "inProgress" for turn in turns)):
                raise _RecoveredTurnTerminal(exact[0]["status"])
            raise _HolderStopUnknown("Recovered observer stopped; original execution terminal remains unknown")
        if interrupt_at is None:
            # Latch before sending: a timeout/ambiguous response must not resend.
            interrupt_at = time.monotonic()
            report("host_stop_interrupting", {"host_stop": control})
            client.request("turn/interrupt", {"threadId": thread_id, "turnId": turn_id})
        elif time.monotonic() - interrupt_at >= 5:
            raise _HolderStopUnknown("Owner stop timed out without exact turn/completed evidence")

    def stop_requested() -> bool:
        current = _read_json(request_path)
        if current.get("request_id") != request.get("request_id"):
            raise RuntimeError("Hold request changed while a turn was owned")
        return current.get("stop_requested") is True

    def verify_output() -> dict[str, Any]:
        nonlocal verification
        verification = verify_candidate_output(request, total_turn_count)
        return verification

    def report(next_phase: str, details: dict[str, Any] | None = None) -> None:
        nonlocal phase, thread_id, turn_id
        phase = next_phase
        # Capture dispatch identity before ack replacement can fail. Do not recover
        # a turn by guessing the latest turn on a reused thread.
        if details and details.get("thread_id"):
            thread_id = str(details["thread_id"])
        if details and details.get("turn_id"):
            turn_id = str(details["turn_id"])
        _write_json(ack_path, {
            "request_id": request_id,
            "status": "accepted",
            "phase": phase,
            "pid": os.getpid(),
            "hold_id": hold_id,
            "thread_id": thread_id,
            "turn_id": turn_id,
            "turn_count": initial_turn_count,
            "session_turn_count": initial_turn_count,
            "total_turn_count": initial_total_turn_count,
            "max_turns": max_turns,
            "observed_at": _now(),
            **(details or {}),
        })

    try:
        report("hold_started")
        with ProcessLock(request_path.with_suffix(".lock"), owner_alive=_pid_is_alive):
            if stop_requested() and request.get("mode") != "recover":
                cancelled = {"request_id": request_id, "hold_id": hold_id, "status": "cancelled",
                             "phase": "stopped_before_dispatch", "terminal_confirmed": True,
                             "total_turn_count": 0, "observed_at": _now()}
                _write_json(result_path, cancelled)
                _write_json(ack_path, cancelled)
                return 0
            client = AppServerClient(NativeTaskLauncherConfig(launcher_config_path))
            report("launcher_config_loaded")
            input_binding = _turn_input_binding(request, initial_total_turn_count)
            mode = str(request.get("mode") or "create")
            if mode == "recover":
                thread_id = str(request.get("thread_id") or "").strip()
                turn_id = str(request.get("turn_id") or "").strip()
                if not thread_id or not turn_id:
                    raise RuntimeError("hold recovery requires thread_id and turn_id")
                client.start()
                report("recovery_attached", {"thread_id": thread_id, "turn_id": turn_id})
            elif mode == "resume":
                thread_id = str(request.get("thread_id") or "").strip()
                if not thread_id:
                    raise RuntimeError("hold resume requires thread_id")
                terminal_confirmed = False
                created = client.resume_turn_async(
                    thread_id,
                    str(request.get("prompt") or "").strip(),
                    client_user_message_id=request_id,
                    model=request.get("model"),
                    reasoning_effort=request.get("reasoning_effort"),
                    input_binding=input_binding,
                    on_phase=report,
                )
            else:
                terminal_confirmed = False
                created = client.create_task({**request, "input_binding": input_binding}, on_phase=report)
                thread_id = str(created.get("thread_id") or "").strip()
                turn_id = str(created.get("turn_id") or "").strip()
            if mode == "resume":
                turn_id = str(created.get("turn_id") or "").strip()
            if not thread_id or not turn_id:
                raise RuntimeError("App Server task creation did not return thread_id and turn_id")
        _write_json(ack_path, {
            "request_id": request_id,
            "status": "holding",
            "phase": "turn_holding",
            "pid": os.getpid(),
            "thread_id": thread_id,
            "turn_id": turn_id,
            "hold_id": hold_id,
            "turn_count": initial_turn_count,
            "session_turn_count": initial_turn_count,
            "total_turn_count": initial_total_turn_count,
            "max_turns": max_turns,
            "observed_at": _now(),
        })
        turn_count = initial_turn_count
        total_turn_count = initial_total_turn_count
        turn_monitor = monitor or HoldTurnMonitor()
        policy = _notification_policy(request.get("notifications"))
        events_path = result_path.with_name("monitor-events.jsonl")
        while True:
            verification = {"status": "not_checked"}
            monitor_request = HoldTurnRequest(
                hold_id=hold_id,
                thread_id=thread_id,
                turn_id=turn_id,
                turn_count=turn_count,
                max_turns=max_turns,
                continuation_enabled=auto_continue,
                continue_prompt=continue_prompt,
                notification_policy=policy,
                verify_output=verify_output,
                stop_requested=stop_requested,
                control_poll=control_poll,
            )
            decision = turn_monitor.observe(client, monitor_request)
            terminal_confirmed = decision.result_status in {
                "holding", "completed", "turn_limit_reached", "failed", "blocked",
                "cancelled", "canceled", "interrupted",
            }
            with ProcessLock(request_path.with_suffix(".lock"), owner_alive=_pid_is_alive):
                _validate_monitor_command(decision, hold_id=hold_id, turn_id=turn_id)
                if decision.action == "CONTINUE" and stop_requested():
                    decision = turn_monitor._stop(monitor_request, decision.command_id, "cancelled",
                                                  decision.final_message, "stop_requested")
                _append_monitor_events(events_path, decision)
                final_message = decision.final_message
                terminal_status = decision.result_status
                if not terminal_confirmed:
                    break  # Unknown execution is not an item terminal record or continuation permit.
                phase = "history_recording"
                history_path = Path(str(request.get("turn_history_path") or result_path.with_name("turn-history.sqlite")))
                binding = request.get("input_binding") or {}
                is_batch_lane = "lane_item_count" in binding and lane_batch_size(binding) > 1
                append_terminal_turn_history(
                    history_path, task_id=_task_id(request, hold_id), hold_id=hold_id,
                    request_id=request_id, thread_id=thread_id, turn_id=turn_id,
                    turn_number=total_turn_count, candidate_id=_candidate_id(request, total_turn_count),
                    status="failed" if verification.get("status") == "review" else terminal_status,
                    final_answer=final_message, completed_at=_now(),
                    candidate_ids=(lane_batch_ids(request["input_binding"], total_turn_count)
                                   if is_batch_lane else None),
                    output_verification=verification if "lane_item_count" in binding else None,
                )
                scheduler_failed_items = sum(item.get("status") == "review"
                    for row in read_turn_history(history_path, hold_id=hold_id)
                    for checked in [row.get("output_verification", {})]
                    for item in checked.get("items", [checked]))
                _write_json(ack_path, {
                    "request_id": request_id,
                    "status": "holding" if decision.action == "CONTINUE" else terminal_status,
                    "phase": "monitor_decision",
                    "pid": os.getpid(),
                    "hold_id": hold_id,
                    "thread_id": thread_id,
                    "turn_id": turn_id,
                    "turn_count": turn_count,
                    "session_turn_count": turn_count,
                    "total_turn_count": total_turn_count,
                    "max_turns": max_turns,
                    "monitor_command": {
                        "action": decision.action,
                        "command_id": decision.command_id,
                        "expected_turn_id": decision.expected_turn_id,
                        "reason": decision.reason,
                    },
                    "observed_at": _now(),
                })
                if decision.action != "CONTINUE":
                    break
                terminal_confirmed = False
                turn_id = ""  # A failed next dispatch must not reuse the previous terminal turn.
                started = client.start_turn_async(
                    thread_id,
                    str(decision.continue_prompt or continue_prompt),
                    client_user_message_id=decision.command_id,
                    model=request.get("model"),
                    reasoning_effort=request.get("reasoning_effort"),
                    input_binding=_turn_input_binding(request, total_turn_count + 1),
                    on_phase=report,
                )
                turn_id = str(started.get("turn_id") or "").strip()
                if not turn_id:
                    raise RuntimeError("monitor continuation command did not return turn_id")
                turn_count += 1
                total_turn_count += 1
                _write_json(ack_path, {
                    "request_id": request_id,
                    "status": "holding",
                    "phase": "turn_holding",
                    "pid": os.getpid(),
                    "thread_id": thread_id,
                    "turn_id": turn_id,
                    "hold_id": hold_id,
                    "turn_count": turn_count,
                    "session_turn_count": turn_count,
                    "total_turn_count": total_turn_count,
                    "max_turns": max_turns,
                    "observed_at": _now(),
                })
                if mode == "recover":
                    # This request is an exact recovery binding, not just an
                    # initial thread hint. Advance it from the actual dispatch
                    # response under the same request lock as continuation.
                    current = _read_json(request_path)
                    if current.get("request_id") != request_id:
                        raise RuntimeError("Hold request changed during continuation")
                    _write_json(request_path, {**current, "thread_id": thread_id, "turn_id": turn_id})
        _write_json(result_path, {
            "request_id": request_id,
            "status": terminal_status,
            "phase": "turn_terminal",
            "pid": os.getpid(),
            "thread_id": thread_id,
            "turn_id": turn_id,
            "hold_id": hold_id,
            "turn_count": turn_count,
            "session_turn_count": turn_count,
            "total_turn_count": total_turn_count,
            "max_turns": max_turns,
            "final_message": final_message,
            "output_verification": verification,
            "scheduler_failed_items": scheduler_failed_items,
            "terminal_confirmed": terminal_confirmed,
            "host_stop": interrupt_at is not None,
            "terminal_evidence": "owner_turn_completed",
            "observed_at": _now(),
        })
        return 0
    except Exception as exc:
        failure = {
            "request_id": request_id, "hold_id": hold_id, "status": "failed", "phase": phase,
            "pid": os.getpid(), "error_code": getattr(exc, "code", None),
            "thread_id": thread_id, "turn_id": turn_id,
            "turn_count": turn_count, "total_turn_count": total_turn_count,
            "max_turns": max_turns,
            "terminal_confirmed": terminal_confirmed, "reason": str(exc), "observed_at": _now(),
        }
        if host_stop_selected or interrupt_at is not None:
            failure.update(host_stop=True, execution_evidence="requires_owner_terminal")
        if isinstance(exc, _RecoveredTurnTerminal):
            failure.update(status="cancelled" if exc.status == "completed" else exc.status,
                           phase="host_stop_exit_pending", terminal_confirmed=False,
                           host_stop=True, execution_evidence="holder_exact_terminal_readback",
                           owner_terminal_readback={"thread_id": thread_id, "turn_id": turn_id,
                                                    "status": exc.status, "observed_at": _now()})
        if isinstance(exc, _HolderStopUnknown):
            failure.update(status="unknown", phase="host_stop_unknown", terminal_confirmed=False,
                           execution_evidence="requires_owner_terminal", host_stop=True)
        _write_json(result_path, failure)
        _write_json(ack_path, failure)
        return 1
    finally:
        if client is not None:
            owned_process = getattr(client, "process", None)
            client.close()
            if result_path.is_file():
                saved = _read_json(result_path)
                if saved.get("host_stop"):
                    _write_json(result_path, {**saved, "holder_client_exit_confirmed": (
                        owned_process is not None and owned_process.poll() is not None)})


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--launcher-config", type=Path, required=True)
    parser.add_argument("--request-path", type=Path, required=True)
    parser.add_argument("--ack-path", type=Path, required=True)
    parser.add_argument("--result-path", type=Path, required=True)
    args = parser.parse_args()
    return hold_task(
        args.launcher_config,
        args.request_path,
        args.ack_path,
        args.result_path,
    )


if __name__ == "__main__":
    raise SystemExit(main())
