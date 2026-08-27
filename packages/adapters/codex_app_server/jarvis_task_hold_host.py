"""Independent holder for one Jarvis App Server task turn.

This process is intentionally detached from the MCP stdio process. It writes
an ownership acknowledgement immediately after the exact turn starts, then
holds the App Server client until the turn has reached a terminal state.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from jarvis_monitor import HoldTurnMonitor, HoldTurnRequest, NotificationPolicy
from jarvis_runtime.jarvis_native_task_launcher import AppServerClient, NativeTaskLauncherConfig


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
    temporary.replace(path)


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

    def report(next_phase: str, details: dict[str, Any] | None = None) -> None:
        nonlocal phase
        phase = next_phase
        _write_json(ack_path, {
            "request_id": request_id,
            "status": "accepted",
            "phase": phase,
            "pid": os.getpid(),
            "hold_id": hold_id,
            "turn_count": initial_turn_count,
            "session_turn_count": initial_turn_count,
            "total_turn_count": initial_total_turn_count,
            "max_turns": max_turns,
            "observed_at": _now(),
            **(details or {}),
        })

    try:
        report("hold_started")
        client = AppServerClient(NativeTaskLauncherConfig(launcher_config_path))
        report("launcher_config_loaded")
        if str(request.get("mode") or "create") == "resume":
            thread_id = str(request.get("thread_id") or "").strip()
            if not thread_id:
                raise RuntimeError("hold resume requires thread_id")
            created = client.resume_turn_async(
                thread_id,
                str(request.get("prompt") or "").strip(),
                client_user_message_id=request_id,
                model=request.get("model"),
                reasoning_effort=request.get("reasoning_effort"),
                input_binding=request.get("input_binding"),
                on_phase=report,
            )
        else:
            created = client.create_task(request, on_phase=report)
            thread_id = str(created.get("thread_id") or "").strip()
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
            decision = turn_monitor.observe(client, HoldTurnRequest(
                hold_id=hold_id,
                thread_id=thread_id,
                turn_id=turn_id,
                turn_count=turn_count,
                max_turns=max_turns,
                continuation_enabled=auto_continue,
                continue_prompt=continue_prompt,
                notification_policy=policy,
            ))
            _validate_monitor_command(decision, hold_id=hold_id, turn_id=turn_id)
            _append_monitor_events(events_path, decision)
            final_message = decision.final_message
            terminal_status = decision.result_status
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
            started = client.start_turn_async(
                thread_id,
                str(decision.continue_prompt or continue_prompt),
                client_user_message_id=decision.command_id,
                model=request.get("model"),
                reasoning_effort=request.get("reasoning_effort"),
                input_binding=request.get("input_binding"),
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
            "observed_at": _now(),
        })
        return 0
    except Exception as exc:
        _write_json(ack_path, {
            "request_id": request_id,
            "status": "failed",
            "phase": phase,
            "pid": os.getpid(),
            "error_code": getattr(exc, "code", None),
            "reason": str(exc),
            "observed_at": _now(),
        })
        _write_json(result_path, {
            "request_id": request_id,
            "status": "failed",
            "phase": phase,
            "pid": os.getpid(),
            "error_code": getattr(exc, "code", None),
            "reason": str(exc),
            "observed_at": _now(),
        })
        return 1
    finally:
        if client is not None:
            client.close()


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
