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

from jarvis_native_task_launcher import AppServerClient, NativeTaskLauncherConfig


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


def hold_task(
    launcher_config_path: Path,
    request_path: Path,
    ack_path: Path,
    result_path: Path,
) -> int:
    request = _read_json(request_path)
    request_id = str(request.get("request_id") or "").strip()
    max_turns = max(int(request.get("max_turns") or 1), 1)
    auto_continue = bool(request.get("auto_continue", False))
    continue_prompt = str(request.get("continue_prompt") or "继续").strip() or "继续"
    monitor_id = str(request.get("monitor_id") or f"monitor-{request_id}")
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
            "monitor_id": monitor_id,
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
                raise RuntimeError("monitor resume requires thread_id")
            created = client.resume_turn_async(
                thread_id,
                str(request.get("prompt") or "").strip(),
                client_user_message_id=request_id,
                model=request.get("model"),
                reasoning_effort=request.get("reasoning_effort"),
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
            "monitor_id": monitor_id,
            "turn_count": initial_turn_count,
            "session_turn_count": initial_turn_count,
            "total_turn_count": initial_total_turn_count,
            "max_turns": max_turns,
            "observed_at": _now(),
        })
        turn_count = initial_turn_count
        total_turn_count = initial_total_turn_count
        while True:
            terminal = client.wait_for_turn_terminal(thread_id, turn_id, wait_forever=True)
            terminal_status = str(terminal.get("status") or "unknown")
            if terminal_status != "completed":
                final_message = ""
                break
            final_message = client.wait_for_turn_readback(thread_id, turn_id)
            if turn_count >= max_turns:
                terminal_status = "turn_limit_reached"
                break
            if not auto_continue:
                break
            started = client.start_turn_async(
                thread_id,
                continue_prompt,
                client_user_message_id=f"{request_id}:continue:{turn_count + 1}",
                model=request.get("model"),
                reasoning_effort=request.get("reasoning_effort"),
                on_phase=report,
            )
            turn_id = str(started.get("turn_id") or "").strip()
            if not turn_id:
                raise RuntimeError("monitor continuation did not return turn_id")
            turn_count += 1
            total_turn_count += 1
            _write_json(ack_path, {
                "request_id": request_id,
                "status": "holding",
                "phase": "turn_holding",
                "pid": os.getpid(),
                "thread_id": thread_id,
                "turn_id": turn_id,
                "monitor_id": monitor_id,
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
            "monitor_id": monitor_id,
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
