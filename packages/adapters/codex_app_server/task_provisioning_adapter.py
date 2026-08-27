"""Queue hold work for the normal-user Jarvis host and return its receipt."""

from __future__ import annotations

import json
import re
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from jarvis_control.provisioning import (
    TaskMonitorResumeRequest,
    TaskProvisionReceipt,
    TaskProvisionRequest,
    observed_now,
)


class CodexAppServerTaskProvisioningAdapter:
    """Queue one task; the MCP process never starts or owns an App Server."""

    name = "codex-app-server-provisioning"

    def __init__(
        self,
        config_path: str | Path,
        *,
        state_dir: str | Path,
        config_loader: Callable[[Path], Any] | None = None,
    ) -> None:
        self._config_path = Path(config_path)
        self._state_dir = Path(state_dir)
        self._config_loader = config_loader or _runtime_symbols()["NativeTaskLauncherConfig"]

    def provision(self, request: TaskProvisionRequest) -> TaskProvisionReceipt:
        try:
            config = self._config_loader(self._config_path)
            project, project_path = config.resolve_project(request.project)
            hold_id = request.hold_id or f"hold-{request.request_id}"
            paths = self._paths(hold_id)
            _write_json(paths["request"], {
                "mode": "create",
                "request_id": request.request_id,
                "hold_id": hold_id,
                "project": project,
                "project_path": str(project_path),
                "title": request.title,
                "prompt": request.prompt,
                "model": request.model,
                "reasoning_effort": request.reasoning_effort,
                "max_turns": request.max_turns,
                "auto_continue": request.auto_continue,
                "continue_prompt": request.continue_prompt,
                "notifications": dict(request.notifications or {}),
                "input_binding": dict(request.input_binding or {}),
                "initial_turn_count": 1,
                "initial_total_turn_count": 1,
            })
            _write_json(paths["ack"], _accepted_ack(
                request_id=request.request_id,
                hold_id=hold_id,
                turn_count=1,
                total_turn_count=1,
                max_turns=request.max_turns,
            ))
        except Exception as exc:
            return TaskProvisionReceipt(
                request_id=request.request_id,
                status="failed",
                observed_at=observed_now(),
                reason=str(exc),
            )

        return TaskProvisionReceipt(
            request_id=request.request_id,
            status="accepted",
            observed_at=observed_now(),
            monitor_id=hold_id,
            hold_id=hold_id,
            turn_count=1,
            total_turn_count=1,
            max_turns=request.max_turns,
            phase="queued_for_user_host",
        )

    def preflight_projects(self) -> list[str]:
        """Return configured public project identifiers without writing a task request."""
        config = self._config_loader(self._config_path)
        return sorted(config.allowed_projects)

    def resume_with_monitor(self, request: TaskMonitorResumeRequest) -> TaskProvisionReceipt:
        try:
            config = self._config_loader(self._config_path)
            hold_id = request.hold_id or request.monitor_id or f"hold-{request.request_id}"
            if request.hold_id or request.monitor_id:
                previous = self.hold_status(hold_id)
                if str(previous.get("status") or "") in {"accepted", "holding", "running"}:
                    raise RuntimeError("hold already owns an active turn")
                existing_total = _positive_int(previous.get("total_turn_count")) or _positive_int(previous.get("turn_count")) or 0
                max_turns = _positive_int(previous.get("max_turns")) or request.max_turns
                initial_turn_count = 1
                initial_total_turn_count = existing_total + 1
            else:
                max_turns = request.max_turns
                initial_turn_count = 1
                initial_total_turn_count = 1
            paths = self._paths(hold_id)
            _archive_terminal_result(paths["result"])
            _write_json(paths["request"], {
                "mode": "resume",
                "request_id": request.request_id,
                "hold_id": hold_id,
                "thread_id": request.task_id,
                "prompt": request.prompt,
                "model": request.model,
                "reasoning_effort": request.reasoning_effort,
                "max_turns": max_turns,
                "auto_continue": request.auto_continue,
                "continue_prompt": request.continue_prompt,
                "notifications": dict(request.notifications or {}),
                "input_binding": dict(request.input_binding or {}),
                "initial_turn_count": initial_turn_count,
                "initial_total_turn_count": initial_total_turn_count,
            })
            _write_json(paths["ack"], _accepted_ack(
                request_id=request.request_id,
                hold_id=hold_id,
                turn_count=initial_turn_count,
                total_turn_count=initial_total_turn_count,
                max_turns=max_turns,
            ))
        except Exception as exc:
            return TaskProvisionReceipt(
                request_id=request.request_id, status="failed", observed_at=observed_now(), reason=str(exc)
            )
        return TaskProvisionReceipt(
            request_id=request.request_id,
            status="accepted",
            observed_at=observed_now(),
            monitor_id=hold_id,
            hold_id=hold_id,
            turn_count=initial_turn_count,
            total_turn_count=initial_total_turn_count,
            max_turns=max_turns,
            phase="queued_for_user_host",
        )

    def _paths(self, request_id: str) -> dict[str, Path]:
        safe_id = _safe_id(request_id)
        root = self._state_dir / "task-holds" / safe_id
        root.mkdir(parents=True, exist_ok=True)
        return {
            "request": root / "request.json",
            "ack": root / "ack.json",
            "result": root / "result.json",
            "events": root / "monitor-events.jsonl",
            "deliveries": root / "monitor-notification-deliveries.jsonl",
        }

    def hold_status(self, hold_id: str) -> dict[str, Any]:
        safe_id = _safe_id(str(hold_id))
        for root in (
            self._state_dir / "task-holds" / safe_id,
            self._state_dir / "task-monitors" / safe_id,
        ):
            for path in (root / "result.json", root / "ack.json"):
                if path.is_file():
                    value = json.loads(path.read_text(encoding="utf-8"))
                    if isinstance(value, dict):
                        return value
        raise RuntimeError("hold state was not found")

    def monitor_status(self, monitor_id: str) -> dict[str, Any]:
        """Compatibility alias for lifecycle callers using the old field name."""
        return self.hold_status(monitor_id)

    def pending_hold_notifications(self, hold_id: str) -> list[dict[str, Any]]:
        paths = self._paths(hold_id)
        delivered = {
            str(row.get("event_id") or "")
            for row in _read_jsonl(paths["deliveries"])
            if str(row.get("delivery_status") or "") == "delivered"
        }
        return [
            row for row in _read_jsonl(paths["events"])
            if str(row.get("event_id") or "") not in delivered
        ]

    def record_hold_notification_delivery(
        self, hold_id: str, event_id: str, delivery: dict[str, Any]
    ) -> None:
        paths = self._paths(hold_id)
        _append_jsonl(paths["deliveries"], {
            "event_id": event_id,
            "delivery_status": str(delivery.get("delivery_status") or delivery.get("status") or "failed"),
            "message_id": delivery.get("message_id"),
            "outbox_id": delivery.get("outbox_id"),
            "reason": delivery.get("reason"),
            "observed_at": observed_now().isoformat(),
        })


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
    temporary.replace(path)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    values: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            values.append(value)
    return values


def _append_jsonl(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, ensure_ascii=False) + "\n")


def _accepted_ack(
    *,
    request_id: str,
    hold_id: str,
    turn_count: int,
    total_turn_count: int,
    max_turns: int,
) -> dict[str, Any]:
    return {
        "request_id": request_id,
        "status": "accepted",
        "phase": "queued_for_user_host",
        "hold_id": hold_id,
        "turn_count": turn_count,
        "session_turn_count": turn_count,
        "total_turn_count": total_turn_count,
        "max_turns": max_turns,
        "observed_at": observed_now().isoformat(),
    }


def _archive_terminal_result(result_path: Path) -> None:
    """Preserve the previous terminal hold receipt before one new resume."""
    if not result_path.is_file():
        return
    value = json.loads(result_path.read_text(encoding="utf-8"))
    request_id = _safe_id(str(value.get("request_id") or "previous")) if isinstance(value, dict) else "previous"
    archive_dir = result_path.parent / "history"
    archive_dir.mkdir(parents=True, exist_ok=True)
    destination = archive_dir / f"{request_id}.result.json"
    if destination.exists():
        destination = archive_dir / f"{request_id}-{int(observed_now().timestamp())}.result.json"
    result_path.replace(destination)


def _positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _safe_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._") or "request"


def _runtime_symbols() -> dict[str, Any]:
    runtime_dir = Path(__file__).resolve().parents[2] / "jarvis_runtime"
    if str(runtime_dir) not in sys.path:
        sys.path.insert(0, str(runtime_dir))
    from jarvis_native_task_launcher import NativeTaskLauncherConfig

    return {"NativeTaskLauncherConfig": NativeTaskLauncherConfig}
