"""Queue hold work for the normal-user Jarvis host and return its receipt."""

from __future__ import annotations

import json
import os
import re
import sqlite3
import sys
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from jarvis_runtime.coo_dispatcher_store import ProcessLock

from jarvis_control.provisioning import (
    TaskMonitorResumeRequest,
    TaskProvisionReceipt,
    TaskProvisionRequest,
    observed_now,
)


class CodexAppServerTaskProvisioningAdapter:
    """Queue one task; the MCP process never starts or owns an App Server."""

    name = "codex-app-server-provisioning"
    _HOST_HEALTH_MAX_AGE = timedelta(seconds=30)

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
                "turn_history_path": str(self._state_dir / "turn-history.sqlite"),
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

    def hold_host_health(self) -> dict[str, str]:
        """Check whether the fixed HoldHost can accept a Loop before it is created."""
        try:
            config = self._config_loader(self._config_path)
        except Exception as exc:
            return {"status": "host_not_ready", "reason": f"HoldHost config is unreadable: {exc}"}
        health = _read_json_file(self._state_dir / "hold-host.json")
        if health is None:
            return {"status": "host_not_ready", "reason": "HoldHost health file is unreadable"}
        if str(health.get("status") or "") not in {"ready", "holding"}:
            return {"status": "host_not_ready", "reason": "HoldHost status is not ready"}
        observed_at = _parse_observed_at(health.get("observed_at"))
        if observed_at is None:
            return {"status": "host_not_ready", "reason": "HoldHost observed_at is invalid"}
        if observed_now() - observed_at > self._HOST_HEALTH_MAX_AGE:
            return {"status": "host_not_ready", "reason": "HoldHost observed_at is stale"}
        profile = str(getattr(config, "profile", "") or "").strip()
        if not profile or str(health.get("profile") or "").strip() != profile:
            return {"status": "host_not_ready", "reason": "HoldHost profile does not match"}
        codex_home = str(getattr(config, "expected_codex_home", "") or "").strip()
        if not codex_home or not _same_path(health.get("codex_home"), codex_home):
            return {"status": "host_not_ready", "reason": "HoldHost CODEX_HOME does not match"}
        if not _same_path(health.get("state_dir"), self._state_dir):
            return {"status": "host_not_ready", "reason": "HoldHost state_dir does not match"}
        return {"status": "ready"}

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
                "turn_history_path": str(self._state_dir / "turn-history.sqlite"),
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
        return self._paths_at(root)

    @staticmethod
    def _paths_at(root: Path) -> dict[str, Path]:
        return {
            "request": root / "request.json",
            "ack": root / "ack.json",
            "result": root / "result.json",
            "events": root / "monitor-events.jsonl",
            "deliveries": root / "monitor-notification-deliveries.jsonl",
        }

    def _existing_paths(self, hold_id: str) -> dict[str, Path] | None:
        safe_id = _safe_id(str(hold_id))
        for parent_name in ("task-holds", "task-monitors"):
            parent = self._state_dir / parent_name
            root = parent / safe_id
            if root.is_dir():
                return self._paths_at(root)
            if parent.is_dir():
                for candidate in parent.iterdir():
                    if candidate.is_dir() and _hold_id_from_state(candidate) == hold_id:
                        return self._paths_at(candidate)
        return None

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

    def read_turn_history(
        self, *, task_id: str | None = None, hold_id: str | None = None,
        thread_id: str | None = None, turn_id: str | None = None,
    ) -> list[dict[str, Any]]:
        if not any((task_id, hold_id, thread_id, turn_id)):
            raise ValueError("history requires task_id, hold_id, thread_id, or turn_id")
        return read_turn_history(
            self._state_dir / "turn-history.sqlite", task_id=task_id, hold_id=hold_id,
            thread_id=thread_id, turn_id=turn_id,
        )

    def pending_hold_notifications(
        self, hold_id: str, *, event_type: str | None = None,
    ) -> list[dict[str, Any]]:
        paths = self._existing_paths(hold_id)
        if paths is None:
            return []
        delivered = {
            str(row.get("event_id") or "")
            for row in _read_jsonl(paths["deliveries"])
            if (
                str(row.get("delivery_status") or "") == "delivered"
                and str(row.get("message_id") or "").strip()
            )
        }
        return [
            row for row in _read_jsonl(paths["events"])
            if (
                str(row.get("event_id") or "") not in delivered
                and (event_type is None or str(row.get("event_type") or "") == event_type)
            )
        ]

    def pending_hold_notification_holds(self, *, event_type: str | None = None) -> list[str]:
        """Return exact Hold identities that have durable, undelivered notification events."""
        hold_ids: set[str] = set()
        for parent_name in ("task-holds", "task-monitors"):
            parent = self._state_dir / parent_name
            if not parent.is_dir():
                continue
            for root in parent.iterdir():
                if not root.is_dir():
                    continue
                event_rows = _read_jsonl(root / "monitor-events.jsonl")
                if not event_rows:
                    continue
                delivered = {
                    str(row.get("event_id") or "")
                    for row in _read_jsonl(root / "monitor-notification-deliveries.jsonl")
                    if (
                        str(row.get("delivery_status") or "") == "delivered"
                        and str(row.get("message_id") or "").strip()
                    )
                }
                if not any(
                    str(event.get("event_id") or "")
                    and str(event.get("event_id") or "") not in delivered
                    and (event_type is None or str(event.get("event_type") or "") == event_type)
                    for event in event_rows
                ):
                    continue
                hold_id = _hold_id_from_state(root)
                if hold_id:
                    hold_ids.add(hold_id)
        return sorted(hold_ids)

    def hold_notification_delivery_lock(self, hold_id: str) -> ProcessLock:
        """Serialize one Hold's notification send and its durable receipt."""
        paths = self._existing_paths(hold_id)
        if paths is None:
            raise RuntimeError("hold state was not found")
        return ProcessLock(paths["deliveries"].with_suffix(".lock"))

    def record_hold_notification_delivery(
        self, hold_id: str, event_id: str, delivery: dict[str, Any]
    ) -> None:
        paths = self._existing_paths(hold_id) or self._paths(hold_id)
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


def _read_json_file(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _hold_id_from_state(root: Path) -> str | None:
    for name in ("request.json", "ack.json", "result.json"):
        value = _read_json_file(root / name)
        hold_id = str((value or {}).get("hold_id") or "").strip()
        if hold_id:
            return hold_id
    return None


def _append_jsonl(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, ensure_ascii=False) + "\n")


def append_terminal_turn_history(
    path: Path,
    *,
    task_id: str,
    hold_id: str,
    request_id: str,
    thread_id: str,
    turn_id: str,
    turn_number: int,
    candidate_id: int | None,
    status: str,
    final_answer: str,
    completed_at: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=10)
    try:
        connection.execute("PRAGMA busy_timeout=10000")
        connection.execute(
            """CREATE TABLE IF NOT EXISTS turn_history (
                hold_id TEXT NOT NULL, turn_id TEXT NOT NULL,
                task_id TEXT NOT NULL, request_id TEXT NOT NULL, thread_id TEXT NOT NULL,
                turn_number INTEGER NOT NULL, candidate_id INTEGER,
                status TEXT NOT NULL, final_answer TEXT, has_final_answer INTEGER NOT NULL,
                completed_at TEXT NOT NULL, recorded_at TEXT NOT NULL,
                PRIMARY KEY (hold_id, turn_id)
            )"""
        )
        connection.execute(
            """INSERT OR IGNORE INTO turn_history(
                hold_id,turn_id,task_id,request_id,thread_id,turn_number,candidate_id,
                status,final_answer,has_final_answer,completed_at,recorded_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (hold_id, turn_id, task_id, request_id, thread_id, turn_number, candidate_id,
             status, final_answer or None, int(bool(final_answer)), completed_at, observed_now().isoformat()),
        )
        connection.commit()
    finally:
        connection.close()


def read_turn_history(
    path: Path,
    *,
    task_id: str | None = None,
    hold_id: str | None = None,
    thread_id: str | None = None,
    turn_id: str | None = None,
) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    clauses: list[str] = []
    parameters: list[str] = []
    for column, value in (("task_id", task_id), ("hold_id", hold_id), ("thread_id", thread_id), ("turn_id", turn_id)):
        if value and value.strip():
            clauses.append(f"{column}=?")
            parameters.append(value.strip())
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            "SELECT task_id,hold_id,request_id,thread_id,turn_id,turn_number,candidate_id,"
            "status,final_answer,has_final_answer,completed_at,recorded_at FROM turn_history"
            + where + " ORDER BY recorded_at, turn_number", parameters,
        ).fetchall()
    finally:
        connection.close()
    return [dict(row) for row in rows]


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


def _same_path(left: object, right: object) -> bool:
    try:
        return os.path.normcase(str(Path(str(left)).resolve())) == os.path.normcase(str(Path(str(right)).resolve()))
    except OSError:
        return False


def _parse_observed_at(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _safe_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._") or "request"


def _runtime_symbols() -> dict[str, Any]:
    runtime_dir = Path(__file__).resolve().parents[2] / "jarvis_runtime"
    if str(runtime_dir) not in sys.path:
        sys.path.insert(0, str(runtime_dir))
    from jarvis_native_task_launcher import NativeTaskLauncherConfig

    return {"NativeTaskLauncherConfig": NativeTaskLauncherConfig}
