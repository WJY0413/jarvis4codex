"""Native, read-only progress probe for a fixed Workbench draft roster.

The probe deliberately never contacts Codex or the email sender.  It reads the
local Workbench health endpoint and SQLite database, then uses the existing
Jarvis outbox only for one state-transition notification at a time.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
from typing import Any, Callable
from urllib.request import urlopen


TERMINAL_BLOCKED = {"cancelled", "failed", "pending_review"}
OPEN_DRAFT_STATUSES = {"draft", "approved", "queued"}
NOTIFY_STATES = {
    "HEALTHY_WAITING_FIRST_DUE",
    "QUEUE_PAUSED",
    "APP_UNAVAILABLE",
    "DUE_STALLED",
    "DRAFT_MISSING",
    "BATCH_COMPLETED",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
        temporary = handle.name
    os.replace(temporary, path)


def parse_db_time(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


class WorkbenchQueueProgressProbe:
    """Observe a frozen draft roster and queue state notifications natively."""

    def __init__(
        self,
        root: Path,
        *,
        now_provider: Callable[[], datetime] | None = None,
        health_reader: Callable[[str], int] | None = None,
        enqueue: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    ) -> None:
        self.root = root.resolve()
        self.now_provider = now_provider or (lambda: datetime.now(timezone.utc))
        self.health_reader = health_reader or self._health_status
        self.enqueue = enqueue or self._enqueue_via_dispatcher

    @property
    def dispatcher_root(self) -> Path:
        return self.root / "coo_state" / "dispatcher"

    @staticmethod
    def _health_status(url: str) -> int:
        with urlopen(url, timeout=4) as response:  # nosec B310: validated loopback URL
            return int(response.status)

    def _enqueue_via_dispatcher(self, request: dict[str, Any]) -> dict[str, Any]:
        path = self.root / "output" / "workbench_monitor_outbox" / (
            f"{request['task_id']}.json"
        )
        atomic_write_json(path, request)
        completed = subprocess.run(
            [
                sys.executable,
                str(self.root / "tools" / "coo_dispatcher_store.py"),
                "--root",
                str(self.dispatcher_root),
                "enqueue-outbox",
                "--request",
                str(path),
            ],
            cwd=self.root,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr.strip() or completed.stdout.strip())
        return json.loads(completed.stdout)

    @staticmethod
    def _load_state(path: Path) -> dict[str, Any]:
        if not path.is_file():
            return {}
        try:
            value = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError):
            return {}
        return value if isinstance(value, dict) else {}

    @staticmethod
    def _setting_bool(connection: sqlite3.Connection, key: str) -> bool:
        row = connection.execute(
            "SELECT value FROM app_settings WHERE key=? LIMIT 1", (key,)
        ).fetchone()
        return str(row[0] if row else "").strip().casefold() in {"1", "true", "yes", "on"}

    def _snapshot(self, config: dict[str, Any], now: datetime) -> dict[str, Any]:
        database = Path(str(config["workbench_db"])).resolve()
        expected_ids = [int(value) for value in config["expected_draft_ids"]]
        placeholders = ",".join("?" for _ in expected_ids)
        connection = sqlite3.connect(
            f"file:{database}?mode=ro", uri=True, timeout=5
        )
        connection.row_factory = sqlite3.Row
        try:
            rows = connection.execute(
                f"""
                SELECT d.draft_id,d.status,d.scheduled_at,
                       EXISTS(
                         SELECT 1 FROM activity_records a
                         WHERE a.draft_id=d.draft_id AND lower(coalesce(a.smtp_status,''))='sent'
                       ) AS actual_sent
                FROM email_drafts d
                WHERE d.draft_id IN ({placeholders})
                """,
                expected_ids,
            ).fetchall()
            latest_sent = connection.execute(
                f"""
                SELECT max(sent_at) FROM activity_records
                WHERE draft_id IN ({placeholders})
                  AND lower(coalesce(smtp_status,''))='sent'
                """,
                expected_ids,
            ).fetchone()[0]
            queue_paused = self._setting_bool(connection, "queue_paused")
        finally:
            connection.close()
        by_id = {int(row["draft_id"]): dict(row) for row in rows}
        missing = sorted(set(expected_ids) - set(by_id))
        sent = sum(1 for row in by_id.values() if int(row["actual_sent"] or 0))
        blocked = sum(
            1 for row in by_id.values()
            if str(row["status"] or "").casefold() in TERMINAL_BLOCKED
        )
        open_rows = [
            row for row in by_id.values()
            if not int(row["actual_sent"] or 0)
            and str(row["status"] or "").casefold() in OPEN_DRAFT_STATUSES
        ]
        queued_due: list[dict[str, Any]] = []
        scheduled = [parse_db_time(row["scheduled_at"]) for row in open_rows]
        scheduled = [item for item in scheduled if item is not None]
        grace_seconds = int(config["overdue_grace_seconds"])
        for row in open_rows:
            due_at = parse_db_time(row["scheduled_at"])
            if (
                str(row["status"] or "").casefold() == "queued"
                and due_at is not None
                and due_at.timestamp() <= now.timestamp() - grace_seconds
            ):
                queued_due.append(row)
        terminal_total = sent + blocked
        return {
            "tracked_total": len(expected_ids),
            "found_total": len(rows),
            "actual_sent": sent,
            "terminal_blocked": blocked,
            "open_total": len(open_rows),
            "missing_draft_ids": missing,
            "queue_paused": queue_paused,
            "next_scheduled_at": min(scheduled).isoformat() if scheduled else None,
            "overdue_queued_draft_ids": sorted(int(row["draft_id"]) for row in queued_due),
            "latest_actual_sent_at": latest_sent,
            "completed": not missing and terminal_total == len(expected_ids),
        }

    @staticmethod
    def _content(batch_label: str, state: str, snapshot: dict[str, Any]) -> str:
        return (
            f"Workbench 发送监测 [{batch_label}]：{state}\n"
            f"实际已发 {snapshot.get('actual_sent', 0)}/{snapshot.get('tracked_total', 0)}；"
            f"终态拦截 {snapshot.get('terminal_blocked', 0)}；"
            f"待推进 {snapshot.get('open_total', 0)}。\n"
            "依据为 Workbench health、queue_paused 与 activity_records.smtp_status=sent；未调用 AI。"
        )

    def _maybe_notify(
        self, config: dict[str, Any], state: str, snapshot: dict[str, Any]
    ) -> str | None:
        if state not in NOTIFY_STATES:
            return None
        state_path = Path(str(config["state_path"])).resolve()
        previous = self._load_state(state_path)
        if previous.get("notified_state") == state:
            return str(previous.get("outbox_id") or "") or None
        binding = json.loads(
            (self.dispatcher_root / "binding.json").read_text(encoding="utf-8-sig")
        )
        if not binding.get("live_send_enabled"):
            raise RuntimeError("JARVIS_LIVE_SEND_DISABLED")
        request = {
            "task_id": f"workbench-progress-{config['heartbeat_id']}-{state.casefold()}",
            "source_thread_id": binding["dispatcher_thread_id"],
            "status": "COMPLETE",
            "recipient": f"chat:{binding['primary_chat_id']}",
            "content": self._content(str(config["batch_label"]), state, snapshot),
            "message_kind": "workbench_queue_progress",
            "confirmation_id": None,
        }
        queued = self.enqueue(request)
        record = queued.get("record") if isinstance(queued, dict) else None
        outbox_id = str(record.get("outbox_id") or "") if isinstance(record, dict) else ""
        atomic_write_json(state_path, {
            "notified_state": state,
            "outbox_id": outbox_id or None,
            "updated_at": utc_now(),
        })
        return outbox_id or None

    def run(self, config: dict[str, Any]) -> dict[str, Any]:
        now = self.now_provider()
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        now = now.astimezone(timezone.utc)
        health_error: str | None = None
        try:
            health_status = self.health_reader(str(config["health_url"]))
            health_ok = 200 <= int(health_status) < 300
        except Exception as exc:
            health_ok = False
            health_status = None
            health_error = str(exc)
        snapshot = self._snapshot(config, now)
        snapshot.update({
            "observed_at": now.isoformat(),
            "health_ok": health_ok,
            "health_status": health_status,
            "health_error": health_error,
        })
        if snapshot["completed"]:
            state = "BATCH_COMPLETED"
        elif not health_ok:
            state = "APP_UNAVAILABLE"
        elif snapshot["queue_paused"]:
            state = "QUEUE_PAUSED"
        elif snapshot["missing_draft_ids"]:
            state = "DRAFT_MISSING"
        elif snapshot["overdue_queued_draft_ids"]:
            state = "DUE_STALLED"
        elif snapshot["actual_sent"] == 0 and snapshot["next_scheduled_at"]:
            state = "HEALTHY_WAITING_FIRST_DUE"
        else:
            state = "PROGRESSING"
        outbox_id = self._maybe_notify(config, state, snapshot)
        return {
            "status": state,
            "snapshot": snapshot,
            "outbox_id": outbox_id,
            "terminal": state == "BATCH_COMPLETED",
        }


__all__ = ["WorkbenchQueueProgressProbe", "parse_db_time"]
