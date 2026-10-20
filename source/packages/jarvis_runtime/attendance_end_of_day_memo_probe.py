"""Queue one Jarvis end-of-day checklist from the daily attendance event."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
from typing import Any, Callable


TZ = timezone(timedelta(hours=8), name="Asia/Shanghai")
CONSUMER_ID = "end_of_day_memo"
TERMINAL_CONSUMER_STATES = {"QUEUED", "DELIVERED", "SCHEDULED"}


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
        temp_name = handle.name
    os.replace(temp_name, path)


class AttendanceEndOfDayMemoProbe:
    """Read attendance state and queue one idempotent Jarvis memo when due."""

    def __init__(
        self,
        root: Path,
        *,
        now_provider: Callable[[], datetime] | None = None,
        enqueue: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    ):
        self.root = root.resolve()
        self.now_provider = now_provider or (lambda: datetime.now(TZ))
        self.enqueue = enqueue or self._enqueue_via_dispatcher

    @property
    def dispatcher_root(self) -> Path:
        return self.root / "coo_state" / "dispatcher"

    def _daily_report_state(self, report_date: str) -> str:
        db_path = self.root / "coo_state" / "daily_reports" / "daily_reports.sqlite"
        if not db_path.is_file():
            return "未提交"
        connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5)
        connection.row_factory = sqlite3.Row
        try:
            row = connection.execute(
                """
                SELECT status, submitted_at, submission_evidence
                FROM daily_reports
                WHERE report_date=?
                ORDER BY revision DESC, id DESC
                LIMIT 1
                """,
                (report_date,),
            ).fetchone()
        except sqlite3.Error:
            return "状态未知，请人工核对"
        finally:
            connection.close()
        if not row:
            return "未提交"
        if (
            str(row["status"] or "").lower() == "submitted"
            and str(row["submitted_at"] or "").strip()
            and str(row["submission_evidence"] or "").strip()
        ):
            submitted = datetime.fromisoformat(
                str(row["submitted_at"]).replace("Z", "+00:00")
            )
            return f"已提交（{submitted.astimezone(TZ):%H:%M}）"
        return "未提交"

    @staticmethod
    def _content(expected_off: datetime, daily_report_state: str) -> str:
        return (
            f"下班前 10 分钟备忘录（预计 {expected_off:%H:%M} 下班）\n"
            "请完成下班托管检查：\n"
            "- 发信器：检查服务和调度器是否正常，队列是否按计划运行。\n"
            "- Codex 任务：检查是否正常；需跨下班继续的任务确认已托管。\n"
            f"- 日报：当前记录{daily_report_state}；如未提交请完成并留存成功证据。"
        )

    def _enqueue_via_dispatcher(self, request: dict[str, Any]) -> dict[str, Any]:
        day = str(request["task_id"]).rsplit("-", 1)[-1]
        request_path = (
            self.root
            / "output"
            / "attendance_events"
            / f"{day}-end-of-day-memo-request.json"
        )
        atomic_write_json(request_path, request)
        result = subprocess.run(
            [
                sys.executable,
                str(self.root / "tools" / "coo_dispatcher_store.py"),
                "--root",
                str(self.dispatcher_root),
                "enqueue-outbox",
                "--request",
                str(request_path),
            ],
            cwd=self.root,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip())
        return json.loads(result.stdout)

    def run(self, probe_config: dict[str, Any]) -> dict[str, Any]:
        now = self.now_provider()
        if now.tzinfo is None:
            now = now.replace(tzinfo=TZ)
        now = now.astimezone(TZ)
        day = now.strftime("%Y%m%d")
        event_path = self.root / "coo_state" / "attendance_events" / f"{day}.json"
        if not event_path.is_file():
            return {"status": "NO_ATTENDANCE_EVENT", "event_date": day}
        event = json.loads(event_path.read_text(encoding="utf-8-sig"))
        if (
            event.get("source_read_status") != "SUCCESS"
            or event.get("work_status") != "WORK_STARTED"
            or not event.get("expected_off_at")
        ):
            return {
                "status": "ATTENDANCE_EVENT_NOT_READY",
                "event_key": event.get("event_key"),
            }
        consumers = event.setdefault("consumers", {})
        existing = consumers.get(CONSUMER_ID) or {}
        if existing.get("status") in TERMINAL_CONSUMER_STATES:
            return {
                "status": "ALREADY_QUEUED",
                "event_key": event.get("event_key"),
                "outbox_id": existing.get("outbox_id"),
            }
        expected_off = datetime.fromisoformat(
            str(event["expected_off_at"]).replace("Z", "+00:00")
        ).astimezone(TZ)
        lead_minutes = int(probe_config.get("lead_minutes", 10))
        grace_minutes = int(probe_config.get("grace_minutes", 5))
        remind_at = expected_off - timedelta(minutes=lead_minutes)
        window_ends_at = expected_off + timedelta(minutes=grace_minutes)
        if now < remind_at:
            return {
                "status": "NOT_DUE",
                "event_key": event.get("event_key"),
                "remind_at": remind_at.isoformat(timespec="seconds"),
            }
        if now > window_ends_at:
            return {
                "status": "MISSED_WINDOW",
                "event_key": event.get("event_key"),
                "remind_at": remind_at.isoformat(timespec="seconds"),
            }
        binding = json.loads(
            (self.dispatcher_root / "binding.json").read_text(encoding="utf-8-sig")
        )
        if not binding.get("live_send_enabled"):
            raise RuntimeError("JARVIS_LIVE_SEND_DISABLED")
        report_date = now.strftime("%Y-%m-%d")
        request = {
            "task_id": f"attendance-end-of-day-memo-{day}",
            "source_thread_id": binding["dispatcher_thread_id"],
            "status": "COMPLETE",
            "recipient": f"chat:{binding['primary_chat_id']}",
            "content": self._content(
                expected_off,
                self._daily_report_state(report_date),
            ),
            "message_kind": "attendance_end_of_day_memo",
            "confirmation_id": None,
        }
        queued = self.enqueue(request)
        record = queued.get("record") if isinstance(queued, dict) else None
        outbox_id = record.get("outbox_id") if isinstance(record, dict) else None
        consumers[CONSUMER_ID] = {
            "idempotency_key": f"attendance-end-of-day-memo-{event['event_key']}",
            "input_event_key": event["event_key"],
            "status": "QUEUED",
            "scheduled_at": now.isoformat(timespec="seconds"),
            "delivery_status": "QUEUED",
            "outbox_id": outbox_id,
            "error": None,
        }
        atomic_write_json(event_path, event)
        return {
            "status": "QUEUED",
            "event_key": event.get("event_key"),
            "outbox_id": outbox_id,
            "remind_at": remind_at.isoformat(timespec="seconds"),
            "expected_off_at": expected_off.isoformat(timespec="seconds"),
        }


__all__ = ["AttendanceEndOfDayMemoProbe"]
