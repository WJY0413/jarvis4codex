"""Jarvis-owned heartbeat scheduler and Codex thread recovery service.

This service is independent from Codex native automations. It stores schedules in
SQLite, wakes exact existing threads through Codex App Server, and can relaunch
Codex Desktop when an explicitly authorized recovery request asks for it.
"""

from __future__ import annotations

import argparse
import importlib
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
import ctypes
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from typing import Any, Callable
from urllib.parse import urlparse
import uuid

from jarvis_native_task_launcher import (
    AppServerClient,
    NativeTaskError,
    NativeTaskLauncherConfig,
)


WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = WORKSPACE_ROOT / "jarvis_heartbeat_service.config.json"
SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
ACTIVE = "ACTIVE"
PAUSED = "PAUSED"
TERMINAL = {"COMPLETED", "CANCELLED", "FAILED"}


class HeartbeatError(RuntimeError):
    """Raised when a heartbeat request cannot be accepted or executed safely."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_time(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise HeartbeatError(f"invalid ISO timestamp: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temp, path)


def canonical_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError as exc:
        raise HeartbeatError(f"JSON file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise HeartbeatError(f"invalid JSON file {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise HeartbeatError(f"JSON root must be an object: {path}")
    return value


def resolve_relative(base: Path, value: Any, default: Path) -> Path:
    candidate = Path(str(value)) if value else default
    return candidate.resolve() if candidate.is_absolute() else (base / candidate).resolve()


@dataclass(frozen=True)
class HeartbeatConfig:
    path: Path
    db_path: Path
    health_path: Path
    lock_path: Path
    launcher_config_path: Path
    poll_seconds: float
    retry_seconds: int
    max_failures: int
    min_interval_seconds: int
    max_interval_seconds: int
    max_prompt_chars: int
    desktop_recovery_enabled: bool
    desktop_exe_path: Path | None
    desktop_app_user_model_id: str | None
    desktop_start_timeout_seconds: int
    turn_completion_timeout_seconds: int
    max_concurrent_runs: int
    max_runs_without_explicit_override: int
    receipt_stale_round_limit: int
    require_prompt_continuation_receipt: bool
    prohibited_target_thread_ids: frozenset[str]
    quota_probe_double_read_delay_seconds: float
    wake_payload_mode: str
    contracts_dir: Path
    max_wake_envelope_chars: int
    standard_bridge_package_root: Path | None

    @classmethod
    def load(cls, path: Path) -> "HeartbeatConfig":
        resolved = path.resolve()
        raw = load_json(resolved)
        base = resolved.parent
        desktop_raw = str(raw.get("desktop_exe_path") or "").strip()
        config = cls(
            path=resolved,
            db_path=resolve_relative(
                base,
                raw.get("db_path"),
                WORKSPACE_ROOT / "coo_state" / "dispatcher" / "jarvis-heartbeats.sqlite",
            ),
            health_path=resolve_relative(
                base,
                raw.get("health_path"),
                WORKSPACE_ROOT / "coo_state" / "dispatcher" / "jarvis-heartbeat-health.json",
            ),
            lock_path=resolve_relative(
                base,
                raw.get("lock_path"),
                WORKSPACE_ROOT / "coo_state" / "dispatcher" / "jarvis-heartbeat-service.lock",
            ),
            launcher_config_path=resolve_relative(
                base,
                raw.get("native_task_launcher_config"),
                WORKSPACE_ROOT / "coo_state" / "dispatcher" / "native_task_launcher.config.json",
            ),
            poll_seconds=max(float(raw.get("poll_seconds", 5)), 1.0),
            retry_seconds=max(int(raw.get("retry_seconds", 60)), 5),
            max_failures=max(int(raw.get("max_failures", 5)), 1),
            min_interval_seconds=max(int(raw.get("min_interval_seconds", 30)), 10),
            max_interval_seconds=max(int(raw.get("max_interval_seconds", 2592000)), 60),
            max_prompt_chars=max(int(raw.get("max_prompt_chars", 20000)), 1000),
            desktop_recovery_enabled=bool(raw.get("desktop_recovery_enabled", True)),
            desktop_exe_path=(
                resolve_relative(base, desktop_raw, WORKSPACE_ROOT)
                if desktop_raw
                else None
            ),
            desktop_app_user_model_id=(
                str(raw.get("desktop_app_user_model_id") or "").strip() or None
            ),
            desktop_start_timeout_seconds=max(
                int(raw.get("desktop_start_timeout_seconds", 30)), 5
            ),
            turn_completion_timeout_seconds=max(
                int(raw.get("turn_completion_timeout_seconds", 1800)), 30
            ),
            max_concurrent_runs=max(int(raw.get("max_concurrent_runs", 4)), 1),
            max_runs_without_explicit_override=max(
                int(raw.get("max_runs_without_explicit_override", 48)), 1
            ),
            receipt_stale_round_limit=max(int(raw.get("receipt_stale_round_limit", 3)), 1),
            require_prompt_continuation_receipt=bool(raw.get("require_prompt_continuation_receipt", False)),
            prohibited_target_thread_ids=frozenset(
                str(value).strip()
                for value in (raw.get("prohibited_target_thread_ids") or [])
                if str(value).strip()
            ),
            quota_probe_double_read_delay_seconds=max(
                float(raw.get("quota_probe_double_read_delay_seconds", 1.0)), 0.0
            ),
            wake_payload_mode=str(raw.get("wake_payload_mode") or "inline").strip(),
            contracts_dir=resolve_relative(
                base,
                raw.get("heartbeat_contracts_dir"),
                WORKSPACE_ROOT / "coo_state" / "dispatcher" / "heartbeat_contracts",
            ),
            max_wake_envelope_chars=max(
                int(raw.get("max_wake_envelope_chars", 512)), 256
            ),
            standard_bridge_package_root=(
                Path(str(raw["standard_bridge_package_root"])).expanduser().resolve()
                if str(raw.get("standard_bridge_package_root") or "").strip()
                else None
            ),
        )
        if config.max_interval_seconds < config.min_interval_seconds:
            raise HeartbeatError("max_interval_seconds is below min_interval_seconds")
        if not config.launcher_config_path.is_file():
            raise HeartbeatError(
                f"native task launcher config not found: {config.launcher_config_path}"
            )
        if config.wake_payload_mode not in {"inline", "contract_ref"}:
            raise HeartbeatError(
                "wake_payload_mode must be inline or contract_ref"
            )
        if config.standard_bridge_package_root and not (
            config.standard_bridge_package_root / "jarvis_codex_bridge"
        ).is_dir():
            raise HeartbeatError("standard_bridge_package_root has no jarvis_codex_bridge package")
        return config


class HeartbeatStore:
    def __init__(self, config: HeartbeatConfig):
        self.config = config
        self.config.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.config.db_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @contextmanager
    def session(self):
        connection = self.connect()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.session() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS heartbeats (
                    heartbeat_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    target_thread_id TEXT NOT NULL,
                    parent_thread_id TEXT,
                    prompt TEXT NOT NULL,
                    interval_seconds INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    next_run_epoch REAL,
                    run_count INTEGER NOT NULL DEFAULT 0,
                    max_runs INTEGER,
                    expires_at TEXT,
                    model TEXT,
                    reasoning_effort TEXT,
                    ensure_desktop INTEGER NOT NULL DEFAULT 0,
                    source_event_key TEXT NOT NULL,
                    confirmation_evidence TEXT NOT NULL,
                    request_hash TEXT NOT NULL,
                    execution_mode TEXT NOT NULL DEFAULT 'prompt',
                    probe_type TEXT,
                    probe_threshold_percent REAL,
                    probe_config_json TEXT,
                    trigger_state TEXT NOT NULL DEFAULT 'not_applicable',
                    trigger_run_id TEXT,
                    trigger_remaining_percent REAL,
                    triggered_at TEXT,
                    delivery_message_id TEXT,
                    terminal_receipt_fingerprint TEXT,
                    terminal_receipt_status TEXT NOT NULL DEFAULT 'not_applicable',
                    terminal_receipt_turn_id TEXT,
                    last_run_at TEXT,
                    last_turn_id TEXT,
                    last_thread_status TEXT,
                    failure_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    last_progress_fingerprint TEXT,
                    unchanged_receipt_count INTEGER NOT NULL DEFAULT 0,
                    busy_since_epoch REAL,
                    busy_notice_sent INTEGER NOT NULL DEFAULT 0,
                    queue_quiet_fingerprint TEXT,
                    queue_abnormal_alert_fingerprint TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS heartbeat_runs (
                    run_id TEXT PRIMARY KEY,
                    heartbeat_id TEXT,
                    source_event_key TEXT,
                    client_user_message_id TEXT,
                    target_thread_id TEXT NOT NULL,
                    scheduled_at TEXT,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    outcome TEXT NOT NULL,
                    desktop_status TEXT,
                    thread_status TEXT,
                    turn_id TEXT,
                    error TEXT,
                    probe_result_json TEXT,
                    FOREIGN KEY (heartbeat_id) REFERENCES heartbeats(heartbeat_id)
                );

                CREATE TABLE IF NOT EXISTS heartbeat_audit (
                    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    recorded_at TEXT NOT NULL,
                    action TEXT NOT NULL,
                    heartbeat_id TEXT,
                    source_event_key TEXT,
                    detail_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_heartbeats_due
                    ON heartbeats(status, next_run_epoch);
                CREATE INDEX IF NOT EXISTS idx_heartbeat_runs_heartbeat
                    ON heartbeat_runs(heartbeat_id, started_at);
                """
            )
            existing_columns = {
                row["name"] for row in connection.execute("PRAGMA table_info(heartbeats)")
            }
            migrations = {
                "execution_mode": "TEXT NOT NULL DEFAULT 'prompt'",
                "probe_type": "TEXT",
                "probe_threshold_percent": "REAL",
                "probe_config_json": "TEXT",
                "trigger_state": "TEXT NOT NULL DEFAULT 'not_applicable'",
                "trigger_run_id": "TEXT",
                "trigger_remaining_percent": "REAL",
                "triggered_at": "TEXT",
                "delivery_message_id": "TEXT",
                "parent_thread_id": "TEXT",
                "terminal_receipt_fingerprint": "TEXT",
                "terminal_receipt_status": "TEXT NOT NULL DEFAULT 'not_applicable'",
                "terminal_receipt_turn_id": "TEXT",
                "last_progress_fingerprint": "TEXT",
                "unchanged_receipt_count": "INTEGER NOT NULL DEFAULT 0",
                "busy_since_epoch": "REAL",
                "busy_notice_sent": "INTEGER NOT NULL DEFAULT 0",
                "queue_quiet_fingerprint": "TEXT",
                "queue_abnormal_alert_fingerprint": "TEXT",
            }
            for column, declaration in migrations.items():
                if column not in existing_columns:
                    connection.execute(
                        f"ALTER TABLE heartbeats ADD COLUMN {column} {declaration}"
                    )
            run_columns = {
                row["name"] for row in connection.execute("PRAGMA table_info(heartbeat_runs)")
            }
            if "probe_result_json" not in run_columns:
                connection.execute(
                    "ALTER TABLE heartbeat_runs ADD COLUMN probe_result_json TEXT"
                )
            if "client_user_message_id" not in run_columns:
                connection.execute(
                    "ALTER TABLE heartbeat_runs ADD COLUMN client_user_message_id TEXT"
                )

    def audit(
        self,
        connection: sqlite3.Connection,
        action: str,
        heartbeat_id: str | None,
        source_event_key: str | None,
        detail: dict[str, Any],
    ) -> None:
        connection.execute(
            """
            INSERT INTO heartbeat_audit(
                recorded_at, action, heartbeat_id, source_event_key, detail_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                utc_now(),
                action,
                heartbeat_id,
                source_event_key,
                json.dumps(detail, ensure_ascii=False, sort_keys=True),
            ),
        )

    @staticmethod
    def row_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        return dict(row) if row is not None else None

    def validate_thread_id(self, value: Any) -> str:
        text = str(value or "").strip()
        try:
            parsed = uuid.UUID(text)
        except ValueError as exc:
            raise HeartbeatError("target_thread_id must be a UUID") from exc
        return str(parsed)

    def validate_request(self, request: dict[str, Any]) -> dict[str, Any]:
        heartbeat_id = str(request.get("heartbeat_id") or "").strip()
        if not SAFE_ID.fullmatch(heartbeat_id):
            raise HeartbeatError("heartbeat_id is missing or unsafe")
        name = str(request.get("name") or "").strip()
        if not name:
            raise HeartbeatError("name is required")
        thread_id = self.validate_thread_id(request.get("target_thread_id"))
        if thread_id in self.config.prohibited_target_thread_ids:
            raise HeartbeatError("target_thread_id is retired from Heartbeat routing")
        parent_thread_id: str | None = None
        prompt = str(request.get("prompt") or "").strip()
        if not prompt:
            raise HeartbeatError("prompt is required")
        if len(prompt) > self.config.max_prompt_chars:
            raise HeartbeatError("prompt exceeds max_prompt_chars")
        execution_mode = str(request.get("execution_mode") or "prompt").strip()
        if execution_mode not in {"prompt", "native_probe"}:
            raise HeartbeatError("execution_mode must be prompt or native_probe")
        probe_raw = request.get("probe")
        probe: dict[str, Any] | None = None
        if execution_mode == "native_probe":
            if not isinstance(probe_raw, dict):
                raise HeartbeatError("native_probe requires a probe object")
            probe_type = str(probe_raw.get("type") or "").strip()
            if probe_type == "codex_weekly_remaining_gte":
                threshold = float(probe_raw.get("threshold_percent"))
                if not 0.0 <= threshold <= 100.0:
                    raise HeartbeatError("probe threshold_percent must be between 0 and 100")
                if probe_raw.get("double_read") is not True:
                    raise HeartbeatError("quota threshold probe requires double_read=true")
                probe = {
                    "type": probe_type,
                    "threshold_percent": threshold,
                    "double_read": True,
                }
            elif probe_type == "attendance_end_of_day_memo":
                lead_minutes = int(probe_raw.get("lead_minutes", 10))
                grace_minutes = int(probe_raw.get("grace_minutes", 5))
                if not 1 <= lead_minutes <= 120:
                    raise HeartbeatError("probe lead_minutes must be between 1 and 120")
                if not 0 <= grace_minutes <= 120:
                    raise HeartbeatError("probe grace_minutes must be between 0 and 120")
                probe = {
                    "type": probe_type,
                    "lead_minutes": lead_minutes,
                    "grace_minutes": grace_minutes,
                }
            elif probe_type == "workbench_queue_progress":
                database = Path(str(probe_raw.get("workbench_db") or "")).expanduser()
                state_path = Path(str(probe_raw.get("state_path") or "")).expanduser()
                health_url = str(probe_raw.get("health_url") or "").strip()
                batch_label = str(probe_raw.get("batch_label") or "").strip()
                expected_raw = probe_raw.get("expected_draft_ids")
                if not database.is_absolute() or not database.is_file():
                    raise HeartbeatError("workbench_db must be an existing absolute file")
                if not state_path.is_absolute() or not state_path.resolve().is_relative_to(WORKSPACE_ROOT):
                    raise HeartbeatError("state_path must be an absolute path under the Jarvis workspace")
                parsed_url = urlparse(health_url)
                if (
                    parsed_url.scheme != "http"
                    or parsed_url.hostname not in {"127.0.0.1", "localhost", "::1"}
                ):
                    raise HeartbeatError("health_url must be a loopback http URL")
                if not batch_label or len(batch_label) > 120:
                    raise HeartbeatError("batch_label is required and must be at most 120 characters")
                if not isinstance(expected_raw, list) or not expected_raw:
                    raise HeartbeatError("expected_draft_ids must be a nonempty list")
                try:
                    expected_ids = [int(value) for value in expected_raw]
                except (TypeError, ValueError) as exc:
                    raise HeartbeatError("expected_draft_ids must contain integers") from exc
                if any(value <= 0 for value in expected_ids) or len(set(expected_ids)) != len(expected_ids):
                    raise HeartbeatError("expected_draft_ids must be positive and unique")
                if len(expected_ids) > 2000:
                    raise HeartbeatError("expected_draft_ids exceeds 2000")
                grace = int(probe_raw.get("overdue_grace_seconds", 300))
                if not 60 <= grace <= 3600:
                    raise HeartbeatError("overdue_grace_seconds must be between 60 and 3600")
                probe = {
                    "type": probe_type,
                    "workbench_db": str(database.resolve()),
                    "state_path": str(state_path.resolve()),
                    "health_url": health_url,
                    "batch_label": batch_label,
                    "expected_draft_ids": expected_ids,
                    "overdue_grace_seconds": grace,
                }
            elif probe_type in {"codex_thread_terminal", "codex_thread_terminal_continue"}:
                parent_candidate = (
                    request.get("parent_thread_id")
                    or request.get("source_thread_id")
                )
                if not parent_candidate:
                    raise HeartbeatError(
                        "terminal probe requires parent_thread_id or source_thread_id"
                    )
                parent_thread_id = self.validate_thread_id(parent_candidate)
                if parent_thread_id == thread_id:
                    raise HeartbeatError("parent_thread_id must differ from target_thread_id")
                receipt_target = str(probe_raw.get("receipt_target_thread_id") or "").strip()
                if receipt_target:
                    receipt_target = self.validate_thread_id(receipt_target)
                probe = {"type": probe_type}
                if receipt_target:
                    probe["receipt_target_thread_id"] = receipt_target
                continuation_target = str(
                    probe_raw.get("continuation_target_thread_id") or ""
                ).strip()
                if continuation_target:
                    probe["continuation_target_thread_id"] = self.validate_thread_id(
                        continuation_target
                    )
                continuation_prompt = str(
                    probe_raw.get("continuation_prompt") or ""
                ).strip()
                if continuation_prompt:
                    probe["continuation_prompt"] = continuation_prompt
            elif probe_type == "master_queue_continuation":
                queue_path = Path(str(probe_raw.get("master_queue_path") or "")).expanduser()
                if not queue_path.is_absolute():
                    raise HeartbeatError("master_queue_path must be absolute")
                receipt = probe_raw.get("terminal_receipt")
                if not isinstance(receipt, dict):
                    raise HeartbeatError("master_queue_continuation requires terminal_receipt")
                statuses = receipt.get("terminal_statuses")
                item_fields = receipt.get("required_item_fields")
                artifact_fields = receipt.get("required_artifact_fields")
                if (not isinstance(statuses, list) or not statuses or
                    not isinstance(item_fields, list) or not item_fields or
                    not isinstance(artifact_fields, list)):
                    raise HeartbeatError("terminal_receipt requires terminal_statuses, required_item_fields, and required_artifact_fields")
                for values in (statuses, item_fields, artifact_fields):
                    if any(not isinstance(value, str) or not value.strip() for value in values):
                        raise HeartbeatError("terminal_receipt fields must be nonempty strings")
                probe = {
                    "type": probe_type,
                    "master_queue_path": str(queue_path.resolve()),
                    "terminal_receipt": {
                        "terminal_statuses": sorted(set(statuses)),
                        "required_item_fields": sorted(set(item_fields)),
                        "required_artifact_fields": sorted(set(artifact_fields)),
                    },
                }
            else:
                raise HeartbeatError("unsupported native probe type")
        elif probe_raw not in (None, {}):
            raise HeartbeatError("prompt heartbeat cannot include probe configuration")
        interval = int(request.get("interval_seconds") or 0)
        if not self.config.min_interval_seconds <= interval <= self.config.max_interval_seconds:
            raise HeartbeatError(
                "interval_seconds must be between "
                f"{self.config.min_interval_seconds} and {self.config.max_interval_seconds}"
            )
        source_event_key = str(request.get("source_event_key") or "").strip()
        confirmation = str(request.get("confirmation_evidence") or "").strip()
        if not source_event_key:
            raise HeartbeatError("source_event_key is required")
        if not confirmation:
            raise HeartbeatError("confirmation_evidence is required for schedule changes")
        max_runs_raw = request.get("max_runs")
        if max_runs_raw in (None, ""):
            raise HeartbeatError("max_runs is required")
        max_runs = int(max_runs_raw)
        if max_runs < 1:
            raise HeartbeatError("max_runs must be positive")
        override_evidence = str(request.get("max_runs_override_evidence") or "").strip()
        if max_runs > self.config.max_runs_without_explicit_override and not override_evidence:
            raise HeartbeatError(
                "max_runs exceeds the default limit; explicit max_runs_override_evidence is required"
            )
        expires = parse_time(request.get("expires_at"))
        start_at = parse_time(request.get("start_at"))
        now = datetime.now(timezone.utc)
        if expires is None:
            raise HeartbeatError("expires_at is required")
        if expires <= now:
            raise HeartbeatError("expires_at must be in the future")
        next_run = start_at or (
            now if bool(request.get("start_immediately")) else datetime.fromtimestamp(
                now.timestamp() + interval, timezone.utc
            )
        )
        normalized = {
            "heartbeat_id": heartbeat_id,
            "name": name[:160],
            "target_thread_id": thread_id,
            "parent_thread_id": parent_thread_id,
            "prompt": prompt,
            "execution_mode": execution_mode,
            "probe_type": probe["type"] if probe else None,
            "probe_threshold_percent": probe.get("threshold_percent") if probe else None,
            "probe_config_json": (
                json.dumps(probe, ensure_ascii=False, sort_keys=True)
                if probe else None
            ),
            "trigger_state": "monitoring" if probe else "not_applicable",
            "interval_seconds": interval,
            "max_runs": max_runs,
            "expires_at": expires.isoformat() if expires else None,
            "max_runs_override_evidence": override_evidence[:500] or None,
            "model": str(request.get("model") or "").strip() or None,
            "reasoning_effort": str(request.get("reasoning_effort") or "").strip() or None,
            "ensure_desktop": bool(request.get("ensure_desktop", False)),
            "source_event_key": source_event_key,
            "confirmation_evidence": confirmation[:500],
            "next_run_epoch": next_run.timestamp(),
        }
        normalized["request_hash"] = canonical_hash(
            {key: value for key, value in normalized.items() if key != "next_run_epoch"}
        )
        return normalized

    def create(self, request: dict[str, Any]) -> dict[str, Any]:
        value = self.validate_request(request)
        now = utc_now()
        with self.session() as connection:
            existing = connection.execute(
                "SELECT * FROM heartbeats WHERE heartbeat_id=?",
                (value["heartbeat_id"],),
            ).fetchone()
            if existing is not None:
                if existing["request_hash"] == value["request_hash"]:
                    return {"created": False, "idempotent": True, "heartbeat": dict(existing)}
                raise HeartbeatError("heartbeat_id already exists with different content")
            connection.execute(
                """
                INSERT INTO heartbeats(
                    heartbeat_id, name, target_thread_id, parent_thread_id, prompt, interval_seconds,
                    status, next_run_epoch, max_runs, expires_at, model,
                    reasoning_effort, ensure_desktop, source_event_key,
                    confirmation_evidence, request_hash, execution_mode, probe_type,
                    probe_threshold_percent, probe_config_json, trigger_state,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    value["heartbeat_id"], value["name"], value["target_thread_id"], value["parent_thread_id"],
                    value["prompt"], value["interval_seconds"], ACTIVE,
                    value["next_run_epoch"], value["max_runs"], value["expires_at"],
                    value["model"], value["reasoning_effort"],
                    int(value["ensure_desktop"]), value["source_event_key"],
                    value["confirmation_evidence"], value["request_hash"],
                    value["execution_mode"], value["probe_type"],
                    value["probe_threshold_percent"], value["probe_config_json"],
                    value["trigger_state"], now, now,
                ),
            )
            self.audit(
                connection,
                "heartbeat_created",
                value["heartbeat_id"],
                value["source_event_key"],
                {"request_hash": value["request_hash"]},
            )
            row = connection.execute(
                "SELECT * FROM heartbeats WHERE heartbeat_id=?",
                (value["heartbeat_id"],),
            ).fetchone()
            return {"created": True, "idempotent": False, "heartbeat": dict(row)}

    def update(self, request: dict[str, Any]) -> dict[str, Any]:
        value = self.validate_request(request)
        reactivate = request.get("reactivate") is True
        heartbeat_id = value["heartbeat_id"]
        with self.session() as connection:
            current = connection.execute(
                "SELECT * FROM heartbeats WHERE heartbeat_id=?",
                (heartbeat_id,),
            ).fetchone()
            if current is None:
                raise HeartbeatError(f"heartbeat not found: {heartbeat_id}")
            inflight = connection.execute(
                """
                SELECT run_id FROM heartbeat_runs
                WHERE heartbeat_id=? AND completed_at IS NULL AND outcome='inflight'
                LIMIT 1
                """,
                (heartbeat_id,),
            ).fetchone()
            if inflight is not None:
                raise HeartbeatError("heartbeat cannot be updated while a run is inflight")
            if current["status"] in TERMINAL and not reactivate:
                raise HeartbeatError(
                    "terminal heartbeat update requires reactivate=true"
                )
            desired_status = ACTIVE if reactivate else str(current["status"])
            next_run_epoch = (
                value["next_run_epoch"]
                if desired_status == ACTIVE
                else current["next_run_epoch"]
            )
            connection.execute(
                """
                UPDATE heartbeats
                SET name=?,target_thread_id=?,parent_thread_id=?,prompt=?,interval_seconds=?,status=?,
                    next_run_epoch=?,max_runs=?,expires_at=?,model=?,reasoning_effort=?,
                    ensure_desktop=?,source_event_key=?,confirmation_evidence=?,
                    request_hash=?,execution_mode=?,probe_type=?,
                    probe_threshold_percent=?,probe_config_json=?,trigger_state=?,
                    trigger_run_id=NULL,trigger_remaining_percent=NULL,triggered_at=NULL,
                    delivery_message_id=NULL,failure_count=0,last_error=NULL,updated_at=?
                WHERE heartbeat_id=?
                """,
                (
                    value["name"], value["target_thread_id"], value["parent_thread_id"], value["prompt"],
                    value["interval_seconds"], desired_status, next_run_epoch,
                    value["max_runs"], value["expires_at"], value["model"],
                    value["reasoning_effort"], int(value["ensure_desktop"]),
                    value["source_event_key"], value["confirmation_evidence"],
                    value["request_hash"], value["execution_mode"],
                    value["probe_type"], value["probe_threshold_percent"],
                    value["probe_config_json"], value["trigger_state"], utc_now(),
                    heartbeat_id,
                ),
            )
            self.audit(
                connection,
                "heartbeat_updated",
                heartbeat_id,
                value["source_event_key"],
                {
                    "previous_request_hash": current["request_hash"],
                    "request_hash": value["request_hash"],
                    "previous_status": current["status"],
                    "status": desired_status,
                    "reactivated": reactivate,
                },
            )
        return self.get(heartbeat_id)

    def list(self, status: str | None = None) -> list[dict[str, Any]]:
        with self.session() as connection:
            if status:
                rows = connection.execute(
                    "SELECT * FROM heartbeats WHERE status=? ORDER BY created_at",
                    (status.upper(),),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM heartbeats ORDER BY created_at"
                ).fetchall()
        return [dict(row) for row in rows]

    def get(self, heartbeat_id: str) -> dict[str, Any]:
        with self.session() as connection:
            row = connection.execute(
                "SELECT * FROM heartbeats WHERE heartbeat_id=?", (heartbeat_id,)
            ).fetchone()
            inflight = connection.execute(
                """
                SELECT * FROM heartbeat_runs
                WHERE heartbeat_id=? AND completed_at IS NULL AND outcome='inflight'
                ORDER BY started_at DESC,run_id DESC LIMIT 1
                """,
                (heartbeat_id,),
            ).fetchone()
            latest = connection.execute(
                """
                SELECT * FROM heartbeat_runs
                WHERE heartbeat_id=?
                ORDER BY started_at DESC,run_id DESC LIMIT 1
                """,
                (heartbeat_id,),
            ).fetchone()
        if row is None:
            raise HeartbeatError(f"heartbeat not found: {heartbeat_id}")
        result = dict(row)
        result["inflight_run"] = dict(inflight) if inflight is not None else None
        result["latest_run"] = dict(latest) if latest is not None else None
        return result

    def set_status(
        self,
        heartbeat_id: str,
        status: str,
        source_event_key: str,
        confirmation_evidence: str,
    ) -> dict[str, Any]:
        desired = status.upper()
        if desired not in {ACTIVE, PAUSED, "CANCELLED"}:
            raise HeartbeatError("unsupported heartbeat status transition")
        if not source_event_key or not confirmation_evidence:
            raise HeartbeatError("source event and confirmation evidence are required")
        now_epoch = time.time()
        with self.session() as connection:
            current = connection.execute(
                "SELECT * FROM heartbeats WHERE heartbeat_id=?", (heartbeat_id,)
            ).fetchone()
            if current is None:
                raise HeartbeatError(f"heartbeat not found: {heartbeat_id}")
            if current["status"] in TERMINAL and current["status"] != desired:
                raise HeartbeatError("terminal heartbeat cannot be resumed")
            next_epoch = (
                now_epoch + int(current["interval_seconds"])
                if desired == ACTIVE
                else current["next_run_epoch"]
            )
            connection.execute(
                """
                UPDATE heartbeats
                SET status=?, next_run_epoch=?, source_event_key=?,
                    confirmation_evidence=?, updated_at=?
                WHERE heartbeat_id=?
                """,
                (
                    desired, next_epoch, source_event_key,
                    confirmation_evidence[:500], utc_now(), heartbeat_id,
                ),
            )
            self.audit(
                connection,
                f"heartbeat_{desired.lower()}",
                heartbeat_id,
                source_event_key,
                {"previous_status": current["status"]},
            )
        return self.get(heartbeat_id)

    def arm_trigger_handoff(
        self,
        *,
        heartbeat_id: str,
        run_id: str,
        remaining_percent: float,
        source_event_key: str,
    ) -> bool:
        """Freeze a native probe schedule before its one allowed AI handoff."""
        now = utc_now()
        with self.session() as connection:
            changed = connection.execute(
                """
                UPDATE heartbeats
                SET trigger_state='handoff_inflight',trigger_run_id=?,
                    trigger_remaining_percent=?,triggered_at=?,next_run_epoch=NULL,
                    updated_at=?
                WHERE heartbeat_id=? AND status=? AND execution_mode='native_probe'
                  AND trigger_state='monitoring' AND trigger_run_id IS NULL
                """,
                (run_id, remaining_percent, now, now, heartbeat_id, ACTIVE),
            ).rowcount
            if changed == 1:
                self.audit(
                    connection,
                    "heartbeat_probe_trigger_armed",
                    heartbeat_id,
                    source_event_key,
                    {"run_id": run_id, "remaining_percent": remaining_percent},
                )
            return changed == 1

    def finish_trigger_handoff(
        self,
        *,
        heartbeat_id: str,
        run_id: str,
        delivered_handoff_started: bool,
        source_event_key: str,
    ) -> None:
        state = "handoff_pending_delivery" if delivered_handoff_started else "monitoring"
        with self.session() as connection:
            changed = connection.execute(
                """
                UPDATE heartbeats
                SET trigger_state=?,trigger_run_id=CASE WHEN ? THEN trigger_run_id ELSE NULL END,
                    trigger_remaining_percent=CASE WHEN ? THEN trigger_remaining_percent ELSE NULL END,
                    triggered_at=CASE WHEN ? THEN triggered_at ELSE NULL END,
                    updated_at=?
                WHERE heartbeat_id=? AND trigger_run_id=?
                  AND trigger_state='handoff_inflight'
                """,
                (
                    state,
                    int(delivered_handoff_started),
                    int(delivered_handoff_started),
                    int(delivered_handoff_started),
                    utc_now(),
                    heartbeat_id,
                    run_id,
                ),
            ).rowcount
            if changed != 1:
                raise HeartbeatError("native probe trigger handoff state conflict")
            self.audit(
                connection,
                "heartbeat_probe_handoff_started" if delivered_handoff_started
                else "heartbeat_probe_handoff_released",
                heartbeat_id,
                source_event_key,
                {"run_id": run_id, "trigger_state": state},
            )

    def complete_delivery(
        self,
        heartbeat_id: str,
        message_id: str,
        source_event_key: str,
    ) -> dict[str, Any]:
        delivered_id = str(message_id or "").strip()
        if not delivered_id:
            raise HeartbeatError("delivered message_id is required")
        if len(delivered_id) > 256:
            raise HeartbeatError("delivered message_id is too long")
        if not source_event_key.strip():
            raise HeartbeatError("source_event_key is required")
        now = utc_now()
        with self.session() as connection:
            changed = connection.execute(
                """
                UPDATE heartbeats
                SET status='CANCELLED',trigger_state='delivered_cancelled',
                    delivery_message_id=?,next_run_epoch=NULL,updated_at=?
                WHERE heartbeat_id=? AND status=?
                  AND trigger_state='handoff_pending_delivery'
                """,
                (delivered_id, now, heartbeat_id, ACTIVE),
            ).rowcount
            if changed != 1:
                raise HeartbeatError(
                    "heartbeat is not awaiting a delivered trigger notification"
                )
            self.audit(
                connection,
                "heartbeat_probe_delivery_completed",
                heartbeat_id,
                source_event_key,
                {"message_id": delivered_id},
            )
        return self.get(heartbeat_id)

    def claim_terminal_receipt(
        self, heartbeat_id: str, fingerprint: str, source_event_key: str,
    ) -> bool:
        """Claim one parent receipt for a newly observed child terminal state."""
        with self.session() as connection:
            changed = connection.execute(
                """
                UPDATE heartbeats
                SET terminal_receipt_fingerprint=?, terminal_receipt_status='inflight',
                    terminal_receipt_turn_id=NULL, updated_at=?
                WHERE heartbeat_id=? AND status=?
                  AND (terminal_receipt_fingerprint IS NULL
                       OR terminal_receipt_fingerprint<>?)
                """,
                (fingerprint, utc_now(), heartbeat_id, ACTIVE, fingerprint),
            ).rowcount
            if changed:
                self.audit(connection, "heartbeat_terminal_receipt_claimed", heartbeat_id,
                           source_event_key, {"terminal_fingerprint": fingerprint})
            return changed == 1

    def finish_terminal_receipt(
        self, heartbeat_id: str, *, delivered: bool, turn_id: str | None,
        source_event_key: str, error: str | None = None,
    ) -> None:
        with self.session() as connection:
            if delivered:
                connection.execute(
                    """UPDATE heartbeats SET terminal_receipt_status='delivered',
                       terminal_receipt_turn_id=?, next_run_epoch=NULL, updated_at=?
                       WHERE heartbeat_id=? AND terminal_receipt_status='inflight'""",
                    (turn_id, utc_now(), heartbeat_id),
                )
            else:
                connection.execute(
                    """UPDATE heartbeats SET terminal_receipt_fingerprint=NULL,
                       terminal_receipt_status='retry', last_error=?, updated_at=?
                       WHERE heartbeat_id=? AND terminal_receipt_status='inflight'""",
                    (error, utc_now(), heartbeat_id),
                )
            self.audit(connection, "heartbeat_terminal_receipt_delivered" if delivered
                       else "heartbeat_terminal_receipt_retry", heartbeat_id,
                       source_event_key, {"turn_id": turn_id, "error": error})

    def recover_native_probe_handoffs(self) -> list[dict[str, Any]]:
        """Recover trigger handoffs without creating a duplicate AI turn."""
        recovered: list[dict[str, Any]] = []
        now = utc_now()
        with self.session() as connection:
            rows = connection.execute(
                """
                SELECT h.*,r.turn_id,r.completed_at,r.outcome AS run_outcome
                FROM heartbeats h
                LEFT JOIN heartbeat_runs r ON r.run_id=h.trigger_run_id
                WHERE h.status=? AND h.execution_mode='native_probe'
                  AND h.trigger_state='handoff_inflight'
                """,
                (ACTIVE,),
            ).fetchall()
            for row in rows:
                heartbeat_id = str(row["heartbeat_id"])
                run_id = str(row["trigger_run_id"] or "")
                turn_id = str(row["turn_id"] or "").strip()
                if turn_id:
                    connection.execute(
                        """
                        UPDATE heartbeats
                        SET trigger_state='handoff_pending_delivery',next_run_epoch=NULL,
                            updated_at=?
                        WHERE heartbeat_id=?
                        """,
                        (now, heartbeat_id),
                    )
                    if row["completed_at"] is None:
                        connection.execute(
                            """
                            UPDATE heartbeat_runs
                            SET completed_at=?,outcome='probe_trigger_handoff_recovered',
                                error='service restarted after trigger turn was persisted'
                            WHERE run_id=? AND completed_at IS NULL
                            """,
                            (now, run_id),
                        )
                    action = "heartbeat_probe_handoff_recovered_pending_delivery"
                    state = "handoff_pending_delivery"
                else:
                    error = "service restarted before trigger turn was persisted"
                    if run_id:
                        connection.execute(
                            """
                            UPDATE heartbeat_runs
                            SET completed_at=?,outcome='failed',error=?
                            WHERE run_id=? AND completed_at IS NULL
                            """,
                            (now, error, run_id),
                        )
                    connection.execute(
                        """
                        UPDATE heartbeats
                        SET trigger_state='monitoring',trigger_run_id=NULL,
                            trigger_remaining_percent=NULL,triggered_at=NULL,
                            next_run_epoch=?,failure_count=failure_count+1,last_error=?,
                            updated_at=?
                        WHERE heartbeat_id=?
                        """,
                        (time.time() + self.config.retry_seconds, error, now, heartbeat_id),
                    )
                    action = "heartbeat_probe_handoff_recovered_for_retry"
                    state = "monitoring"
                self.audit(
                    connection,
                    action,
                    heartbeat_id,
                    row["source_event_key"],
                    {"run_id": run_id, "turn_id": turn_id or None},
                )
                recovered.append({
                    "heartbeat_id": heartbeat_id,
                    "run_id": run_id,
                    "trigger_state": state,
                    "turn_id": turn_id or None,
                })
        return recovered

    def due(self, now_epoch: float | None = None) -> list[dict[str, Any]]:
        current_epoch = now_epoch if now_epoch is not None else time.time()
        current_iso = datetime.fromtimestamp(current_epoch, timezone.utc).isoformat()
        with self.session() as connection:
            expired = connection.execute(
                """
                SELECT heartbeat_id FROM heartbeats
                WHERE status=? AND expires_at IS NOT NULL AND expires_at<=?
                """,
                (ACTIVE, current_iso),
            ).fetchall()
            for row in expired:
                connection.execute(
                    "UPDATE heartbeats SET status='COMPLETED', updated_at=? WHERE heartbeat_id=?",
                    (utc_now(), row["heartbeat_id"]),
                )
                self.audit(
                    connection, "heartbeat_expired", row["heartbeat_id"], None, {}
                )
            rows = connection.execute(
                """
                SELECT * FROM heartbeats
                WHERE status=? AND next_run_epoch IS NOT NULL AND next_run_epoch<=?
                  AND (max_runs IS NULL OR run_count<max_runs)
                ORDER BY next_run_epoch, heartbeat_id
                """,
                (ACTIVE, current_epoch),
            ).fetchall()
            claimed: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                item["scheduled_epoch"] = float(row["next_run_epoch"])
                item["next_run_epoch"] = current_epoch + int(row["interval_seconds"])
                connection.execute(
                    """
                    UPDATE heartbeats
                    SET next_run_epoch=?, last_run_at=?, updated_at=?
                    WHERE heartbeat_id=? AND status=?
                    """,
                    (
                        item["next_run_epoch"],
                        current_iso,
                        current_iso,
                        row["heartbeat_id"],
                        ACTIVE,
                    ),
                )
                claimed.append(item)
            return claimed

    def claim_due(self, now_epoch: float | None = None) -> list[dict[str, Any]]:
        """Atomically advance each due schedule and persist its inflight receipt."""
        current_epoch = now_epoch if now_epoch is not None else time.time()
        current_iso = datetime.fromtimestamp(current_epoch, timezone.utc).isoformat()
        with self.session() as connection:
            expired = connection.execute(
                """
                SELECT heartbeat_id FROM heartbeats
                WHERE status=? AND expires_at IS NOT NULL AND expires_at<=?
                """,
                (ACTIVE, current_iso),
            ).fetchall()
            for row in expired:
                connection.execute(
                    "UPDATE heartbeats SET status='COMPLETED', updated_at=? WHERE heartbeat_id=?",
                    (current_iso, row["heartbeat_id"]),
                )
                self.audit(
                    connection, "heartbeat_expired", row["heartbeat_id"], None, {}
                )
            rows = connection.execute(
                """
                SELECT * FROM heartbeats
                WHERE status=? AND next_run_epoch IS NOT NULL AND next_run_epoch<=?
                  AND (max_runs IS NULL OR run_count<max_runs)
                ORDER BY next_run_epoch,heartbeat_id
                """,
                (ACTIVE, current_epoch),
            ).fetchall()
            claims: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                scheduled_epoch = float(row["next_run_epoch"])
                next_run_epoch = current_epoch + int(row["interval_seconds"])
                scheduled_at = datetime.fromtimestamp(
                    scheduled_epoch, timezone.utc
                ).isoformat()
                run_id = str(uuid.uuid4())
                connection.execute(
                    """
                    UPDATE heartbeats
                    SET next_run_epoch=?,last_run_at=?,updated_at=?
                    WHERE heartbeat_id=? AND status=?
                    """,
                    (
                        next_run_epoch,
                        current_iso,
                        current_iso,
                        row["heartbeat_id"],
                        ACTIVE,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO heartbeat_runs(
                        run_id,heartbeat_id,source_event_key,target_thread_id,scheduled_at,
                        started_at,completed_at,outcome,desktop_status,thread_status,turn_id,error
                    ) VALUES (?,?,?,?,?,?,NULL,'inflight',NULL,'claim_recorded',NULL,NULL)
                    """,
                    (
                        run_id,
                        row["heartbeat_id"],
                        row["source_event_key"],
                        row["target_thread_id"],
                        scheduled_at,
                        current_iso,
                    ),
                )
                self.audit(
                    connection,
                    "heartbeat_run_claimed",
                    row["heartbeat_id"],
                    row["source_event_key"],
                    {"run_id": run_id, "scheduled_at": scheduled_at},
                )
                item.update(
                    {
                        "scheduled_epoch": scheduled_epoch,
                        "next_run_epoch": next_run_epoch,
                        "claimed_run_id": run_id,
                        "claimed_started_at": current_iso,
                    }
                )
                claims.append(item)
            return claims

    def record_run(
        self,
        heartbeat: dict[str, Any] | None,
        source_event_key: str | None,
        target_thread_id: str,
        result: dict[str, Any],
        *,
        scheduled_at: str | None = None,
    ) -> dict[str, Any]:
        run_id = str(result.get("run_id") or uuid.uuid4())
        now = utc_now()
        outcome = str(result.get("outcome") or "failed")
        with self.session() as connection:
            existing_run = connection.execute(
                "SELECT heartbeat_id,target_thread_id FROM heartbeat_runs WHERE run_id=?",
                (run_id,),
            ).fetchone()
            heartbeat_id_value = heartbeat.get("heartbeat_id") if heartbeat else None
            if existing_run is None:
                connection.execute(
                    """
                    INSERT INTO heartbeat_runs(
                        run_id, heartbeat_id, source_event_key, client_user_message_id, target_thread_id,
                        scheduled_at, started_at, completed_at, outcome,
                        desktop_status, thread_status, turn_id, error, probe_result_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id, heartbeat_id_value, source_event_key,
                        result.get("client_user_message_id"), target_thread_id,
                        scheduled_at, str(result.get("started_at") or now), now, outcome,
                        result.get("desktop_status"), result.get("thread_status"),
                        result.get("turn_id"), result.get("error"),
                        json.dumps(result.get("probe_result"), ensure_ascii=False, sort_keys=True)
                        if result.get("probe_result") is not None else None,
                    ),
                )
            else:
                if (
                    existing_run["heartbeat_id"] != heartbeat_id_value
                    or existing_run["target_thread_id"] != target_thread_id
                ):
                    raise HeartbeatError("inflight heartbeat run identity conflict")
                connection.execute(
                    """
                    UPDATE heartbeat_runs
                    SET completed_at=?, outcome=?, desktop_status=?, thread_status=?,
                        turn_id=COALESCE(?,turn_id), error=?, probe_result_json=?,
                        client_user_message_id=COALESCE(?,client_user_message_id)
                    WHERE run_id=? AND completed_at IS NULL
                    """,
                    (
                        now, outcome, result.get("desktop_status"),
                        result.get("thread_status"), result.get("turn_id"),
                        result.get("error"),
                        json.dumps(result.get("probe_result"), ensure_ascii=False, sort_keys=True)
                        if result.get("probe_result") is not None else None,
                        result.get("client_user_message_id"),
                        run_id,
                    ),
                )
            if heartbeat:
                heartbeat_id = heartbeat["heartbeat_id"]
                current = connection.execute(
                    """
                    SELECT status, run_count, failure_count, last_progress_fingerprint,
                           unchanged_receipt_count, busy_since_epoch, busy_notice_sent,
                           queue_quiet_fingerprint, queue_abnormal_alert_fingerprint
                    FROM heartbeats
                    WHERE heartbeat_id=?
                    """,
                    (heartbeat_id,),
                ).fetchone()
                if current is None:
                    raise HeartbeatError(f"unknown heartbeat: {heartbeat_id}")
                # A due row is claimed before its wake completes. If Cooper
                # cancels it in that window, retain the raw run audit but do
                # not let the in-flight completion revive the terminal row.
                if current["status"] != ACTIVE:
                    self.audit(
                        connection,
                        "heartbeat_run_discarded_after_terminal",
                        heartbeat_id,
                        source_event_key,
                        {"run_id": run_id, "outcome": outcome, "status": current["status"]},
                    )
                    return {"run_id": run_id, **result}
                success = outcome in {
                    "turn_completed", "thread_resumed", "probe_below_threshold",
                    "probe_trigger_handoff", "probe_not_due",
                    "probe_notification_queued", "probe_thread_status",
                    "workbench_probe_progress", "workbench_probe_completed",
                    "parent_terminal_receipt_delivered",
                    "terminal_continuation_completed",
                    "queue_continuation_released", "queue_no_change_silent",
                    "queue_exhausted_silent", "queue_terminal_queue_exhausted",
                }
                receipt = result.get("continuation_receipt")
                receipt_status = ""
                progress_fingerprint = ""
                if self.config.require_prompt_continuation_receipt and heartbeat.get("execution_mode") == "prompt":
                    if not isinstance(receipt, dict):
                        outcome = "continuation_receipt_missing"
                        result["outcome"] = outcome
                        result["error"] = "required continuation receipt is missing"
                        success = False
                    else:
                        receipt_status = str(receipt.get("continuation_status") or "").strip()
                        progress_fingerprint = str(receipt.get("progress_fingerprint") or "").strip()
                        if receipt_status == "CONTINUE_SAFE" and progress_fingerprint:
                            success = True
                        elif receipt_status == "TERMINAL":
                            success = False
                        else:
                            success = False
                busy_deferral = outcome in {"already_active", "deferred_busy"}
                busy_since = current["busy_since_epoch"]
                busy_notice_sent = int(current["busy_notice_sent"] or 0)
                if busy_deferral:
                    busy_since = float(busy_since) if busy_since is not None else time.time()
                    busy_elapsed = max(time.time() - busy_since, 0.0)
                    if busy_elapsed >= float(heartbeat["interval_seconds"]) and not busy_notice_sent:
                        busy_notice_sent = 1
                        self.audit(
                            connection,
                            "target_busy_over_interval",
                            heartbeat_id,
                            source_event_key,
                            {"run_id": run_id, "busy_seconds": round(busy_elapsed, 3), "target_thread_id": target_thread_id},
                        )
                else:
                    busy_since = None
                    busy_notice_sent = 0
                failures = (
                    int(current["failure_count"] or 0)
                    if busy_deferral
                    else (0 if success else int(current["failure_count"] or 0) + 1)
                )
                status = ACTIVE
                run_count = int(current["run_count"] or 0) + (1 if success else 0)
                unchanged = 0
                if result.get("complete_heartbeat") is True:
                    status = "COMPLETED"
                elif receipt_status == "TERMINAL":
                    status = "COMPLETED"
                elif self.config.require_prompt_continuation_receipt and heartbeat.get("execution_mode") == "prompt" and not busy_deferral:
                    if receipt_status != "CONTINUE_SAFE":
                        status = PAUSED
                    else:
                        previous = str(current["last_progress_fingerprint"] or "")
                        unchanged = int(current["unchanged_receipt_count"] or 0) + 1 if previous == progress_fingerprint else 1
                        if unchanged >= self.config.receipt_stale_round_limit:
                            status = PAUSED
                            result["outcome"] = "continuation_receipt_stale"
                            result["error"] = "progress fingerprint unchanged for configured receipt rounds"
                max_runs = heartbeat.get("max_runs")
                if status == ACTIVE and max_runs is not None and run_count >= int(max_runs):
                    status = "COMPLETED"
                elif failures >= self.config.max_failures:
                    status = "FAILED"
                next_epoch = heartbeat.get("next_run_epoch")
                if outcome in {"probe_trigger_handoff", "parent_terminal_receipt_delivered"}:
                    next_epoch = None
                queue_fingerprint = str(result.get("queue_fingerprint") or "")
                queue_quiet_fingerprint = current["queue_quiet_fingerprint"]
                queue_alert_fingerprint = current["queue_abnormal_alert_fingerprint"]
                if queue_fingerprint:
                    if result.get("queue_quiet"):
                        if queue_quiet_fingerprint == queue_fingerprint:
                            if queue_alert_fingerprint != queue_fingerprint:
                                result["outcome"] = "HEARTBEAT_ABNORMAL_PERSISTENCE"
                                outcome = result["outcome"]
                                result["alert"] = {
                                    "code": "HEARTBEAT_ABNORMAL_PERSISTENCE",
                                    "queue_fingerprint": queue_fingerprint,
                                    "action": "await_cooper_decision",
                                }
                                queue_alert_fingerprint = queue_fingerprint
                                next_epoch = None
                        else:
                            queue_quiet_fingerprint = queue_fingerprint
                            queue_alert_fingerprint = None
                    else:
                        queue_quiet_fingerprint = None
                        queue_alert_fingerprint = None
                if not success and status == ACTIVE:
                    next_epoch = time.time() + self.config.retry_seconds
                    if busy_deferral and busy_since is not None:
                        busy_elapsed = max(time.time() - busy_since, 0.0)
                        if busy_elapsed >= float(heartbeat["interval_seconds"]) / 2:
                            # The scheduled slot is suppressed; only the lightweight retry remains.
                            next_epoch = time.time() + max(self.config.retry_seconds, int(heartbeat["interval_seconds"]) // 2)
                connection.execute(
                    """
                    UPDATE heartbeats
                    SET status=?, run_count=?, failure_count=?, last_error=?,
                        last_turn_id=?, last_thread_status=?, next_run_epoch=?,
                        last_progress_fingerprint=?, unchanged_receipt_count=?,
                        busy_since_epoch=?, busy_notice_sent=?,
                        queue_quiet_fingerprint=?, queue_abnormal_alert_fingerprint=?,
                        updated_at=?
                    WHERE heartbeat_id=?
                    """,
                    (
                        status,
                        run_count,
                        failures,
                        result.get("error"),
                        result.get("turn_id"),
                        result.get("thread_status"),
                        next_epoch,
                        progress_fingerprint or current["last_progress_fingerprint"],
                        unchanged,
                        busy_since,
                        busy_notice_sent,
                        queue_quiet_fingerprint,
                        queue_alert_fingerprint,
                        now,
                        heartbeat_id,
                    ),
                )
                self.audit(
                    connection,
                    "heartbeat_run_recorded",
                    heartbeat_id,
                    source_event_key,
                    {"run_id": run_id, "outcome": outcome, "status": status},
                )
        return {"run_id": run_id, **result}

    def begin_run(
        self,
        heartbeat: dict[str, Any],
        source_event_key: str,
        target_thread_id: str,
        *,
        scheduled_at: str | None = None,
    ) -> dict[str, Any]:
        """Persist a due-claim receipt before any potentially long Codex wait."""
        run_id = str(uuid.uuid4())
        started_at = utc_now()
        with self.session() as connection:
            connection.execute(
                """
                INSERT INTO heartbeat_runs(
                    run_id,heartbeat_id,source_event_key,target_thread_id,scheduled_at,
                    started_at,completed_at,outcome,desktop_status,thread_status,turn_id,error
                ) VALUES (?,?,?,?,?,?,NULL,'inflight',NULL,'claim_recorded',NULL,NULL)
                """,
                (
                    run_id, heartbeat["heartbeat_id"], source_event_key,
                    target_thread_id, scheduled_at, started_at,
                ),
            )
            self.audit(
                connection,
                "heartbeat_run_claimed",
                heartbeat["heartbeat_id"],
                source_event_key,
                {"run_id": run_id, "scheduled_at": scheduled_at},
            )
        return {"run_id": run_id, "started_at": started_at, "outcome": "inflight"}

    def record_inflight_turn(
        self,
        *,
        heartbeat_id: str,
        run_id: str,
        turn_id: str,
        thread_status: str,
        desktop_status: str,
        source_event_key: str,
    ) -> None:
        if not turn_id.strip():
            raise HeartbeatError("inflight turn receipt requires turn_id")
        now = utc_now()
        with self.session() as connection:
            changed = connection.execute(
                """
                UPDATE heartbeat_runs
                SET turn_id=?,thread_status=?,desktop_status=?
                WHERE run_id=? AND heartbeat_id=? AND completed_at IS NULL
                """,
                (turn_id, thread_status, desktop_status, run_id, heartbeat_id),
            ).rowcount
            if changed != 1:
                raise HeartbeatError("inflight heartbeat run receipt conflict")
            connection.execute(
                """
                UPDATE heartbeats SET last_turn_id=?,last_thread_status=?,updated_at=?
                WHERE heartbeat_id=?
                """,
                (turn_id, thread_status, now, heartbeat_id),
            )
            self.audit(
                connection,
                "heartbeat_turn_started",
                heartbeat_id,
                source_event_key,
                {"run_id": run_id, "turn_id": turn_id, "thread_status": thread_status},
            )

    def integrity(self) -> dict[str, Any]:
        with self.session() as connection:
            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
            foreign_keys = connection.execute("PRAGMA foreign_key_check").fetchall()
            counts = {
                status: connection.execute(
                    "SELECT COUNT(*) FROM heartbeats WHERE status=?", (status,)
                ).fetchone()[0]
                for status in (ACTIVE, PAUSED, "COMPLETED", "CANCELLED", "FAILED")
            }
            runs = connection.execute("SELECT COUNT(*) FROM heartbeat_runs").fetchone()[0]
        return {
            "integrity_check": integrity,
            "foreign_key_violations": len(foreign_keys),
            "heartbeat_counts": counts,
            "run_count": runs,
        }

    def inflight_runs(self) -> list[dict[str, Any]]:
        with self.session() as connection:
            rows = connection.execute(
                """
                SELECT run_id,heartbeat_id,target_thread_id,scheduled_at,started_at,
                       outcome,desktop_status,thread_status,turn_id,error
                FROM heartbeat_runs
                WHERE completed_at IS NULL AND outcome='inflight'
                ORDER BY started_at,run_id
                """
            ).fetchall()
        return [dict(row) for row in rows]


def windows_process_paths() -> list[str]:
    if os.name != "nt":
        return []
    command = (
        "Get-Process -ErrorAction SilentlyContinue | "
        "Where-Object { $_.ProcessName -ieq 'Codex' -or "
        "($_.ProcessName -ieq 'ChatGPT' -and $_.Path -like '*OpenAI.Codex_*') } | "
        "Select-Object -ExpandProperty Path | ConvertTo-Json -Compress"
    )
    completed = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", command],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=15,
        check=False,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    if completed.returncode != 0 or not completed.stdout.strip():
        return []
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    return []


class DesktopRecovery:
    def __init__(
        self,
        config: HeartbeatConfig,
        *,
        process_paths: Callable[[], list[str]] = windows_process_paths,
        starter: Callable[[str], None] | None = None,
    ):
        self.config = config
        self.process_paths = process_paths
        self.starter = starter or self._start_visible

    def _start_visible(self, path: str) -> None:
        if os.name != "nt":
            raise HeartbeatError("Codex Desktop recovery is Windows-only")
        app_id = self.config.desktop_app_user_model_id
        if app_id:
            subprocess.Popen(
                ["explorer.exe", f"shell:AppsFolder\\{app_id}"],
            )
            return
        os.startfile(path)  # type: ignore[attr-defined]

    def resolve_executable(self, observed: list[str] | None = None) -> Path:
        configured = self.config.desktop_exe_path
        if configured is not None and configured.is_file():
            return configured.resolve()
        for raw in observed or self.process_paths():
            path = Path(raw)
            parts = [part.lower() for part in path.parts]
            if len(parts) >= 3 and parts[-2:] == ["resources", "codex.exe"]:
                for executable_name in ("ChatGPT.exe", "Codex.exe"):
                    candidate = path.parent.parent / executable_name
                    if candidate.is_file():
                        return candidate.resolve()
            if path.name.lower() in {"chatgpt.exe", "codex.exe"} and path.parent.name.lower() == "app":
                if path.is_file():
                    return path.resolve()
        raise HeartbeatError("could not resolve app\\Codex.exe")

    @staticmethod
    def is_running(executable: Path, observed: list[str]) -> bool:
        expected = str(executable.resolve()).casefold()
        return any(str(Path(value).resolve()).casefold() == expected for value in observed)

    def ensure_running(self) -> dict[str, Any]:
        if not self.config.desktop_recovery_enabled:
            return {"desktop_status": "disabled", "desktop_exe": None}
        observed = self.process_paths()
        executable = self.resolve_executable(observed)
        if self.is_running(executable, observed):
            return {"desktop_status": "already_running", "desktop_exe": str(executable)}
        self.starter(str(executable))
        deadline = time.monotonic() + self.config.desktop_start_timeout_seconds
        while time.monotonic() < deadline:
            time.sleep(1)
            if self.is_running(executable, self.process_paths()):
                return {"desktop_status": "started", "desktop_exe": str(executable)}
        return {
            "desktop_status": "start_unverified",
            "desktop_exe": str(executable),
        }


class WakeController:
    def __init__(
        self,
        config: HeartbeatConfig,
        *,
        client_factory: Callable[[NativeTaskLauncherConfig], AppServerClient] = AppServerClient,
        desktop_factory: Callable[[HeartbeatConfig], DesktopRecovery] = DesktopRecovery,
    ):
        self.config = config
        self.launcher_config = NativeTaskLauncherConfig(config.launcher_config_path)
        self.launcher_config.turn_completion_timeout_seconds = (
            config.turn_completion_timeout_seconds
        )
        self.client_factory = client_factory
        self.desktop_factory = desktop_factory

    def wake(
        self,
        target_thread_id: str,
        prompt: str | None,
        *,
        source_event_key: str,
        ensure_desktop: bool = False,
        model: str | None = None,
        reasoning_effort: str | None = None,
        on_turn_started: Callable[..., None] | None = None,
        client_user_message_id: str | None = None,
    ) -> dict[str, Any]:
        started_at = utc_now()
        desktop = {"desktop_status": "not_requested", "desktop_exe": None}
        resolved_client_user_message_id = client_user_message_id
        if prompt and not str(resolved_client_user_message_id or "").strip():
            return {
                "outcome": "failed",
                "started_at": started_at,
                "thread_status": "unknown",
                "error": "client_user_message_id is required for turn/start",
                "client_user_message_id": None,
                **desktop,
            }
        try:
            if ensure_desktop:
                desktop = self.desktop_factory(self.config).ensure_running()
            client = self.client_factory(self.launcher_config)
            try:
                client.start()
                client.request("thread/resume", {"threadId": target_thread_id})
                read = client.request(
                    "thread/read",
                    {"threadId": target_thread_id, "includeTurns": False},
                )
                thread = read.get("thread")
                if not isinstance(thread, dict):
                    raise HeartbeatError("thread/read response is missing thread")
                thread_status = self.status_text(thread.get("status"))
                if not prompt:
                    return {
                        "outcome": "thread_resumed",
                        "started_at": started_at,
                        "thread_status": thread_status,
                        **desktop,
                    }
                if thread_status.lower() in {"active", "running", "inprogress", "in_progress"}:
                    return {
                        "outcome": "deferred_busy",
                        "started_at": started_at,
                        "thread_status": thread_status,
                        "client_user_message_id": resolved_client_user_message_id,
                        **desktop,
                    }
                params: dict[str, Any] = {
                    "threadId": target_thread_id,
                    "clientUserMessageId": resolved_client_user_message_id,
                    "input": [{"type": "text", "text": prompt}],
                }
                if model:
                    params["model"] = model
                if reasoning_effort:
                    params["effort"] = reasoning_effort
                started = client.request("turn/start", params)
                turn = started.get("turn")
                turn_id = str(turn.get("id") or "") if isinstance(turn, dict) else ""
                if not turn_id:
                    raise HeartbeatError("turn/start response is missing turn.id")
                if on_turn_started is not None:
                    on_turn_started(
                        turn_id=turn_id,
                        thread_status="inProgress",
                        desktop_status=str(desktop["desktop_status"]),
                    )
                terminal = client.wait_for_turn_terminal(target_thread_id, turn_id)
                terminal_status = self.status_text(terminal.get("status"))
                if terminal_status != "completed":
                    detail = terminal.get("error")
                    raise HeartbeatError(
                        "woken turn ended without completion: "
                        f"status={terminal_status}; error={detail}"
                    )
                readback = client.request(
                    "thread/read",
                    {"threadId": target_thread_id, "includeTurns": False},
                )
                readback_thread = readback.get("thread")
                readback_status = (
                    self.status_text(readback_thread.get("status"))
                    if isinstance(readback_thread, dict)
                    else "unknown"
                )
                return {
                    "outcome": "turn_completed",
                    "started_at": started_at,
                    "thread_status": readback_status,
                    "turn_id": turn_id,
                    "turn_status": terminal_status,
                    "client_user_message_id": resolved_client_user_message_id,
                    **desktop,
                }
            finally:
                client.close()
        except Exception as exc:
            return {
                "outcome": "failed",
                "started_at": started_at,
                "thread_status": "unknown",
                "error": str(exc),
                "client_user_message_id": resolved_client_user_message_id,
                **desktop,
            }

    def run_existing_task(
        self,
        target_thread_id: str,
        prompt: str,
        *,
        source_event_key: str,
        ensure_desktop: bool = False,
        model: str | None = None,
        reasoning_effort: str | None = None,
        client_user_message_id: str | None = None,
    ) -> dict[str, Any]:
        """Run an existing task through the same native path as Jarvis inbox.

        This deliberately delegates to ``AppServerClient.run_existing_task``
        rather than reimplementing ``thread/resume`` plus ``turn/start``.  The
        terminal-continuation flow therefore has the same Desktop/App Server
        semantics as a Feishu message routed to the fixed Jarvis inbox.
        """
        started_at = utc_now()
        desktop = {"desktop_status": "not_requested", "desktop_exe": None}
        try:
            if ensure_desktop:
                desktop = self.desktop_factory(self.config).ensure_running()
            client = self.client_factory(self.launcher_config)
            try:
                result = client.run_existing_task(
                    target_thread_id,
                    {
                        "prompt": prompt,
                        "model": model,
                        "reasoning_effort": reasoning_effort,
                    },
                    client_user_message_id=(client_user_message_id or source_event_key),
                    model=model,
                    reasoning_effort=reasoning_effort,
                )
                return {
                    "outcome": "turn_completed",
                    "started_at": started_at,
                    "thread_status": "idle",
                    "turn_id": str(result.get("turn_id") or ""),
                    "turn_status": str(result.get("turn_status") or "completed"),
                    "final_message": str(result.get("final_message") or ""),
                    **desktop,
                }
            finally:
                client.close()
        except Exception as exc:
            return {
                "outcome": "failed",
                "started_at": started_at,
                "thread_status": "unknown",
                "error": str(exc),
                **desktop,
            }

    @staticmethod
    def status_text(value: Any) -> str:
        if isinstance(value, dict):
            return str(value.get("type") or value.get("status") or "unknown")
        return str(value or "unknown")


class NativeQuotaProbe:
    """Read the authoritative Codex weekly quota without starting an AI turn."""

    def __init__(
        self,
        config: HeartbeatConfig,
        *,
        client_factory: Callable[[NativeTaskLauncherConfig], AppServerClient] = AppServerClient,
    ):
        self.launcher_config = NativeTaskLauncherConfig(config.launcher_config_path)
        self.client_factory = client_factory

    def read_weekly_remaining(self) -> dict[str, Any]:
        client = self.client_factory(self.launcher_config)
        try:
            client.start()
            snapshot = client.request("account/rateLimits/read", {})
        finally:
            client.close()
        limits = snapshot.get("rateLimits")
        if not isinstance(limits, dict):
            by_id = snapshot.get("rateLimitsByLimitId")
            limits = by_id.get("codex") if isinstance(by_id, dict) else None
        primary = limits.get("primary") if isinstance(limits, dict) else None
        used_raw = primary.get("usedPercent") if isinstance(primary, dict) else None
        if used_raw is None:
            raise HeartbeatError("authoritative weekly quota is unavailable")
        used = float(used_raw)
        if not 0.0 <= used <= 100.0:
            raise HeartbeatError("authoritative weekly quota is out of range")
        return {
            "used_percent": used,
            "remaining_percent": 100.0 - used,
            "resets_at": primary.get("resetsAt"),
        }


class NativeThreadTerminalProbe:
    """Read one Codex thread without resuming it or starting a model turn."""

    def __init__(
        self,
        config: HeartbeatConfig,
        *,
        client_factory: Callable[[NativeTaskLauncherConfig], AppServerClient] = AppServerClient,
    ):
        self.launcher_config = NativeTaskLauncherConfig(config.launcher_config_path)
        self.client_factory = client_factory

    def inspect(self, thread_id: str) -> dict[str, Any]:
        client = self.client_factory(self.launcher_config)
        try:
            client.start()
            snapshot = client.request(
                "thread/read",
                {"threadId": thread_id, "includeTurns": True},
            )
        finally:
            client.close()
        thread = snapshot.get("thread")
        if not isinstance(thread, dict):
            raise HeartbeatError("thread/read response is missing thread")
        returned_thread_id = str(thread.get("id") or thread_id)
        if returned_thread_id != thread_id:
            raise HeartbeatError("thread/read returned a different thread id")
        thread_status = WakeController.status_text(thread.get("status"))
        turns = [item for item in (thread.get("turns") or []) if isinstance(item, dict)]
        last_turn = turns[-1] if turns else None
        last_status = (
            WakeController.status_text(last_turn.get("status"))
            if last_turn is not None
            else "unknown"
        )
        normalized = last_status.replace("_", "").casefold()
        last_error = last_turn.get("error") if last_turn else None
        if normalized in {"active", "inprogress", "pending", "queued", "running"}:
            status = "RUNNING"
        elif normalized in {"failed", "error"}:
            status = "FAILED"
        elif normalized == "completed" and not last_error:
            status = "TURN_COMPLETED_UNVERIFIED"
        elif normalized in {"aborted", "canceled", "cancelled", "interrupted"} or (
            normalized == "completed" and bool(last_error)
        ):
            status = "NEEDS_ATTENTION"
        else:
            status = "UNKNOWN"
        started_at = (
            last_turn.get("startedAt", last_turn.get("started_at"))
            if last_turn else None
        )
        completed_at = (
            last_turn.get("completedAt", last_turn.get("completed_at"))
            if last_turn else None
        )
        items = last_turn.get("items") if last_turn else None
        has_final_answer = any(
            isinstance(item, dict)
            and item.get("type") == "agentMessage"
            and item.get("phase") == "final_answer"
            and bool(str(item.get("text") or "").strip())
            for item in (items or [])
        )
        terminal = status in {"TURN_COMPLETED_UNVERIFIED", "FAILED", "NEEDS_ATTENTION"}
        terminal_fingerprint = (
            canonical_hash({
                "thread_id": thread_id,
                "last_turn_id": str(last_turn.get("id") or "") if last_turn else None,
                "last_turn_status": last_status if last_turn else None,
                "error": last_error,
                "started_at": started_at,
                "completed_at": completed_at,
                "has_final_answer": has_final_answer,
            })
            if terminal else None
        )
        return {
            "status": status,
            "thread_id": thread_id,
            "thread_status": thread_status,
            "last_turn_id": str(last_turn.get("id") or "") if last_turn else None,
            "last_turn_status": last_status if last_turn else None,
            "error": last_error,
            "started_at": started_at,
            "completed_at": completed_at,
            "duration_ms": self._duration_ms(started_at, completed_at),
            "has_final_answer": has_final_answer,
            "terminal_fingerprint": terminal_fingerprint,
            "completion_observed_at": utc_now() if terminal else None,
        }

    @staticmethod
    def _duration_ms(started_at: Any, completed_at: Any) -> int | None:
        if started_at in (None, "") or completed_at in (None, ""):
            return None

        def parse_timestamp(value: Any) -> datetime | None:
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                seconds = float(value)
                if abs(seconds) >= 100_000_000_000:
                    seconds /= 1000.0
                try:
                    return datetime.fromtimestamp(seconds, timezone.utc)
                except (OverflowError, OSError, ValueError):
                    return None
            return parse_time(value)

        try:
            started = parse_timestamp(started_at)
            completed = parse_timestamp(completed_at)
        except HeartbeatError:
            return None
        if started is None or completed is None:
            return None
        return max(int(round((completed - started).total_seconds() * 1000)), 0)


def load_standard_bridge(config: HeartbeatConfig) -> Any:
    """Load the configured, versioned Bridge package on demand."""
    root = config.standard_bridge_package_root
    if root is None:
        raise HeartbeatError("standard Bridge package root is not configured")
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    return importlib.import_module("jarvis_codex_bridge")


class StandardBridgeHeartbeatTransport:
    """Jarvis adapter for the standard package; creation is intentionally absent."""

    name = "jarvis-existing-codex-thread"

    def __init__(self, config: HeartbeatConfig, controller: WakeController, bridge: Any):
        self.config = config
        self.controller = controller
        self.bridge = bridge
        self.launcher_config = NativeTaskLauncherConfig(config.launcher_config_path)

    def health(self) -> dict[str, object]:
        return {"adapter": self.name, "status": "configured"}

    def read_thread(self, thread_id: str) -> Any:
        client = AppServerClient(self.launcher_config)
        try:
            client.start()
            response = client.request("thread/read", {"threadId": thread_id, "includeTurns": True})
        finally:
            client.close()
        thread = response.get("thread")
        if not isinstance(thread, dict) or str(thread.get("id") or "") != thread_id:
            raise HeartbeatError("standard Bridge thread/read identity mismatch")
        turns = tuple(
            self.bridge.TurnState(
                turn_id=str(turn.get("id") or ""),
                status=WakeController.status_text(turn.get("status")),
                items=tuple(item for item in (turn.get("items") or []) if isinstance(item, dict)),
                error=str(turn.get("error")) if turn.get("error") is not None else None,
            )
            for turn in (thread.get("turns") or [])
            if isinstance(turn, dict)
        )
        return self.bridge.ThreadState(
            thread_id=thread_id,
            status=WakeController.status_text(thread.get("status")),
            turns=turns,
        )

    def resume_existing(self, request: Any) -> Any:
        result = self.controller.run_existing_task(
            request.thread_id,
            request.prompt,
            source_event_key=request.source_ref,
            ensure_desktop=False,
            model=request.model,
            reasoning_effort=request.reasoning_effort,
            client_user_message_id=request.request_id,
        )
        if result.get("outcome") != "turn_completed":
            raise HeartbeatError(f"standard Bridge resume failed: {result.get('outcome')}")
        return self.bridge.StartedTurn(
            thread_id=request.thread_id,
            turn_id=str(result.get("turn_id") or ""),
            status="completed",
        )


class PidLock:
    def __init__(self, path: Path):
        self.path = path
        self.owned = False

    @staticmethod
    def running(pid: int) -> bool:
        if pid <= 0:
            return False
        if pid == os.getpid():
            return True
        if os.name == "nt":
            handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
            if handle:
                ctypes.windll.kernel32.CloseHandle(handle)
                return True
            return ctypes.get_last_error() == 5
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        for _ in range(2):
            try:
                descriptor = os.open(
                    self.path,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                )
            except FileExistsError:
                try:
                    value = json.loads(self.path.read_text(encoding="utf-8-sig"))
                    pid = int(value.get("pid") or 0)
                except (OSError, ValueError, json.JSONDecodeError):
                    pid = 0
                if self.running(pid):
                    raise HeartbeatError(f"heartbeat service already running with pid {pid}")
                self.path.unlink(missing_ok=True)
                continue
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump({"pid": os.getpid(), "acquired_at": utc_now()}, handle)
            self.owned = True
            return
        raise HeartbeatError("could not acquire heartbeat service lock")

    def release(self) -> None:
        if self.owned:
            self.path.unlink(missing_ok=True)
            self.owned = False

    def __enter__(self) -> "PidLock":
        self.acquire()
        return self

    def __exit__(self, *_: Any) -> None:
        self.release()


class MasterQueueContinuationProbe:
    """One-item queue adapter for registered visible worker threads.

    The queue file is the project-owned control surface.  A short-lived adjacent
    lock makes the read/claim/write boundary atomic without reaching into a
    project database or worker output.  A claim is deliberately never retried:
    an ambiguous post-claim wake is a controller decision, not permission to
    send a second turn.
    """

    def __init__(self, *, lock_timeout_seconds: float = 10.0):
        self.lock_timeout_seconds = lock_timeout_seconds

    @contextmanager
    def _lock(self, queue_path: Path):
        lock_path = queue_path.with_suffix(queue_path.suffix + ".jarvis.lock")
        deadline = time.monotonic() + self.lock_timeout_seconds
        descriptor: int | None = None
        while descriptor is None:
            try:
                descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                if time.monotonic() >= deadline:
                    raise HeartbeatError(f"master queue lock is busy: {lock_path}")
                time.sleep(0.05)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump({"pid": os.getpid(), "acquired_at": utc_now()}, handle)
            yield
        finally:
            lock_path.unlink(missing_ok=True)

    @staticmethod
    def _lanes(queue: dict[str, Any]) -> list[dict[str, Any]]:
        lanes = queue.get("lanes")
        if not isinstance(lanes, list):
            raise HeartbeatError("master queue lanes must be a list")
        return [lane for lane in lanes if isinstance(lane, dict)]

    @staticmethod
    def _item_valid(item: dict[str, Any]) -> bool:
        return (
            isinstance(item.get("company_id"), str) and bool(item["company_id"].strip())
            and isinstance(item.get("company_name"), str) and bool(item["company_name"].strip())
            and isinstance(item.get("official_website"), str) and item["official_website"].startswith(("http://", "https://"))
            and item.get("rating") in {"A", "A-", "B+"}
            and isinstance(item.get("source_csv"), str) and bool(item["source_csv"].strip())
            and isinstance(item.get("source_row"), int)
        )

    @staticmethod
    def _artifact_exists(queue_path: Path, value: Any) -> bool:
        if not isinstance(value, str) or not value.strip():
            return False
        candidate = Path(value)
        if not candidate.is_absolute():
            candidate = queue_path.parent / candidate
        return candidate.is_file()

    @staticmethod
    def _artifact_path(queue_path: Path, value: Any) -> Path | None:
        if not isinstance(value, str) or not value.strip():
            return None
        candidate = Path(value)
        if not candidate.is_absolute():
            candidate = queue_path.parent / candidate
        return candidate.resolve()

    def _validated_result_matches_receipt(
        self, queue_path: Path, item: dict[str, Any], receipt: dict[str, Any]
    ) -> bool:
        """Require an explicit validator acceptance of the exact result bytes."""
        fields = set(receipt["required_artifact_fields"])
        if not {"result", "validation"}.issubset(fields):
            return True
        result_path = self._artifact_path(queue_path, item.get("result"))
        validation_path = self._artifact_path(queue_path, item.get("validation"))
        if result_path is None or validation_path is None:
            return False
        try:
            validation = json.loads(validation_path.read_text(encoding="utf-8-sig"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return False
        if validation.get("valid") is not True or validation.get("error_count") != 0:
            return False
        input_path = self._artifact_path(queue_path, validation.get("input"))
        if input_path != result_path:
            return False
        expected_hash = str(validation.get("computed_sha256") or "").strip().lower()
        actual_hash = hashlib.sha256(result_path.read_bytes()).hexdigest().lower()
        return expected_hash == actual_hash

    def _terminal_valid(self, queue_path: Path, item: dict[str, Any], receipt: dict[str, Any]) -> bool:
        if item.get("status") not in set(receipt["terminal_statuses"]):
            return False
        return (
            all(field in item and item[field] not in (None, "") for field in receipt["required_item_fields"])
            and all(self._artifact_exists(queue_path, item.get(field)) for field in receipt["required_artifact_fields"])
            and self._validated_result_matches_receipt(queue_path, item, receipt)
        )

    @staticmethod
    def _fingerprint(queue: dict[str, Any]) -> str:
        lanes: list[dict[str, Any]] = []
        for lane in MasterQueueContinuationProbe._lanes(queue):
            items = lane.get("pending_new_master_items")
            lanes.append({
                "lane": lane.get("lane"),
                "thread_id": lane.get("thread_id"),
                "items": items if isinstance(items, list) else "invalid",
            })
        return canonical_hash({"lanes": lanes})

    def claim_or_observe(self, heartbeat: dict[str, Any], run_id: str) -> dict[str, Any]:
        config = json.loads(str(heartbeat.get("probe_config_json") or "{}"))
        queue_path = Path(str(config["master_queue_path"]))
        receipt = config["terminal_receipt"]
        with self._lock(queue_path):
            queue = load_json(queue_path)
            lanes = self._lanes(queue)
            terminal_items: list[str] = []
            changed = False
            active_claim = False
            for lane in lanes:
                items = lane.get("pending_new_master_items")
                if not isinstance(items, list):
                    raise HeartbeatError("pending_new_master_items must be a list")
                for item in items:
                    if not isinstance(item, dict):
                        raise HeartbeatError("queue item must be an object")
                    claim = item.get("jarvis_claim")
                    ours = isinstance(claim, dict) and claim.get("heartbeat_id") == heartbeat["heartbeat_id"]
                    if ours and self._terminal_valid(queue_path, item, receipt):
                        item["status"] = "completed"
                        claim["terminal_fingerprint"] = canonical_hash({
                            field: item.get(field)
                            for field in receipt["required_item_fields"] + receipt["required_artifact_fields"]
                        })
                        claim["terminal_observed_at"] = utc_now()
                        terminal_items.append(str(item.get("company_id")))
                        changed = True
                    elif ours and item.get("status") in {"claimed", "active"}:
                        active_claim = True
            if active_claim:
                fingerprint = self._fingerprint(queue)
                return {"status": "NO_CHANGE", "queue_fingerprint": fingerprint, "terminal_items": terminal_items}
            for lane in lanes:
                items = lane["pending_new_master_items"]
                for item in items:
                    if item.get("status") != "pending":
                        continue
                    if not self._item_valid(item):
                        continue
                    thread_id = str(lane.get("thread_id") or "").strip()
                    try:
                        uuid.UUID(thread_id)
                    except ValueError:
                        raise HeartbeatError(f"lane {lane.get('lane')} has no valid registered thread_id")
                    claim = {
                        "heartbeat_id": heartbeat["heartbeat_id"], "run_id": run_id,
                        "claimed_at": utc_now(), "state": "claimed",
                    }
                    item["status"] = "claimed"
                    item["jarvis_claim"] = claim
                    atomic_write_json(queue_path, queue)
                    return {
                        "status": "CLAIMED", "queue_fingerprint": self._fingerprint(queue),
                        "lane": str(lane.get("lane") or ""), "thread_id": thread_id,
                        "item": item,
                    }
            if changed:
                atomic_write_json(queue_path, queue)
            return {
                "status": "QUEUE_EXHAUSTED", "queue_fingerprint": self._fingerprint(queue),
                "terminal_items": terminal_items,
            }

    def mark_wake_started(self, heartbeat: dict[str, Any], run_id: str, company_id: str, turn_id: str | None) -> None:
        config = json.loads(str(heartbeat.get("probe_config_json") or "{}"))
        queue_path = Path(str(config["master_queue_path"]))
        with self._lock(queue_path):
            queue = load_json(queue_path)
            for lane in self._lanes(queue):
                for item in lane.get("pending_new_master_items", []):
                    claim = item.get("jarvis_claim") if isinstance(item, dict) else None
                    if (isinstance(claim, dict) and item.get("company_id") == company_id
                        and claim.get("heartbeat_id") == heartbeat["heartbeat_id"]
                        and claim.get("run_id") == run_id):
                        claim["state"] = "woken"
                        claim["turn_id"] = turn_id
                        claim["wake_started_at"] = utc_now()
                        atomic_write_json(queue_path, queue)
                        return
        raise HeartbeatError("claimed queue item disappeared before wake receipt")


class HeartbeatService:
    def __init__(
        self,
        config: HeartbeatConfig,
        *,
        store: HeartbeatStore | None = None,
        controller: WakeController | None = None,
        quota_probe: NativeQuotaProbe | None = None,
        thread_probe: NativeThreadTerminalProbe | None = None,
        end_of_day_probe: Any | None = None,
        master_queue_probe: MasterQueueContinuationProbe | None = None,
        workbench_progress_probe: Any | None = None,
    ):
        self.config = config
        self.store = store or HeartbeatStore(config)
        self.recovered_probe_handoffs = self.store.recover_native_probe_handoffs()
        self.controller = controller or WakeController(config)
        self.quota_probe = quota_probe or NativeQuotaProbe(config)
        self.thread_probe = thread_probe or NativeThreadTerminalProbe(config)
        self.end_of_day_probe = end_of_day_probe
        self.master_queue_probe = master_queue_probe or MasterQueueContinuationProbe()
        self.workbench_progress_probe = workbench_progress_probe
        self.stop_requested = False
        self._executor = ThreadPoolExecutor(
            max_workers=config.max_concurrent_runs,
            thread_name_prefix="jarvis-heartbeat",
        )
        self._state_lock = threading.RLock()
        self._futures: dict[Future[dict[str, Any]], dict[str, Any]] = {}
        self._target_inflight: set[str] = set()
        self._recent_results: list[dict[str, Any]] = []
        self._last_tick_due_count = 0

    def _prepare_wake_prompt(
        self,
        heartbeat: dict[str, Any],
        rendered_prompt: str,
        run_id: str,
    ) -> str:
        if self.config.wake_payload_mode == "inline":
            return rendered_prompt
        contract_path = (
            self.config.contracts_dir
            / str(heartbeat["heartbeat_id"])
            / f"{run_id}.json"
        )
        contract = {
            "version": 1,
            "heartbeat_id": str(heartbeat["heartbeat_id"]),
            "run_id": run_id,
            "target_thread_id": str(heartbeat["target_thread_id"]),
            "source_event_key": str(heartbeat["source_event_key"]),
            "request_hash": str(heartbeat["request_hash"]),
            "prompt": rendered_prompt,
            "continuation_receipt_path": str(
                self.config.contracts_dir / str(heartbeat["heartbeat_id"]) / "receipts" / f"{run_id}.json"
            ),
            "continuation_receipt_required": self.config.require_prompt_continuation_receipt,
        }
        atomic_write_json(contract_path, contract)
        contract_sha256 = hashlib.sha256(contract_path.read_bytes()).hexdigest()
        envelope = "\n".join(
            [
                "JARVIS_HEARTBEAT_CONTRACT_V1",
                f"heartbeat_id={heartbeat['heartbeat_id']}",
                f"contract_path={contract_path}",
                f"contract_sha256={contract_sha256}",
                "action=Read UTF-8 JSON, verify file SHA-256, execute contract.prompt, and write the required continuation receipt.",
            ]
        )
        if len(envelope) > self.config.max_wake_envelope_chars:
            raise HeartbeatError(
                "heartbeat wake envelope exceeds max_wake_envelope_chars: "
                f"{len(envelope)} > {self.config.max_wake_envelope_chars}"
            )
        return envelope

    def _read_continuation_receipt(self, heartbeat: dict[str, Any], run_id: str) -> dict[str, Any] | None:
        path = self.config.contracts_dir / str(heartbeat["heartbeat_id"]) / "receipts" / f"{run_id}.json"
        try:
            receipt = load_json(path)
        except HeartbeatError:
            return None
        if receipt.get("heartbeat_id") != heartbeat["heartbeat_id"] or receipt.get("run_id") != run_id:
            return None
        status = str(receipt.get("continuation_status") or "")
        if status not in {"CONTINUE_SAFE", "AWAITING_DECISION", "BLOCKED", "TERMINAL"}:
            return None
        if status == "CONTINUE_SAFE" and not str(receipt.get("progress_fingerprint") or "").strip():
            return None
        return receipt

    def _health_payload(self) -> dict[str, Any]:
        inflight = self.store.inflight_runs()
        with self._state_lock:
            recent = list(self._recent_results[-20:])
            due_count = self._last_tick_due_count
        return {
            "status": "running",
            "pid": os.getpid(),
            "heartbeat_at": utc_now(),
            "last_tick_due_count": due_count,
            "last_tick_results": [
                {
                    "heartbeat_id": item.get("heartbeat_id"),
                    "run_id": item.get("run_id"),
                    "outcome": item.get("outcome"),
                    "turn_id": item.get("turn_id"),
                    "error": item.get("error"),
                }
                for item in recent
            ],
            "max_concurrent_runs": self.config.max_concurrent_runs,
            "wake_payload_mode": self.config.wake_payload_mode,
            "heartbeat_contracts_dir": str(self.config.contracts_dir),
            "max_wake_envelope_chars": self.config.max_wake_envelope_chars,
            "active_inflight_count": len(inflight),
            "inflight_runs": inflight,
            "poll_seconds": self.config.poll_seconds,
            "database": str(self.config.db_path),
        }

    def _write_health(self) -> dict[str, Any]:
        health = self._health_payload()
        with self._state_lock:
            atomic_write_json(self.config.health_path, health)
        return health

    def _record_result(self, result: dict[str, Any]) -> None:
        with self._state_lock:
            self._recent_results.append(result)
            del self._recent_results[:-20]

    @staticmethod
    def _terminal_receipt_prompt(heartbeat: dict[str, Any], probe_result: dict[str, Any]) -> str:
        """Structured observer receipt.  This is the only monitor-originated turn."""
        receipt = {
            "heartbeat_id": heartbeat["heartbeat_id"],
            "child_thread_id": heartbeat["target_thread_id"],
            "parent_thread_id": heartbeat["parent_thread_id"],
            "status": probe_result.get("status"),
            "terminal_fingerprint": probe_result.get("terminal_fingerprint"),
            "child_turn_id": probe_result.get("last_turn_id"),
            "child_turn_status": probe_result.get("last_turn_status"),
            "error": probe_result.get("error"),
            "monitor_status": "terminal_observed",
        }
        return "JARVIS_TERMINAL_RECEIPT_V1\n" + json.dumps(
            receipt, ensure_ascii=False, sort_keys=True,
        )

    def _standard_bridge(self, heartbeat: dict[str, Any]) -> tuple[Any, Any, Any]:
        bridge_module = load_standard_bridge(self.config)
        transport = StandardBridgeHeartbeatTransport(
            self.config, self.controller, bridge_module
        )
        root = self.config.contracts_dir / str(heartbeat["heartbeat_id"])
        bridge = bridge_module.ExistingThreadBridge(
            transport,
            bridge_module.JsonlReceiptJournal(root / "standard_bridge_receipts.jsonl"),
        )
        monitor = bridge_module.ThreadTerminalMonitor(
            transport, root / "standard_monitor_state.json"
        )
        return bridge_module, bridge, monitor

    def _execute_claim(
        self,
        heartbeat: dict[str, Any],
        scheduled_at: str,
        inflight: dict[str, Any],
    ) -> dict[str, Any]:
        def record_turn_started(
            *, turn_id: str, thread_status: str, desktop_status: str,
        ) -> None:
            self.store.record_inflight_turn(
                heartbeat_id=str(heartbeat["heartbeat_id"]),
                run_id=str(inflight["run_id"]),
                turn_id=turn_id,
                thread_status=thread_status,
                desktop_status=desktop_status,
                source_event_key=str(heartbeat["source_event_key"]),
            )
            self._write_health()

        try:
            if heartbeat.get("execution_mode") == "native_probe":
                probe_config = json.loads(str(heartbeat.get("probe_config_json") or "{}"))
                if probe_config.get("type") == "attendance_end_of_day_memo":
                    if self.end_of_day_probe is None:
                        from attendance_end_of_day_memo_probe import AttendanceEndOfDayMemoProbe

                        self.end_of_day_probe = AttendanceEndOfDayMemoProbe(WORKSPACE_ROOT)
                    probe_result = self.end_of_day_probe.run(probe_config)
                    result = {
                        "outcome": (
                            "probe_notification_queued"
                            if probe_result.get("status") == "QUEUED"
                            else "probe_not_due"
                        ),
                        "started_at": inflight["started_at"],
                        "thread_status": "not_requested",
                        "desktop_status": "not_requested",
                        "probe_result": probe_result,
                    }
                elif probe_config.get("type") == "workbench_queue_progress":
                    if self.workbench_progress_probe is None:
                        from workbench_queue_progress_probe import WorkbenchQueueProgressProbe

                        self.workbench_progress_probe = WorkbenchQueueProgressProbe(WORKSPACE_ROOT)
                    probe_result = self.workbench_progress_probe.run({
                        **probe_config,
                        "heartbeat_id": str(heartbeat["heartbeat_id"]),
                    })
                    terminal = bool(probe_result.get("terminal"))
                    result = {
                        "outcome": (
                            "workbench_probe_completed" if terminal
                            else (
                                "probe_notification_queued"
                                if probe_result.get("outbox_id") else "workbench_probe_progress"
                            )
                        ),
                        "started_at": inflight["started_at"],
                        "thread_status": "not_requested",
                        "desktop_status": "not_requested",
                        "probe_result": probe_result,
                        "complete_heartbeat": terminal,
                    }
                elif probe_config.get("type") == "master_queue_continuation":
                    queue_result = self.master_queue_probe.claim_or_observe(
                        heartbeat, str(inflight["run_id"])
                    )
                    queue_status = str(queue_result["status"])
                    wake_chain: list[dict[str, Any]] = []
                    while queue_status == "CLAIMED":
                        item = dict(queue_result["item"])
                        company_id = str(item["company_id"])
                        receipt = probe_config["terminal_receipt"]
                        wake_prompt = "JARVIS_QUEUE_CONTINUATION_V1\n" + json.dumps({
                            "heartbeat_id": heartbeat["heartbeat_id"],
                            "run_id": inflight["run_id"],
                            "master_queue_path": probe_config["master_queue_path"],
                            "lane": queue_result["lane"],
                            "item": {key: value for key, value in item.items() if key != "jarvis_claim"},
                            "terminal_receipt": receipt,
                            "instruction": str(heartbeat["prompt"]),
                        }, ensure_ascii=False, sort_keys=True)
                        wake_result = self.controller.wake(
                            str(queue_result["thread_id"]), wake_prompt,
                            source_event_key=heartbeat["source_event_key"],
                            ensure_desktop=False,
                            model=heartbeat.get("model"),
                            reasoning_effort=heartbeat.get("reasoning_effort"),
                            on_turn_started=record_turn_started,
                            client_user_message_id=(
                                f"jarvis-queue-{heartbeat['heartbeat_id']}-{company_id}"
                            ),
                        )
                        if wake_result.get("outcome") == "turn_completed":
                            self.master_queue_probe.mark_wake_started(
                                heartbeat, str(inflight["run_id"]), company_id,
                                str(wake_result.get("turn_id") or "") or None,
                            )
                            wake_chain.append({
                                "company_id": company_id,
                                "turn_id": wake_result.get("turn_id"),
                            })
                            # Immediate accept-then-roll: a completed turn is
                            # read from its durable result/validator receipts
                            # now, rather than waiting for the next heartbeat.
                            # A non-valid receipt leaves the claim in place and
                            # can never generate a duplicate worker wake.
                            queue_result = self.master_queue_probe.claim_or_observe(
                                heartbeat, str(inflight["run_id"])
                            )
                            queue_status = str(queue_result["status"])
                        else:
                            # Keep the durable claim.  A retry could create a duplicate turn.
                            result = {
                                **wake_result, "outcome": "queue_claim_awaiting_decision",
                                "error": str(wake_result.get("outcome") or "wake failed after claim"),
                                "probe_result": queue_result,
                                "queue_fingerprint": queue_result["queue_fingerprint"],
                            }
                            break
                    else:
                        if wake_chain:
                            result = {
                                "outcome": "queue_continuation_released",
                                "started_at": inflight["started_at"],
                                "thread_status": "idle",
                                "desktop_status": "not_requested",
                                "turn_id": wake_chain[-1]["turn_id"],
                                "probe_result": queue_result,
                                "queue_fingerprint": queue_result["queue_fingerprint"],
                                "immediate_completion_chain": wake_chain,
                            }
                        else:
                            result = {
                                "outcome": (
                                    "queue_no_change_silent" if queue_status == "NO_CHANGE"
                                    else "queue_exhausted_silent"
                                ),
                                "started_at": inflight["started_at"],
                                "thread_status": "not_requested",
                                "desktop_status": "not_requested",
                                "probe_result": queue_result,
                                "queue_quiet": True,
                                "queue_fingerprint": queue_result["queue_fingerprint"],
                            }
                            if queue_result.get("terminal_items"):
                                result["outcome"] = "queue_terminal_queue_exhausted"
                elif probe_config.get("type") == "codex_thread_terminal_continue":
                    probe_result = self.thread_probe.inspect(str(heartbeat["target_thread_id"]))
                    result = {
                        "outcome": "probe_thread_status",
                        "started_at": inflight["started_at"],
                        "thread_status": str(probe_result.get("thread_status") or "unknown"),
                        "desktop_status": "not_requested",
                        "probe_result": probe_result,
                    }
                    # Always establish/read the monitor baseline.  A task that
                    # was already stopped when monitoring was attached must
                    # never trigger continuation merely because it is stopped.
                    bridge_module, bridge, monitor = self._standard_bridge(heartbeat)
                    route = bridge_module.ReceiptRoute(
                        str(heartbeat["target_thread_id"]),
                        str(probe_config.get("receipt_target_thread_id") or heartbeat["target_thread_id"]),
                    )
                    monitor_receipt = monitor.observe(str(heartbeat["heartbeat_id"]), route)
                    result["monitor_receipt"] = monitor_receipt.as_dict()
                    terminal_fingerprint = str(probe_result.get("terminal_fingerprint") or "")
                    if (
                        probe_result.get("status") == "TURN_COMPLETED_UNVERIFIED"
                        and terminal_fingerprint
                        and monitor_receipt.state == "terminal_changed"
                    ):
                        if self.store.claim_terminal_receipt(
                            str(heartbeat["heartbeat_id"]), terminal_fingerprint,
                            str(heartbeat["source_event_key"]),
                        ):
                            rule = bridge_module.TerminalContinuationRule(
                                monitor_id=str(heartbeat["heartbeat_id"]),
                                observed_thread_id=str(heartbeat["target_thread_id"]),
                                resume_target_thread_id=str(
                                    probe_config.get("continuation_target_thread_id")
                                    or heartbeat["target_thread_id"]
                                ),
                                prompt=str(
                                    probe_config.get("continuation_prompt")
                                    or heartbeat["prompt"]
                                ),
                                source_ref=str(heartbeat["source_event_key"]),
                                model=heartbeat.get("model"),
                                reasoning_effort=heartbeat.get("reasoning_effort"),
                            )
                            continuation = bridge.resume_existing(
                                rule.resume_request(monitor_receipt)
                            )
                            result.update({
                                "outcome": "terminal_continuation_completed" if continuation.status == "completed" else str(continuation.status),
                                "continuation_turn_id": continuation.turn_id,
                                "continuation_output": continuation.output,
                                "monitor_receipt": monitor_receipt.as_dict(),
                            })
                            receipt_target = probe_config.get("receipt_target_thread_id")
                            if continuation.status == "completed" and receipt_target:
                                delivery = bridge.resume_existing(
                                    monitor.receipt_delivery_request(
                                        monitor_receipt,
                                        source_ref=str(heartbeat["source_event_key"]),
                                    )
                                )
                                result["receipt_delivery"] = delivery.as_dict()
                            self.store.finish_terminal_receipt(
                                str(heartbeat["heartbeat_id"]),
                                delivered=continuation.status == "completed",
                                turn_id=continuation.turn_id,
                                source_event_key=str(heartbeat["source_event_key"]),
                                error=continuation.reason,
                            )
                elif probe_config.get("type") == "codex_thread_terminal":
                    probe_result = self.thread_probe.inspect(
                        str(heartbeat["target_thread_id"])
                    )
                    result = {
                        "outcome": "probe_thread_status",
                        "started_at": inflight["started_at"],
                        "thread_status": str(
                            probe_result.get("thread_status") or "unknown"
                        ),
                        "desktop_status": "not_requested",
                        "probe_result": probe_result,
                    }
                    terminal_fingerprint = str(probe_result.get("terminal_fingerprint") or "")
                    if probe_result.get("status") in {"TURN_COMPLETED_UNVERIFIED", "FAILED", "NEEDS_ATTENTION"} and terminal_fingerprint:
                        if self.store.claim_terminal_receipt(
                            str(heartbeat["heartbeat_id"]), terminal_fingerprint,
                            str(heartbeat["source_event_key"]),
                        ):
                            try:
                                parent_result = self.controller.wake(
                                    str(heartbeat["parent_thread_id"]),
                                    self._terminal_receipt_prompt(heartbeat, probe_result),
                                    source_event_key=heartbeat["source_event_key"],
                                    ensure_desktop=False,
                                    client_user_message_id=(
                                        f"jarvis-terminal-receipt-{heartbeat['heartbeat_id']}-{terminal_fingerprint[:12]}"
                                    ),
                                )
                                delivered = parent_result.get("outcome") == "turn_completed"
                                self.store.finish_terminal_receipt(
                                    str(heartbeat["heartbeat_id"]), delivered=delivered,
                                    turn_id=str(parent_result.get("turn_id") or "") or None,
                                    source_event_key=str(heartbeat["source_event_key"]),
                                    error=None if delivered else str(parent_result.get("outcome") or "parent wake failed"),
                                )
                                result["outcome"] = "parent_terminal_receipt_delivered" if delivered else "parent_terminal_receipt_retry"
                                result["parent_thread_id"] = heartbeat["parent_thread_id"]
                                result["parent_turn_id"] = parent_result.get("turn_id")
                            except Exception as receipt_error:
                                self.store.finish_terminal_receipt(
                                    str(heartbeat["heartbeat_id"]), delivered=False, turn_id=None,
                                    source_event_key=str(heartbeat["source_event_key"]), error=str(receipt_error),
                                )
                                result["outcome"] = "parent_terminal_receipt_retry"
                                result["error"] = str(receipt_error)
                else:
                    first = self.quota_probe.read_weekly_remaining()
                    threshold = float(probe_config["threshold_percent"])
                    reads = [first]
                    if float(first["remaining_percent"]) >= threshold:
                        if self.config.quota_probe_double_read_delay_seconds:
                            time.sleep(self.config.quota_probe_double_read_delay_seconds)
                        second = self.quota_probe.read_weekly_remaining()
                        reads.append(second)
                    if len(reads) == 1 or float(reads[-1]["remaining_percent"]) < threshold:
                        result = {
                            "outcome": "probe_below_threshold",
                            "started_at": inflight["started_at"],
                            "thread_status": "not_requested",
                            "desktop_status": "not_requested",
                            "probe_result": {
                                "threshold_percent": threshold,
                                "reads": reads,
                            },
                        }
                    else:
                        remaining = float(reads[-1]["remaining_percent"])
                        armed = self.store.arm_trigger_handoff(
                            heartbeat_id=str(heartbeat["heartbeat_id"]),
                            run_id=str(inflight["run_id"]),
                            remaining_percent=remaining,
                            source_event_key=str(heartbeat["source_event_key"]),
                        )
                        if not armed:
                            raise HeartbeatError("native probe trigger was already handed off")
                        prompt = str(heartbeat["prompt"]).replace(
                            "{remaining_percent}", f"{remaining:g}"
                        )
                        wake_prompt = self._prepare_wake_prompt(
                            heartbeat,
                            prompt,
                            str(inflight["run_id"]),
                        )
                        wake_result = self.controller.wake(
                            heartbeat["target_thread_id"],
                            wake_prompt,
                            source_event_key=heartbeat["source_event_key"],
                            ensure_desktop=False,
                            model=heartbeat.get("model"),
                            reasoning_effort=heartbeat.get("reasoning_effort"),
                            on_turn_started=record_turn_started,
                            client_user_message_id=(
                                f"jarvis-heartbeat-{heartbeat['heartbeat_id']}-trigger-v1"
                            ),
                        )
                        handoff_started = wake_result.get("outcome") == "turn_completed"
                        self.store.finish_trigger_handoff(
                            heartbeat_id=str(heartbeat["heartbeat_id"]),
                            run_id=str(inflight["run_id"]),
                            delivered_handoff_started=handoff_started,
                            source_event_key=str(heartbeat["source_event_key"]),
                        )
                        result = {
                            **wake_result,
                            "outcome": (
                                "probe_trigger_handoff" if handoff_started
                                else str(wake_result.get("outcome") or "failed")
                            ),
                            "probe_result": {
                                "threshold_percent": threshold,
                                "reads": reads,
                            },
                        }
            else:
                # A monitor must be a native probe.  Never rely on wording such as
                # "monitor only" while still injecting a turn into the observed child.
                monitor_text = f"{heartbeat.get('name', '')}\n{heartbeat.get('prompt', '')}".lower()
                if re.search(r"\bmonitor\b|监控|监测", monitor_text):
                    result = {
                        "outcome": "rejected_unsafe_monitor_prompt",
                        "started_at": inflight["started_at"],
                        "thread_status": "untouched",
                        "desktop_status": "not_requested",
                        "error": "monitoring requires execution_mode=native_probe and probe.type=codex_thread_terminal; child thread was not contacted",
                    }
                else:
                    wake_prompt = self._prepare_wake_prompt(
                        heartbeat,
                        str(heartbeat["prompt"]),
                        str(inflight["run_id"]),
                    )
                    result = self.controller.wake(
                        heartbeat["target_thread_id"],
                        wake_prompt,
                        source_event_key=heartbeat["source_event_key"],
                        ensure_desktop=bool(heartbeat["ensure_desktop"]),
                        model=heartbeat.get("model"),
                        reasoning_effort=heartbeat.get("reasoning_effort"),
                        on_turn_started=record_turn_started,
                        client_user_message_id=(
                            f"jarvis-heartbeat-{heartbeat['heartbeat_id']}-"
                            f"run-{inflight['run_id']}"
                        ),
                    )
                    if result.get("outcome") == "turn_completed":
                        result["continuation_receipt"] = self._read_continuation_receipt(
                            heartbeat, str(inflight["run_id"])
                        )
        except Exception as exc:
            result = {
                "outcome": "failed",
                "started_at": inflight["started_at"],
                "thread_status": "unknown",
                "desktop_status": "not_requested",
                "error": str(exc),
            }
        result = {**result, "run_id": inflight["run_id"]}
        return self.store.record_run(
            heartbeat,
            heartbeat["source_event_key"],
            heartbeat["target_thread_id"],
            result,
            scheduled_at=scheduled_at,
        )

    def _finish_future(self, future: Future[dict[str, Any]]) -> None:
        metadata: dict[str, Any] | None
        with self._state_lock:
            metadata = self._futures.pop(future, None)
            if metadata:
                self._target_inflight.discard(str(metadata["target_thread_id"]))
        try:
            result = future.result()
        except Exception as exc:  # _execute_claim is defensive; retain health if it fails.
            result = {
                "heartbeat_id": metadata.get("heartbeat_id") if metadata else None,
                "outcome": "failed",
                "error": str(exc),
            }
        if metadata:
            result.setdefault("heartbeat_id", metadata["heartbeat_id"])
        self._record_result(result)
        if not self.stop_requested:
            self._write_health()

    def run_once(self, *, wait_for_completion: bool = True) -> dict[str, Any]:
        due = self.store.claim_due()
        results: list[dict[str, Any]] = []
        submitted: list[Future[dict[str, Any]]] = []
        with self._state_lock:
            self._last_tick_due_count = len(due)
        for heartbeat in due:
            scheduled_at = datetime.fromtimestamp(
                float(heartbeat["scheduled_epoch"]), timezone.utc
            ).isoformat()
            target_thread_id = str(heartbeat["target_thread_id"])
            with self._state_lock:
                target_busy = target_thread_id in self._target_inflight
                capacity_busy = len(self._futures) >= self.config.max_concurrent_runs
            inflight = {
                "run_id": heartbeat["claimed_run_id"],
                "started_at": heartbeat["claimed_started_at"],
                "outcome": "inflight",
            }
            if target_busy or capacity_busy:
                reason = "target_thread_busy" if target_busy else "scheduler_capacity_busy"
                recorded = self.store.record_run(
                    heartbeat,
                    heartbeat["source_event_key"],
                    target_thread_id,
                    {
                        **inflight,
                        "outcome": "deferred_busy",
                        "thread_status": "busy",
                        "desktop_status": "not_requested",
                        "error": reason,
                    },
                    scheduled_at=scheduled_at,
                )
                recorded["heartbeat_id"] = heartbeat["heartbeat_id"]
                results.append(recorded)
                self._record_result(recorded)
                continue
            with self._state_lock:
                self._target_inflight.add(target_thread_id)
                future = self._executor.submit(
                    self._execute_claim, heartbeat, scheduled_at, inflight
                )
                self._futures[future] = {
                    "heartbeat_id": heartbeat["heartbeat_id"],
                    "target_thread_id": target_thread_id,
                    "run_id": inflight["run_id"],
                }
                future.add_done_callback(self._finish_future)
                submitted.append(future)
        self._write_health()
        if wait_for_completion:
            for future in as_completed(submitted):
                results.append(future.result())
        health = self._write_health()
        return {"due_count": len(due), "results": results, "health": health}

    def shutdown(self, *, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait, cancel_futures=not wait)

    def run_forever(self) -> int:
        def request_stop(*_: Any) -> None:
            self.stop_requested = True

        signal.signal(signal.SIGTERM, request_stop)
        if hasattr(signal, "SIGBREAK"):
            signal.signal(signal.SIGBREAK, request_stop)  # type: ignore[attr-defined]
        with PidLock(self.config.lock_path):
            while not self.stop_requested:
                try:
                    self.run_once(wait_for_completion=False)
                except Exception as exc:
                    atomic_write_json(
                        self.config.health_path,
                        {
                            "status": "degraded",
                            "pid": os.getpid(),
                            "heartbeat_at": utc_now(),
                            "last_error": str(exc),
                            "database": str(self.config.db_path),
                        },
                    )
                deadline = time.monotonic() + self.config.poll_seconds
                while not self.stop_requested and time.monotonic() < deadline:
                    time.sleep(min(0.5, max(deadline - time.monotonic(), 0.05)))
        self.shutdown(wait=False)
        atomic_write_json(
            self.config.health_path,
            {"status": "stopped", "pid": os.getpid(), "stopped_at": utc_now()},
        )
        return 0


def print_json(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


def command_request(path: str) -> dict[str, Any]:
    return load_json(Path(path).resolve())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("create")
    create.add_argument("--request", required=True)
    update = sub.add_parser("update")
    update.add_argument("--request", required=True)
    list_parser = sub.add_parser("list")
    list_parser.add_argument("--status")
    view = sub.add_parser("view")
    view.add_argument("--id", required=True)
    for action in ("pause", "resume", "cancel"):
        item = sub.add_parser(action)
        item.add_argument("--id", required=True)
        item.add_argument("--source-event-key", required=True)
        item.add_argument("--confirmation-evidence", required=True)
    delivery = sub.add_parser("complete-delivery")
    delivery.add_argument("--id", required=True)
    delivery.add_argument("--message-id", required=True)
    delivery.add_argument("--source-event-key", required=True)
    wake = sub.add_parser("wake-now")
    wake.add_argument("--request", required=True)
    recover = sub.add_parser("recover-codex")
    recover.add_argument("--request", required=True)
    sub.add_parser("run-once")
    sub.add_parser("run-forever")
    sub.add_parser("health-check")
    sub.add_parser("integrity-check")
    sub.add_parser("probe-quota")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    config = HeartbeatConfig.load(args.config)
    store = HeartbeatStore(config)
    if args.command == "create":
        print_json({"ok": True, **store.create(command_request(args.request))})
        return 0
    if args.command == "update":
        print_json({"ok": True, "heartbeat": store.update(command_request(args.request))})
        return 0
    if args.command == "list":
        print_json({"ok": True, "heartbeats": store.list(args.status)})
        return 0
    if args.command == "view":
        print_json({"ok": True, "heartbeat": store.get(args.id)})
        return 0
    if args.command in {"pause", "resume", "cancel"}:
        target = {"pause": PAUSED, "resume": ACTIVE, "cancel": "CANCELLED"}[args.command]
        result = store.set_status(
            args.id,
            target,
            args.source_event_key,
            args.confirmation_evidence,
        )
        print_json({"ok": True, "heartbeat": result})
        return 0
    if args.command == "complete-delivery":
        result = store.complete_delivery(
            args.id, args.message_id, args.source_event_key
        )
        print_json({"ok": True, "heartbeat": result})
        return 0
    if args.command in {"wake-now", "recover-codex"}:
        request = command_request(args.request)
        thread_id = store.validate_thread_id(request.get("target_thread_id"))
        source_event_key = str(request.get("source_event_key") or "").strip()
        if not source_event_key:
            raise HeartbeatError("source_event_key is required")
        client_user_message_id = str(request.get("client_user_message_id") or "").strip()
        if args.command == "wake-now" and not client_user_message_id:
            raise HeartbeatError("client_user_message_id is required for wake-now")
        prompt = (
            None
            if args.command == "recover-codex"
            else str(request.get("prompt") or "").strip()
        )
        if args.command == "wake-now" and not prompt:
            raise HeartbeatError("prompt is required for wake-now")
        if prompt and len(prompt) > config.max_prompt_chars:
            raise HeartbeatError("prompt exceeds max_prompt_chars")
        ensure_desktop = bool(
            request.get("ensure_desktop", args.command == "recover-codex")
        )
        result = WakeController(config).wake(
            thread_id,
            prompt,
            source_event_key=source_event_key,
            ensure_desktop=ensure_desktop,
            model=str(request.get("model") or "").strip() or None,
            reasoning_effort=(
                str(request.get("reasoning_effort") or "").strip() or None
            ),
            client_user_message_id=(
                client_user_message_id if args.command == "wake-now" else None
            ),
        )
        recorded = store.record_run(
            None,
            source_event_key,
            thread_id,
            result,
        )
        succeeded = result.get("outcome") in {"turn_completed", "thread_resumed"}
        print_json({"ok": succeeded, "result": recorded})
        return 0 if succeeded else 2
    if args.command == "run-once":
        print_json({"ok": True, **HeartbeatService(config, store=store).run_once()})
        return 0
    if args.command == "run-forever":
        return HeartbeatService(config, store=store).run_forever()
    if args.command == "health-check":
        health = load_json(config.health_path) if config.health_path.exists() else None
        fresh = False
        if health:
            timestamp = parse_time(health.get("heartbeat_at"))
            fresh = bool(
                timestamp
                and (datetime.now(timezone.utc) - timestamp).total_seconds()
                <= max(config.poll_seconds * 4, 30)
            )
        print_json({"ok": fresh, "health": health})
        return 0 if fresh else 2
    if args.command == "probe-quota":
        print_json({
            "ok": True,
            "quota": NativeQuotaProbe(config).read_weekly_remaining(),
        })
        return 0
    if args.command == "integrity-check":
        result = store.integrity()
        ok = result["integrity_check"] == "ok" and result["foreign_key_violations"] == 0
        print_json({"ok": ok, **result})
        return 0 if ok else 2
    raise HeartbeatError(f"unknown command: {args.command}")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except HeartbeatError as exc:
        print_json({"ok": False, "error": str(exc)})
        raise SystemExit(2)
