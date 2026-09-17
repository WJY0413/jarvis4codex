#!/usr/bin/env python3
"""Durable local state operations for the Jarvis COO dispatcher.

This module does not consume Feishu events, resume Codex, send messages, or
create worker threads. It provides the phase-one state, confirmation, outbox,
identity, locking, and audit primitives used by those later adapters.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
import uuid
from contextlib import AbstractContextManager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable


WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = WORKSPACE_ROOT / "coo_state" / "dispatcher"

STATES = {
    "IDLE",
    "DISCUSSING",
    "PACKET_DRAFT",
    "AWAITING_CONFIRMATION",
    "DISPATCHED",
    "MONITORING",
    "RESULT_REVIEW",
    "COMPLETE",
    "CANCELLED",
    "BLOCKED",
}
BUSINESS_STATUSES = {
    "not_started",
    "running",
    "succeeded",
    "failed",
    "blocked",
    "cancelled",
}
DELIVERY_STATUSES = {"not_attempted", "queued", "delivered", "failed", "retry_pending", "unknown", "expired"}
EVENT_PROCESSING_STATUSES = {"queued", "processing", "completed", "failed", "ignored"}
EVENT_ENRICHMENT_FIELDS = {
    "attachments",
    "context_events",
    "not_before_epoch",
    "attachment_status",
    "jarvis_route",
}
PACKET_REQUIRED_FIELDS = {
    "task_understanding",
    "target_project",
    "execution_scope",
    "data_sources",
    "must_load_skills",
    "write_allowed",
    "write_forbidden",
    "risk_actions",
    "target_execution_thread",
    "deliverables",
    "completion_criteria",
}


class DispatcherError(RuntimeError):
    """Raised when a dispatcher control-plane contract is violated."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DispatcherError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DispatcherError(f"{path} must contain a JSON object")
    return value


def atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + f".{os.getpid()}.{uuid.uuid4().hex}.tmp")
    temp.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temp, path)


def append_jsonl(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    if not path.exists():
        return
    for line_number, raw in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), 1):
        if not raw.strip():
            continue
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise DispatcherError(f"invalid JSONL at {path}:{line_number}: {exc}") from exc
        if not isinstance(value, dict):
            raise DispatcherError(f"{path}:{line_number} must contain a JSON object")
        yield value


class ProcessLock(AbstractContextManager["ProcessLock"]):
    """Small cross-process single-writer lock using atomic file creation."""

    def __init__(self, path: Path, timeout_seconds: float = 10.0, stale_seconds: float = 300.0,
                 *, owner_alive: Callable[[int], bool] | None = None):
        self.path = path
        self.timeout_seconds = timeout_seconds
        self.stale_seconds = stale_seconds
        self.acquired = False
        self.owner_alive = owner_alive

    def __enter__(self) -> "ProcessLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    handle.write(
                        json.dumps(
                            {"pid": os.getpid(), "acquired_at": utc_now()},
                            ensure_ascii=False,
                        )
                    )
                self.acquired = True
                return self
            except (FileExistsError, PermissionError):
                try:
                    age = time.time() - self.path.stat().st_mtime
                    if self.owner_alive is not None:
                        if self._remove_dead_owner():
                            continue
                    elif age > self.stale_seconds:
                        try:
                            self.path.unlink(missing_ok=True)
                        except PermissionError:
                            pass
                        continue
                except FileNotFoundError:
                    continue
                except PermissionError:
                    pass
                if time.monotonic() >= deadline:
                    raise DispatcherError(f"dispatcher lock timeout: {self.path}")
                time.sleep(0.025)

    def _remove_dead_owner(self) -> bool:
        """Delete only the inspected dead-owner file, excluding competing reclaimers."""
        def dead(raw: str) -> bool:
            owner = json.loads(raw)
            pid = owner.get("pid") if isinstance(owner, dict) else None
            return type(pid) is int and pid > 0 and self.owner_alive(pid) is False

        try:
            if os.name == "nt":
                import ctypes
                from ctypes import wintypes
                kernel = ctypes.WinDLL("kernel32", use_last_error=True)
                kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                    wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
                kernel.CreateFileW.restype = wintypes.HANDLE
                kernel.ReadFile.argtypes = [wintypes.HANDLE, wintypes.LPVOID, wintypes.DWORD,
                                           ctypes.POINTER(wintypes.DWORD), wintypes.LPVOID]
                kernel.SetFileInformationByHandle.argtypes = [wintypes.HANDLE, ctypes.c_int,
                                                             wintypes.LPVOID, wintypes.DWORD]
                kernel.CloseHandle.argtypes = [wintypes.HANDLE]
                # Exclusive handle prevents a stale read/unlink race with a new live owner.
                handle = kernel.CreateFileW(str(self.path), 0x80000000 | 0x10000, 0, None, 3, 0x80, None)
                if handle == wintypes.HANDLE(-1).value:
                    return False
                try:
                    buffer, count = ctypes.create_string_buffer(4096), wintypes.DWORD()
                    if not kernel.ReadFile(handle, buffer, len(buffer), ctypes.byref(count), None):
                        return False
                    if not dead(buffer.raw[:count.value].decode("utf-8")):
                        return False
                    disposition = wintypes.BOOL(True)
                    return bool(kernel.SetFileInformationByHandle(handle, 4, ctypes.byref(disposition),
                                                                  ctypes.sizeof(disposition)))
                finally:
                    kernel.CloseHandle(handle)
            else:
                import fcntl
                with self.path.open("r", encoding="utf-8") as handle:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    opened, current = os.fstat(handle.fileno()), self.path.stat()
                    if (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino):
                        return False
                    if not dead(handle.read()):
                        return False
                    self.path.unlink()
                    return True
        except (OSError, ValueError, TypeError):
            return False

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self.acquired:
            deadline = time.monotonic() + self.timeout_seconds
            while True:
                try:
                    self.path.unlink(missing_ok=True)
                    break
                except PermissionError:
                    # A dead-owner inspector may briefly hold an exclusive read handle.
                    if self.owner_alive is None or time.monotonic() >= deadline:
                        raise
                    time.sleep(0.025)
            self.acquired = False


class DispatcherStore:
    def __init__(self, root: Path = DEFAULT_ROOT):
        self.root = root.resolve()
        self.binding_path = self.root / "binding.json"
        self.state_path = self.root / "state.json"
        self.event_ledger_path = self.root / "event_ledger.jsonl"
        self.pending_dir = self.root / "pending_packets"
        self.execution_registry_path = self.root / "execution_registry.jsonl"
        self.outbox_path = self.root / "outbox.jsonl"
        self.delivery_log_path = self.root / "delivery_log.jsonl"
        self.audit_path = self.root / "audit.jsonl"
        self.lock_path = self.root / "dispatcher.lock"

    def bootstrap(
        self,
        dispatcher_thread_id: str,
        *,
        dispatcher_name: str = "Jarvis",
        project: str = "Chief of Staff",
        binding_overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.root.mkdir(parents=True, exist_ok=True)
        self.pending_dir.mkdir(parents=True, exist_ok=True)
        for path in (
            self.event_ledger_path,
            self.execution_registry_path,
            self.outbox_path,
            self.delivery_log_path,
            self.audit_path,
        ):
            path.touch(exist_ok=True)
        binding = {
            "version": 1,
            "dispatcher_name": dispatcher_name,
            "dispatcher_thread_id": dispatcher_thread_id,
            "project": project,
            "thread_role": "fixed_coo_dispatcher",
            "source_requirement_thread_id": "019f74a1-2bb2-7fd3-aa58-294c07591723",
            "expected_bot_profile": None,
            "expected_app_id": None,
            "expected_bot_open_id": None,
            "expected_tenant_key": None,
            "cooper_open_id": None,
            "allowed_chat_ids": [],
            "identity_status": "unverified",
            "live_ingress_enabled": False,
            "live_send_enabled": False,
            "live_dispatch_enabled": False,
            "updated_at": utc_now(),
        }
        if binding_overrides:
            binding.update(binding_overrides)
        state = {
            "version": 1,
            "dispatcher_name": dispatcher_name,
            "dispatcher_thread_id": dispatcher_thread_id,
            "status": "IDLE",
            "current_event_id": None,
            "current_confirmation_id": None,
            "confirmation_status": "none",
            "current_execution_thread_id": None,
            "business_status": "not_started",
            "delivery_status": "not_attempted",
            "last_delivery_error": None,
            "live_service_started": False,
            "last_error": None,
            "updated_at": utc_now(),
        }
        with ProcessLock(self.lock_path):
            if not self.binding_path.exists():
                atomic_write_json(self.binding_path, binding)
            if not self.state_path.exists():
                atomic_write_json(self.state_path, state)
            self._audit(
                "bootstrap",
                {"dispatcher_thread_id": dispatcher_thread_id, "new_binding": True},
            )
        return self.status()

    def status(self) -> dict[str, Any]:
        return {"binding": read_json(self.binding_path), "state": read_json(self.state_path)}

    def _state(self) -> dict[str, Any]:
        return read_json(self.state_path)

    def _binding(self) -> dict[str, Any]:
        return read_json(self.binding_path)

    def _save_state(self, state: dict[str, Any]) -> None:
        if state.get("status") not in STATES:
            raise DispatcherError(f"invalid dispatcher status: {state.get('status')}")
        state["updated_at"] = utc_now()
        atomic_write_json(self.state_path, state)

    def _audit(self, action: str, detail: dict[str, Any]) -> None:
        append_jsonl(
            self.audit_path,
            {"recorded_at": utc_now(), "action": action, "detail": detail},
        )

    def ingest_event(self, event: dict[str, Any]) -> dict[str, Any]:
        event_key = str(event.get("event_id") or event.get("message_id") or "").strip()
        if not event_key:
            raise DispatcherError("event_id or message_id is required")
        message_id = str(event.get("message_id") or "").strip()
        content = str(event.get("content") or "")
        with ProcessLock(self.lock_path):
            for record in iter_jsonl(self.event_ledger_path):
                if record.get("event_key") == event_key:
                    return {"accepted": False, "duplicate": True, "event_key": event_key}
            record = {
                "record_type": "event_ingested",
                "recorded_at": utc_now(),
                "event_key": event_key,
                "event_id": str(event.get("event_id") or ""),
                "message_id": message_id,
                "chat_id": str(event.get("chat_id") or ""),
                "conversation_id": str(
                    event.get("conversation_id") or event.get("chat_id") or ""
                ),
                "sender_open_id": str(event.get("sender_open_id") or ""),
                "sender_id": str(
                    event.get("sender_id") or event.get("sender_open_id") or ""
                ),
                "source_channel": str(event.get("source_channel") or "feishu"),
                "source_event_id": str(event.get("source_event_id") or ""),
                "source_message_id": str(event.get("source_message_id") or ""),
                "correlation_id": str(event.get("correlation_id") or ""),
                "reply_channel": str(event.get("reply_channel") or ""),
                "reply_conversation_id": str(
                    event.get("reply_conversation_id") or ""
                ),
                "event_type": str(event.get("event_type") or "message"),
                "message_type": str(event.get("message_type") or ""),
                "chat_type": str(event.get("chat_type") or ""),
                "reply_to": str(event.get("reply_to") or ""),
                "root_id": str(event.get("root_id") or ""),
                "thread_id": str(event.get("thread_id") or ""),
                "timestamp": str(event.get("timestamp") or ""),
                "content": content,
                "attachments": list(event.get("attachments") or []),
                "context_events": list(event.get("context_events") or []),
                "not_before_epoch": float(event.get("not_before_epoch") or 0),
                "attachment_status": str(event.get("attachment_status") or ""),
                "processing_status": "queued",
            }
            append_jsonl(self.event_ledger_path, record)
            state = self._state()
            if state["status"] in {"IDLE", "COMPLETE", "CANCELLED"}:
                state["status"] = "DISCUSSING"
            state["current_event_id"] = event_key
            self._save_state(state)
            self._audit("event_ingested", {"event_key": event_key, "message_id": message_id})
            return {"accepted": True, "duplicate": False, "event_key": event_key}

    def record_event_status(
        self,
        event_key: str,
        processing_status: str,
        *,
        error: str | None = None,
        response_outbox_id: str | None = None,
    ) -> dict[str, Any]:
        if processing_status not in EVENT_PROCESSING_STATUSES:
            raise DispatcherError(f"invalid event processing status: {processing_status}")
        with ProcessLock(self.lock_path):
            if not any(
                record.get("event_key") == event_key
                and record.get("record_type", "event_ingested") == "event_ingested"
                for record in iter_jsonl(self.event_ledger_path)
            ):
                raise DispatcherError(f"unknown event_key: {event_key}")
            record = {
                "record_type": "event_status",
                "recorded_at": utc_now(),
                "event_key": event_key,
                "processing_status": processing_status,
                "error": error,
                "response_outbox_id": response_outbox_id,
            }
            append_jsonl(self.event_ledger_path, record)
            state = self._state()
            if processing_status == "processing":
                state["status"] = "DISCUSSING"
            elif processing_status == "failed":
                state["last_error"] = error or f"event {event_key} processing failed"
            self._save_state(state)
            self._audit("event_status_recorded", record)
            return record

    def record_event_enrichment(
        self,
        event_key: str,
        enrichment: dict[str, Any],
    ) -> dict[str, Any]:
        unknown = sorted(set(enrichment) - EVENT_ENRICHMENT_FIELDS)
        if unknown:
            raise DispatcherError(
                "unsupported event enrichment fields: " + ", ".join(unknown)
            )
        with ProcessLock(self.lock_path):
            if not any(
                record.get("event_key") == event_key
                and record.get("record_type", "event_ingested") == "event_ingested"
                for record in iter_jsonl(self.event_ledger_path)
            ):
                raise DispatcherError(f"unknown event_key: {event_key}")
            record = {
                "record_type": "event_enriched",
                "recorded_at": utc_now(),
                "event_key": event_key,
                "enrichment": enrichment,
            }
            append_jsonl(self.event_ledger_path, record)
            self._audit(
                "event_enriched",
                {
                    "event_key": event_key,
                    "fields": sorted(enrichment),
                },
            )
            return record

    def _materialized_events(self) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
        ingested: dict[str, dict[str, Any]] = {}
        statuses: dict[str, str] = {}
        for record in iter_jsonl(self.event_ledger_path):
            event_key = str(record.get("event_key") or "")
            if not event_key:
                continue
            record_type = record.get("record_type", "event_ingested")
            if record_type == "event_ingested":
                ingested.setdefault(event_key, dict(record))
                statuses.setdefault(
                    event_key,
                    str(record.get("processing_status") or "queued"),
                )
            elif record_type == "event_enriched" and event_key in ingested:
                enrichment = record.get("enrichment")
                if isinstance(enrichment, dict):
                    ingested[event_key].update(enrichment)
            elif record_type == "event_status":
                statuses[event_key] = str(record.get("processing_status") or "")
        return ingested, statuses

    def pending_events(self) -> list[dict[str, Any]]:
        ingested, statuses = self._materialized_events()
        return [
            record
            for event_key, record in ingested.items()
            if statuses.get(event_key) in {"queued", "processing", "failed"}
        ]

    def event_by_message_id(self, message_id: str) -> dict[str, Any] | None:
        target = str(message_id or "").strip()
        if not target:
            return None
        ingested, statuses = self._materialized_events()
        for event_key, record in reversed(list(ingested.items())):
            if str(record.get("message_id") or "") != target:
                continue
            result = dict(record)
            result["processing_status"] = statuses.get(event_key)
            return result
        return None

    def event_processing_status(self, event_key: str) -> str | None:
        status = None
        for record in iter_jsonl(self.event_ledger_path):
            if str(record.get("event_key") or "") != event_key:
                continue
            if record.get("record_type", "event_ingested") == "event_ingested":
                status = str(record.get("processing_status") or "queued")
            elif record.get("record_type") == "event_status":
                status = str(record.get("processing_status") or "")
        return status

    def create_packet(self, packet: dict[str, Any]) -> dict[str, Any]:
        missing = sorted(field for field in PACKET_REQUIRED_FIELDS if field not in packet)
        if missing:
            raise DispatcherError("packet missing required fields: " + ", ".join(missing))
        confirmation_id = str(packet.get("confirmation_id") or "").strip()
        if not confirmation_id:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            confirmation_id = f"confirm-{stamp}-{uuid.uuid4().hex[:8]}"
        expires_at = str(packet.get("expires_at") or "").strip()
        if not expires_at:
            expires_at = (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat()
        with ProcessLock(self.lock_path):
            state = self._state()
            previous_id = state.get("current_confirmation_id")
            if previous_id:
                previous_path = self.pending_dir / f"{previous_id}.json"
                if previous_path.exists():
                    previous = read_json(previous_path)
                    if previous.get("status") == "pending":
                        previous["status"] = "superseded"
                        previous["superseded_at"] = utc_now()
                        atomic_write_json(previous_path, previous)
            value = dict(packet)
            value.update(
                {
                    "confirmation_id": confirmation_id,
                    "expires_at": expires_at,
                    "status": "pending",
                    "created_at": utc_now(),
                }
            )
            atomic_write_json(self.pending_dir / f"{confirmation_id}.json", value)
            state["status"] = "AWAITING_CONFIRMATION"
            state["current_confirmation_id"] = confirmation_id
            state["confirmation_status"] = "pending"
            self._save_state(state)
            self._audit(
                "packet_created",
                {"confirmation_id": confirmation_id, "previous_id": previous_id},
            )
            return value

    def decide_packet(
        self,
        confirmation_id: str,
        decision: str,
        *,
        actor: str = "Cooper",
        modification: str | None = None,
    ) -> dict[str, Any]:
        decision = decision.lower()
        if decision not in {"confirm", "modify", "cancel"}:
            raise DispatcherError("decision must be confirm, modify, or cancel")
        with ProcessLock(self.lock_path):
            state = self._state()
            if confirmation_id != state.get("current_confirmation_id"):
                raise DispatcherError("confirmation id is not the active packet")
            path = self.pending_dir / f"{confirmation_id}.json"
            packet = read_json(path)
            if packet.get("status") != "pending":
                raise DispatcherError(f"packet is not pending: {packet.get('status')}")
            try:
                expiry = datetime.fromisoformat(str(packet["expires_at"]).replace("Z", "+00:00"))
            except (KeyError, ValueError) as exc:
                raise DispatcherError("packet expires_at is invalid") from exc
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=timezone.utc)
            if expiry < datetime.now(timezone.utc):
                packet["status"] = "expired"
                atomic_write_json(path, packet)
                state["status"] = "BLOCKED"
                state["confirmation_status"] = "expired"
                self._save_state(state)
                raise DispatcherError("packet confirmation has expired")
            packet["decided_at"] = utc_now()
            packet["decided_by"] = actor
            if decision == "confirm":
                packet["status"] = "confirmed"
                state["status"] = "AWAITING_CONFIRMATION"
                state["confirmation_status"] = "confirmed"
            elif decision == "modify":
                packet["status"] = "modification_requested"
                packet["modification"] = modification or ""
                state["status"] = "DISCUSSING"
                state["confirmation_status"] = "modification_requested"
            else:
                packet["status"] = "cancelled"
                state["status"] = "CANCELLED"
                state["confirmation_status"] = "cancelled"
            atomic_write_json(path, packet)
            self._save_state(state)
            self._audit(
                "packet_decided",
                {
                    "confirmation_id": confirmation_id,
                    "decision": decision,
                    "actor": actor,
                },
            )
            return packet

    def mark_dispatched(
        self,
        confirmation_id: str,
        execution_thread_id: str,
        *,
        execution_mode: str,
    ) -> dict[str, Any]:
        with ProcessLock(self.lock_path):
            binding = self._binding()
            if not binding.get("live_dispatch_enabled"):
                raise DispatcherError("live dispatch is disabled in binding.json")
            state = self._state()
            if confirmation_id != state.get("current_confirmation_id"):
                raise DispatcherError("confirmation id is not active")
            packet = read_json(self.pending_dir / f"{confirmation_id}.json")
            if packet.get("status") != "confirmed":
                raise DispatcherError("packet must be confirmed before dispatch")
            record = {
                "recorded_at": utc_now(),
                "confirmation_id": confirmation_id,
                "execution_thread_id": execution_thread_id,
                "execution_mode": execution_mode,
                "status": "dispatched",
            }
            append_jsonl(self.execution_registry_path, record)
            packet["status"] = "dispatched"
            packet["execution_thread_id"] = execution_thread_id
            packet["dispatched_at"] = utc_now()
            atomic_write_json(self.pending_dir / f"{confirmation_id}.json", packet)
            state["status"] = "DISPATCHED"
            state["current_execution_thread_id"] = execution_thread_id
            self._save_state(state)
            self._audit("packet_dispatched", record)
            return record

    def register_native_task(self, task: dict[str, Any]) -> dict[str, Any]:
        required = {
            "request_id",
            "thread_id",
            "project",
            "project_path",
            "source_channel",
            "source_thread_id",
            "origin",
        }
        missing = sorted(field for field in required if not str(task.get(field) or "").strip())
        if missing:
            raise DispatcherError(
                "native task registration missing fields: " + ", ".join(missing)
            )
        with ProcessLock(self.lock_path):
            for existing in iter_jsonl(self.execution_registry_path):
                if existing.get("record_type") != "native_task_created":
                    continue
                if existing.get("request_id") == task["request_id"]:
                    return {"registered": False, "duplicate": True, "record": existing}
                correlation_id = str(task.get("correlation_id") or "")
                if correlation_id and existing.get("correlation_id") == correlation_id:
                    return {"registered": False, "duplicate": True, "record": existing}
            record = dict(task)
            record.update(
                {
                    "record_type": "native_task_created",
                    "recorded_at": utc_now(),
                    "status": str(task.get("status") or "created"),
                    "controller_thread_id": self._binding().get("dispatcher_thread_id"),
                }
            )
            append_jsonl(self.execution_registry_path, record)
            state = self._state()
            state["last_native_task_thread_id"] = task["thread_id"]
            state["last_native_task_request_id"] = task["request_id"]
            self._save_state(state)
            self._audit(
                "native_task_registered",
                {
                    "request_id": task["request_id"],
                    "thread_id": task["thread_id"],
                    "project": task["project"],
                },
            )
            return {"registered": True, "duplicate": False, "record": record}

    def verify_bot_identity(self, observed: dict[str, Any]) -> dict[str, Any]:
        field_map = {
            "expected_bot_profile": "bot_profile",
            "expected_app_id": "app_id",
            "expected_bot_open_id": "bot_open_id",
            "expected_tenant_key": "tenant_key",
        }
        with ProcessLock(self.lock_path):
            binding = self._binding()
            configured = {
                expected: binding.get(expected)
                for expected in field_map
                if binding.get(expected)
            }
            mismatches = []
            if not configured:
                mismatches.append("no expected bot identity fields are configured")
            for expected_field, expected_value in configured.items():
                observed_field = field_map[expected_field]
                if observed.get(observed_field) != expected_value:
                    mismatches.append(
                        f"{observed_field}: expected {expected_value!r}, "
                        f"observed {observed.get(observed_field)!r}"
                    )
            binding["identity_status"] = "verified" if not mismatches else "mismatch"
            binding["identity_checked_at"] = utc_now()
            atomic_write_json(self.binding_path, binding)
            state = self._state()
            if mismatches:
                state["status"] = "BLOCKED"
                state["last_error"] = "bot identity mismatch: " + "; ".join(mismatches)
            elif (
                state.get("status") == "BLOCKED"
                and str(state.get("last_error") or "").startswith("bot identity mismatch:")
            ):
                state["status"] = "IDLE"
                state["last_error"] = None
            self._save_state(state)
            self._audit("bot_identity_checked", {"mismatches": mismatches})
            return {"verified": not mismatches, "mismatches": mismatches}

    def enqueue_outbox(self, request: dict[str, Any]) -> dict[str, Any]:
        required = {"task_id", "source_thread_id", "status", "recipient", "content"}
        missing = sorted(field for field in required if not str(request.get(field) or "").strip())
        if missing:
            raise DispatcherError("outbox request missing fields: " + ", ".join(missing))
        attachments = request.get("attachments") or []
        if not isinstance(attachments, list):
            raise DispatcherError("outbox attachments must be a list")
        for attachment in attachments:
            if not isinstance(attachment, dict):
                raise DispatcherError("outbox attachment must be an object")
            if not str(
                attachment.get("relative_path") or attachment.get("local_path") or ""
            ).strip():
                raise DispatcherError("outbox attachment is missing a path")
        stable = json.dumps(
            {
                "task_id": request["task_id"],
                "source_thread_id": request["source_thread_id"],
                "status": request["status"],
                "confirmation_id": request.get("confirmation_id"),
                "recipient": request["recipient"],
                "content": request["content"],
                "message_kind": request.get("message_kind"),
                "interaction_mode": request.get("interaction_mode"),
                "decision_context": request.get("decision_context"),
                "attachments": attachments,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        idempotency_key = "jarvis-" + hashlib.sha256(stable).hexdigest()[:32]
        with ProcessLock(self.lock_path):
            records = list(iter_jsonl(self.outbox_path))
            repeated = 0
            for record in reversed(records):
                if (
                    str(record.get("recipient") or "") == str(request["recipient"])
                    and str(record.get("content") or "") == str(request["content"])
                ):
                    repeated += 1
                    continue
                break
            if repeated >= 5:
                return {
                    "queued": False,
                    "duplicate": True,
                    "reason": "consecutive_identical_message_limit",
                    "record": records[-1] if records else None,
                }
            idempotency_key = f"{idempotency_key}:repeat:{repeated + 1}"
            record = dict(request)
            record.update(
                {
                    "outbox_id": f"outbox-{uuid.uuid4().hex}",
                    "idempotency_key": idempotency_key,
                    "delivery_status": "queued",
                    "created_at": utc_now(),
                }
            )
            append_jsonl(self.outbox_path, record)
            state = self._state()
            state["delivery_status"] = "queued"
            self._save_state(state)
            self._audit(
                "outbox_queued",
                {
                    "outbox_id": record["outbox_id"],
                    "idempotency_key": idempotency_key,
                },
            )
            return {"queued": True, "duplicate": False, "record": record}

    def pending_outbox(
        self,
        *,
        retry_after_seconds: float = 0,
        max_attempts: int | None = None,
    ) -> list[dict[str, Any]]:
        attempts: dict[str, list[dict[str, Any]]] = {}
        for record in iter_jsonl(self.delivery_log_path):
            outbox_id = str(record.get("outbox_id") or "")
            if outbox_id:
                attempts.setdefault(outbox_id, []).append(record)
        pending = []
        now = datetime.now(timezone.utc)
        for record in iter_jsonl(self.outbox_path):
            outbox_id = str(record.get("outbox_id") or "")
            history = attempts.get(outbox_id, [])
            latest = history[-1] if history else None
            if latest and latest.get("delivery_status") in {
                "delivered",
                "expired",
                "unknown",
            }:
                continue
            if latest and latest.get("delivery_status") == "retry_pending":
                retry_not_before = datetime.fromisoformat(
                    str(latest.get("retry_not_before") or utc_now()).replace("Z", "+00:00")
                )
                if retry_not_before.tzinfo is None:
                    retry_not_before = retry_not_before.replace(tzinfo=timezone.utc)
                if now < retry_not_before:
                    continue
            elif max_attempts is not None and len(history) >= max_attempts:
                continue
            elif history and retry_after_seconds > 0:
                try:
                    last_at = datetime.fromisoformat(
                        str(history[-1]["recorded_at"]).replace("Z", "+00:00")
                    )
                    if last_at.tzinfo is None:
                        last_at = last_at.replace(tzinfo=timezone.utc)
                    if (now - last_at).total_seconds() < retry_after_seconds:
                        continue
                except (KeyError, ValueError):
                    pass
            pending.append(record)
        return pending

    def record_delivery(
        self,
        outbox_id: str,
        delivery_status: str,
        *,
        message_id: str | None = None,
        message_ids: list[str] | None = None,
        error: str | None = None,
        retry_not_before: str | None = None,
    ) -> dict[str, Any]:
        if delivery_status not in {
            "delivered",
            "failed",
            "retry_pending",
            "unknown",
            "expired",
        }:
            raise DispatcherError(
                "delivery status must be delivered, failed, retry_pending, unknown, or expired"
            )
        normalized_message_ids = [
            str(value).strip()
            for value in (message_ids or [])
            if str(value).strip()
        ]
        if message_id and message_id not in normalized_message_ids:
            normalized_message_ids.insert(0, message_id)
        if delivery_status == "delivered" and not normalized_message_ids:
            raise DispatcherError("delivered status requires message_id")
        with ProcessLock(self.lock_path):
            record = {
                "recorded_at": utc_now(),
                "outbox_id": outbox_id,
                "delivery_status": delivery_status,
                "message_id": normalized_message_ids[0] if normalized_message_ids else None,
                "message_ids": normalized_message_ids,
                "error": error,
                "retry_not_before": retry_not_before,
            }
            append_jsonl(self.delivery_log_path, record)
            state = self._state()
            state["delivery_status"] = delivery_status
            state["last_delivery_error"] = (
                None if delivery_status == "delivered" else error or f"delivery {delivery_status}"
            )
            self._save_state(state)
            self._audit("delivery_recorded", record)
            return record

    def record_business_status(self, business_status: str, *, detail: str | None = None) -> dict[str, Any]:
        if business_status not in BUSINESS_STATUSES:
            raise DispatcherError(f"invalid business status: {business_status}")
        with ProcessLock(self.lock_path):
            state = self._state()
            state["business_status"] = business_status
            if detail:
                state["business_detail"] = detail
            self._save_state(state)
            self._audit(
                "business_status_recorded",
                {"business_status": business_status, "detail": detail},
            )
            return state


def load_object(path: Path) -> dict[str, Any]:
    return read_json(path.resolve())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    sub = parser.add_subparsers(dest="command", required=True)

    bootstrap = sub.add_parser("bootstrap")
    bootstrap.add_argument("--thread-id", required=True)

    sub.add_parser("status")

    ingest = sub.add_parser("ingest")
    ingest.add_argument("--event", type=Path, required=True)

    packet = sub.add_parser("create-packet")
    packet.add_argument("--packet", type=Path, required=True)

    decide = sub.add_parser("decide")
    decide.add_argument("--confirmation-id", required=True)
    decide.add_argument("--decision", choices=("confirm", "modify", "cancel"), required=True)
    decide.add_argument("--modification")

    dispatch = sub.add_parser("mark-dispatched")
    dispatch.add_argument("--confirmation-id", required=True)
    dispatch.add_argument("--thread-id", required=True)
    dispatch.add_argument("--execution-mode", required=True)

    identity = sub.add_parser("verify-identity")
    identity.add_argument("--observed", type=Path, required=True)

    outbox = sub.add_parser("enqueue-outbox")
    outbox.add_argument("--request", type=Path, required=True)

    delivery = sub.add_parser("record-delivery")
    delivery.add_argument("--outbox-id", required=True)
    delivery.add_argument(
        "--delivery-status",
        choices=("delivered", "failed", "retry_pending", "unknown", "expired"),
        required=True,
    )
    delivery.add_argument("--message-id")
    delivery.add_argument("--error")

    business = sub.add_parser("record-business")
    business.add_argument("--business-status", choices=sorted(BUSINESS_STATUSES), required=True)
    business.add_argument("--detail")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    store = DispatcherStore(args.root)
    try:
        if args.command == "bootstrap":
            result = store.bootstrap(args.thread_id)
        elif args.command == "status":
            result = store.status()
        elif args.command == "ingest":
            result = store.ingest_event(load_object(args.event))
        elif args.command == "create-packet":
            result = store.create_packet(load_object(args.packet))
        elif args.command == "decide":
            result = store.decide_packet(
                args.confirmation_id,
                args.decision,
                modification=args.modification,
            )
        elif args.command == "mark-dispatched":
            result = store.mark_dispatched(
                args.confirmation_id,
                args.thread_id,
                execution_mode=args.execution_mode,
            )
        elif args.command == "verify-identity":
            result = store.verify_bot_identity(load_object(args.observed))
        elif args.command == "enqueue-outbox":
            result = store.enqueue_outbox(load_object(args.request))
        elif args.command == "record-delivery":
            result = store.record_delivery(
                args.outbox_id,
                args.delivery_status,
                message_id=args.message_id,
                error=args.error,
            )
        elif args.command == "record-business":
            result = store.record_business_status(args.business_status, detail=args.detail)
        else:
            raise DispatcherError(f"unsupported command: {args.command}")
    except DispatcherError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps({"ok": True, "result": result}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
