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
    validate_saved_output,
    lane_batch_ids,
    lane_batch_size,
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
        host_initializer: Callable[..., dict[str, str]] | None = None,
        pid_alive: Callable[[int], bool] | None = None,
        pid_started_at: Callable[[int], datetime | str | None] | None = None,
    ) -> None:
        self._config_path = Path(config_path)
        self._state_dir = Path(state_dir)
        self._config_loader = config_loader or _runtime_symbols()["NativeTaskLauncherConfig"]
        self._host_initializer = host_initializer or _initialize_user_host
        self._pid_alive = pid_alive or _pid_is_alive
        self._pid_started_at = pid_started_at or _pid_started_at

    def provision(self, request: TaskProvisionRequest) -> TaskProvisionReceipt:
        try:
            config = self._config_loader(self._config_path)
            project, project_path = config.resolve_project(request.project)
            hold_id = request.hold_id or f"hold-{request.request_id}"
            paths = self._paths(hold_id)
            _write_json(paths["request"], {
                "mode": "create",
                "request_id": request.request_id,
                "source_ref": request.source_ref,
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

    def hold_host_health(self, *, required_workers: int = 1) -> dict[str, str]:
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
        pid = _positive_int(health.get("pid"))
        if pid is None or not self._pid_alive(pid):
            return {"status": "host_not_ready", "reason": "HoldHost PID is not live"}
        host_started_at = _parse_observed_at(health.get("host_started_at"))
        if host_started_at is None:
            return {"status": "host_not_ready", "reason": "HoldHost PID health identity is missing"}
        process_started_at = _parse_observed_at(self._pid_started_at(pid))
        if process_started_at is None or process_started_at - host_started_at > timedelta(seconds=5):
            return {"status": "host_not_ready", "reason": "HoldHost PID does not match health identity"}
        capacity = _positive_int(health.get("worker_capacity")) or 1
        if capacity < max(required_workers, 1):
            return {"status": "host_not_ready", "reason": "HoldHost worker capacity is insufficient"}
        profile = str(getattr(config, "profile", "") or "").strip()
        if not profile or str(health.get("profile") or "").strip() != profile:
            return {"status": "host_not_ready", "reason": "HoldHost profile does not match"}
        codex_home = str(getattr(config, "expected_codex_home", "") or "").strip()
        if not codex_home or not _same_path(health.get("codex_home"), codex_home):
            return {"status": "host_not_ready", "reason": "HoldHost CODEX_HOME does not match"}
        if not _same_path(health.get("state_dir"), self._state_dir):
            return {"status": "host_not_ready", "reason": "HoldHost state_dir does not match"}
        return {"status": "ready"}

    def ensure_hold_host_ready(self, *, required_workers: int) -> dict[str, str]:
        """Reuse a live matching Host or start one before a Loop provisions work."""
        required_workers = max(int(required_workers), 1)
        health = self.hold_host_health(required_workers=required_workers)
        if health.get("status") == "ready":
            return {"status": "ready", "phase": "already_running"}
        try:
            started = self._host_initializer(
                state_dir=self._state_dir,
                launcher_config=self._config_path,
                workers=max(1, required_workers),
                poll_seconds=2.0,
                wait_seconds=15.0,
            )
        except Exception as exc:
            return {"status": "host_not_ready", "reason": f"HoldHost self-healing failed: {exc}"}
        health = self.hold_host_health(required_workers=required_workers)
        if health.get("status") != "ready":
            return health
        return {"status": "ready", "phase": str(started.get("phase") or "started")}

    def resume_with_monitor(self, request: TaskMonitorResumeRequest) -> TaskProvisionReceipt:
        try:
            input_binding = dict(request.input_binding or {})
            config = self._config_loader(self._config_path)
            hold_id = request.hold_id or request.monitor_id or f"hold-{request.request_id}"
            if request.hold_id or request.monitor_id:
                previous = self.hold_status(hold_id)
                if str(previous.get("status") or "") in {"accepted", "holding", "running"}:
                    raise RuntimeError("hold already owns an active turn")
                if previous.get("thread_id") != request.task_id or previous.get("hold_released") is not True:
                    raise RuntimeError("existing Hold identity or terminal release is unverified")
                existing_total = _positive_int(previous.get("total_turn_count")) or _positive_int(previous.get("turn_count")) or 0
                previous_paths = self._existing_paths(hold_id)
                saved_request = _read_json_file(previous_paths["request"]) if previous_paths else None
                saved_binding = (saved_request or {}).get("input_binding") or {}
                if "result_verification" in saved_binding or "result_verification" in (request.input_binding or {}):
                    if (not saved_request or "result_verification" not in saved_binding
                            or previous.get("output_verification", {}).get("status") != "verified"
                            or verify_candidate_output(saved_request, existing_total).get("status") != "verified"):
                        raise RuntimeError("bound Hold current candidate is unverified; use a new request including the unfinished item")
                if "output_schema" in saved_binding.get("result_verification", {}) or "lane_item_count" in saved_binding:
                    if input_binding and input_binding != saved_binding:
                        raise RuntimeError("existing Hold batch/output_schema binding cannot change on resume; use a new request")
                    input_binding = dict(saved_binding)
                max_turns = _positive_int(previous.get("max_turns")) or request.max_turns
                initial_turn_count = 1
                initial_total_turn_count = existing_total + 1
                if "lane_item_count" in input_binding:
                    # Total count indexes the finite lane; Monitor budgets this session only.
                    lane_batch_ids(input_binding, initial_total_turn_count)
                    size = lane_batch_size(input_binding)
                    max_turns = (len(input_binding["candidate_ids"]) + size - 1) // size - existing_total
            else:
                max_turns = request.max_turns
                initial_turn_count = 1
                initial_total_turn_count = 1
            paths = self._paths(hold_id)
            _archive_terminal_result(paths["result"])
            _write_json(paths["request"], {
                "mode": "resume",
                "request_id": request.request_id,
                "source_ref": request.source_ref,
                "hold_id": hold_id,
                "thread_id": request.task_id,
                "prompt": request.prompt,
                "model": request.model,
                "reasoning_effort": request.reasoning_effort,
                "max_turns": max_turns,
                "auto_continue": request.auto_continue,
                "continue_prompt": request.continue_prompt,
                "notifications": dict(request.notifications or {}),
                "input_binding": input_binding,
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
                        binding = (_read_json_file(root / "request.json") or {}).get("input_binding") or {}
                        if "lane_item_count" in binding and "result_verification" not in binding:
                            value.setdefault("output_verification", {"status": "legacy_unverified"})
                        terminal = str(value.get("status") or "") in {
                            "completed", "failed", "interrupted", "cancelled", "canceled", "turn_limit_reached", "blocked",
                        }
                        return {**value, "hold_released": (
                            terminal and path.name == "result.json" and value.get("terminal_confirmed", True) is True
                            and not (root / ".user-host-claim").exists()
                        )}
        raise RuntimeError("hold state was not found")

    def monitor_status(self, monitor_id: str) -> dict[str, Any]:
        """Compatibility alias for lifecycle callers using the old field name."""
        return self.hold_status(monitor_id)

    def thread_execution_evidence(self, thread_id: str, turn_id: str) -> dict[str, Any] | None:
        """Read existing owner receipts; a claim/PID never proves a running turn."""
        matches = []
        for name in ("task-holds", "task-monitors"):
            parent = self._state_dir / name
            if not parent.is_dir():
                continue
            for root in parent.iterdir():
                if not root.is_dir():
                    continue
                request = _read_json_file(root / "request.json") or {}
                ack = _read_json_file(root / "ack.json") or {}
                result = _read_json_file(root / "result.json") or {}
                if thread_id not in {request.get("thread_id"), ack.get("thread_id"), result.get("thread_id")}:
                    continue
                historical_identity = {"request_id": request.get("request_id"), "thread_id": thread_id,
                                       "turn_id": ack.get("turn_id")}
                if request.get("hold_id"):
                    historical_identity["hold_id"] = request["hold_id"]
                if (turn_id and ack.get("turn_id") and ack["turn_id"] != turn_id
                        and request.get("request_id") and request.get("thread_id") in (None, "", thread_id)
                        and all(ack.get(k) == v and result.get(k) == v for k, v in historical_identity.items())
                        and result.get("terminal_confirmed") is True
                        and result.get("status") in {"completed", "failed", "interrupted", "cancelled",
                                                     "canceled", "blocked", "turn_limit_reached"}
                        and not (root / ".user-host-claim").exists()
                        and _read_json_file(root / "request.json") == request
                        and _read_json_file(root / "ack.json") == ack):
                    continue  # A released older Hold does not own a later ordinary native turn.
                matches.append((root, request, ack, result))
        if not matches:
            return None
        unknown = {"status": "unknown", "source": "hold_evidence", "thread_id": thread_id,
                   "turn_id": turn_id, "reason": "no unambiguous current owner terminal evidence"}
        if any(request.get("thread_id") and request["thread_id"] != thread_id for _, request, _, _ in matches):
            return {**unknown, "reason": "current Hold request thread identity mismatch"}
        exact = [row for row in matches if row[2].get("turn_id") == turn_id]
        if len(exact) != 1 or not turn_id:
            return unknown
        root, request, ack, result = exact[0]
        if any(other != root and not saved.get("terminal_confirmed")
               for other, _, _, saved in matches):
            return unknown
        owner = _read_json_file(root / ".user-host-claim" / "owner.json") or {}
        pid = owner.get("pid")
        alive = self._pid_alive(pid) if isinstance(pid, int) and pid > 0 else None
        evidence = {**unknown, "hold_id": request.get("hold_id") or request.get("monitor_id"),
                    "owner_pid": pid, "owner_alive": alive, "owner_status": ack.get("status")}
        if alive is False:
            evidence["reason"] = "owner process is not alive; current turn outcome is unknown"
        identity = {"request_id": request.get("request_id"), "thread_id": thread_id, "turn_id": turn_id}
        if request.get("hold_id"):
            identity["hold_id"] = request["hold_id"]
        if (not identity["request_id"] or any(ack.get(k) != v or result.get(k) != v for k, v in identity.items())
                or result.get("terminal_confirmed") is not True):
            return evidence
        if (_read_json_file(root / "request.json") != request
                or _read_json_file(root / "ack.json") != ack):
            return {**unknown, "reason": "owner state changed during read"}
        status = str(result.get("status") or "").lower()
        verification = result.get("output_verification") or {}
        if verification.get("status") == "blocked":
            status = "blocked"
        elif status == "turn_limit_reached":
            status = "completed"
        if status not in {"completed", "failed", "cancelled", "canceled", "interrupted", "blocked"}:
            return evidence
        return {**evidence, "status": status, "source": "hold_terminal_result",
                "reason": result.get("reason"), "output_verification": verification,
                "terminal_confirmed": True}

    def request_hold_stop(self, hold_id: str) -> dict[str, Any]:
        """Latch stop intent in the existing request, serialized with turn dispatch."""
        paths = self._existing_paths(hold_id)
        if paths is None:
            raise RuntimeError("hold state was not found")
        with ProcessLock(paths["request"].with_suffix(".lock"), owner_alive=self._pid_alive):
            request = json.loads(paths["request"].read_text(encoding="utf-8"))
            if str(request.get("hold_id") or request.get("monitor_id") or "") != hold_id:
                raise RuntimeError("stop request hold identity mismatch")
            if not request.get("stop_requested"):
                _write_json(paths["request"], {**request, "stop_requested": True})
        return {"status": "stop_requested", "hold_id": hold_id}

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
                discovered_hold_id = _hold_id_from_state(root)
                if discovered_hold_id:
                    hold_ids.add(discovered_hold_id)
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


def verify_candidate_output(request: dict[str, Any], turn_number: int) -> dict[str, Any]:
    """Verify every real ID in this turn before declaring a batch complete."""
    binding = request.get("input_binding") or {}
    if "lane_item_count" not in binding:
        return {"status": "not_required"}
    try:
        ids = lane_batch_ids(binding, turn_number)
        if lane_batch_size(binding) == 1:
            return _verify_candidate_output_item(request, turn_number, ids[0])
        identity = {"candidate_ids": ids, "request_id": request["request_id"], "turn_number": turn_number}
        items = []
        for candidate in ids:
            checked = _verify_candidate_output_item(request, turn_number, candidate)
            items.append(checked)
            if checked.get("status") == "blocked":
                return {"status": "blocked", **identity, "items": items,
                        "reason": f"batch candidate {candidate} unverified: "
                                  f"{checked.get('reason') or checked.get('terminal_status') or checked.get('status')}"}
        return {"status": "review" if any(item["status"] == "review" for item in items) else "verified",
                **identity, "items": items}
    except (ValueError, KeyError, TypeError, IndexError) as exc:
        return {"status": "blocked", "reason": f"candidate_output_unverified: {exc}"}


def _verify_candidate_output_item(request: dict[str, Any], turn_number: int, candidate: int) -> dict[str, Any]:
    """Read the bound business receipt and saved JSON output; never infer from text."""
    binding = request.get("input_binding") or {}
    if "lane_item_count" not in binding:
        return {"status": "not_required"}
    contract = binding.get("result_verification")
    if contract is None:
        return {"status": "legacy_unverified", "reason": "bound request predates saved-output verification"}
    expected = {"candidate_id": candidate, "request_id": request["request_id"], "turn_number": turn_number}
    evidence: dict[str, Any] = {}
    safety_error = False
    try:
        boundary = Path(binding["output_boundary"]).resolve(strict=True)

        def read_output(value: str) -> tuple[Path, dict[str, Any]]:
            nonlocal safety_error
            path = Path(value)
            path = (path if path.is_absolute() else boundary / path).resolve()
            if not path.is_relative_to(boundary):
                safety_error = True
                raise ValueError("output path is outside the allowed boundary")
            raw = path.read_text(encoding="utf-8-sig")
            evidence[str(path)] = raw
            data = json.loads(raw)
            if not isinstance(data, dict):
                raise ValueError("saved output must be a JSON object")
            if (type(data.get("candidate_id")) not in {int, str}
                    or str(data["candidate_id"]) != str(candidate)):
                safety_error = True
                raise ValueError("saved candidate_id does not match current candidate")
            return path, data

        receipt_path, receipt = read_output(contract["receipt_paths"][str(candidate)])
        output_path, output = read_output(receipt["output_path"])
        allowed = contract["terminal_statuses"]
        if not isinstance(allowed, list) or not allowed or any(
            not isinstance(value, str) or not value.strip() or value.casefold() in
            {"accepted", "holding", "running", "pending", "inprogress", "queued"} for value in allowed
        ):
            raise ValueError("terminal_statuses must explicitly name valid business terminal states")
        for label, data in (("receipt", receipt), ("output", output)):
            for key, value in expected.items():
                if type(data.get(key)) is not type(value) or data.get(key) != value:
                    raise ValueError(f"{label} {key} does not match current candidate/run/turn")
            if data.get("status") not in allowed:
                raise ValueError(f"{label} has no allowed terminal status")
        if receipt["status"] != output["status"]:
            raise ValueError("receipt and output terminal status mismatch")
        if "output_schema" in contract:
            validate_saved_output(contract["output_schema"], output)
        if receipt["status"].strip().casefold() in {
            "failed", "blocked", "partial", "partially_completed", "incomplete", "error",
            "cancelled", "canceled", "interrupted",
        }:
            raise ValueError(f"business terminal status: {receipt['status']}")
        return {"status": "verified", **expected, "receipt_path": str(receipt_path),
                "output_path": str(output_path), "terminal_status": receipt["status"]}
    except (OSError, ValueError, KeyError, TypeError, IndexError) as exc:
        return {"status": "blocked" if safety_error else "review", **expected,
                "scheduler_outcome": "blocked" if safety_error else "failed",
                "reason": f"candidate_output_unverified: {exc}", "original_evidence": evidence}


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
    candidate_ids: list[int] | None = None,
    output_verification: dict[str, Any] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=10)
    try:
        connection.execute("PRAGMA busy_timeout=10000")
        connection.execute("BEGIN IMMEDIATE")
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
        columns = {row[1] for row in connection.execute("PRAGMA table_info(turn_history)")}
        for name in ("candidate_ids_json", "output_verification_json"):
            if name not in columns:
                connection.execute(f"ALTER TABLE turn_history ADD COLUMN {name} TEXT")
        connection.execute(
            """INSERT OR IGNORE INTO turn_history(
                hold_id,turn_id,task_id,request_id,thread_id,turn_number,candidate_id,
                status,final_answer,has_final_answer,completed_at,recorded_at,candidate_ids_json,output_verification_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (hold_id, turn_id, task_id, request_id, thread_id, turn_number, candidate_id,
             status, final_answer or None, int(bool(final_answer)), completed_at, observed_now().isoformat(),
             json.dumps(candidate_ids) if candidate_ids is not None else None,
             json.dumps(output_verification, ensure_ascii=False) if output_verification is not None else None),
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
        connection.execute("BEGIN")
        columns = {row[1] for row in connection.execute("PRAGMA table_info(turn_history)")}
        extra = ",candidate_ids_json,output_verification_json" if "candidate_ids_json" in columns else ""
        rows = connection.execute(
            "SELECT task_id,hold_id,request_id,thread_id,turn_id,turn_number,candidate_id,"
            "status,final_answer,has_final_answer,completed_at,recorded_at" + extra + " FROM turn_history"
            + where + " ORDER BY recorded_at, turn_number", parameters,
        ).fetchall()
    finally:
        connection.close()
    result = []
    for row in rows:
        value = dict(row)
        for name in ("candidate_ids", "output_verification"):
            raw = value.pop(name + "_json", None)
            if raw is not None:
                value[name] = json.loads(raw)
        result.append(value)
    return result


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


def _pid_is_alive(pid: int) -> bool:
    from .jarvis_hold_host_service import _pid_is_alive as hold_host_pid_is_alive

    return hold_host_pid_is_alive(pid)


def _pid_started_at(pid: int) -> datetime | None:
    from .jarvis_hold_host_service import _pid_started_at as hold_host_pid_started_at

    return hold_host_pid_started_at(pid)


def _initialize_user_host(**kwargs: Any) -> dict[str, str]:
    from .jarvis_hold_host_service import initialize_user_host

    return initialize_user_host(**kwargs)


def _safe_id(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._") or "request"


def _runtime_symbols() -> dict[str, Any]:
    runtime_dir = Path(__file__).resolve().parents[2] / "jarvis_runtime"
    if str(runtime_dir) not in sys.path:
        sys.path.insert(0, str(runtime_dir))
    from jarvis_native_task_launcher import NativeTaskLauncherConfig

    return {"NativeTaskLauncherConfig": NativeTaskLauncherConfig}
