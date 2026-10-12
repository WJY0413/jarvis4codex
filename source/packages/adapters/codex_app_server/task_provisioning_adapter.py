"""Queue hold work for the normal-user Jarvis host and return its receipt."""

from __future__ import annotations

import json
import hashlib
import os
import re
import sqlite3
import sys
import math
from contextlib import nullcontext
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from jarvis_schema import MAX_CONCURRENCY

from jarvis_runtime.coo_dispatcher_store import ProcessLock
from jarvis_runtime.cancellation import (
    cancel_management, cancellation_path, require_dispatch_open, report_guard,
    dispatch_roots, _hold_guard, _dispatch_guard, _require_roots_open,
)

from jarvis_control.provisioning import (
    TaskMonitorResumeRequest,
    TaskProvisionReceipt,
    TaskProvisionRequest,
    observed_now,
    validate_saved_output,
    lane_batch_ids,
    lane_batch_size,
    final_answer_mode,
)


def physical_memory_sample() -> dict[str, Any]:
    """One live whole-machine physical sample; never use process RSS or quota."""
    if sys.platform == "win32":
        import ctypes
        class MemoryStatus(ctypes.Structure):
            _fields_ = [("length", ctypes.c_uint32), ("load", ctypes.c_uint32)] + [
                (name, ctypes.c_uint64) for name in
                ("total", "available", "total_page", "available_page", "total_virtual", "available_virtual", "extended")]
        value = MemoryStatus()
        value.length = ctypes.sizeof(value)
        query = ctypes.WinDLL("kernel32", use_last_error=True).GlobalMemoryStatusEx
        query.argtypes, query.restype = [ctypes.POINTER(MemoryStatus)], ctypes.c_int
        if not query(ctypes.byref(value)):
            raise OSError(ctypes.get_last_error(), "GlobalMemoryStatusEx failed")
        total, available, source = value.total, value.available, "Windows.GlobalMemoryStatusEx"
    elif sys.platform.startswith("linux"):
        fields = {}
        for line in Path("/proc/meminfo").read_text(encoding="ascii").splitlines():
            parts = line.split()
            if parts and parts[0] in {"MemTotal:", "MemAvailable:"}:
                if len(parts) != 3 or parts[2] != "kB" or parts[0] in fields:
                    raise ValueError("invalid /proc/meminfo physical field")
                fields[parts[0]] = int(parts[1]) * 1024
        if "MemTotal:" not in fields or "MemAvailable:" not in fields:
            raise ValueError("/proc/meminfo requires MemTotal and MemAvailable")
        total, available, source = fields["MemTotal:"], fields["MemAvailable:"], "Linux./proc/meminfo"
    else:
        raise RuntimeError("physical memory sampling is unsupported on this platform")
    if type(total) is not int or type(available) is not int or total <= 0 or not 0 <= available <= total:
        raise ValueError("physical memory total/available is invalid")
    return {"source": source, "total_bytes": total, "available_bytes": available,
            "observed_at": observed_now().isoformat()}


def physical_memory_admission(config: Any) -> dict[str, Any]:
    """Validate policy only for a new admission; errors never disable read/close."""
    threshold = getattr(config, "memory_admission_max_used_percent", 95)
    evidence = {"status": "blocked", "observed_at": observed_now().isoformat()}
    if (type(threshold) not in (int, float) or not 0 < threshold <= 100
            or not math.isfinite(threshold)):
        return {**evidence, "error_code": "memory_policy_invalid",
                "reason": "memory_admission_max_used_percent must be a finite number in (0,100]"}
    evidence["threshold_percent"] = threshold
    try:
        sample = physical_memory_sample()
        total, available = sample["total_bytes"], sample["available_bytes"]
        if type(total) is not int or type(available) is not int or total <= 0 or not 0 <= available <= total:
            raise ValueError("physical memory total/available is invalid")
    except Exception as exc:
        return {**evidence, "error_code": "physical_memory_unknown", "reason": str(exc)}
    evidence.update(sample, used_percent=100 * (total - available) / total)
    if 100 * (total - available) >= threshold * total:
        return {**evidence, "status": "blocked", "error_code": "physical_memory_pressure",
                "reason": "whole-machine physical memory usage is at or above the admission threshold"}
    return {**evidence, "status": "allowed"}


class MemoryAdmissionError(RuntimeError):
    def __init__(self, evidence):
        self.evidence = evidence
        self.code = evidence["error_code"]
        super().__init__("memory admission refused: " + json.dumps(evidence, sort_keys=True))


def require_memory_admission(config: Any) -> dict[str, Any]:
    evidence = physical_memory_admission(config)
    if evidence["status"] != "allowed":
        raise MemoryAdmissionError(evidence)
    return evidence


class CodexAppServerTaskProvisioningAdapter:
    """Queue one task; the MCP process never starts or owns an App Server."""

    name = "codex-app-server-provisioning"
    management_cancellation_version = "jarvis-management-cancellation/v1"
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
        recovery_backend: Any | None = None,
    ) -> None:
        self._config_path = Path(config_path)
        self._state_dir = Path(state_dir)
        self._config_loader = config_loader or _runtime_symbols()["NativeTaskLauncherConfig"]
        self._host_initializer = host_initializer or _initialize_user_host
        self._pid_alive = pid_alive or _pid_is_alive
        self._pid_started_at = pid_started_at or _pid_started_at
        self._recovery_backend = recovery_backend  # Internal trusted dependency; never read from caller/model input.

    def provision(self, request: TaskProvisionRequest) -> TaskProvisionReceipt:
        try:
            config = self._config_loader(self._config_path)
            project, project_path = config.resolve_project(request.project)
            require_memory_admission(config)
            hold_id = request.hold_id or f"hold-{request.request_id}"
            paths = self._paths(hold_id)
            with _hold_guard(paths["request"].parent):
                require_dispatch_open(paths["request"].parent)
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
                status="blocked" if isinstance(exc, MemoryAdmissionError) else "failed",
                observed_at=observed_now(),
                reason=str(exc),
                error_code=getattr(exc, "code", None),
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

    def creation_enabled(self) -> bool:
        return self._config_loader(self._config_path).live_creation_enabled is True

    def contract_snapshot(self) -> dict[str, Any]:
        config = self._config_loader(self._config_path)
        projects = {name: config.resolve_project(name)[1] for name in config.allowed_projects}
        from jarvis_runtime.linux_cloud import child_environment
        if getattr(config, "linux_cloud", False):
            child_environment(config)  # Validate supported isolated routing; do not output values.
        return {"projects": sorted(projects), "projects_exist": all(path.is_dir() for path in projects.values()),
                "configuration_valid": True, "live_creation_enabled": config.live_creation_enabled,
                "codex_home_resolution": {"input": config.expected_codex_home_input,
                    "base_dir": config.expected_codex_home_base_dir, "resolved": config.expected_codex_home},
                "profile_is_label_only": config.profile_is_label_only, "worker_capacity": config.worker_capacity,
                "runtime_constraints": config.runtime_constraints, "runtime_environments": config.runtime_environments,
                "linux_cloud": config.linux_cloud, "model_policy": {"model": config.default_model, "reasoning_effort": config.default_reasoning_effort, "source": "frozen deployment; no TEST caller override"}}

    def preflight_projects(self) -> list[str]:
        """Return configured public project identifiers without writing a task request."""
        config = self._config_loader(self._config_path)
        return sorted(config.allowed_projects)

    def update_runtime(self, *, action: str, request_id: str) -> dict[str, Any]:
        """Check or activate automatic Codex Desktop selection for new Jarvis turns."""
        if action not in {"check", "apply"}:
            raise ValueError("action must be check or apply")
        raw = _read_json_file(self._config_path)
        if raw is None:
            raise RuntimeError("launcher config is unreadable")
        resolver = _runtime_symbols()["resolve_codex_runtime"]
        selected = dict(resolver("desktop_auto"))
        configured = str(raw.get("codex_cli") or "auto")
        health = _read_json_file(self._state_dir / "hold-host.json") or {}
        active_count = int(health.get("active_count") or 0)
        active_hold_ids = [str(value) for value in health.get("active_hold_ids") or []]
        data: dict[str, Any] = {
            "action": action,
            "configured": configured,
            "selected": selected,
            "update_required": configured != "desktop_auto",
            "changed": False,
            "hold_host": {
                "status": health.get("status"),
                "pid": health.get("pid"),
                "active_count": active_count,
                "active_hold_ids": active_hold_ids,
            },
        }
        if action == "check" or configured == "desktop_auto":
            return {"status": "completed", "data": data}
        if active_count or active_hold_ids:
            return {
                "status": "blocked",
                "reason": "active Jarvis holds prevent a launcher policy update",
                "data": data,
            }
        stamp = observed_now().strftime("%Y%m%d-%H%M%S-%f")
        backup_dir = self._state_dir / "runtime-update-backups" / stamp
        backup_dir.mkdir(parents=True, exist_ok=False)
        backup_path = backup_dir / self._config_path.name
        backup_path.write_bytes(self._config_path.read_bytes())
        updated = dict(raw)
        updated["codex_cli"] = "desktop_auto"
        _write_json(self._config_path, updated)
        readback = _read_json_file(self._config_path)
        if readback is None or readback.get("codex_cli") != "desktop_auto":
            self._config_path.write_bytes(backup_path.read_bytes())
            raise RuntimeError("launcher config update failed readback and was rolled back")
        verified = dict(resolver(str(readback["codex_cli"])))
        data.update({
            "configured": "desktop_auto",
            "selected": verified,
            "update_required": False,
            "changed": True,
            "backup_path": str(backup_path),
        })
        return {"status": "completed", "data": data}

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
            return {"status": "host_not_ready", "reason": "HoldHost CODEX_HOME does not match",
                    "expected_codex_home": codex_home, "observed_codex_home": health.get("codex_home"),
                    "resolution": health.get("codex_home_resolution")}
        if not _same_path(health.get("state_dir"), self._state_dir):
            return {"status": "host_not_ready", "reason": "HoldHost state_dir does not match"}
        return {"status": "ready"}

    def ensure_hold_host_ready(self, *, required_workers: int) -> dict[str, str]:
        """Reuse a live matching Host or start one before a Loop provisions work."""
        if type(required_workers) is not int or not 1 <= required_workers <= MAX_CONCURRENCY:
            return {"status": "rejected", "error_code": "invalid_capacity_request"}
        config = self._config_loader(self._config_path)
        admission = physical_memory_admission(config)
        if admission["status"] != "allowed":
            return {"status": "blocked", "error_code": admission["error_code"],
                    "reason": admission["reason"], "memory_admission": admission}
        required_workers = max(required_workers, getattr(config, "worker_capacity", 1), 1)
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
        if started.get("status") in {"unsupported", "rejected", "requires_readback"}:
            return started
        health = self.hold_host_health(required_workers=required_workers)
        if health.get("status") != "ready":
            return health
        return {"status": "ready", "phase": str(started.get("phase") or "started")}

    def resume_with_monitor(self, request: TaskMonitorResumeRequest) -> TaskProvisionReceipt:
        try:
            config = self._config_loader(self._config_path)
            hold_id = request.hold_id or request.monitor_id or f"hold-{request.request_id}"
            hint_root = self._state_dir / "task-holds" / _safe_id(hold_id)
            known_roots = dispatch_roots(hint_root, {"thread_id": request.task_id})
            ancestors = {_state_reference(root, self._state_dir) for root in known_roots if root != hint_root}
            declaration = {
                "mode": "resume", "request_id": request.request_id, "hold_id": hold_id,
                "thread_id": request.task_id, "source_ref": request.source_ref, "prompt": request.prompt,
                "model": request.model, "reasoning_effort": request.reasoning_effort,
                "max_turns": request.max_turns, "auto_continue": request.auto_continue,
                "continue_prompt": request.continue_prompt,
                "notifications": dict(request.notifications or {}), "input_binding": dict(request.input_binding or {}),
            }
            paths = self._existing_paths(hold_id) or self._paths_at(hint_root)
            historical_root = paths["request"].parent / "history" / _safe_id(request.request_id)
            historical = _read_json_file(historical_root / "request.json")
            if historical is not None:
                existing = historical.get("resume_declaration") or _legacy_resume_declaration(historical)
                if (historical.get("request_id") != request.request_id
                        or json.dumps(existing, sort_keys=True) != json.dumps(declaration, sort_keys=True)):
                    raise RuntimeError("request_id is already bound to a conflicting retired operation")
                record = _read_json_file(historical_root / "result.json") or _read_json_file(historical_root / "ack.json") or {}
                return TaskProvisionReceipt(request_id=request.request_id, status="requires_readback", observed_at=observed_now(),
                    hold_id=hold_id, monitor_id=hold_id, thread_id=request.task_id, turn_id=record.get("turn_id"),
                    phase="historical_readback", error_code="generation_retired",
                    reason="Original resume operation was paused/superseded; historical receipt retained; no replacement dispatch")
            snapshot = _resume_snapshot(paths)
            saved = _snapshot_json(snapshot, "request")
            if not (request.hold_id or request.monitor_id) and (not saved or saved.get("request_id") != request.request_id):
                _require_roots_open(known_roots)
                _require_original_hold_selected(known_roots, hint_root, request.task_id)
            transfer = _pending_original_transfer(paths, saved, declaration)
            prepared = None
            evidence = {}
            if transfer is None and (not saved or saved.get("request_id") != request.request_id):
                require_memory_admission(config)
                _require_roots_open(known_roots)
                prepared, evidence = self._prepare_resume(request, hold_id, snapshot)
            binding = {**declaration, "dispatch_ancestors": sorted(ancestors)}
            with _dispatch_guard(paths["request"].parent, binding) as roots:
                resolved = self._existing_paths(hold_id)
                if resolved is not None and resolved["request"].parent != paths["request"].parent:
                    raise RuntimeError("resolved Hold identity changed during admission")
                current_snapshot = _resume_snapshot(paths)
                current = _snapshot_json(current_snapshot, "request")
                if transfer is not None:
                    if current_snapshot != snapshot:
                        raise _OriginalRecoveryBlocked("identity_changed", "Interrupted transfer state changed before retry")
                    _complete_original_transfer(paths, transfer, roots, self._recovery_backend)
                    return self._resume_readback(request.request_id, hold_id)
                if current and current.get("request_id") == request.request_id:
                    existing = current.get("resume_declaration")
                    if existing is None:
                        existing = _legacy_resume_declaration(current)
                    if json.dumps(existing, sort_keys=True) != json.dumps(declaration, sort_keys=True):
                        raise RuntimeError("request_id is already bound to a conflicting resume declaration")
                    return self._resume_readback(request.request_id, hold_id)
                if current_snapshot != snapshot or prepared is None:
                    raise _OriginalRecoveryBlocked("identity_changed", "Hold generation or lifecycle evidence changed during admission")
                if any(_file_identity(Path(path)) != identity for path, identity in evidence.items()):
                    raise RuntimeError("verified output evidence changed during admission")
                require_memory_admission(config)
                _require_roots_open(roots)
                if not (request.hold_id or request.monitor_id):
                    _require_original_hold_selected(roots, paths["request"].parent, request.task_id)
                ancestors.update(_state_reference(root, self._state_dir) for root in roots
                    if root != paths["request"].parent)
                recovery = prepared.pop("_original_recovery", None)
                predecessor = ({"supersedes_request_id": recovery["identity"]["request_id"],
                                "predecessor_hold_released": False} if recovery is not None else {})
                payload = {**declaration, **prepared, **predecessor, "dispatch_ancestors": sorted(ancestors),
                    "resume_declaration": declaration,
                    "turn_history_path": str(self._state_dir / "turn-history.sqlite")}
                accepted = _accepted_ack(request_id=request.request_id, hold_id=hold_id,
                    turn_count=prepared["initial_turn_count"], total_turn_count=prepared["initial_total_turn_count"],
                    max_turns=prepared["max_turns"])
                if recovery is not None:
                    recovery["operation_request_id"] = request.request_id
                    recovery["transfer"] = {"schema": "jarvis-resume-transfer/v1", "declaration": declaration,
                        "target_request": payload, "target_ack": accepted,
                        "target_hashes": {"request": hashlib.sha256(_json_bytes(payload)).hexdigest(),
                                          "ack": hashlib.sha256(_json_bytes(accepted)).hexdigest()},
                        "original": {key: value.hex() if isinstance(value, bytes) else value
                                     for key, value in snapshot.items()},
                        "original_hashes": {key: hashlib.sha256(value).hexdigest()
                                            for key, value in snapshot.items() if isinstance(value, bytes)}}
                    pause = _pause_original_generation(paths, snapshot, recovery, self._recovery_backend)
                    _complete_original_transfer(paths, pause, roots, self._recovery_backend)
                else:
                    _archive_terminal_result(paths["result"])
                    _write_json(paths["request"], payload)
                    _write_json(paths["ack"], accepted)
        except _OriginalRecoveryBlocked as exc:
            return TaskProvisionReceipt(request_id=request.request_id, status="requires_readback", observed_at=observed_now(),
                error_code=exc.code, phase="original_backend_recovery", reason=str(exc), hold_id=hold_id,
                monitor_id=hold_id, thread_id=request.task_id, turn_id=exc.identity.get("turn_id"))
        except Exception as exc:
            return TaskProvisionReceipt(request_id=request.request_id,
                status="blocked" if isinstance(exc, MemoryAdmissionError) else "failed", observed_at=observed_now(),
                reason=str(exc), error_code=getattr(exc, "code", None))
        return TaskProvisionReceipt(request_id=request.request_id, status="accepted", observed_at=observed_now(),
            monitor_id=hold_id, hold_id=hold_id, turn_count=prepared["initial_turn_count"],
            total_turn_count=prepared["initial_total_turn_count"], max_turns=prepared["max_turns"], phase="queued_for_user_host")

    def _prepare_resume(self, request, hold_id, snapshot):
        """Read-only planning and business verification; no admission lock is held."""
        saved = _snapshot_json(snapshot, "request")
        input_binding = dict(request.input_binding or {})
        max_turns, total = request.max_turns, 1
        evidence = {}
        if not (request.hold_id or request.monitor_id):
            if saved or snapshot["ack"] is not None or snapshot["result"] is not None:
                raise RuntimeError("implicit Hold is already bound to another or unverified request_id")
        else:
            previous = self.hold_status(hold_id)
            if previous.get("thread_id") != request.task_id:
                raise RuntimeError("legacy receipt says Hold already owns a turn; exact thread identity is unverified")
            recovery = None
            if (previous.get("hold_released") is not True
                    or (_snapshot_json(snapshot, "result") or {}).get("hold_released") is False):
                recovery = _plan_original_recovery(snapshot, hold_id, request.task_id, self._recovery_backend)
            existing_total = _positive_int(previous.get("total_turn_count")) or _positive_int(previous.get("turn_count")) or 0
            saved_binding = (saved or {}).get("input_binding") or {}
            if "result_verification" in saved_binding or "result_verification" in input_binding:
                if (not saved or "result_verification" not in saved_binding
                        or previous.get("output_verification", {}).get("status") != "verified"):
                    raise RuntimeError("bound Hold current candidate is unverified; use a new request including the unfinished item")
                try:
                    evidence = _output_evidence_identity(saved, existing_total)
                    verified = verify_candidate_output(saved, existing_total).get("status") == "verified"
                    same_output = _output_evidence_identity(saved, existing_total) == evidence
                except (OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
                    raise RuntimeError("bound Hold current candidate is unverified; output evidence unavailable") from exc
                if not verified or not same_output:
                    raise RuntimeError("bound Hold current candidate is unverified or its output evidence changed")
            if "output_schema" in saved_binding.get("result_verification", {}) or "lane_item_count" in saved_binding:
                if input_binding and input_binding != saved_binding:
                    raise RuntimeError("existing Hold batch/output_schema binding cannot change on resume; use a new request")
                input_binding = dict(saved_binding)
            max_turns = _positive_int(previous.get("max_turns")) or request.max_turns
            total = existing_total + 1
            if "lane_item_count" in input_binding:
                lane_batch_ids(input_binding, total)
                size = lane_batch_size(input_binding)
                max_turns = (len(input_binding["candidate_ids"]) + size - 1) // size - existing_total
        return {"input_binding": input_binding, "max_turns": max_turns,
                "initial_turn_count": 1, "initial_total_turn_count": total,
                "_original_recovery": recovery if (request.hold_id or request.monitor_id) else None}, evidence

    def _resume_readback(self, request_id: str, hold_id: str) -> TaskProvisionReceipt:
        """Exact replay only observes the existing lifecycle; it never queues work."""
        try:
            data = self.hold_status(hold_id)
        except RuntimeError:
            data = {}
        if data.get("request_id") != request_id:
            return TaskProvisionReceipt(request_id=request_id, status="requires_readback", observed_at=observed_now(),
                hold_id=hold_id, monitor_id=hold_id, reason="existing resume lifecycle identity is unverified")
        fields = {key: data[key] for key in ("thread_id", "turn_id", "output", "reason", "error_code", "phase",
                   "turn_count", "total_turn_count", "max_turns") if key in data}
        if "output" not in fields and "final_message" in data:
            fields["output"] = data["final_message"]
        return TaskProvisionReceipt(request_id=request_id, status=data.get("status") or "requires_readback",
            observed_at=_parse_observed_at(data.get("observed_at")) or observed_now(),
            hold_id=hold_id, monitor_id=hold_id, **fields)

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
                        close = _read_json_file(root / "close.json") or {}
                        current = _read_json_file(root / "request.json") or {}
                        pause = current.get("resume_pause") or {}
                        if (pause.get("request_id") == current.get("request_id") == value.get("request_id")
                                and pause.get("execution_state") == "stopped" and pause.get("original_generation_fenced") is True):
                            return {**value, "status": "paused", "logical_running": False,
                                    "hold_released": False, "execution_state": "stopped", "resume_pause": pause}
                        mismatched = (current.get("request_id") and value.get("request_id") != current["request_id"]
                            and (value.get("request_id") or close.get("request_id") == current["request_id"]))
                        if mismatched:
                            return {**value, "terminal_confirmed": False, "hold_released": False,
                                "reason": "lifecycle evidence is not bound to the current request_id"}
                        non_dispatch = (close.get("request_id") == current.get("request_id") == value.get("request_id")
                                        and close.get("non_dispatch_confirmed") is True)
                        cancelled = cancellation_path(root, current) is not None
                        released = (terminal and path.name == "result.json" and (value.get("terminal_confirmed") is True or non_dispatch)
                                    and not (root / ".user-host-claim").exists())
                        bound_close = close if close.get("request_id") == current.get("request_id") else {}
                        management_status = ("closed" if released else "closing" if bound_close.get("execution_state") == "running" else "closed_unconfirmed")
                        management = {"scheduling_closed": True, "management_closed": True,
                            "management_status": management_status, "close_report": close,
                            "cancellation_record": str(cancellation_path(root, current)),
                            "cancellation": _read_json_file(cancellation_path(root, current))} if cancelled else {}
                        return {**value, **management, **({"terminal_confirmed": True} if non_dispatch else {}), "hold_released": (
                            terminal and path.name == "result.json" and (value.get("terminal_confirmed") is True or non_dispatch)
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

    def close_hold(self, hold_id: str, *, request_id: str | None = None, parent_close=None) -> dict[str, Any]:
        """Cancel scheduling unconditionally; execution closure needs real owner evidence."""
        from adapters.codex_app_server.jarvis_hold_host_service import JarvisHoldHost
        paths = self._existing_paths(hold_id)
        if paths is None:
            raise RuntimeError("hold state was not found")
        root = paths["request"].parent
        current = _read_json_file(paths["request"]) or {}
        binding = current.get("request_id")
        if not binding or (current.get("hold_id") or current.get("monitor_id")) != hold_id:
            raise RuntimeError("close requires an exact durable Hold/request identity")
        for name in ("ack", "result"):
            saved = _read_json_file(paths[name]) or {}
            if saved.get("request_id") and saved["request_id"] != binding:
                raise RuntimeError(f"{name} identity conflicts; no unrelated request cancelled")
        cancellation = cancel_management(root, scope="hold", subject_id=hold_id,
            request_id=binding, close_request_id=request_id or f"close:{binding}",
            original=paths["request"].read_bytes())
        close_path = root / "close.json"
        try:
            with report_guard(root / ".close-report.guard"):
                report = _read_json_file(close_path) or {}
                if report and report.get("request_id") != binding:
                    raise RuntimeError("existing close report names another request")
                report.setdefault("schema", "jarvis-close-report/v1")
                report.update(hold_id=hold_id, request_id=binding,
                    close_request_id=cancellation["close_request_id"], requested_at=cancellation["requested_at"],
                    scheduling_closed=True, management_closed=True, cancellation=cancellation)
                if parent_close:
                    if report.get("parent_close") not in (None, dict(parent_close)):
                        raise RuntimeError("conflicting parent Loop close binding")
                    report["parent_close"] = dict(parent_close)
                _write_json(close_path, report)
        except Exception as exc:
            # Cancellation is already durable even when a report writer is unavailable.
            return {"status": "closed_unconfirmed", "hold_id": hold_id, "request_id": binding,
                "scheduling_closed": True, "management_closed": True,
                "terminal_confirmed": False, "hold_released": False,
                "close_report": {"report_status": "unresolved", "execution_state": "unknown",
                    "scheduling_closed": True, "management_closed": True,
                    "terminal_confirmed": False, "hold_released": False,
                    "external_execution_unresolved": True,
                    "cancellation": cancellation, "report_error": str(exc)}}
        delivery_error = None
        try:
            # Compatibility delivery only. Never reap a PID-based stale lock.
            with ProcessLock(paths["request"].with_suffix(".lock"), timeout_seconds=0.1, owner_alive=lambda pid: True):
                latest = _read_json_file(paths["request"]) or {}
                if latest.get("request_id") != binding:
                    raise RuntimeError("request changed after cancellation; dispatch remains blocked")
                if latest.get("stop_requested") is not True:
                    _write_json(paths["request"], {**latest, "stop_requested": True})
        except Exception as exc:
            delivery_error = str(exc)
        try:
            data = self.hold_status(hold_id)
        except Exception as exc:
            data = {"request_id": binding, "hold_id": hold_id, "status": "unknown", "reason": str(exc)}
        action = {"status": "not_sent", "reason": "no exact current owner identity"}
        if data.get("thread_id") and data.get("turn_id") and not data.get("hold_released"):
            try:
                action = JarvisHoldHost(state_dir=self._state_dir, launcher_config=self._config_path).request_host_stop(
                    hold_id, data["thread_id"], data["turn_id"])
            except Exception as exc:
                action = {"status": "unconfirmed", "reason": str(exc)}
        if delivery_error:
            action = {**action, "legacy_stop_delivery_error": delivery_error}
        try:
            report = refresh_close_report(root, data, action=action)
        except Exception as exc:
            report = {"request_id": binding, "hold_id": hold_id,
                "report_status": "unresolved", "management_closed": True,
                "scheduling_closed": True, "execution_state": "unknown",
                "terminal_confirmed": False, "hold_released": False,
                "cancellation": cancellation, "report_error": str(exc)}
        if report.get("request_id") != binding or report.get("hold_id") != hold_id:
            report = {"request_id": binding, "hold_id": hold_id, "report_status": "unresolved",
                "execution_state": "unknown", "terminal_confirmed": False, "hold_released": False,
                "report_error": "close report identity does not match this request"}
        outcome = ("closed" if report.get("report_status") == "completed" else
                   "closing" if report.get("execution_state") == "running" else "closed_unconfirmed")
        return {**data, "status": outcome, "management_status": outcome,
            "lifecycle_status": data.get("status"), "scheduling_closed": True,
            "management_closed": True, "request_id": binding, "hold_id": hold_id,
            "terminal_confirmed": report.get("terminal_confirmed") is True,
            "hold_released": report.get("hold_released") is True, "close_report": report}

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


def receive_candidate_final_answer(request: dict[str, Any], turn_number: int, *,
                                   thread_id: str, turn_id: str, raw: str) -> dict[str, Any]:
    """Accept only the owning Hold's completed-turn text; business content is never identity."""
    binding = request.get("input_binding") or {}
    try:
        if not final_answer_mode(binding) or "lane_item_count" not in binding:
            raise ValueError("final answer intake requires an explicit finite single-item lane")
        if not thread_id or not turn_id or not raw:
            raise ValueError("exact completed turn and final answer are required")
        candidate = lane_batch_ids(binding, turn_number)[0]
        contract = binding["result_verification"]
        boundary = Path(binding["output_boundary"]).resolve()
        path = Path(contract["receipt_paths"][str(candidate)])
        receipt_path = (path if path.is_absolute() else boundary / path).resolve()
        if not receipt_path.is_relative_to(boundary):
            raise ValueError("receipt path is outside the allowed boundary")
        identity = {"candidate_id": candidate, "request_id": request["request_id"],
                    "turn_number": turn_number, "thread_id": thread_id, "turn_id": turn_id}
        raw_bytes = raw.encode("utf-8")
        raw_hash = hashlib.sha256(raw_bytes).hexdigest()
        # Content-addressed evidence survives replay/recovery and later runs using the receipt slot.
        evidence_key = hashlib.sha256((json.dumps(identity, sort_keys=True) + raw_hash).encode()).hexdigest()
        raw_path = receipt_path.with_name(f"{receipt_path.stem}.{evidence_key}.raw.txt")
        payload_path = receipt_path.with_name(f"{receipt_path.stem}.{evidence_key}.payload.json")
        for evidence_path in (raw_path, payload_path):
            if not evidence_path.resolve().is_relative_to(boundary):
                raise ValueError("evidence path is outside the allowed boundary")
        def save_evidence(path: Path, content: bytes) -> None:
            if path.exists():
                if path.read_bytes() != content:
                    raise ValueError("existing evidence differs")
                return
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
            try:
                temporary.write_bytes(content)
                temporary.replace(path)
            finally:
                temporary.unlink(missing_ok=True)

        save_evidence(raw_path, raw_bytes)
        issues: list[str] = []
        payload = None
        parsed = False
        try:
            def reject_constant(value: str) -> None:
                raise ValueError(f"non-JSON constant: {value}")
            payload = json.loads(raw, parse_constant=reject_constant)
            parsed = True
        except (ValueError, RecursionError) as exc:
            issues.append(f"JSON parse failed: {exc}")
        if parsed and "output_schema" in contract:
            try:
                validate_saved_output(contract["output_schema"], payload)
            except ValueError as exc:
                issues.append(str(exc))
        payload_bytes = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
        save_evidence(payload_path, payload_bytes)
        receipt = {"schema": "jarvis-final-answer-receipt/v1", **identity, "status": "received",
                   "received": True, "raw_path": str(raw_path), "raw_sha256": raw_hash,
                   "output_path": str(payload_path), "output_sha256": hashlib.sha256(payload_bytes).hexdigest(),
                   "payload_parsed": parsed, "schema_issues": issues, "review_needed": True,
                   "verification_status": "not_verified", "research_completion": "not_assessed"}
        _write_json(receipt_path, receipt)
        verified = verify_candidate_output(request, turn_number)
        if verified.get("status") == "verified" and any(verified.get(key) != value for key, value in identity.items()):
            raise ValueError("saved receipt does not match the owning native turn")
        return verified
    except (OSError, ValueError, KeyError, TypeError, IndexError) as exc:
        return {"status": "blocked", "reason": f"final_answer_intake_failed: {exc}"}


def verify_candidate_output(request: dict[str, Any], turn_number: int) -> dict[str, Any]:
    """Verify every real ID in this turn before declaring a batch complete."""
    binding = request.get("input_binding") or {}
    if "lane_item_count" not in binding:
        return {"status": "not_required"}
    contract = binding.get("result_verification") or {}
    schema_ref = contract.get("schema_ref") if isinstance(contract, dict) else None
    if schema_ref is not None:
        try:
            if (not isinstance(schema_ref, dict)
                    or set(schema_ref) != {"schema_id", "schema_version", "schema_sha256"}
                    or "output_schema" not in contract):
                raise ValueError("registered schema binding is incomplete")
            canonical = json.dumps(contract["output_schema"], ensure_ascii=False, allow_nan=False,
                                   sort_keys=True, separators=(",", ":")).encode("utf-8")
            if hashlib.sha256(canonical).hexdigest() != schema_ref["schema_sha256"]:
                raise ValueError("registered schema snapshot hash mismatch")
        except (ValueError, TypeError, KeyError) as exc:
            return {"status": "blocked", "reason": f"candidate_output_unverified: {exc}"}
    try:
        ids = lane_batch_ids(binding, turn_number)
        if lane_batch_size(binding) == 1:
            result = _verify_candidate_output_item(request, turn_number, ids[0])
            return {**result, **({"schema_ref": schema_ref} if schema_ref is not None else {})}
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
                **identity, "items": items, **({"schema_ref": schema_ref} if schema_ref is not None else {})}
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

        def read_output(value: str, *, require_object: bool = True) -> tuple[Path, Any]:
            nonlocal safety_error
            path = Path(value)
            path = (path if path.is_absolute() else boundary / path).resolve()
            if not path.is_relative_to(boundary):
                safety_error = True
                raise ValueError("output path is outside the allowed boundary")
            raw = path.read_text(encoding="utf-8-sig")
            evidence[str(path)] = raw
            data = json.loads(raw)
            if require_object and not isinstance(data, dict):
                raise ValueError("saved output must be a JSON object")
            if require_object and (type(data.get("candidate_id")) not in {int, str}
                    or str(data["candidate_id"]) != str(candidate)):
                safety_error = True
                raise ValueError("saved candidate_id does not match current candidate")
            return path, data

        receipt_path, receipt = read_output(contract["receipt_paths"][str(candidate)])
        if final_answer_mode(binding):
            for key, value in expected.items():
                if type(receipt.get(key)) is not type(value) or receipt.get(key) != value:
                    raise ValueError(f"receipt {key} does not match current candidate/run/turn")
            if (receipt.get("schema") != "jarvis-final-answer-receipt/v1" or receipt.get("status") != "received"
                    or receipt.get("received") is not True
                    or any(not isinstance(receipt.get(key), str) or not receipt[key] for key in ("thread_id", "turn_id"))
                    or receipt.get("review_needed") is not True or receipt.get("verification_status") != "not_verified"
                    or receipt.get("research_completion") != "not_assessed"):
                raise ValueError("final answer receipt has no bound unverified intake identity")
            output_path, _ = read_output(receipt["output_path"], require_object=False)
            raw_path = Path(receipt["raw_path"]).resolve(strict=True)
            if not raw_path.is_relative_to(boundary):
                raise ValueError("raw path is outside the allowed boundary")
            if (hashlib.sha256(output_path.read_bytes()).hexdigest() != receipt.get("output_sha256")
                    or hashlib.sha256(raw_path.read_bytes()).hexdigest() != receipt.get("raw_sha256")):
                raise ValueError("final answer evidence hash mismatch")
            return {"status": "verified", **expected, "receipt_path": str(receipt_path),
                    "output_path": str(output_path), "terminal_status": "received",
                    "thread_id": receipt["thread_id"], "turn_id": receipt["turn_id"],
                    "review_needed": True, "verification_status": "not_verified"}
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
        unsafe = safety_error or final_answer_mode(binding)
        return {"status": "blocked" if unsafe else "review", **expected,
                "scheduler_outcome": "blocked" if unsafe else "failed",
                "reason": f"candidate_output_unverified: {exc}", "original_evidence": evidence}


def refresh_close_report(root: Path, evidence: dict[str, Any], *, action=None, failure: str | None = None) -> dict[str, Any]:
    """Update only an existing close record bound to this exact request."""
    path = root / "close.json"
    if not path.exists():
        return {}
    with report_guard(root / ".close-report.guard"):
        report = _read_json_file(path) or {}
        request = _read_json_file(root / "request.json") or {}
        if report.get("request_id") != request.get("request_id") or evidence.get("request_id") != request.get("request_id"):
            return report
        latest = _read_json_file(root / "result.json") or {}
        # A close caller can arrive after the Host with an older holding snapshot.
        # Prefer only exact persisted stopped-unknown evidence, never infer terminal.
        if (latest.get("request_id") == request.get("request_id")
                and latest.get("hold_id") == report.get("hold_id") == request.get("hold_id")
                and all(latest.get(key) and evidence.get(key) in (None, "", latest[key])
                        and request.get(key) in (None, "", latest[key]) for key in ("thread_id", "turn_id"))
                and latest.get("terminal_confirmed") is False
                and latest.get("holder_exit_confirmed") is True
                and latest.get("local_observer_stopped") is True):
            evidence = {**evidence, **latest, "hold_released": False}
        released = evidence.get("hold_released") is True
        terminal = evidence.get("terminal_confirmed") is True
        no_claim = not (root / ".user-host-claim").exists()
        not_sent = (evidence.get("dispatch_evidence") == "not_sent" and not evidence.get("turn_id")
                    and evidence.get("local_observer_stopped") is True and no_claim)
        if not_sent or report.get("non_dispatch_confirmed") is True:
            terminal, released = True, no_claim
        completed = terminal and released
        stopped = evidence.get("local_observer_stopped") is True and (
            no_claim or evidence.get("holder_exit_confirmed") is True)
        cancelled = cancellation_path(root, request) is not None
        owned_stop_pending = isinstance(action, dict) and action.get("status") == "stop_requested" and not stopped
        unknown = not terminal and not owned_stop_pending and (cancelled or evidence.get("status") in {"failed", "unknown", "interrupted"})
        fields = {"scheduling_closed": cancelled or request.get("stop_requested") is True or completed,
            "lifecycle_status": evidence.get("lifecycle_status") or evidence.get("status"),
            "execution_state": "not_started" if not_sent else "terminal" if terminal else "unknown" if unknown else "running",
            "terminal_confirmed": terminal, "hold_released": released,
            "management_closed": cancelled or completed or stopped,
            "external_execution_unresolved": not terminal and (cancelled or stopped),
            "report_status": "failed" if failure else "completed" if completed else "unresolved" if unknown or terminal else "pending",
            "thread_id": evidence.get("thread_id"), "turn_id": evidence.get("turn_id"),
            "reason": failure or evidence.get("reason")}
        if not_sent:
            fields["non_dispatch_confirmed"] = True
        if action is not None:
            fields["last_action"] = action
        if report.get("report_status") == "completed" and not failure:
            if report.get("parent_close"):
                from jarvis_control.loop import LoopStore
                LoopStore(root.parents[1] / "loops").record_child_close(report)
            return report
        if any(report.get(key) != value for key, value in fields.items()):
            report.update(fields, observed_at=observed_now().isoformat())
            _write_json(path, report)
        if report.get("parent_close"):
            from jarvis_control.loop import LoopStore
            LoopStore(root.parents[1] / "loops").record_child_close(report)
        return report


def _state_reference(root: Path, state: Path) -> str:
    def normalized(path):
        value = os.path.normcase(str(path.resolve()))
        # Windows final-path APIs may return the equivalent extended prefix.
        return value[4:] if value.startswith("\\\\?\\") else value
    relative = Path(os.path.relpath(normalized(root), normalized(state)))
    if len(relative.parts) != 2 or relative.parts[0] not in {"task-holds", "task-monitors", "loops"}:
        raise RuntimeError("dispatch root is outside this state registry")
    return relative.as_posix()


def _require_original_hold_selected(roots, target, thread_id):
    """An implicit new Hold cannot bypass an unresolved existing management root."""
    for root in roots:
        if root == target or root.parent.name not in {"task-holds", "task-monitors"}:
            continue
        request = _read_json_file(root / "request.json") or {}
        result = _read_json_file(root / "result.json") or {}
        ack = _read_json_file(root / "ack.json") or {}
        # Legacy create requests omit thread_id; native ack/result still bind them.
        if not any(record.get("thread_id") == thread_id for record in (request, ack, result)):
            continue
        hold_id = request.get("hold_id") or request.get("monitor_id")
        close = _read_json_file(root / "close.json") or {}
        owned_exit = (all(result.get(key) is True for key in ("holder_exit_confirmed", "holder_client_exit_confirmed"))
            or (result.get("local_observer_stopped") is True and result.get("host_stop") is not True
                and all(result.get(key) is not False for key in ("holder_exit_confirmed", "holder_client_exit_confirmed"))))
        release_proof = (result.get("hold_released") is True
            or ("hold_released" not in result and (owned_exit
                or (close.get("request_id") == request.get("request_id")
                    and close.get("hold_released") is True and close.get("terminal_confirmed") is True
                    and close.get("holder_exit_confirmed") is True))))
        retired = (request.get("request_id") and hold_id
            and result.get("request_id") == request["request_id"]
            and result.get("hold_id", hold_id) == hold_id
            and result.get("thread_id") == thread_id
            and all(record.get("thread_id") in (None, "", thread_id) for record in (request, ack))
            and (not ack or (ack.get("request_id") == request["request_id"]
                and (ack.get("hold_id") or ack.get("monitor_id") or hold_id) == hold_id
                and ack.get("turn_id") in (None, "", result.get("turn_id"))))
            and result.get("status") in {"completed", "failed", "interrupted", "cancelled", "canceled", "blocked", "turn_limit_reached"}
            and result.get("terminal_confirmed") is True and release_proof
            and not request.get("resume_pause")
            and not (root / ".user-host-claim").exists())
        if not retired:
            raise _OriginalRecoveryBlocked("exact_original_hold_required",
                "Native thread has an unreleased or unverified original Hold; select its exact Hold for recovery",
                {"hold_id": hold_id, "thread_id": thread_id})


class _OriginalRecoveryBlocked(RuntimeError):
    def __init__(self, code: str, reason: str, identity=None):
        super().__init__(reason)
        self.code, self.identity = code, dict(identity or {})


def _original_generation_identity(snapshot, hold_id, thread_id):
    request = _snapshot_json(snapshot, "request") or {}
    ack = _snapshot_json(snapshot, "ack") or {}
    result = _snapshot_json(snapshot, "result") or {}
    identity = {"request_id": request.get("request_id"), "hold_id": hold_id,
                "thread_id": thread_id, "turn_id": result.get("turn_id") or ack.get("turn_id")}
    if (not all(identity.values()) or (request.get("hold_id") or request.get("monitor_id")) != hold_id
            or any(record.get("request_id") != identity["request_id"] for record in (ack, result) if record)
            or any(record.get("thread_id") not in (None, "", thread_id) for record in (request, ack, result))
            or any(record.get("turn_id") not in (None, "", identity["turn_id"]) for record in (ack, result))):
        raise _OriginalRecoveryBlocked("identity_unverified", "Original request/Hold/thread/turn evidence is incomplete or conflicting", identity)
    identity["dispatch_commits_sha256"] = hashlib.sha256(json.dumps(snapshot["dispatch_commits"], sort_keys=True,
        separators=(",", ":")).encode()).hexdigest()
    if snapshot["legacy_request_lock"] is not None:
        raise _OriginalRecoveryBlocked("capability_missing", "generation_safe_legacy_request_lock retirement is unavailable; original lock retained", identity)
    owner = _snapshot_json(snapshot, "owner") or {}
    if owner.get("request_id") not in (None, "", identity["request_id"]):
        raise _OriginalRecoveryBlocked("identity_unverified", "Original claim belongs to a different request generation", identity)
    return identity


def _plan_original_recovery(snapshot, hold_id, thread_id, backend):
    identity = _original_generation_identity(snapshot, hold_id, thread_id)
    capabilities = backend.recovery_capabilities() if backend is not None and callable(getattr(backend, "recovery_capabilities", None)) else set()
    required = {"owner_independent_execution_v1", "compare_pause_and_fence_v1", "prior_dispatch_settlement_v1",
                "durable_operation_fencing_v1"}
    if (not required.issubset(capabilities) or not callable(getattr(backend, "inspect_execution", None))
            or not callable(getattr(backend, "pause_if_unchanged", None))):
        raise _OriginalRecoveryBlocked("capability_missing", "Original backend lacks owner_independent_execution, compare-pause/fence or prior-dispatch-settlement capability; independent observer cannot authorize recovery", identity)
    evidence = backend.inspect_execution(dict(identity))
    _validate_original_evidence(identity, evidence)
    return {"identity": identity, "evidence": evidence}


def _validate_original_evidence(identity, evidence, *, fenced=False):
    if (not isinstance(evidence, dict) or any(evidence.get(k) != v for k, v in identity.items())
            or evidence.get("source") != "original_backend" or evidence.get("owner_independent") is not True
            or evidence.get("query_complete") is not True or evidence.get("execution_state") != "stopped"
            or evidence.get("no_active_turns") is not True
            or not isinstance(evidence.get("evidence_token"), str) or not evidence["evidence_token"]):
        raise _OriginalRecoveryBlocked("execution_unverified", "Original backend has no complete identity-bound no-active execution proof; observer interrupted/null is insufficient", identity)
    if evidence.get("pending_dispatch_settled") is not True:
        raise _OriginalRecoveryBlocked("old_generation_dispatch_unresolved", "Old committed-but-not-written dispatch is not authoritatively settled/fenced", identity)
    if fenced and evidence.get("original_generation_fenced") is not True:
        raise _OriginalRecoveryBlocked("recovery_fence_unverified", "Original backend did not fence the prior generation", identity)


def _pause_original_generation(paths, snapshot, plan, backend):
    # Caller holds the existing thread/dispatch gates. Backend compare-and-pause
    # must also exclude previously committed commands which could arrive late.
    identity = plan["identity"]
    operation = plan.get("operation_request_id", identity["request_id"])
    proof = backend.pause_if_unchanged({**identity, "operation_request_id": operation}, plan["evidence"]["evidence_token"])
    _validate_original_evidence(identity, proof, fenced=True)
    if (proof.get("durable_operation_fence") is not True or proof.get("operation_request_id") != operation):
        raise _OriginalRecoveryBlocked("recovery_fence_unverified",
            "Backend has no durable operation-bound fence covering prior generation and committed dispatch", identity)
    if proof["evidence_token"] != plan["evidence"]["evidence_token"] or _resume_snapshot(paths) != snapshot:
        raise _OriginalRecoveryBlocked("identity_changed", "Original generation/query evidence changed before handoff", identity)
    request = _snapshot_json(snapshot, "request")
    pause = {**proof, "schema": "jarvis-resume-pause/v1", "hold_released": False,
             "logical_running": False, "paused_at": observed_now().isoformat()}
    if "transfer" in plan:
        pause["transfer"] = plan["transfer"]
    _persist_transfer_json(paths["request"], {**request, "resume_pause": pause})
    return pause


def _json_bytes(value):
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def _persist_transfer_json(path, value):
    from adapters.codex_app_server.jarvis_task_hold_host import _write_json as atomic_write
    atomic_write(path, value, durable=True)
    from jarvis_codex_bridge.journal import fsync_directory
    fsync_directory(path.parent)


def _pending_original_transfer(paths, saved, declaration):
    pause = (saved or {}).get("resume_pause") or {}
    if not pause.get("transfer") and (saved or {}).get("supersedes_request_id"):
        archive = paths["request"].parent / "history" / _safe_id(saved["supersedes_request_id"])
        pause = _read_json_file(archive / "pause.json") or {}
    transfer = pause.get("transfer")
    if not transfer:
        return None
    ack = _read_json_file(paths["ack"]) or {}
    committed_id = (transfer.get("declaration") or {}).get("request_id")
    if saved.get("request_id") == ack.get("request_id") == committed_id:
        return None  # A completed transfer is read back or becomes a later generation's original state.
    if transfer.get("declaration") != declaration:
        raise _OriginalRecoveryBlocked("identity_unverified", "Interrupted transfer belongs to a conflicting resume declaration")
    return pause


def _transfer_snapshot(pause):
    transfer = pause["transfer"]
    snapshot = dict(transfer["original"])
    for key in ("request", "ack", "result", "owner", "legacy_request_lock"):
        value = snapshot[key]
        snapshot[key] = bytes.fromhex(value) if value is not None else None
        if snapshot[key] is not None and hashlib.sha256(snapshot[key]).hexdigest() != transfer["original_hashes"].get(key):
            raise _OriginalRecoveryBlocked("identity_unverified", "Interrupted transfer original backup hash conflicts")
    return snapshot


def _check_transfer_state(paths, pause, roots):
    _require_roots_open(roots)
    snapshot = _transfer_snapshot(pause)
    target = pause["transfer"]["target_request"]
    original = _snapshot_json(snapshot, "request")
    paused = _json_bytes({**original, "resume_pause": pause})
    current = _resume_snapshot(paths)
    if (current["request"] not in (paused, _json_bytes(target))
            or current["ack"] not in (snapshot["ack"], _json_bytes(pause["transfer"]["target_ack"]))
            or current["result"] not in (snapshot["result"], None)
            or current["legacy_request_lock"] != snapshot["legacy_request_lock"]
            or current["dispatch_commits"] != snapshot["dispatch_commits"]):
        raise _OriginalRecoveryBlocked("identity_changed", "Interrupted transfer identity/receipts/dispatch changed")
    if current["request"] == paused and current["ack"] != snapshot["ack"]:
        raise _OriginalRecoveryBlocked("identity_changed", "Successor ack appeared before its request")
    root = paths["request"].parent
    archive = root / "history" / _safe_id(pause["request_id"])
    claim, retired = root / ".user-host-claim", archive / ".user-host-claim"
    if archive.is_symlink() or claim.is_symlink() or retired.is_symlink():
        raise _OriginalRecoveryBlocked("identity_unverified", "Symlink transfer state is not accepted")
    retired_owner = retired / "owner.json"
    if (current["claimed"] and (not snapshot["claimed"] or current["owner"] != snapshot["owner"])
            or (retired.exists() and (not snapshot["claimed"]
                or (retired_owner.read_bytes() if retired_owner.exists() else None) != snapshot["owner"]))
            or (snapshot["claimed"] and not claim.exists() and not retired.exists())
            or (claim.exists() and retired.exists())):
        raise _OriginalRecoveryBlocked("identity_changed", "Original claim changed or an unrelated claim appeared")
    if snapshot["result"] is not None and current["result"] is None:
        backup = archive / "result.json"
        if not backup.is_file() or backup.read_bytes() != snapshot["result"]:
            raise _OriginalRecoveryBlocked("identity_changed", "Original result disappeared without its exact backup")
    return snapshot, archive


def _archive_original_generation(paths, pause, roots):
    snapshot, archive = _check_transfer_state(paths, pause, roots)
    archive.mkdir(parents=True, exist_ok=True)
    for name in ("request", "ack", "result"):
        _check_transfer_state(paths, pause, roots)
        target = archive / f"{name}.json"
        raw = snapshot[name]
        if target.is_symlink() or (target.exists() and (raw is None or target.read_bytes() != raw)):
            raise _OriginalRecoveryBlocked("identity_unverified", "Original archive content conflicts; retained for readback")
        if raw is not None and not target.exists():
            with target.open("xb") as handle:
                handle.write(raw); handle.flush(); os.fsync(handle.fileno())
    _check_transfer_state(paths, pause, roots)
    saved_pause = archive / "pause.json"
    if saved_pause.is_symlink() or (saved_pause.exists() and saved_pause.read_bytes() != _json_bytes(pause)):
        raise _OriginalRecoveryBlocked("identity_unverified", "Interrupted transfer declaration conflicts with archive")
    if not saved_pause.exists():
        _persist_transfer_json(saved_pause, pause)
    _check_transfer_state(paths, pause, roots)
    claim = paths["request"].parent / ".user-host-claim"
    if claim.exists():
        claim.rename(archive / ".user-host-claim")  # Logical retirement, never proof of physical owner exit.
    from jarvis_codex_bridge.journal import fsync_directory
    fsync_directory(archive)
    fsync_directory(archive.parent)
    fsync_directory(paths["request"].parent)


def _complete_original_transfer(paths, pause, roots, backend):
    transfer = pause.get("transfer") or {}
    declaration = transfer.get("declaration") or {}
    original = _transfer_snapshot(pause)
    identity = _original_generation_identity(original, declaration.get("hold_id"), declaration.get("thread_id"))
    capabilities = backend.recovery_capabilities() if backend is not None and callable(getattr(backend, "recovery_capabilities", None)) else set()
    if "durable_operation_fencing_v1" not in capabilities:
        raise _OriginalRecoveryBlocked("capability_missing", "Persistent original-backend operation fence is unavailable", identity)
    _validate_original_evidence(identity, pause, fenced=True)
    targets = transfer.get("target_hashes") or {}
    if any(hashlib.sha256(_json_bytes(transfer.get("target_" + name, {}))).hexdigest() != targets.get(key)
           for name, key in (("request", "request"), ("ack", "ack"))):
        raise _OriginalRecoveryBlocked("identity_unverified", "Interrupted transfer target bytes conflict with fixed hashes", identity)
    if (pause.get("durable_operation_fence") is not True
            or pause.get("operation_request_id") != declaration.get("request_id")
            or transfer.get("schema") != "jarvis-resume-transfer/v1"
            or transfer.get("target_request", {}).get("resume_declaration") != declaration
            or transfer.get("target_request", {}).get("supersedes_request_id") != identity["request_id"]
            or transfer.get("target_ack", {}).get("request_id") != declaration.get("request_id")
            or _snapshot_json(original, "request").get("request_id") != identity["request_id"]):
        raise _OriginalRecoveryBlocked("recovery_fence_unverified", "Saved fence/transfer is not bound to this operation", identity)
    _archive_original_generation(paths, pause, roots)
    _check_transfer_state(paths, pause, roots)
    if paths["result"].exists():
        _archive_terminal_result(paths["result"])
    _check_transfer_state(paths, pause, roots)
    if paths["request"].read_bytes() != _json_bytes(transfer["target_request"]):
        _persist_transfer_json(paths["request"], transfer["target_request"])
    _check_transfer_state(paths, pause, roots)
    if not paths["ack"].exists() or paths["ack"].read_bytes() != _json_bytes(transfer["target_ack"]):
        _persist_transfer_json(paths["ack"], transfer["target_ack"])


def reconcile_original_generation(root, backend):
    paths = CodexAppServerTaskProvisioningAdapter._paths_at(root)
    snapshot = _resume_snapshot(paths)
    request = _snapshot_json(snapshot, "request") or {}
    ack = _snapshot_json(snapshot, "ack") or {}
    identity = {"hold_id": request.get("hold_id") or request.get("monitor_id"),
                "thread_id": request.get("thread_id") or ack.get("thread_id")}
    plan = _plan_original_recovery(snapshot, identity["hold_id"], identity["thread_id"], backend)
    with _dispatch_guard(root, {**request, "thread_id": identity["thread_id"]}) as roots:
        _require_roots_open(roots)
        if _resume_snapshot(paths) != snapshot:
            raise _OriginalRecoveryBlocked("identity_changed", "Original state changed during recovery planning", plan["identity"])
        pause = _pause_original_generation(paths, snapshot, plan, backend)
    return {"status": "paused", **plan["identity"], "logical_running": False,
            "execution_state": "stopped", "hold_released": False, "resume_pause": pause}


def _resume_snapshot(paths):
    values = {name: path.read_bytes() if path.exists() else None
              for name, path in paths.items() if name in {"request", "ack", "result"}}
    claim = paths["request"].parent / ".user-host-claim"
    values["claimed"] = claim.exists()
    owner = claim / "owner.json"
    values["owner"] = owner.read_bytes() if owner.exists() else None
    legacy_lock = paths["request"].with_suffix(".lock")
    values["legacy_request_lock"] = legacy_lock.read_bytes() if legacy_lock.exists() else None
    commits = paths["request"].parent / "dispatch-commits"
    values["dispatch_commits"] = {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(commits.glob("*.json"))} if commits.is_dir() else {}
    return values


def _snapshot_json(snapshot, name):
    raw = snapshot[name]
    if raw is None:
        return None
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise RuntimeError("invalid persisted Hold record")
    return value


def _legacy_resume_declaration(saved):
    defaults = {"model": None, "reasoning_effort": None, "max_turns": 1, "auto_continue": False,
        "continue_prompt": TaskMonitorResumeRequest.__dataclass_fields__["continue_prompt"].default,
        "notifications": {}, "input_binding": {}}
    keys = ("mode", "request_id", "hold_id", "thread_id", "source_ref", "prompt", *defaults)
    declaration = {key: saved.get(key, defaults.get(key)) for key in keys}
    declaration["hold_id"] = saved.get("hold_id") or saved.get("monitor_id")
    # The v1 create producer always persists project/project_path/title. Recovery
    # changes execution mode only. Recognize the complete v1 resume shape, never
    # infer resume intent from a sparse/ambiguous recover record.
    if (saved.get("mode") == "recover" and isinstance(saved.get("turn_id"), str) and saved["turn_id"]
            and all(key in saved for key in ("request_id", "thread_id", "source_ref", "prompt", *defaults,
                "initial_turn_count", "initial_total_turn_count"))
            and not any(key in saved for key in ("project", "project_path", "title"))):
        declaration["mode"] = "resume"
    return declaration


def _file_identity(path):
    stat = path.stat()
    return str(path.resolve(strict=True)), stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def _output_evidence_identity(request, turn_number):
    """Pin the verifier's files outside locks; final checks use bounded stat calls."""
    binding = request.get("input_binding") or {}
    contract = binding.get("result_verification") or {}
    boundary_path = Path(binding["output_boundary"]).absolute()
    boundary = boundary_path.resolve(strict=True)
    identities = {}
    def path_for(value, *, relative_to_boundary=True):
        path = Path(value)
        if not path.is_absolute() and relative_to_boundary:
            path = boundary_path / path
        path = path.absolute()
        if not path.resolve(strict=True).is_relative_to(boundary):
            raise RuntimeError("output evidence is outside the allowed boundary")
        return path
    for candidate in lane_batch_ids(binding, turn_number):
        receipt = path_for(contract["receipt_paths"][str(candidate)])
        identities[str(receipt)] = _file_identity(receipt)
        value = json.loads(receipt.read_text(encoding="utf-8-sig"))
        output = path_for(value["output_path"])
        identities[str(output)] = _file_identity(output)
        if final_answer_mode(binding):
            raw = path_for(value["raw_path"], relative_to_boundary=False)
            identities[str(raw)] = _file_identity(raw)
    return identities


def _write_json(path: Path, value: dict[str, Any]) -> None:
    from adapters.codex_app_server.jarvis_task_hold_host import _write_json as atomic_write
    stop_update = path.name == "request.json" and value.get("stop_requested") is True
    with _hold_guard(path.parent, value) if stop_update else nullcontext():
        if stop_update:
            current = json.loads(path.read_text(encoding="utf-8"))
            expected = {key: item for key, item in value.items() if key != "stop_requested"}
            actual = {key: item for key, item in current.items() if key != "stop_requested"}
            if actual != expected:
                raise RuntimeError("stop request generation changed; original stop was not applied")
        atomic_write(path, value)


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


def _count_scheduler_failed_items(path: Path, *, hold_id: str) -> int:
    """Count review items while retaining only one verification row at a time.

    Payload history remains available through read_turn_history. This internal
    projection intentionally excludes final answers and candidate identity JSON.
    Schema discovery and rows use one read snapshot, including during migration.
    """
    if not path.is_file():
        return 0
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        connection.execute("BEGIN")
        columns = {row[1] for row in connection.execute("PRAGMA table_info(turn_history)")}
        if not columns:
            # An existing unrelated/empty DB is not an empty history table.
            connection.execute("SELECT 1 FROM turn_history LIMIT 0")
        has_candidates = "candidate_ids_json" in columns
        has_verification = "output_verification_json" in columns
        if has_candidates != has_verification:
            raise sqlite3.OperationalError("turn_history has an incomplete JSON column pair")
        if not has_verification:
            return 0  # Legacy rows have no verification, as in the payload reader.
        selected_hold = hold_id.strip()
        where = " WHERE hold_id=?" if selected_hold else ""
        parameters = (selected_hold,) if selected_hold else ()
        failed_items = 0
        for (raw,) in connection.execute(
            "SELECT output_verification_json FROM turn_history" + where, parameters,
        ):
            checked = json.loads(raw) if raw is not None else {}
            # Preserve missing-items fallback and failures for malformed evidence.
            failed_items += sum(item.get("status") == "review"
                                for item in checked.get("items", [checked]))
        return failed_items
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
    from jarvis_native_task_launcher import NativeTaskLauncherConfig, resolve_codex_runtime

    return {
        "NativeTaskLauncherConfig": NativeTaskLauncherConfig,
        "resolve_codex_runtime": resolve_codex_runtime,
    }
