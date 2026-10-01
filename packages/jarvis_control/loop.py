"""Bounded, receipt-driven orchestration over the existing Hold and Monitor APIs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
import os
from urllib.parse import quote
from pathlib import Path
from typing import Any, Mapping, Protocol

from jarvis_runtime.coo_dispatcher_store import ProcessLock
from jarvis_runtime.cancellation import cancel_management, cancellation_path, require_dispatch_open
from .provisioning import output_schema_validator, lane_batch_size, lane_batch_ids, final_answer_mode


ACTIVE = {"accepted", "holding", "running"}
TERMINAL = {"completed", "failed", "interrupted", "cancelled", "canceled", "turn_limit_reached", "blocked"}


def _cleanup_evidence_confirmed(state: Mapping[str, Any]) -> bool:
    """Do not trust legacy cleanup labels without owned-Hold release evidence."""
    for child in state.get("children", []):
        receipt = child.get("last_receipt") or {}
        if not child.get("hold_id"):
            if receipt.get("status") in ACTIVE:
                return False
            continue
        data = receipt.get("data") or {}
        if (receipt.get("status") != "completed"
                or (data.get("lifecycle_status") or data.get("status")) not in TERMINAL
                or data.get("hold_released") is not True
                or data.get("terminal_confirmed") is not True):
            return False
    return True


class LoopRuntime(Protocol):
    def hold(self, **kwargs: Any) -> dict[str, Any]: ...
    def monitor(self, **kwargs: Any) -> dict[str, Any]: ...
    def heartbeat(self, **kwargs: Any) -> dict[str, Any]: ...
    def stop_hold(self, hold_id: str) -> dict[str, Any]: ...
    def close_hold(self, hold_id: str, **kwargs: Any) -> dict[str, Any]: ...


def _aggregate_close_reports(state: dict[str, Any]) -> None:
    close = state.get("close") or {}
    reports = close.get("child_reports") or {}
    children = [child for child in state.get("children", []) if child.get("hold_id")]
    if not children or not all(child["hold_id"] in reports for child in children):
        return
    complete = all(reports[child["hold_id"]].get("report_status") == "completed" for child in children)
    stopped = all(reports[child["hold_id"]].get("management_closed") is True for child in children)
    if not (state.get("cleanup") or {}).get("heartbeat_cancelled"):
        return
    failures = [f"{child['hold_id']}: {reports[child['hold_id']].get('reason') or 'close failed'}"
        for child in children if reports[child["hold_id"]].get("report_status") == "failed"]
    running = any(reports[child["hold_id"]].get("execution_state") == "running"
                  and reports[child["hold_id"]].get("terminal_confirmed") is not True for child in children)
    close.update(report_status="failed" if failures else "completed" if complete else "pending" if running else "unresolved" if stopped else "pending",
        reason="; ".join(failures) or None,
        scheduling_closed=True, execution_state="terminal" if complete else "running" if running else "unknown",
        terminal_confirmed=complete, management_closed=stopped, external_execution_unresolved=stopped and not complete)
    if complete:
        state["cleanup"]["status"] = "completed"
        state["status"] = state["cleanup"]["target"]
    elif running:
        state["status"] = "stopping"
    elif stopped:
        state["status"] = "closed_unconfirmed"


@dataclass(frozen=True)
class LoopResult:
    status: str
    loop_id: str | None
    data: dict[str, Any]
    reason: str | None = None


class LoopStore:
    """Durable control-plane state; no adapter or production runtime state is read."""

    def __init__(self, root: Path) -> None:
        self._root = root

    def load(self, loop_id: str) -> dict[str, Any]:
        try:
            value = json.loads(self._path(loop_id).read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise ValueError("loop state was not found") from exc
        if (not isinstance(value, dict) or value.get("schema") != "jarvis-loop-state/v1"
                or value.get("loop_id") != loop_id):
            raise ValueError("loop state is invalid")
        return value

    def create(self, loop_id: str, state: dict[str, Any]) -> dict[str, Any]:
        if self._path(loop_id).exists():
            return self.load(loop_id)
        self.save(loop_id, state)
        return state

    def exists(self, loop_id: str) -> bool:
        return self._path(loop_id).exists()

    def active_loop_ids(self) -> list[str]:
        if not self._root.is_dir():
            return []
        loop_ids: list[str] = []
        for path in self._root.glob("*/state.json"):
            try:
                state = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(state, Mapping) or state.get("schema") != "jarvis-loop-state/v1":
                continue
            if state.get("status") in {"blocked", "completed", "stopped", "expired", "closed_unconfirmed"}:
                continue
            loop_id = str(state.get("loop_id") or "").strip()
            if loop_id:
                loop_ids.append(loop_id)
        return loop_ids

    def save(self, loop_id: str, state: dict[str, Any]) -> None:
        path = self._path(loop_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        with ProcessLock(path.with_suffix(".lock")):
            # A concurrent tick must not undo a persisted terminal/stop intent.
            if path.exists():
                latest = self.load(loop_id)
                prior_cleanup = latest.get("cleanup") or {}
                incoming_cleanup = state.get("cleanup") or {}
                if prior_cleanup and (
                    not incoming_cleanup or prior_cleanup["target"] != incoming_cleanup.get("target")
                    or (prior_cleanup.get("status") == "completed" and incoming_cleanup.get("status") != "completed"
                        and _cleanup_evidence_confirmed(latest))
                ):
                    state.clear()
                    state.update(latest)
                    return
                prior_close = latest.get("close") or {}
                if prior_close and prior_close.get("close_request_id") != (state.get("close") or {}).get("close_request_id"):
                    state.clear()
                    state.update(latest)
                    return
                if prior_close.get("close_request_id") == (state.get("close") or {}).get("close_request_id") and prior_close.get("child_reports"):
                    state["close"]["child_reports"] = prior_close["child_reports"]
                    for child in state.get("children", []):
                        report = prior_close["child_reports"].get(child.get("hold_id"))
                        if report:
                            child["hold_released"] = report.get("hold_released") is True
                            child["last_receipt"] = {"status": "completed", "data": {
                                **report, "status": report.get("lifecycle_status") or ("cancelled" if report.get("terminal_confirmed") else "unknown")}}
                    _aggregate_close_reports(state)
            self._write_locked(path, state)

    @staticmethod
    def _write_locked(path: Path, state: Mapping[str, Any]) -> None:
        """Caller holds this state's existing lock (including dispatch commit)."""
        temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
        try:
            temporary.write_text(json.dumps(dict(state), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            os.replace(temporary, path)
        finally:
            if temporary.exists():
                temporary.unlink()

    def record_child_close(self, report: Mapping[str, Any]) -> None:
        """Merge one bound owner report atomically; never dispatch or scan history."""
        parent = report.get("parent_close") or {}
        loop_id = str(parent.get("loop_id") or "")
        path = self._path(loop_id)
        with ProcessLock(path.with_suffix(".lock")):
            state = self.load(loop_id)
            close = state.get("close") or {}
            if not close or close.get("close_request_id") != parent.get("close_request_id"):
                return
            child = next((item for item in state.get("children", []) if item.get("hold_id") == report.get("hold_id")), None)
            if child is None:
                return
            reports = close.setdefault("child_reports", {})
            prior = reports.get(report["hold_id"]) or {}
            if prior and (prior.get("request_id") != report.get("request_id") or prior.get("report_status") == "completed"):
                return
            data = (child.get("last_receipt") or {}).get("data") or {}
            if data.get("request_id") and data["request_id"] != report.get("request_id"):
                return
            reports[report["hold_id"]] = dict(report)
            child["hold_released"] = report.get("hold_released") is True
            child["last_receipt"] = {"status": "completed", "data": {
                **report, "status": report.get("lifecycle_status") or ("cancelled" if report.get("terminal_confirmed") else "unknown")}}
            _aggregate_close_reports(state)
            temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
            temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            os.replace(temporary, path)

    def _path(self, loop_id: str) -> Path:
        safe = "".join(char if char.isalnum() or char in "._-" else "_" for char in loop_id).strip("._")
        return self._root / (safe or "loop") / "state.json"


class LoopController:
    """Advance slots only after the existing Monitor returns lifecycle readback."""

    def __init__(self, store: LoopStore) -> None:
        self._store = store

    def start(self, runtime: LoopRuntime, **raw: Any) -> LoopResult:
        try:
            request = _validate_start(raw)
        except ValueError as exc:
            return LoopResult("invalid_request", None, {}, str(exc))
        loop_id = f"loop-{request['request_id']}"
        if self._store.exists(loop_id):
            try:
                existing = self._store.load(loop_id)
            except ValueError as exc:
                return LoopResult("blocked", loop_id, {}, str(exc))
        else:
            existing = None
        if existing is not None:
            return self._result(existing)

        state: dict[str, Any] = {
            "schema": "jarvis-loop-state/v1", "loop_id": loop_id, "request_id": request["request_id"],
            "project": request["project"], "max_rounds": request["max_rounds"], "max_turns": request["max_turns"],
            "auto_continue": request["auto_continue"], "continue_prompt": request["continue_prompt"],
            "turns_per_thread": request["turns_per_thread"],
            "controller_skill": request["controller_skill"], "business_skill": request["business_skill"],
            "model": request["model"], "reasoning_effort": request["reasoning_effort"],
            "notifications": request["notifications"],
            "interval_seconds": request["interval_seconds"], "expires_at": request["expires_at"],
            "target_thread_count": request["target_thread_count"], "status": "acquiring",
            "heartbeat_id": None, "heartbeat": None, "children": [],
        }
        for spec in request["threads"]:
            child = {**spec, "thread_id": spec.get("task_id"), "hold_id": None, "round": 0,
                     "phase": "acquiring", "lifecycle": None, "last_receipt": None}
            child["holder_owns_continuation"] = bool(request["auto_continue"])
            self._acquire(runtime, state, child)
            state["children"].append(child)

        if all(child["phase"] == "blocked" for child in state["children"]):
            state["status"] = "blocked"

        if state["status"] != "blocked":
            heartbeat_id = f"{loop_id}:reconcile"
            heartbeat = runtime.heartbeat(
                action="create", request_id=f"{loop_id}:reconcile-heartbeat",
                source_ref=f"jarvis_loop:{loop_id}:reconcile-heartbeat",
                options=_reconcile_heartbeat_options(
                    loop_id=loop_id, request_id=request["request_id"], heartbeat_id=heartbeat_id,
                    interval_seconds=request["interval_seconds"], expires_at=request["expires_at"],
                ),
            )
            state["heartbeat_id"] = heartbeat_id
            state["heartbeat"] = heartbeat
            if str(heartbeat.get("status") or "").lower() not in {"active", "accepted", "completed"}:
                state["status"] = "blocked"
                state["reason"] = str(heartbeat.get("reason") or "loop reconciliation heartbeat was not accepted")

        if state["status"] != "blocked":
            self._observe(runtime, state)
            self._refresh(state)
        else:
            self._finish(runtime, state, "blocked")
        self._store.create(loop_id, state)
        self._store.save(loop_id, state)
        return self._result(state)

    def _acquire(self, runtime: LoopRuntime, state: Mapping[str, Any], child: dict[str, Any]) -> None:
        require_dispatch_open(self._store._path(state["loop_id"]).parent)
        offset = child.get("completed_rounds", 0)
        key = f"{state['loop_id']}:{child['slot']}" + (f":after-{offset}" if offset else "")
        resume = child["acquire"] == "resume" and not offset
        budget = (_round_limit(child, state["max_rounds"]) - offset
                  if child["holder_owns_continuation"] else state["max_turns"])
        if state.get("turns_per_thread") is not None:
            budget = min(budget, state["turns_per_thread"], _round_limit(child, state["max_rounds"]) - offset)
        child["thread_turn_limit"] = budget
        receipt = runtime.hold(
            request_id=f"{key}:acquire", prompt=child["prompt"],
            source_ref=(f"jarvis_loop_task:{quote(state['loop_id'], safe='')}" if state.get("turns_per_thread") is not None
                        else f"jarvis_loop:{quote(state['loop_id'], safe='')}:{child['slot']}"),
            task_id=child.get("task_id") if resume else None,
            project=None if resume else state["project"],
            title=child.get("title") or (f"Jarvis loop {state['request_id']} {child['slot']}" if offset else None),
            hold_id=None if resume else key,
            model=state["model"], reasoning_effort=state["reasoning_effort"],
            max_turns=budget, auto_continue=child["holder_owns_continuation"],
            continue_prompt=child.get("continue_prompt", child["prompt"] if child.get("lane") else state["continue_prompt"]),
            notifications=state["notifications"], input_binding=_holder_lane_binding(child),
        )
        child["last_receipt"] = receipt
        child["hold_id"] = _nested_text(receipt, "data", "monitor_id") or _nested_text(receipt, "data", "hold_id")
        child["thread_id"] = receipt.get("target_thread_id") or (child.get("task_id") if resume else None)
        child["lifecycle"] = None
        child["phase"] = "acquiring" if receipt.get("status") in ACTIVE and child["hold_id"] else "blocked"

    def tick(self, runtime: LoopRuntime, *, loop_id: str) -> LoopResult:
        return self.status(runtime, loop_id=loop_id)

    def status(self, runtime: LoopRuntime, *, loop_id: str) -> LoopResult:
        try:
            state = self._store.load(loop_id)
        except ValueError as exc:
            return LoopResult("invalid_request", loop_id, {}, str(exc))
        if cancellation_path(self._store._path(loop_id).parent) and not state.get("close"):
            return self.close(runtime, loop_id=loop_id)
        if state.get("status") == "closed_unconfirmed":
            return self._result(state)
        if state.get("close") and not state.get("cleanup"):
            self._finish(runtime, state, "stopped")
            self._store.save(loop_id, state)
            return self._result(state)
        if (state.get("cleanup", {}).get("status") == "pending"
                or (state.get("cleanup", {}).get("status") == "completed" and not _cleanup_evidence_confirmed(state))):
            self._finish(runtime, state, state["cleanup"]["target"])
            self._store.save(loop_id, state)
        elif state["status"] not in {"blocked", "completed", "stopped", "expired"}:
            if _expired(state["expires_at"]):
                self._finish(runtime, state, "expired")
            else:
                self._observe(runtime, state)
                if state["auto_continue"]:
                    self._reconcile_holder_terminals(runtime, state)
                else:
                    self._finalize_observed_terminals(runtime, state)
            self._store.save(loop_id, state)
        return self._result(state)

    def reconcile(self, runtime: LoopRuntime) -> LoopResult:
        """Close Holder-owned terminal slots without scheduling another Worker turn."""
        reconciled: list[dict[str, Any]] = []
        for loop_id in self._store.active_loop_ids():
            result = self.status(runtime, loop_id=loop_id)
            reconciled.append({"loop_id": loop_id, "status": result.status})
        return LoopResult("completed", None, {"reconciled": reconciled})

    @staticmethod
    def preflight_contract(*, allowed_projects: list[str]) -> dict[str, Any]:
        """Describe start inputs without creating Loop, Hold, or heartbeat state."""
        return {
            "allowed_projects": sorted(allowed_projects),
            "start_contract": {
                "required": [
                    "request_id", "project", "prompt", "target_thread_count", "max_rounds", "expires_at",
                ],
                "defaults": {
                    "controller_skill": None,
                    "business_skill": None,
                    "max_turns": 999,
                    "turns_per_thread": None,
                    "continue_prompt": "same as prompt when omitted",
                    "auto_continue": True,
                    "interval_seconds": 1800,
                    "model": "gpt-5.6-luna",
                    "reasoning_effort": "max",
                    "notifications": True,
                },
                "threads": {
                    "item": {
                        "slot": "non-empty unique string",
                        "acquire": "create|resume (default create)",
                        "create_requires": ["title"],
                        "resume_requires": ["task_id"],
                        "lane": {
                            "optional": True,
                            "candidate_ids": "unique positive integers",
                            "batch_size": "optional positive integer, default 1; any size, tail batch uses remaining IDs",
                            "database_path": "non-empty path",
                            "output_boundary": "non-empty path",
                            "result_verification": "receipt_paths by candidate id and explicit terminal_statuses; optional output_schema (Draft 2020-12, document-local refs, no $id)",
                            "lane_identity": "read-only worker slot injected by Loop",
                            "lane_item_count": "read-only total candidate count injected by Loop",
                        },
                    },
                    "count": "must equal target_thread_count when supplied",
                },
            },
        }

    def close(self, runtime: LoopRuntime, *, loop_id: str, request_id: str | None = None) -> LoopResult:
        try:
            state = self._store.load(loop_id)
            marker = cancel_management(self._store._path(loop_id).parent, scope="loop", subject_id=loop_id,
                request_id=state["request_id"], close_request_id=(state.get("close") or {}).get("close_request_id") or request_id or f"close:{loop_id}",
                original=self._store._path(loop_id).read_bytes())
            if state.get("close", {}).get("report_status") not in {"completed", "unresolved"}:
                state.setdefault("close", {"schema": "jarvis-close-report/v1", "loop_id": loop_id,
                    "close_request_id": request_id or f"close:{loop_id}", "requested_at": datetime.now(timezone.utc).isoformat(),
                    "report_status": "pending", "scheduling_closed": True, "terminal_confirmed": False,
                    "cancellation": marker})
                self._store.save(loop_id, state)  # Durable intent before cancel/child effects.
            state.setdefault("close", {}).update(cancellation=marker, scheduling_closed=True)
            self._store.save(loop_id, state)
            if state.get("status") == "closed_unconfirmed":
                return self._result(state)
            if state.get("cleanup", {}).get("status") == "completed" and _cleanup_evidence_confirmed(state):
                state["close"].update(report_status="completed", scheduling_closed=True,
                    execution_state="terminal", terminal_confirmed=True, management_closed=True,
                    external_execution_unresolved=False)
                self._store.save(loop_id, state)
                return self._result(state)
            return self.stop(runtime, loop_id=loop_id)
        except (OSError, ValueError, RuntimeError) as exc:
            if "marker" not in locals():
                return LoopResult("failed", loop_id, {}, str(exc))
            state["status"] = "closed_unconfirmed"
            state.setdefault("close", {}).update(scheduling_closed=True, management_closed=True,
                terminal_confirmed=False, execution_state="unknown", report_status="unresolved",
                cancellation=marker, external_execution_unresolved=True, reason=str(exc))
            try: self._store.save(loop_id, state)
            except (OSError, ValueError, RuntimeError) as save_error:
                state["close"]["report_error"] = str(save_error)
            return self._result(state)

    def stop(self, runtime: LoopRuntime, *, loop_id: str) -> LoopResult:
        try:
            state = self._store.load(loop_id)
        except ValueError as exc:
            return LoopResult("invalid_request", loop_id, {}, str(exc))
        if state.get("cleanup", {}).get("status") == "completed" and _cleanup_evidence_confirmed(state):
            return self._result(state)
        self._finish(runtime, state, state.get("cleanup", {}).get("target", "stopped"))
        self._store.save(loop_id, state)
        return self._result(state)

    def _finish(self, runtime: LoopRuntime, state: dict[str, Any], target: str) -> None:
        """Latch cleanup before effects; all terminal paths drain the same owned Holds."""
        cleanup = state.setdefault("cleanup", {
            "target": target, "status": "pending",
            "host": "retained", "host_reason": "shared_or_unproven_exclusive_ownership",
        })
        if cleanup.get("status") == "completed" and not _cleanup_evidence_confirmed(state):
            cleanup["status"] = "pending"
        state["status"] = "stopping" if target == "stopped" else "finalizing"
        self._store.save(state["loop_id"], state)
        cleanup = state["cleanup"]
        if cleanup.get("status") == "completed":
            return
        if not cleanup.get("heartbeat_cancelled"):
            try:
                state["heartbeat"] = self._cancel(runtime, state)
            except Exception as exc:
                state["heartbeat"] = {"status": "failed", "reason": str(exc)}
            cleanup["heartbeat_cancelled"] = state["heartbeat"].get("status") in {
                "completed", "cancelled", "canceled", "not_required",
            }
        released = True
        management_closed = True
        for child in state["children"]:
            hold_id = child.get("hold_id")
            if not hold_id:
                # Failed acquisition can still have left a persistent queued request.
                released = released and child.get("last_receipt", {}).get("status") not in ACTIVE
                management_closed = management_closed and child.get("last_receipt", {}).get("status") not in ACTIVE
                continue
            if state.get("close"):
                closer = getattr(runtime, "close_hold", None)
                try:
                    child["close_receipt"] = closer(hold_id,
                        request_id=f"{state['loop_id']}:{child['slot']}:close",
                        parent_close={"loop_id": state["loop_id"], "close_request_id": state["close"]["close_request_id"]}
                    ) if callable(closer) else {"status": "unsupported"}
                except Exception as exc:
                    child["close_receipt"] = {"status": "failed", "reason": str(exc)}
                management_closed = management_closed and child["close_receipt"].get("status") in {"closed", "closed_unconfirmed"}
                if child["close_receipt"].get("status") in {"failed", "unsupported"}:
                    # No fallback stop after the child's durable write failed.
                    released = False
                    child["hold_released"] = False
                    continue
            elif child.get("stop_receipt", {}).get("status") != "stop_requested":
                stopper = getattr(runtime, "stop_hold", None)
                child["stop_receipt"] = stopper(hold_id) if callable(stopper) else {"status": "unsupported"}
            try:
                receipt = runtime.monitor(action="status", request_id=f"{state['loop_id']}:{child['slot']}:cleanup",
                                          source_ref=f"jarvis_loop:{state['loop_id']}:{child['slot']}", hold_id=hold_id)
            except Exception as exc:
                receipt = {"status": "requires_readback", "reason": str(exc)}
            child["last_receipt"] = receipt
            data = receipt.get("data") or {}
            lifecycle = data.get("lifecycle_status") or data.get("status")
            child["hold_released"] = (receipt.get("status") == "completed" and lifecycle in TERMINAL
                                      and data.get("hold_released") is True
                                      and data.get("terminal_confirmed") is True)
            released = released and child["hold_released"]
        if released and cleanup.get("heartbeat_cancelled"):
            cleanup["status"] = "completed"
            state["status"] = cleanup["target"]
        if state.get("close"):
            complete = released and cleanup.get("heartbeat_cancelled") is True
            local_closed = (cancellation_path(self._store._path(state["loop_id"]).parent) is not None
                            or (management_closed and cleanup.get("heartbeat_cancelled") is True))
            failures = [f"{item.get('hold_id')}: {item['close_receipt'].get('reason') or item['close_receipt']['status']}"
                for item in state["children"] if (item.get("close_receipt") or {}).get("status") in {"failed", "unsupported", "invalid_request"}]
            owner_running = any((item.get("close_receipt") or {}).get("status") == "closing"
                                for item in state["children"])
            state["close"].update(report_status="completed" if complete else "pending" if owner_running else "unresolved" if local_closed else "failed" if failures else "pending",
                reason="; ".join(failures) or None,
                scheduling_closed=True, execution_state="terminal" if complete else "running" if owner_running else "unknown",
                terminal_confirmed=complete, management_closed=local_closed,
                external_execution_unresolved=local_closed and not complete,
                children=[{"hold_id": item.get("hold_id"), "receipt": item.get("close_receipt")} for item in state["children"]])
            if local_closed and not complete and not owner_running:
                state["status"] = "closed_unconfirmed"

    def _observe(self, runtime: LoopRuntime, state: dict[str, Any]) -> None:
        for child in state["children"]:
            if child.get("phase") in {"completed", "blocked"}:
                continue
            hold_id = child.get("hold_id")
            if not hold_id:
                continue
            receipt = runtime.monitor(action="status", request_id=f"{state['loop_id']}:{child['slot']}:monitor",
                                      source_ref=f"jarvis_loop:{state['loop_id']}:{child['slot']}", hold_id=hold_id)
            child["last_receipt"] = receipt
            if receipt.get("status") != "completed":
                continue
            data = receipt.get("data") or {}
            lifecycle = str(data.get("lifecycle_status") or data.get("status") or "")
            child["lifecycle"] = lifecycle
            child["thread_id"] = receipt.get("target_thread_id") or data.get("thread_id") or child.get("thread_id")
            if lifecycle in ACTIVE:
                child["phase"] = "running"
                if not child["round"]:
                    child["round"] = 1
            elif lifecycle in TERMINAL:
                if child.get("holder_owns_continuation"):
                    try:
                        child["round"] = max(int(child.get("round") or 0), _completed_turns(child, data, state))
                    except (TypeError, ValueError):
                        pass
                child["phase"] = "terminal"

    def _reconcile_holder_terminals(self, runtime: LoopRuntime, state: dict[str, Any]) -> bool:
        changed = False
        for child in state["children"]:
            if not child.get("holder_owns_continuation") or child.get("phase") in {"completed", "blocked"}:
                continue
            hold_id = child.get("hold_id")
            if not hold_id:
                continue
            receipt = runtime.monitor(
                action="status", request_id=f"{state['loop_id']}:{child['slot']}:terminal-monitor",
                source_ref=f"jarvis_loop:{state['loop_id']}:{child['slot']}", hold_id=hold_id,
            )
            if receipt.get("status") != "completed":
                continue
            data = receipt.get("data") or {}
            lifecycle = str(data.get("lifecycle_status") or data.get("status") or "")
            if lifecycle not in TERMINAL:
                continue
            child["last_receipt"] = receipt
            child["lifecycle"] = lifecycle
            child["thread_id"] = receipt.get("target_thread_id") or data.get("thread_id") or child.get("thread_id")
            completed_turns = None
            try:
                completed_turns = _completed_turns(child, data, state)
                child["round"] = max(int(child.get("round") or 0), completed_turns)
            except (TypeError, ValueError):
                pass
            child["phase"] = "completed" if lifecycle in {"completed", "turn_limit_reached"} else "blocked"
            if state.get("turns_per_thread") is not None and child["phase"] == "completed":
                if data.get("hold_released") is not True or data.get("terminal_confirmed") is not True:
                    child["phase"] = "terminal"
                    continue
                if completed_turns != child.get("completed_rounds", 0) + child["thread_turn_limit"]:
                    child["phase"] = "blocked"
                elif child.get("lane"):
                    self._check_bound_terminal(child, {**data, "total_turn_count": child["round"]}, rounds=child["round"])
                if child["phase"] == "completed" and child["round"] < _round_limit(child, state["max_rounds"]):
                    # Serialize the final close check, dispatch and child binding with close intent.
                    path = self._store._path(state["loop_id"])
                    with ProcessLock(path.with_suffix(".lock")):
                        latest = self._store.load(state["loop_id"])
                        if (cancellation_path(path.parent) or latest.get("close") or latest.get("cleanup")):
                            state.clear()
                            state.update(latest)
                            return False
                        child["completed_rounds"] = child["round"]
                        self._acquire(runtime, state, child)
                        self._store._write_locked(path, state)
                changed = True
                continue
            if child.get("lane"):
                self._check_bound_terminal(child, data)
            changed = True
        if not changed:
            return False
        if all(child.get("phase") in {"completed", "blocked"} for child in state["children"]):
            self._finish(runtime, state, "blocked" if any(child.get("phase") == "blocked"
                         for child in state["children"]) else "completed")
        else:
            self._refresh(state)
        return True

    def _finalize_observed_terminals(self, runtime: LoopRuntime, state: dict[str, Any]) -> None:
        for child in state["children"]:
            if child.get("phase") != "terminal":
                continue
            child["phase"] = "completed" if child.get("lifecycle") in {"completed", "turn_limit_reached"} else "blocked"
            if child.get("lane"):
                self._check_bound_terminal(child, child.get("last_receipt", {}).get("data") or {})
        if all(child.get("phase") in {"completed", "blocked"} for child in state["children"]):
            self._finish(runtime, state, "blocked" if any(child.get("phase") == "blocked"
                         for child in state["children"]) else "completed")
        else:
            self._refresh(state)

    def _refresh(self, state: dict[str, Any]) -> None:
        if state.get("status") == "blocked":
            return
        if any(child.get("phase") == "terminal" for child in state["children"]):
            state["status"] = "resume_pending"
        elif any(child.get("phase") == "acquiring" for child in state["children"]):
            state["status"] = "acquiring"
        else:
            state["status"] = "running"

    @staticmethod
    def _check_bound_terminal(child: dict[str, Any], data: Mapping[str, Any], *, rounds: int | None = None) -> None:
        if "result_verification" not in child["lane"]:
            child["business_status"] = "legacy_unverified"
            return
        verification = data.get("output_verification") or {}
        candidate_ids = child["lane"]["candidate_ids"]
        size = lane_batch_size(child["lane"])
        rounds = rounds or (len(candidate_ids) + size - 1) // size
        identity_matches = (verification.get("candidate_id") == candidate_ids[rounds - 1] if size == 1 else
                            verification.get("candidate_ids") == lane_batch_ids(child["lane"], rounds))
        if (verification.get("status") in {"verified", "review"}
                and identity_matches and data.get("total_turn_count") == rounds):
            if final_answer_mode(child["lane"]):
                if (verification.get("terminal_status") != "received" or verification.get("review_needed") is not True
                        or verification.get("verification_status") != "not_verified"):
                    child["business_status"] = "unverified"
                    child["phase"] = "blocked"
                    return
                child["intake_status"] = "received"
                child["business_status"] = "review_needed"
            else:
                child["business_status"] = ("review" if verification["status"] == "review"
                    or data.get("scheduler_failed_items") or child.get("business_status") == "review"
                    else "mechanically_verified")
        else:
            child["business_status"] = "unverified"
            child["phase"] = "blocked"

    def _cancel(self, runtime: LoopRuntime, state: Mapping[str, Any]) -> dict[str, Any]:
        if not state.get("heartbeat_id") or not state.get("heartbeat"):
            return {"status": "not_required"}
        return runtime.heartbeat(action="cancel", request_id=f"{state['loop_id']}:stop",
                                 source_ref=f"jarvis_loop:{state['loop_id']}", heartbeat_id=state["heartbeat_id"])

    @staticmethod
    def _result(state: Mapping[str, Any]) -> LoopResult:
        data = {key: state.get(key) for key in ("loop_id", "project", "status", "target_thread_count", "heartbeat_id", "heartbeat", "max_rounds", "max_turns", "turns_per_thread", "auto_continue", "continue_prompt", "controller_skill", "business_skill", "model", "reasoning_effort", "notifications", "interval_seconds", "expires_at", "children", "cleanup", "close")}
        data["business_status"] = ("review" if any(child.get("business_status") in {"review", "unverified"}
            or child.get("phase") == "blocked" for child in state["children"]) else "not_evaluated")
        return LoopResult(str(state["status"]), str(state["loop_id"]), data)


def _validate_start(raw: Mapping[str, Any]) -> dict[str, Any]:
    required = ("request_id", "project", "prompt", "target_thread_count", "max_rounds", "expires_at")
    missing = [name for name in required if raw.get(name) in (None, "")]
    if missing:
        raise ValueError("required loop fields: " + ", ".join(missing))
    target_count, max_rounds = raw["target_thread_count"], raw["max_rounds"]
    if not isinstance(target_count, int) or target_count < 1:
        raise ValueError("target_thread_count must be a positive integer")
    if not isinstance(max_rounds, int) or max_rounds < 1:
        raise ValueError("max_rounds must be a positive integer")
    max_turns = raw.get("max_turns", 999)
    turns_per_thread = raw.get("turns_per_thread")
    if turns_per_thread is not None and (type(turns_per_thread) is not int or turns_per_thread < 1):
        raise ValueError("turns_per_thread must be a positive integer")
    interval = raw.get("interval_seconds", 1800)
    if max_turns is None:
        max_turns = 999
    if interval is None:
        interval = 1800
    if not isinstance(max_turns, int) or max_turns < 1:
        raise ValueError("max_turns must be a positive integer")
    if not isinstance(interval, int) or interval < 1:
        raise ValueError("interval_seconds must be a positive integer")
    auto_continue = raw.get("auto_continue", True)
    if auto_continue is None:
        auto_continue = True
    if not isinstance(auto_continue, bool):
        raise ValueError("auto_continue must be a boolean")
    expires = _parse_time(raw["expires_at"])
    if expires <= datetime.now(timezone.utc):
        raise ValueError("expires_at must be in the future")
    controller_skill = str(raw.get("controller_skill") or "").strip()
    business_skill = str(raw.get("business_skill") or "").strip()
    task_prompt = str(raw["prompt"]).strip()
    if not task_prompt:
        raise ValueError("prompt is required")
    continuation = raw.get("continue_prompt")
    if continuation is not None and (not isinstance(continuation, str) or not continuation.strip()):
        raise ValueError("continue_prompt must be a non-empty string")
    threads = raw.get("threads")
    if threads is None:
        title = str(raw.get("title") or f"Jarvis loop {raw['request_id']}").strip()
        threads = [{"slot": f"worker-{number}", "acquire": "create", "title": title}
                   for number in range(1, target_count + 1)]
    if not isinstance(threads, list) or len(threads) != target_count:
        raise ValueError("threads must exactly match target_thread_count")
    normalized: list[dict[str, Any]] = []
    slots: set[str] = set()
    for raw_child in threads:
        if not isinstance(raw_child, Mapping):
            raise ValueError("each thread must be an object")
        slot, acquire = str(raw_child.get("slot") or "").strip(), str(raw_child.get("acquire") or "create").strip()
        if "prompt" in raw_child:
            raise ValueError("thread prompt is not supported; use the loop prompt")
        if not slot or slot in slots or acquire not in {"create", "resume"}:
            raise ValueError("each thread requires a unique slot and acquire=create|resume")
        slots.add(slot)
        child: dict[str, Any] = {"slot": slot, "acquire": acquire}
        lane = raw_child.get("lane")
        if lane is not None:
            if not isinstance(lane, Mapping):
                raise ValueError("thread lane must be an object")
            candidate_ids = lane.get("candidate_ids")
            database_path = str(lane.get("database_path") or "").strip()
            output_boundary = str(lane.get("output_boundary") or "").strip()
            if (not isinstance(candidate_ids, list) or not candidate_ids
                    or any(type(value) is not int or value < 1 for value in candidate_ids)
                    or len(set(candidate_ids)) != len(candidate_ids)
                    or not database_path or not output_boundary):
                raise ValueError("thread lane requires unique positive candidate_ids, database_path, and output_boundary")
            batch_size = lane_batch_size(lane)
            rounds = (len(candidate_ids) + batch_size - 1) // batch_size
            if max_rounds < rounds:
                raise ValueError("max_rounds must cover every candidate in each thread lane")
            verification = lane.get("result_verification")
            if (not isinstance(verification, Mapping)
                    or not isinstance(verification.get("receipt_paths"), Mapping)
                    or set(verification["receipt_paths"]) != {str(value) for value in candidate_ids}
                    or any(not isinstance(value, str) or not value.strip() for value in verification["receipt_paths"].values())
                    or not isinstance(verification.get("terminal_statuses"), list)
                    or not verification["terminal_statuses"]
                    or any(not isinstance(value, str) or not value.strip() or value.casefold() in
                           {"accepted", "holding", "running", "pending", "inprogress", "queued"}
                           for value in verification["terminal_statuses"])):
                raise ValueError("new bound lanes require result_verification receipt_paths and terminal_statuses")
            if "output_schema" in verification:
                output_schema_validator(verification["output_schema"])
            final_answer_mode(lane)
            if batch_size > 1:
                if len(set(verification["receipt_paths"].values())) != len(candidate_ids):
                    raise ValueError("batch lanes require separate receipt_paths for every candidate")
            child["lane"] = {"candidate_ids": list(candidate_ids), "database_path": database_path,
                             "output_boundary": output_boundary, "result_verification": dict(verification),
                             **({"batch_size": batch_size} if "batch_size" in lane else {})}
        child["prompt"] = _worker_prompt(task_prompt, controller_skill, business_skill, lane_bound="lane" in child,
                                         batched=lane_batch_size(child.get("lane") or {}) > 1,
                                         final_answer=final_answer_mode(child.get("lane") or {}))
        if continuation is not None:
            child["continue_prompt"] = _worker_prompt(continuation.strip(), controller_skill, business_skill,
                lane_bound="lane" in child, batched=lane_batch_size(child.get("lane") or {}) > 1,
                final_answer=final_answer_mode(child.get("lane") or {}))
        if acquire == "create":
            child["title"] = str(raw_child.get("title") or raw.get("title") or "").strip()
            if not child["title"]:
                raise ValueError("create thread requires title")
        else:
            child["task_id"] = str(raw_child.get("task_id") or "").strip()
            if not child["task_id"]:
                raise ValueError("resume thread requires task_id")
        normalized.append(child)
    seen_candidates: set[tuple[str, int]] = set()
    for child in normalized:
        lane = child.get("lane")
        if lane is not None:
            keys = {(str(Path(lane["database_path"]).resolve()).casefold(), value) for value in lane["candidate_ids"]}
            if seen_candidates & keys:
                raise ValueError("thread lanes must not overlap candidate_ids in the same database")
            seen_candidates.update(keys)
    notifications = raw.get("notifications", True)
    if notifications is None:
        notifications = True
    if isinstance(notifications, bool):
        notifications = {"milestones": [], "terminal": notifications}
    elif isinstance(notifications, Mapping):
        notifications = dict(notifications)
    else:
        raise ValueError("notifications must be a boolean or object")
    continue_prompt = _worker_prompt(continuation.strip() if continuation is not None else task_prompt, controller_skill, business_skill,
                                     lane_bound=any("lane" in child for child in normalized),
                                     batched=any(lane_batch_size(child.get("lane") or {}) > 1 for child in normalized))
    return {"request_id": str(raw["request_id"]).strip(), "project": str(raw["project"]).strip(), "threads": normalized,
            "target_thread_count": target_count, "max_rounds": max_rounds, "max_turns": max_turns,
            "turns_per_thread": turns_per_thread,
            "auto_continue": auto_continue, "continue_prompt": continue_prompt,
            "controller_skill": controller_skill, "business_skill": business_skill,
            "model": str(raw.get("model") or "gpt-5.6-luna").strip() or "gpt-5.6-luna",
            "reasoning_effort": str(raw.get("reasoning_effort") or "max").strip() or "max",
            "notifications": notifications, "interval_seconds": interval, "expires_at": expires.isoformat()}


def _worker_prompt(task_prompt: str, controller_skill: str, business_skill: str, *, lane_bound: bool, batched: bool = False, final_answer: bool = False) -> str:
    binding_rule = (
        "每个 Worker 回合仅处理 binding 中的一个任务项；安全写回后输出结构化单项回执并等待下一回合，"
        "不得遍历、预取、并行处理或宣称整条 lane 已完成。\n"
        "按 result_verification 指定位置保存现有 JSON 正式回执，output_path 指向已保存的 JSON 结果；"
        "两者均须包含本候选 candidate_id、注入的 request_id、turn_number 和约定终态 status。\n"
        if lane_bound else ""
    )
    if lane_bound and batched:
        binding_rule = (
            "每个 Worker 回合仅处理本回合 binding.candidate_ids 中的全部真实任务项 ID，不得预取或处理下批。\n"
            "每个任务项分别按 receipt_paths 保存 JSON 正式回执及 output_path 指向的 JSON 结果；"
            "两者均包含该任务项 candidate_id、本回合 request_id、turn_number 和约定终态 status。"
            "output_schema 仍逐项校验，不校验整批包装。\n"
            "全部任务项产物和正式回执完成后结束本回合，Holder 按真实 ID 覆盖生成并保存本轮整批核验回执。"
            "缺项、错项、重复或部分失败不得宣称整批完成；尾批按实际注入数量处理。\n"
        )
    if final_answer:
        binding_rule = (
            "每回合只处理 binding.candidate_ids 中的一个任务项，不预取或处理其他项。\n"
            "唯一交付是最终回复中的 JSON，保持发现原貌；业务字段可省略，缺来源不阻止接收。"
            "不要写文件、运行收尾脚本、填写 QA 或正式回执；ID 与运行身份由 Holder 绑定。"
            "Holder 自动保留原文、保存 JSON 和接收回执并推进；接收不代表业务核实通过。\n"
        )
    if lane_bound:
        binding_rule += ("仅身份无法确认、串公司、越界或重复执行风险等运行安全异常，单独输出 JARVIS_RUN_STATUS: blocked；"
                         "普通字段、计数、枚举或业务结果失败使用 failed/review，不输出该运行安全控制行。\n")
    rules = []
    if controller_skill and not final_answer:
        rules.append(f"执行、续跑和回执规则，必须严格遵守 ${controller_skill}。")
    if binding_rule:
        rules.append(binding_rule.rstrip())
    if business_skill:
        rules.append(f"处理任务项并完成本次工作，必须严格遵守 ${business_skill}。")
    rule_text = "\n".join(rules)
    rules_prefix = f"{rule_text}\n\n" if rule_text else ""
    return f"你是本次 Jarvis Worker。\n\n{rules_prefix}任务：{task_prompt}"


def _holder_lane_binding(child: Mapping[str, Any]) -> dict[str, Any] | None:
    """Persist a finite lane; Holder exposes only the current slice to Workers."""
    lane = child.get("lane")
    if not isinstance(lane, Mapping):
        return None
    candidate_ids = lane.get("candidate_ids")
    if not isinstance(candidate_ids, list):
        return None
    candidate_ids = candidate_ids[child.get("completed_rounds", 0) * lane_batch_size(lane):]
    return {
        "candidate_ids": list(candidate_ids),
        "database_path": lane["database_path"],
        "output_boundary": lane["output_boundary"],
        "lane_identity": child["slot"],
        "lane_item_count": len(candidate_ids),
        **({"batch_size": lane["batch_size"]} if "batch_size" in lane else {}),
        **({"result_verification": {**lane["result_verification"], "receipt_paths": {
            str(value): lane["result_verification"]["receipt_paths"][str(value)] for value in candidate_ids
        }}} if "result_verification" in lane else {}),
    }


def _completed_turns(child: Mapping[str, Any], data: Mapping[str, Any], state: Mapping[str, Any]) -> int:
    if state.get("turns_per_thread") is not None:
        return child.get("completed_rounds", 0) + int(data.get("session_turn_count", data.get("turn_count", data.get("total_turn_count"))) or 0)
    return int(data.get("total_turn_count") or 0)


def _round_limit(child: Mapping[str, Any], default: int) -> int:
    lane = child.get("lane")
    if isinstance(lane, Mapping) and isinstance(lane.get("candidate_ids"), list):
        size = lane_batch_size(lane)
        return (len(lane["candidate_ids"]) + size - 1) // size
    return default


def _nested_text(value: Mapping[str, Any], *keys: str) -> str | None:
    current: Any = value
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    text = str(current or "").strip()
    return text or None


def _parse_time(value: object) -> datetime:
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("expires_at must be an ISO timestamp") from exc
    return result.replace(tzinfo=timezone.utc) if result.tzinfo is None else result.astimezone(timezone.utc)


def _expired(value: object) -> bool:
    return _parse_time(value) <= datetime.now(timezone.utc)


def _reconcile_heartbeat_options(
    *, loop_id: str, request_id: str, heartbeat_id: str, interval_seconds: int, expires_at: str
) -> dict[str, Any]:
    remaining_seconds = max((_parse_time(expires_at) - datetime.now(timezone.utc)).total_seconds(), 0)
    return {
        "heartbeat_id": heartbeat_id,
        "name": f"Jarvis loop reconciliation {loop_id}",
        "function": "JarvisControl.loop_tick",
        "arguments": {"loop_id": loop_id},
        "interval_seconds": interval_seconds,
        "max_runs": max(1, math.ceil(remaining_seconds / interval_seconds) + 1),
        "expires_at": expires_at,
        "source_event_key": f"jarvis_loop:{loop_id}:reconcile",
        "confirmation_evidence": f"jarvis_loop.start:{request_id}",
    }
