"""Bounded, receipt-driven orchestration over the existing Hold and Monitor APIs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
from typing import Any, Mapping, Protocol

from jarvis_runtime.coo_dispatcher_store import ProcessLock
from .provisioning import output_schema_validator, lane_batch_size, lane_batch_ids


ACTIVE = {"accepted", "holding", "running"}
TERMINAL = {"completed", "failed", "interrupted", "cancelled", "canceled", "turn_limit_reached", "blocked"}


class LoopRuntime(Protocol):
    def hold(self, **kwargs: Any) -> dict[str, Any]: ...
    def monitor(self, **kwargs: Any) -> dict[str, Any]: ...
    def heartbeat(self, **kwargs: Any) -> dict[str, Any]: ...
    def stop_hold(self, hold_id: str) -> dict[str, Any]: ...


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
        if not isinstance(value, dict) or value.get("schema") != "jarvis-loop-state/v1":
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
            if state.get("status") in {"blocked", "completed", "stopped", "expired"}:
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
                    or (prior_cleanup.get("status") == "completed" and incoming_cleanup.get("status") != "completed")
                ):
                    state.clear()
                    state.update(latest)
                    return
            temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
            try:
                temporary.write_text(json.dumps(dict(state), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
                os.replace(temporary, path)
            finally:
                if temporary.exists():
                    temporary.unlink()

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
            receipt = runtime.hold(
                request_id=f"{loop_id}:{child['slot']}:acquire", prompt=child["prompt"],
                source_ref=f"jarvis_loop:{loop_id}:{child['slot']}", task_id=child.get("task_id"),
                project=request["project"] if child["acquire"] == "create" else None,
                title=child.get("title"), hold_id=None if child["acquire"] == "resume" else f"{loop_id}:{child['slot']}",
                model=request["model"], reasoning_effort=request["reasoning_effort"],
                max_turns=_round_limit(child, request["max_rounds"]) if child["holder_owns_continuation"] else request["max_turns"],
                auto_continue=child["holder_owns_continuation"],
                continue_prompt=child["prompt"] if child.get("lane") else request["continue_prompt"], notifications=request["notifications"],
                input_binding=_holder_lane_binding(child),
            )
            child["last_receipt"] = receipt
            child["hold_id"] = _nested_text(receipt, "data", "monitor_id") or _nested_text(receipt, "data", "hold_id")
            child["thread_id"] = receipt.get("target_thread_id") or child["thread_id"]
            if receipt.get("status") not in ACTIVE or not child["hold_id"]:
                child["phase"] = "blocked"
                state["status"] = "blocked"
            state["children"].append(child)

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

    def tick(self, runtime: LoopRuntime, *, loop_id: str) -> LoopResult:
        return self.status(runtime, loop_id=loop_id)

    def status(self, runtime: LoopRuntime, *, loop_id: str) -> LoopResult:
        try:
            state = self._store.load(loop_id)
        except ValueError as exc:
            return LoopResult("invalid_request", loop_id, {}, str(exc))
        if state.get("cleanup", {}).get("status") == "pending":
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

    def stop(self, runtime: LoopRuntime, *, loop_id: str) -> LoopResult:
        try:
            state = self._store.load(loop_id)
        except ValueError as exc:
            return LoopResult("invalid_request", loop_id, {}, str(exc))
        if state.get("cleanup", {}).get("status") == "completed":
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
        state["status"] = "stopping" if target == "stopped" else "finalizing"
        self._store.save(state["loop_id"], state)
        cleanup = state["cleanup"]
        if cleanup.get("status") == "completed":
            return
        if not cleanup.get("heartbeat_cancelled"):
            state["heartbeat"] = self._cancel(runtime, state)
            cleanup["heartbeat_cancelled"] = state["heartbeat"].get("status") in {
                "completed", "cancelled", "canceled", "not_required",
            }
        released = True
        for child in state["children"]:
            hold_id = child.get("hold_id")
            if not hold_id:
                # Failed acquisition can still have left a persistent queued request.
                released = released and child.get("last_receipt", {}).get("status") not in ACTIVE
                continue
            if child.get("stop_receipt", {}).get("status") != "stop_requested":
                stopper = getattr(runtime, "stop_hold", None)
                child["stop_receipt"] = stopper(hold_id) if callable(stopper) else {"status": "unsupported"}
            receipt = runtime.monitor(action="status", request_id=f"{state['loop_id']}:{child['slot']}:cleanup",
                                      source_ref=f"jarvis_loop:{state['loop_id']}:{child['slot']}", hold_id=hold_id)
            child["last_receipt"] = receipt
            data = receipt.get("data") or {}
            lifecycle = data.get("lifecycle_status") or data.get("status")
            child["hold_released"] = (receipt.get("status") == "completed" and lifecycle in TERMINAL
                                      and data.get("hold_released", True) is True
                                      and data.get("terminal_confirmed", True) is True)
            released = released and child["hold_released"]
        if released and cleanup.get("heartbeat_cancelled"):
            cleanup["status"] = "completed"
            state["status"] = cleanup["target"]

    def _observe(self, runtime: LoopRuntime, state: dict[str, Any]) -> None:
        for child in state["children"]:
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
                        child["round"] = max(int(child.get("round") or 0), int(data.get("total_turn_count") or 0))
                    except (TypeError, ValueError):
                        pass
                child["phase"] = "terminal"

    def _reconcile_holder_terminals(self, runtime: LoopRuntime, state: dict[str, Any]) -> bool:
        changed = False
        for child in state["children"]:
            if not child.get("holder_owns_continuation") or child.get("phase") == "completed":
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
            try:
                child["round"] = max(int(child.get("round") or 0), int(data.get("total_turn_count") or 0))
            except (TypeError, ValueError):
                pass
            child["phase"] = "completed" if lifecycle in {"completed", "turn_limit_reached"} else "blocked"
            if child.get("lane"):
                self._check_bound_terminal(child, data)
            changed = True
        if not changed:
            return False
        if any(child.get("phase") == "blocked" for child in state["children"]):
            self._finish(runtime, state, "blocked")
        elif all(child.get("phase") == "completed" for child in state["children"]):
            self._finish(runtime, state, "completed")
        return True

    def _finalize_observed_terminals(self, runtime: LoopRuntime, state: dict[str, Any]) -> None:
        for child in state["children"]:
            if child.get("phase") != "terminal":
                continue
            child["phase"] = "completed" if child.get("lifecycle") in {"completed", "turn_limit_reached"} else "blocked"
            if child.get("lane"):
                self._check_bound_terminal(child, child.get("last_receipt", {}).get("data") or {})
        if any(child.get("phase") == "blocked" for child in state["children"]):
            self._finish(runtime, state, "blocked")
        elif all(child.get("phase") == "completed" for child in state["children"]):
            self._finish(runtime, state, "completed")
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
    def _check_bound_terminal(child: dict[str, Any], data: Mapping[str, Any]) -> None:
        if "result_verification" not in child["lane"]:
            child["business_status"] = "legacy_unverified"
            return
        verification = data.get("output_verification") or {}
        candidate_ids = child["lane"]["candidate_ids"]
        size = lane_batch_size(child["lane"])
        rounds = (len(candidate_ids) + size - 1) // size
        identity_matches = (verification.get("candidate_id") == candidate_ids[-1] if size == 1 else
                            verification.get("candidate_ids") == lane_batch_ids(child["lane"], rounds))
        if (verification.get("status") == "verified"
                and identity_matches and data.get("total_turn_count") == rounds):
            child["business_status"] = "mechanically_verified"
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
        data = {key: state.get(key) for key in ("loop_id", "project", "status", "target_thread_count", "heartbeat_id", "heartbeat", "max_rounds", "max_turns", "auto_continue", "continue_prompt", "controller_skill", "business_skill", "model", "reasoning_effort", "notifications", "interval_seconds", "expires_at", "children", "cleanup")}
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
            if batch_size > 1:
                if len(set(verification["receipt_paths"].values())) != len(candidate_ids):
                    raise ValueError("batch lanes require separate receipt_paths for every candidate")
            child["lane"] = {"candidate_ids": list(candidate_ids), "database_path": database_path,
                             "output_boundary": output_boundary, "result_verification": dict(verification),
                             **({"batch_size": batch_size} if "batch_size" in lane else {})}
        child["prompt"] = _worker_prompt(task_prompt, controller_skill, business_skill, lane_bound="lane" in child,
                                         batched=lane_batch_size(child.get("lane") or {}) > 1)
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
    continue_prompt = _worker_prompt(task_prompt, controller_skill, business_skill,
                                     lane_bound=any("lane" in child for child in normalized),
                                     batched=any(lane_batch_size(child.get("lane") or {}) > 1 for child in normalized))
    return {"request_id": str(raw["request_id"]).strip(), "project": str(raw["project"]).strip(), "threads": normalized,
            "target_thread_count": target_count, "max_rounds": max_rounds, "max_turns": max_turns,
            "auto_continue": auto_continue, "continue_prompt": continue_prompt,
            "controller_skill": controller_skill, "business_skill": business_skill,
            "model": str(raw.get("model") or "gpt-5.6-luna").strip() or "gpt-5.6-luna",
            "reasoning_effort": str(raw.get("reasoning_effort") or "max").strip() or "max",
            "notifications": notifications, "interval_seconds": interval, "expires_at": expires.isoformat()}


def _worker_prompt(task_prompt: str, controller_skill: str, business_skill: str, *, lane_bound: bool, batched: bool = False) -> str:
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
    rules = []
    if controller_skill:
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
    return {
        "candidate_ids": list(candidate_ids),
        "database_path": lane["database_path"],
        "output_boundary": lane["output_boundary"],
        "lane_identity": child["slot"],
        "lane_item_count": len(candidate_ids),
        **({"batch_size": lane["batch_size"]} if "batch_size" in lane else {}),
        **({"result_verification": dict(lane["result_verification"])} if "result_verification" in lane else {}),
    }


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
