"""Bounded, receipt-driven orchestration over the existing Hold and Monitor APIs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
from typing import Any, Mapping, Protocol


ACTIVE = {"accepted", "holding", "running"}
TERMINAL = {"completed", "failed", "interrupted", "cancelled", "canceled", "turn_limit_reached"}


class LoopRuntime(Protocol):
    def hold(self, **kwargs: Any) -> dict[str, Any]: ...
    def monitor(self, **kwargs: Any) -> dict[str, Any]: ...
    def heartbeat(self, **kwargs: Any) -> dict[str, Any]: ...


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

    def save(self, loop_id: str, state: Mapping[str, Any]) -> None:
        path = self._path(loop_id)
        path.parent.mkdir(parents=True, exist_ok=True)
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
            "heartbeat_id": f"heartbeat-{loop_id}", "heartbeat": None, "children": [],
        }
        for spec in request["threads"]:
            child = {**spec, "thread_id": spec.get("task_id"), "hold_id": None, "round": 0,
                     "phase": "acquiring", "lifecycle": None, "last_receipt": None}
            receipt = runtime.hold(
                request_id=f"{loop_id}:{child['slot']}:acquire", prompt=child["prompt"],
                source_ref=f"jarvis_loop:{loop_id}:{child['slot']}", task_id=child.get("task_id"),
                project=request["project"] if child["acquire"] == "create" else None,
                title=child.get("title"), hold_id=f"{loop_id}:{child['slot']}",
                model=request["model"], reasoning_effort=request["reasoning_effort"],
                max_turns=request["max_turns"], auto_continue=False,
                continue_prompt=request["continue_prompt"], notifications=request["notifications"],
                input_binding=_current_turn_binding(child, round_number=1),
            )
            child["last_receipt"] = receipt
            child["hold_id"] = _nested_text(receipt, "data", "monitor_id") or _nested_text(receipt, "data", "hold_id")
            child["thread_id"] = receipt.get("target_thread_id") or child["thread_id"]
            if receipt.get("status") not in ACTIVE or not child["hold_id"]:
                child["phase"] = "blocked"
                state["status"] = "blocked"
            state["children"].append(child)

        if state["status"] != "blocked":
            self._observe(runtime, state)
            heartbeat = runtime.heartbeat(
                action="create", request_id=f"{loop_id}:heartbeat", source_ref=f"jarvis_loop:{loop_id}",
                heartbeat_id=state["heartbeat_id"], options={
                    "name": f"Jarvis loop {loop_id}", "function": "JarvisControl.loop_tick",
                    "arguments": {"loop_id": loop_id}, "interval_seconds": request["interval_seconds"],
                    "start_immediately": False, "max_runs": _run_limit(request["expires_at"], request["interval_seconds"]),
                    "expires_at": request["expires_at"], "source_event_key": request["request_id"],
                    "confirmation_evidence": f"jarvis_loop:{request['request_id']}",
                },
            )
            state["heartbeat"] = heartbeat
            if heartbeat.get("status") not in {"active", "accepted", "completed"}:
                state["status"] = "blocked"
            else:
                self._refresh(state)
        self._store.create(loop_id, state)
        self._store.save(loop_id, state)
        return self._result(state)

    def tick(self, runtime: LoopRuntime, *, loop_id: str) -> LoopResult:
        try:
            state = self._store.load(loop_id)
        except ValueError as exc:
            return LoopResult("invalid_request", loop_id, {}, str(exc))
        if state["status"] in {"blocked", "completed", "stopped", "expired"}:
            return self._result(state)
        if _expired(state["expires_at"]):
            state["status"] = "expired"
            state["heartbeat"] = self._cancel(runtime, state)
            self._store.save(loop_id, state)
            return self._result(state)
        self._observe(runtime, state)
        for child in state["children"]:
            if child.get("phase") != "terminal":
                continue
            if child.get("lifecycle") not in {"completed", "turn_limit_reached"}:
                child["phase"] = "blocked"; state["status"] = "blocked"; continue
            if not state["auto_continue"]:
                child["phase"] = "completed"; continue
            lane = child.get("lane")
            round_limit = len(lane["candidate_ids"]) if isinstance(lane, Mapping) else int(state["max_rounds"])
            if int(child["round"]) >= round_limit:
                child["phase"] = "completed"; continue
            if not child.get("thread_id") or not child.get("hold_id"):
                child["phase"] = "blocked"; state["status"] = "blocked"; continue
            next_round = int(child["round"]) + 1
            receipt = runtime.hold(
                request_id=f"{loop_id}:{child['slot']}:round-{next_round}", task_id=child["thread_id"],
                prompt=state["continue_prompt"], source_ref=f"jarvis_loop:{loop_id}:{child['slot']}",
                model=state["model"], reasoning_effort=state["reasoning_effort"],
                hold_id=child["hold_id"], max_turns=state["max_turns"], auto_continue=False,
                continue_prompt=state["continue_prompt"], notifications=state["notifications"],
                input_binding=_current_turn_binding(child, round_number=next_round),
            )
            child["last_receipt"] = receipt
            if receipt.get("status") in ACTIVE:
                child["round"] = next_round; child["phase"] = "running"
            else:
                child["phase"] = "blocked"; state["status"] = "blocked"
        if all(child.get("phase") == "completed" for child in state["children"]):
            state["status"] = "completed"; state["heartbeat"] = self._cancel(runtime, state)
        else:
            self._refresh(state)
        self._store.save(loop_id, state)
        return self._result(state)

    def status(self, *, loop_id: str) -> LoopResult:
        try:
            return self._result(self._store.load(loop_id))
        except ValueError as exc:
            return LoopResult("invalid_request", loop_id, {}, str(exc))

    @staticmethod
    def preflight_contract(*, allowed_projects: list[str]) -> dict[str, Any]:
        """Describe start inputs without creating Loop, Hold, or heartbeat state."""
        return {
            "allowed_projects": sorted(allowed_projects),
            "start_contract": {
                "required": [
                    "request_id", "project", "prompt", "business_skill", "target_thread_count", "max_rounds", "expires_at",
                ],
                "defaults": {
                    "controller_skill": "jarvis-run-controller",
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
                            "database_path": "non-empty path",
                            "output_boundary": "non-empty path",
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
        state["status"] = "stopped"; state["heartbeat"] = self._cancel(runtime, state)
        self._store.save(loop_id, state)
        return self._result(state)

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
                child["phase"] = "terminal"

    def _refresh(self, state: dict[str, Any]) -> None:
        if state.get("status") == "blocked":
            return
        if any(child.get("phase") == "terminal" for child in state["children"]):
            state["status"] = "resume_pending"
        elif any(child.get("phase") == "acquiring" for child in state["children"]):
            state["status"] = "acquiring"
        else:
            state["status"] = "running"

    def _cancel(self, runtime: LoopRuntime, state: Mapping[str, Any]) -> dict[str, Any]:
        return runtime.heartbeat(action="cancel", request_id=f"{state['loop_id']}:stop",
                                 source_ref=f"jarvis_loop:{state['loop_id']}", heartbeat_id=state["heartbeat_id"])

    @staticmethod
    def _result(state: Mapping[str, Any]) -> LoopResult:
        data = {key: state.get(key) for key in ("loop_id", "project", "status", "target_thread_count", "heartbeat_id", "heartbeat", "max_rounds", "max_turns", "auto_continue", "continue_prompt", "controller_skill", "business_skill", "model", "reasoning_effort", "notifications", "interval_seconds", "expires_at", "children")}
        return LoopResult(str(state["status"]), str(state["loop_id"]), data)


def _validate_start(raw: Mapping[str, Any]) -> dict[str, Any]:
    required = ("request_id", "project", "prompt", "business_skill", "target_thread_count", "max_rounds", "expires_at")
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
    controller_skill = str(raw.get("controller_skill") or "jarvis-run-controller").strip() or "jarvis-run-controller"
    business_skill = str(raw["business_skill"]).strip()
    if not business_skill:
        raise ValueError("business_skill is required")
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
            raise ValueError("thread prompt is not supported; use business_skill")
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
                    or any(not isinstance(value, int) or value < 1 for value in candidate_ids)
                    or len(set(candidate_ids)) != len(candidate_ids)
                    or not database_path or not output_boundary):
                raise ValueError("thread lane requires unique positive candidate_ids, database_path, and output_boundary")
            if max_rounds < len(candidate_ids):
                raise ValueError("max_rounds must cover every candidate in each thread lane")
            child["lane"] = {"candidate_ids": list(candidate_ids), "database_path": database_path,
                             "output_boundary": output_boundary}
        child["prompt"] = _worker_prompt(task_prompt, controller_skill, business_skill, lane_bound="lane" in child)
        if acquire == "create":
            child["title"] = str(raw_child.get("title") or raw.get("title") or "").strip()
            if not child["title"]:
                raise ValueError("create thread requires title")
        else:
            child["task_id"] = str(raw_child.get("task_id") or "").strip()
            if not child["task_id"]:
                raise ValueError("resume thread requires task_id")
        normalized.append(child)
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
                                     lane_bound=any("lane" in child for child in normalized))
    return {"request_id": str(raw["request_id"]).strip(), "project": str(raw["project"]).strip(), "threads": normalized,
            "target_thread_count": target_count, "max_rounds": max_rounds, "max_turns": max_turns,
            "auto_continue": auto_continue, "continue_prompt": continue_prompt,
            "controller_skill": controller_skill, "business_skill": business_skill,
            "model": str(raw.get("model") or "gpt-5.6-luna").strip() or "gpt-5.6-luna",
            "reasoning_effort": str(raw.get("reasoning_effort") or "max").strip() or "max",
            "notifications": notifications, "interval_seconds": interval, "expires_at": expires.isoformat()}


def _worker_prompt(task_prompt: str, controller_skill: str, business_skill: str, *, lane_bound: bool) -> str:
    binding_rule = (
        "每个 Worker 回合仅处理 binding 中的一家公司；安全写回后输出结构化单公司回执并等待下一回合，"
        "不得遍历、预取、并行处理或宣称整条 lane 已完成。\n"
        if lane_bound else ""
    )
    return (
        f"{task_prompt}\n\n你是本次 Jarvis Worker。\n\n"
        f"执行、续跑和回执规则，必须严格遵守 ${controller_skill}。\n"
        f"{binding_rule}"
        f"处理公司和完成本次业务工作，必须严格遵守 ${business_skill}。"
    )


def _current_turn_binding(child: Mapping[str, Any], *, round_number: int) -> dict[str, Any] | None:
    """Expose exactly one stable lane candidate to one Worker turn."""
    lane = child.get("lane")
    if not isinstance(lane, Mapping):
        return None
    candidate_ids = lane.get("candidate_ids")
    if not isinstance(candidate_ids, list) or round_number < 1 or round_number > len(candidate_ids):
        return None
    return {
        "candidate_ids": [candidate_ids[round_number - 1]],
        "database_path": lane["database_path"],
        "output_boundary": lane["output_boundary"],
    }


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


def _run_limit(expires_at: str, interval_seconds: int) -> int:
    return max(1, math.ceil(max((_parse_time(expires_at) - datetime.now(timezone.utc)).total_seconds(), 0) / interval_seconds))
