#!/usr/bin/env python3
"""Create native Codex tasks outside the Codex thread tree and register them with Jarvis."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
from pathlib import Path
import queue
import re
import shutil
import subprocess
import threading
import time
from typing import Any, Callable, Mapping

try:
    from .linux_cloud import child_environment, config_path, acquire_instance_lock, release_instance_lock
except ImportError:  # direct runtime-script execution
    from linux_cloud import child_environment, config_path, acquire_instance_lock, release_instance_lock

try:  # Supports installed package imports and direct runtime-script execution.
    from .coo_dispatcher_store import (
        DEFAULT_ROOT,
        DispatcherError,
        DispatcherStore,
        ProcessLock,
        append_jsonl,
        iter_jsonl,
        read_json,
        utc_now,
    )
except ImportError:  # pragma: no cover - direct script execution
    from coo_dispatcher_store import (
        DEFAULT_ROOT,
        DispatcherError,
        DispatcherStore,
        ProcessLock,
        append_jsonl,
        iter_jsonl,
        read_json,
        utc_now,
    )


WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = DEFAULT_ROOT / "native_task_launcher.config.json"
ORIGINS = {"cooper_direct", "jarvis_confirmed", "worker_delegated"}
REASONING_EFFORTS = {"none", "minimal", "low", "medium", "high", "xhigh"}
TERMINAL_RESULT_STATUSES = {
    "created",
    "partial",
    "recovered",
    "rejected",
    "cancelled",
}
SAFE_ID = re.compile(r"^[A-Za-z0-9._:-]{1,200}$")
RESULT_CONTRACT_MARKER = "[Jarvis result contract v1]"
LANE_BINDING_MARKER = "[Jarvis lane binding v1]"
RESULT_CONTRACT = f"""
{RESULT_CONTRACT_MARKER}
最终回复必须可直接转发给 Cooper：
1. 第一段先用一句自然、准确的人类语言概括实际结果；不要只写“已完成”。
2. 随后保留关键数字、结论、交付物、风险和下一步；不要用机器状态尾注。
3. 只有存在需要随飞书发送的本地文件时，才在末尾加入 <jarvis_delivery> JSON，attachments 必须位于任务工作区内。
4. 如果任务未完成或结果无法核验，第一句必须明确说明阻塞或失败，禁止伪报完成。
""".strip()
DELIVERY_BLOCK_RE = re.compile(
    r"\s*<jarvis_delivery>\s*\{.*?\}\s*</jarvis_delivery>\s*",
    re.IGNORECASE | re.DOTALL,
)


class NativeTaskError(RuntimeError):
    """Raised when native task creation or routing violates the control contract."""


class _TerminalReadbackControlError(Exception):
    """A control callback failed, rather than a read-only native probe."""

    def __init__(self, error: Exception):
        self.error = error


class OwnerExactTerminalReadback(dict):
    """In-process evidence, never manufactured by a native JSON notification."""


class NativeTaskCreationError(NativeTaskError):
    def __init__(self, message: str, *, thread_id: str | None = None, turn_id: str | None = None,
                 terminal_status: str | None = None):
        super().__init__(message)
        self.thread_id = thread_id
        self.turn_id = turn_id
        self.terminal_status = terminal_status


class HostContextRequiredError(NativeTaskError):
    """The App Server was launched from a context that cannot own Codex state."""

    code = "JARVIS_HOST_CONTEXT_REQUIRED"


def _background_subprocess_kwargs() -> dict[str, Any]:
    if os.name != "nt":
        return {}
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    return {
        "startupinfo": startupinfo,
        "creationflags": getattr(subprocess, "CREATE_NO_WINDOW", 0),
    }


def _as_nonempty_string(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise NativeTaskError(f"{field} is required")
    return text


def _report_phase(
    callback: Callable[[str, dict[str, Any]], None] | None,
    phase: str,
    **details: Any,
) -> None:
    if callback is not None:
        callback(phase, details)


def append_result_contract(prompt: str) -> str:
    """Attach the human result contract once to every native task prompt."""
    text = str(prompt or "").strip()
    if RESULT_CONTRACT_MARKER in text:
        return text
    return f"{text}\n\n{RESULT_CONTRACT}"


def append_lane_binding(prompt: str, input_binding: Mapping[str, Any] | None) -> str:
    """Attach one validated Loop lane to the Worker-visible turn input."""
    if not input_binding:
        return str(prompt or "")
    text = str(prompt or "").strip()
    if input_binding and (input_binding.get("result_verification") or {}).get("mode") == "final_answer_json":
        # Holder already validated the full lane; this is intentionally a reduced per-turn binding.
        text = text.split(LANE_BINDING_MARKER, 1)[0].rstrip()
        binding = json.dumps(dict(input_binding), ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        return (f"{text}\n\n{LANE_BINDING_MARKER}\n"
                "以下 JSON 是本回合唯一任务范围，只处理 candidate_ids 中的一个任务项。"
                "最终回复只输出业务 JSON；不写文件、不运行收尾脚本、不填写 QA/回执或运行身份。"
                "原文、payload、candidate_id/request_id/turn_number 和正式接收回执全部由 Holder 保存与绑定。"
                "接收不代表业务核实通过；缺字段、缺来源和额外信息均可保留供复核。"
                "此交付模式取代历史提示或技能中的写回与流程收尾要求；不要预取或处理其他项。\n"
                f"{binding}")
    if not input_binding or LANE_BINDING_MARKER in text:
        return text
    binding = json.dumps(dict(input_binding), ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return (
        f"{text}\n\n{LANE_BINDING_MARKER}\n"
        "以下 JSON 是当前 Worker 回合唯一允许处理的任务范围；candidate_ids 必须且只会包含一家公司。"
        "仅按 database_path 和 output_boundary 执行；安全写回后输出结构化单公司回执并等待下一回合，"
        "不得遍历、预取、并行处理或宣称整条 lane 已完成。\n"
        f"{binding}"
    )


def summarize_final_message(value: Any, limit: int = 240) -> str:
    """Extract the worker-prepared first human conclusion for Cooper."""
    text = DELIVERY_BLOCK_RE.sub("\n", str(value or "")).strip()
    generic = re.compile(
        r"^(?:已完成|处理完成|任务已完成|完成)[：:。.!！\s]*$",
        re.IGNORECASE,
    )
    for raw_line in text.splitlines():
        line = re.sub(r"^\s*(?:#{1,6}|[-*+] |\d+[.)、]\s*)", "", raw_line).strip()
        line = re.sub(
            r"^(?:给\s*Cooper\s*的一句话总结|一句话总结|结果摘要)[：:]\s*",
            "",
            line,
            flags=re.IGNORECASE,
        ).strip()
        if not line or generic.fullmatch(line):
            continue
        if len(line) > limit:
            return line[: limit - 1].rstrip() + "…"
        return line
    return ""


def parse_direct_task(content: str) -> dict[str, str] | None:
    text = str(content or "").strip()
    if not text.lower().startswith("/task"):
        return None
    body = text[5:].strip()
    if not body:
        raise NativeTaskError(
            "direct task syntax: /task <project> | <title> | <prompt>"
        )
    parts = [part.strip() for part in body.split("|")]
    if len(parts) == 2:
        project, prompt = parts
        title = prompt.splitlines()[0][:80].strip()
    elif len(parts) == 3:
        project, title, prompt = parts
    else:
        raise NativeTaskError(
            "direct task syntax: /task <project> | <title> | <prompt>"
        )
    if not project or not title or not prompt:
        raise NativeTaskError("project, title, and prompt must be non-empty")
    return {"project": project, "title": title[:120], "prompt": prompt}


class NativeTaskLauncherConfig:
    def __init__(self, path: Path):
        self.path = path.resolve()
        raw = read_json(self.path)
        self.version = int(raw.get("version", 1))
        self.dispatcher_thread_id = _as_nonempty_string(
            raw.get("dispatcher_thread_id"), "dispatcher_thread_id"
        )
        try:
            self.codex_cli = config_path(str(raw.get("codex_cli") or "auto"), self.path.parent, command=True)
        except ValueError as exc:
            raise NativeTaskError(str(exc)) from exc
        self.linux_cloud = raw.get("linux_cloud", False)
        if not isinstance(self.linux_cloud, bool):
            raise NativeTaskError("linux_cloud must be boolean")
        self.profile = str(raw.get("profile") or "").strip()
        self.profile_is_label_only = True  # Not passed to Codex; runtime readback is authoritative.
        # Opt-in instance contract. Never infer it from a profile name or
        # change the default launcher policy when the field is absent.
        self.runtime_constraints = None
        if "runtime_constraints" in raw:
            constraints = raw["runtime_constraints"]
            supported = {
                "model_provider": "openai",
                "sandbox": "read-only",
                "approval_policy": "never",
            }
            if not isinstance(constraints, dict) or constraints != supported:
                raise NativeTaskError(
                    "runtime_constraints requires exactly model_provider=openai, "
                    "sandbox=read-only, approval_policy=never"
                )
            self.runtime_constraints = dict(constraints)
        self.runtime_environments = None
        if "runtime_environments" in raw:
            environments = raw["runtime_environments"]
            if not isinstance(environments, list) or environments:
                raise NativeTaskError("runtime_environments supports only an explicit empty list")
            self.runtime_environments = []
        connection = raw.get("jarvis_connection") or {}
        if not isinstance(connection, dict):
            raise NativeTaskError("jarvis_connection must be an object")
        self.jarvis_connection = connection
        self.connection_base_url_override = (
            "https://chatgpt.com/backend-api/codex"
            if connection.get("enabled") is True else None
        )
        home = str(raw.get("expected_codex_home") or "").strip()
        self.expected_codex_home_input = home
        self.expected_codex_home_base_dir = str(self.path.parent)
        try:
            self.expected_codex_home = config_path(home, self.path.parent) if home else ""
        except ValueError as exc:
            raise NativeTaskError(str(exc)) from exc
        if self.linux_cloud:
            if self.runtime_environments != [] or self.runtime_constraints is None or not self.expected_codex_home:
                raise NativeTaskError("linux_cloud requires runtime_environments=[], explicit home and runtime_constraints")
            if self.jarvis_connection:
                raise NativeTaskError("linux_cloud does not support gateway connection overrides")
        self.live_creation_enabled = raw.get("live_creation_enabled", False) is True
        self.default_model = str(raw.get("default_model") or "").strip() or None
        self.default_reasoning_effort = str(raw.get("default_reasoning_effort") or "").strip() or None
        if self.default_reasoning_effort is not None and self.default_reasoning_effort not in REASONING_EFFORTS:
            raise NativeTaskError("unsupported default_reasoning_effort")
        self.worker_capacity = raw.get("worker_capacity", 1)
        if type(self.worker_capacity) is not int or not 1 <= self.worker_capacity <= 64:
            raise NativeTaskError("worker_capacity must be an integer from 1 to 64")
        self.poll_seconds = max(float(raw.get("poll_seconds", 2)), 0.25)
        self.request_timeout_seconds = max(
            int(raw.get("request_timeout_seconds", 60)), 10
        )
        self.turn_completion_timeout_seconds = max(
            int(raw.get("turn_completion_timeout_seconds", 300)), 10
        )
        self.turn_readback_timeout_seconds = max(
            float(raw.get("turn_readback_timeout_seconds", 20)), 1.0
        )
        self.thread_operation_lock_timeout_seconds = max(
            float(raw.get("thread_operation_lock_timeout_seconds", 15)), 1.0
        )
        self.max_attempts = max(int(raw.get("max_attempts", 3)), 1)
        self.cooper_actor_ids = {
            str(value).strip()
            for value in raw.get("cooper_actor_ids", [])
            if str(value).strip()
        }
        projects = raw.get("allowed_projects")
        if not isinstance(projects, dict) or not projects:
            raise NativeTaskError("allowed_projects must be a non-empty JSON object")
        try:
            self.allowed_projects = {
                str(name).strip(): config_path(str(path_value), self.path.parent)
                for name, path_value in projects.items()
                if str(name).strip() and str(path_value).strip()
            }
        except ValueError as exc:
            raise NativeTaskError(str(exc)) from exc

    def resolve_project(self, value: str) -> tuple[str, Path]:
        requested = _as_nonempty_string(value, "project")
        if requested in self.allowed_projects:
            name = requested
            path = Path(self.allowed_projects[requested]).resolve()
        else:
            try:
                requested_path = Path(config_path(requested, self.path.parent))
            except ValueError as exc:
                raise NativeTaskError(str(exc)) from exc
            match = next(
                (
                    (name, Path(path_value).resolve())
                    for name, path_value in self.allowed_projects.items()
                    if Path(path_value).resolve() == requested_path
                ),
                None,
            )
            if match is None:
                raise NativeTaskError(f"project is not allowlisted: {requested}")
            name, path = match
        if not path.is_dir():
            raise NativeTaskError(f"project path does not exist: {path}")
        return name, path


class NativeTaskQueue:
    def __init__(
        self,
        root: Path = DEFAULT_ROOT,
        config: NativeTaskLauncherConfig | None = None,
    ):
        self.root = root.resolve()
        self.config = config or NativeTaskLauncherConfig(
            self.root / "native_task_launcher.config.json"
        )
        self.requests_path = self.root / "native_task_requests.jsonl"
        self.results_path = self.root / "native_task_results.jsonl"
        self.submit_lock_path = self.root / "native_task_submit.lock"
        self.service_lock_path = self.root / "native_task_service.lock"
        self.store = DispatcherStore(self.root)
        self.requests_path.parent.mkdir(parents=True, exist_ok=True)
        self.requests_path.touch(exist_ok=True)
        self.results_path.touch(exist_ok=True)

    def _packet_is_confirmed(self, confirmation_id: str) -> bool:
        if not confirmation_id:
            return False
        path = self.store.pending_dir / f"{confirmation_id}.json"
        return path.exists() and read_json(path).get("status") in {
            "confirmed",
            "dispatched",
        }

    def validate(self, request: dict[str, Any]) -> dict[str, Any]:
        request_id = _as_nonempty_string(request.get("request_id"), "request_id")
        correlation_id = _as_nonempty_string(
            request.get("correlation_id"), "correlation_id"
        )
        if not SAFE_ID.fullmatch(request_id):
            raise NativeTaskError("request_id contains unsafe characters")
        if not SAFE_ID.fullmatch(correlation_id):
            raise NativeTaskError("correlation_id contains unsafe characters")
        origin = _as_nonempty_string(request.get("origin"), "origin")
        if origin not in ORIGINS:
            raise NativeTaskError(f"unsupported origin: {origin}")
        actor_id = _as_nonempty_string(request.get("actor_id"), "actor_id")
        confirmation_id = str(request.get("confirmation_id") or "").strip()

        if origin == "cooper_direct":
            if request.get("route") != "direct_task":
                raise NativeTaskError("cooper_direct requires route=direct_task")
            if actor_id not in self.config.cooper_actor_ids:
                raise NativeTaskError("cooper_direct actor is not allowlisted")
        elif origin == "jarvis_confirmed":
            if not self._packet_is_confirmed(confirmation_id):
                raise NativeTaskError("jarvis_confirmed requires a confirmed packet")
        else:
            if request.get("can_request_visible_tasks") is not True:
                raise NativeTaskError(
                    "worker_delegated requires can_request_visible_tasks=true"
                )
            if not self._packet_is_confirmed(confirmation_id):
                raise NativeTaskError(
                    "worker_delegated requires a confirmed parent packet"
                )

        project, project_path = self.config.resolve_project(
            _as_nonempty_string(request.get("project"), "project")
        )
        title = _as_nonempty_string(request.get("title"), "title")[:120]
        prompt = append_result_contract(
            _as_nonempty_string(request.get("prompt"), "prompt")
        )
        if len(prompt) > 100_000:
            raise NativeTaskError("prompt exceeds 100000 characters")
        reasoning_effort = str(request.get("reasoning_effort") or "").strip().lower()
        if reasoning_effort and reasoning_effort not in REASONING_EFFORTS:
            raise NativeTaskError(
                f"unsupported reasoning_effort: {reasoning_effort}"
            )

        return {
            "record_type": "native_task_request",
            "request_id": request_id,
            "correlation_id": correlation_id,
            "origin": origin,
            "route": str(request.get("route") or "").strip(),
            "source_channel": _as_nonempty_string(
                request.get("source_channel"), "source_channel"
            ),
            "source_thread_id": _as_nonempty_string(
                request.get("source_thread_id"), "source_thread_id"
            ),
            "source_message_id": str(request.get("source_message_id") or "").strip(),
            "actor_id": actor_id,
            "confirmation_id": confirmation_id or None,
            "can_request_visible_tasks": bool(
                request.get("can_request_visible_tasks", False)
            ),
            "project": project,
            "project_path": str(project_path),
            "title": title,
            "prompt": prompt,
            "model": str(request.get("model") or "").strip() or None,
            "reasoning_effort": reasoning_effort or None,
            "requested_at": str(request.get("requested_at") or utc_now()),
            "submitted_at": utc_now(),
        }

    def submit(self, request: dict[str, Any]) -> dict[str, Any]:
        value = self.validate(request)
        with ProcessLock(self.submit_lock_path):
            for existing in iter_jsonl(self.requests_path):
                if existing.get("request_id") == value["request_id"]:
                    return {"queued": False, "duplicate": True, "record": existing}
                if existing.get("correlation_id") == value["correlation_id"]:
                    return {"queued": False, "duplicate": True, "record": existing}
            append_jsonl(self.requests_path, value)
            append_jsonl(
                self.store.audit_path,
                {
                    "recorded_at": utc_now(),
                    "action": "native_task_request_submitted",
                    "detail": {
                        "request_id": value["request_id"],
                        "correlation_id": value["correlation_id"],
                        "origin": value["origin"],
                        "project": value["project"],
                    },
                },
            )
            return {"queued": True, "duplicate": False, "record": value}

    def _result_history(self) -> dict[str, list[dict[str, Any]]]:
        history: dict[str, list[dict[str, Any]]] = {}
        for result in iter_jsonl(self.results_path):
            request_id = str(result.get("request_id") or "")
            if request_id:
                history.setdefault(request_id, []).append(result)
        return history

    def pending(self) -> list[dict[str, Any]]:
        history = self._result_history()
        pending: list[dict[str, Any]] = []
        for request in iter_jsonl(self.requests_path):
            results = history.get(str(request["request_id"]), [])
            if any(
                str(item.get("status") or "") in TERMINAL_RESULT_STATUSES
                for item in results
            ):
                continue
            attempts = sum(
                1
                for item in results
                if item.get("status") == "processing"
            )
            if attempts >= self.config.max_attempts:
                continue
            pending.append(request)
        return pending

    def append_result(self, request_id: str, status: str, **detail: Any) -> dict[str, Any]:
        value = {
            "record_type": "native_task_result",
            "recorded_at": utc_now(),
            "request_id": request_id,
            "status": status,
            **detail,
        }
        append_jsonl(self.results_path, value)
        return value

    def dry_run_plan(self, request: dict[str, Any]) -> dict[str, Any]:
        return {
            "dry_run": True,
            "request_id": request["request_id"],
            "project": request["project"],
            "project_path": request["project_path"],
            "title": request["title"],
            "thread_start": {
                "cwd": request["project_path"],
                "ephemeral": False,
            },
            "thread_name_set": {"name": request["title"]},
            "turn_start": {
                "clientUserMessageId": request["request_id"],
                "input": [{"type": "text", "text": request["prompt"]}],
                "model": request.get("model") or "app_server_default",
                "effort": request.get("reasoning_effort"),
            },
            "coo_callback": {
                "dispatcher_thread_id": self.config.dispatcher_thread_id,
                "event_type": "native_task_created",
            },
        }

    def register_created_task(
        self,
        request: dict[str, Any],
        created: dict[str, Any],
    ) -> dict[str, Any]:
        thread_id = _as_nonempty_string(created.get("thread_id"), "thread_id")
        summary_for_cooper = summarize_final_message(created.get("final_message"))
        registration = self.store.register_native_task(
            {
                "request_id": request["request_id"],
                "correlation_id": request["correlation_id"],
                "thread_id": thread_id,
                "turn_id": created.get("turn_id"),
                "turn_status": created.get("turn_status"),
                "model": created.get("model"),
                "reasoning_effort": created.get("reasoning_effort"),
                "final_message": created.get("final_message"),
                "summary_for_cooper": summary_for_cooper or None,
                "project": request["project"],
                "project_path": request["project_path"],
                "title": request["title"],
                "source_channel": request["source_channel"],
                "source_thread_id": request["source_thread_id"],
                "source_message_id": request.get("source_message_id"),
                "origin": request["origin"],
                "confirmation_id": request.get("confirmation_id"),
                "status": created.get("turn_status") or "running",
            }
        )
        callback_event_type = (
            "native_task_completed"
            if str(created.get("turn_status") or "").lower() == "completed"
            else "native_task_created"
        )
        callback_result = self.enqueue_coo_callback(
            request,
            {**created, "summary_for_cooper": summary_for_cooper or None},
            status=str(created.get("turn_status") or "running"),
            event_type=callback_event_type,
        )
        result = self.append_result(
            request["request_id"],
            "created",
            thread_id=thread_id,
            turn_id=created.get("turn_id"),
            turn_status=created.get("turn_status"),
            model=created.get("model"),
            reasoning_effort=created.get("reasoning_effort"),
            final_message=created.get("final_message"),
            summary_for_cooper=summary_for_cooper or None,
            registration=registration,
            callback_event_id=callback_result["event_id"],
            callback_accepted=callback_result["accepted"],
        )
        return result

    def enqueue_coo_callback(
        self,
        request: dict[str, Any],
        created: dict[str, Any],
        *,
        status: str,
        error: str | None = None,
        event_type: str = "native_task_created",
    ) -> dict[str, Any]:
        thread_id = _as_nonempty_string(created.get("thread_id"), "thread_id")
        binding = read_json(self.store.binding_path)
        event_slug = event_type.replace("_", "-")
        callback = {
            "event_id": f"{event_slug}:{request['request_id']}",
            "message_id": f"{event_slug}:{request['request_id']}",
            "chat_id": str(binding.get("primary_chat_id") or ""),
            "sender_open_id": "system:jarvis-native-task-launcher",
            "source_channel": "internal_control",
            "reply_channel": "feishu",
            "reply_conversation_id": str(binding.get("primary_chat_id") or ""),
            "event_type": event_type,
            "content": json.dumps(
                {
                    "event_type": event_type,
                    "request_id": request["request_id"],
                    "thread_id": thread_id,
                    "turn_id": created.get("turn_id"),
                    "turn_status": created.get("turn_status"),
                    "model": created.get("model"),
                    "reasoning_effort": created.get("reasoning_effort"),
                    "final_message": created.get("final_message"),
                    "summary_for_cooper": (
                        created.get("summary_for_cooper")
                        or summarize_final_message(created.get("final_message"))
                        or None
                    ),
                    "status": status,
                    "error": error,
                    "project": request["project"],
                    "project_path": request["project_path"],
                    "title": request["title"],
                    "source_channel": request["source_channel"],
                    "source_thread_id": request["source_thread_id"],
                    "confirmation_id": request.get("confirmation_id"),
                    "control_owner": "Jarvis",
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        }
        callback_result = self.store.ingest_event(callback)
        return {
            "event_id": callback["event_id"],
            "accepted": bool(callback_result.get("accepted")),
        }

    def recover_partial(
        self,
        client: "AppServerClient",
        request_id: str,
        *,
        model: str | None = None,
        reasoning_effort: str | None = None,
    ) -> dict[str, Any]:
        if not self.config.live_creation_enabled:
            raise NativeTaskError("live native task creation is disabled in config")
        request = next(
            (
                item
                for item in iter_jsonl(self.requests_path)
                if item.get("request_id") == request_id
            ),
            None,
        )
        if request is None:
            raise NativeTaskError(f"native task request not found: {request_id}")
        history = [
            item
            for item in iter_jsonl(self.results_path)
            if item.get("request_id") == request_id
        ]
        recovered = next(
            (item for item in reversed(history) if item.get("status") == "recovered"),
            None,
        )
        if recovered is not None:
            return {
                "recovered": False,
                "duplicate": True,
                "record": recovered,
            }
        partial = next(
            (
                item
                for item in reversed(history)
                if item.get("status") == "partial" and item.get("thread_id")
            ),
            None,
        )
        if partial is None:
            raise NativeTaskError(
                f"native task request has no recoverable partial thread: {request_id}"
            )
        thread_id = _as_nonempty_string(partial.get("thread_id"), "thread_id")
        recovery_id = f"{request_id}:recovery"
        self.append_result(
            request_id,
            "processing_recovery",
            thread_id=thread_id,
            recovery_id=recovery_id,
        )
        recovered_turn = client.run_existing_task(
            thread_id,
            request,
            client_user_message_id=recovery_id,
            model=model,
            reasoning_effort=reasoning_effort,
        )
        callback_result = self.enqueue_coo_callback(
            request,
            recovered_turn,
            status="completed",
            event_type="native_task_recovered",
        )
        record = self.append_result(
            request_id,
            "recovered",
            thread_id=thread_id,
            turn_id=recovered_turn.get("turn_id"),
            turn_status=recovered_turn.get("turn_status"),
            model=recovered_turn.get("model"),
            reasoning_effort=recovered_turn.get("reasoning_effort"),
            final_message=recovered_turn.get("final_message"),
            recovery_id=recovery_id,
            callback_event_id=callback_result["event_id"],
            callback_accepted=callback_result["accepted"],
        )
        return {"recovered": True, "duplicate": False, "record": record}

    def process_one(
        self,
        client: "AppServerClient",
        *,
        execute: bool,
    ) -> dict[str, Any] | None:
        request = next(iter(self.pending()), None)
        if request is None:
            return None
        if not execute:
            return self.dry_run_plan(request)
        raise NativeTaskError(
            "native Codex thread creation has been removed; only existing-thread resume is supported"
        )


def _cli_probe(command: list[str], purpose: str) -> str:
    """Run a bounded, read-only discovery command; never start a task."""
    try:
        result = subprocess.run(
            command, capture_output=True, text=True, encoding="utf-8",
            errors="replace", timeout=10, shell=False,
            **_background_subprocess_kwargs(),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise NativeTaskError(f"{purpose} failed for {command[0]!r}: {type(exc).__name__}") from exc
    if result.returncode:
        raise NativeTaskError(f"{purpose} failed for {command[0]!r}: exit {result.returncode}")
    return result.stdout.strip()


def _npm_codex_command(prefix: Path) -> list[str] | None:
    # Windows global packages live directly under prefix; Unix uses lib/.
    for modules in (prefix / "node_modules", prefix / "lib" / "node_modules"):
        package = modules / "@openai" / "codex"
        manifest = package / "package.json"
        if not manifest.is_file():
            continue
        try:
            raw = json.loads(manifest.read_text(encoding="utf-8-sig"))
            entry = raw.get("bin")
            entry = entry.get("codex") if isinstance(entry, dict) else entry
            if raw.get("name") != "@openai/codex" or not isinstance(entry, str) or not entry:
                raise ValueError("invalid Codex package entry")
            script = (package / entry).resolve()
            if not script.is_relative_to(package.resolve()) or not script.is_file():
                raise ValueError("missing or invalid Codex package entry")
        except (OSError, ValueError, AttributeError) as exc:
            raise NativeTaskError(f"invalid npm Codex installation at {package}") from exc
        # Match npm's sibling-node preference, then resolve Node from PATH.
        sibling = prefix / ("node.exe" if os.name == "nt" else "node")
        node = str(sibling) if sibling.is_file() else shutil.which("node")
        if not node:
            raise NativeTaskError(f"Node executable not found for npm Codex at {package}")
        return [node, str(script)]
    return None


_CODEX_VERSION_RESPONSE = re.compile(
    r"codex-cli\s+(\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?)"
)


def _codex_version(output: str, executable: str) -> str:
    match = _CODEX_VERSION_RESPONSE.fullmatch(output)
    if not match:
        raise NativeTaskError(
            f"unrecognized Codex version response from {executable!r}"
        )
    return match.group(1)


def _codex_version_key(version: str) -> tuple[Any, ...]:
    """Return a deterministic SemVer-style key for selecting a Desktop build."""
    public = version.split("+", 1)[0]
    core, separator, prerelease = public.partition("-")
    major, minor, patch = (int(value) for value in core.split("."))
    if not separator:
        return major, minor, patch, 1, ()
    identifiers = tuple(
        (0, int(value)) if value.isdigit() else (1, value.casefold())
        for value in prerelease.split(".")
    )
    return major, minor, patch, 0, identifiers


def _desktop_codex_command() -> tuple[list[str], str] | None:
    """Find and validate the newest installed Codex Desktop CLI on Windows."""
    if os.name != "nt":
        return None
    local_app_data = os.environ.get("LOCALAPPDATA")
    if not local_app_data:
        return None
    root = Path(local_app_data) / "OpenAI" / "Codex" / "bin"
    if not root.is_dir():
        return None
    candidates: list[tuple[tuple[Any, ...], int, str, str]] = []
    for executable in root.glob("*/codex.exe"):
        if not executable.is_file():
            continue
        resolved = str(executable.resolve())
        try:
            output = _cli_probe([resolved, "--version"], "Codex Desktop version probe")
            version = _codex_version(output, resolved)
            modified_ns = executable.stat().st_mtime_ns
        except (NativeTaskError, OSError) as exc:
            logging.getLogger(__name__).warning(
                "Ignoring unusable Codex Desktop candidate %r: %s", resolved, exc
            )
            continue
        candidates.append((_codex_version_key(version), modified_ns, resolved, version))
    if not candidates:
        return None
    _, _, executable, version = max(candidates)
    logging.getLogger(__name__).info(
        "Codex CLI resolved via Codex Desktop discovery: %r (version %s)",
        executable,
        version,
    )
    return [executable], version


def _resolve_codex_command(configured: str) -> tuple[list[str], str]:
    if configured == "desktop_auto":
        desktop = _desktop_codex_command()
        if desktop is not None:
            return desktop
        logging.getLogger(__name__).warning(
            "No usable Codex Desktop CLI found; falling back to normal auto discovery"
        )
        configured = "auto"
    if configured != "auto":
        executable = shutil.which(configured)
        if not executable:
            raise NativeTaskError(f"configured Codex executable not found: {configured!r}")
        command, source = [executable], "configured"
    else:
        # Prefer the npm shim on Windows, even when Desktop also supplies an exe.
        shim = shutil.which("codex.cmd") if os.name == "nt" else shutil.which("codex")
        if shim:
            command = _npm_codex_command(Path(shim).parent) or [shim]
            source = "PATH"
        else:
            npm = shutil.which("npm.cmd" if os.name == "nt" else "npm")
            command = None
            if npm:
                prefix = _cli_probe([npm, "prefix", "-g"], "npm global prefix discovery")
                if not prefix or not Path(prefix).is_absolute() or "\n" in prefix:
                    raise NativeTaskError("npm global prefix discovery returned an invalid path")
                command = _npm_codex_command(Path(prefix))
            source = "npm global prefix"
            if command is None:
                executable = shutil.which("codex.exe" if os.name == "nt" else "codex")
                if not executable:
                    raise NativeTaskError("Codex executable not found on PATH or in npm global installation")
                command, source = [executable], "PATH executable"
    output = _cli_probe(command + ["--version"], "Codex version probe")
    version = _codex_version(output, command[0])
    logging.getLogger(__name__).info("Codex CLI resolved via %s: %r (version %s)", source, command, version)
    return command, version


def resolve_codex_runtime(configured: str) -> dict[str, Any]:
    """Return the validated command Jarvis would use for a new App Server."""
    command, version = _resolve_codex_command(configured)
    return {
        "configured": configured,
        "command": command,
        "executable": command[0],
        "version": version,
    }


class AppServerClient:
    def __init__(self, config: NativeTaskLauncherConfig):
        self.config = config
        self.cli_command, self.cli_version = _resolve_codex_command(config.codex_cli)
        self.executable = self.cli_command[0]
        self.process: subprocess.Popen[str] | None = None
        self.response_queue: queue.Queue[dict[str, Any]] = queue.Queue()
        self.notifications: queue.Queue[dict[str, Any] | None] = queue.Queue()
        self.stderr_lines: list[str] = []
        self._request_id = 0
        self._write_lock = threading.Lock()
        self.initialize_result: dict[str, Any] | None = None
        self.runtime_context: dict[str, Any] = {}
        self._instance_lock_fd = None
        self.before_turn_dispatch = None  # Optional owner-installed cancellation/commit gate.
        self._terminal_probe_lock = threading.Lock()
        self._terminal_probe_pending_id: int | None = None
        self._terminal_probe_expired = False

    def _runtime_thread_params(self, model: str | None) -> dict[str, Any]:
        model = model or getattr(self.config, "default_model", None)
        constraints = getattr(getattr(self, "config", None), "runtime_constraints", None)
        if constraints is None:
            return {}
        params: dict[str, Any] = {
            "modelProvider": constraints["model_provider"],
            "sandbox": constraints["sandbox"],
            "approvalPolicy": constraints["approval_policy"],
        }
        effort = getattr(self.config, "default_reasoning_effort", None)
        if effort is not None:
            params["config"] = {"model_reasoning_effort": effort}
        if model is not None:
            if not isinstance(model, str) or not model.strip():
                raise NativeTaskError("constrained runtime model must be a non-empty string")
            params["model"] = model
        return params

    def _verify_runtime_context(
        self, response: Mapping[str, Any], *, operation: str,
        thread_id: str, model: str | None,
    ) -> None:
        config = getattr(self, "config", None)
        model = model or getattr(config, "default_model", None)
        constrained = getattr(config, "runtime_constraints", None) is not None
        no_environments = getattr(config, "runtime_environments", None) is not None
        if not constrained and not no_environments:
            return
        # Only non-secret execution-policy fields from the actual response are
        # retained. Do not copy the thread, config, account or authentication.
        self.runtime_context = {
            key: response[key]
            for key in ("model", "modelProvider", "sandbox", "approvalPolicy", "reasoningEffort")
            if key in response
        }
        self.runtime_context.update(operation=operation, thread_id=thread_id)
        initialized = getattr(self, "initialize_result", None) or {}
        if initialized.get("codexHome"):
            observed_home = str(initialized["codexHome"])
            self.runtime_context["codex_home"] = observed_home
            self.runtime_context["codex_home_verified"] = bool(getattr(config, "expected_codex_home", "")) and Path(observed_home).resolve() == Path(config.expected_codex_home).resolve()
        if constrained:
            self.runtime_context["constraints_verified"] = False
        observed_thread = response.get("thread")
        if no_environments:
            self.runtime_context["environments_verified"] = False
            if isinstance(observed_thread, dict) and "environments" in observed_thread:
                self.runtime_context["environments"] = observed_thread["environments"]
        sandbox = response.get("sandbox")
        failures = []
        if not thread_id or not isinstance(observed_thread, dict) or observed_thread.get("id") != thread_id:
            failures.append("thread identity")
        if not isinstance(response.get("model"), str) or not response["model"].strip():
            failures.append("model missing")
        elif model is not None and response["model"] != model:
            failures.append("model mismatch")
        if constrained:
            if response.get("modelProvider") != "openai":
                failures.append("modelProvider")
            if (not isinstance(sandbox, dict) or sandbox.get("type") != "readOnly"
                    or sandbox.get("networkAccess", False) is not False):
                failures.append("sandbox")
            if response.get("approvalPolicy") != "never":
                failures.append("approvalPolicy")
        expected_effort = getattr(config, "default_reasoning_effort", None)
        if expected_effort is not None and response.get("reasoningEffort") != expected_effort:
            failures.append("reasoningEffort")
        if no_environments:
            environments = self.runtime_context.get("environments")
            if not isinstance(environments, list) or environments:
                failures.append("thread.environments")
        if failures:
            raise NativeTaskCreationError(
                "runtime constraint readback failed: " + ", ".join(failures),
                thread_id=thread_id or None,
            )
        if constrained:
            self.runtime_context["constraints_verified"] = True
        if no_environments:
            self.runtime_context["environments_verified"] = True

    def start(self, *, deadline: float | None = None) -> dict[str, Any]:
        try:
            if deadline is not None and time.monotonic() >= deadline:
                raise NativeTaskError("app-server initialization readback deadline exhausted before dispatch")
            return self._start_impl(deadline=deadline)
        except BaseException:
            self.close()
            raise

    def _start_impl(self, *, deadline: float | None = None) -> dict[str, Any]:
        if self.process and self.process.poll() is None:
            return self.initialize_result or {}
        try:
            env = child_environment(self.config)
        except ValueError as exc:
            raise NativeTaskError(str(exc)) from exc
        command = [*self.cli_command]
        base_url = getattr(self.config, "connection_base_url_override", None)
        if base_url:
            command.extend(["-c", f"openai_base_url={base_url}"])
        command.extend(["app-server", "--stdio"])
        try:
            self._instance_lock_fd = acquire_instance_lock(self.config)
        except (ValueError, OSError) as exc:
            raise NativeTaskError(str(exc)) from exc
        try:
            self.process = subprocess.Popen(
                command,
                cwd=str(WORKSPACE_ROOT),
                env=env,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                shell=False,
                **_background_subprocess_kwargs(),
            )
        except BaseException:
            release_instance_lock(self._instance_lock_fd)
            self._instance_lock_fd = None
            raise
        threading.Thread(target=self._read_stdout, daemon=True).start()
        threading.Thread(target=self._read_stderr, daemon=True).start()
        try:
            result = self.request(
                "initialize",
                {
                    "clientInfo": {
                        "name": "jarvis-native-task-launcher",
                        "title": "Jarvis Native Task Launcher",
                        "version": "0.1.0",
                    },
                    "capabilities": {"experimentalApi": True},
                },
                deadline=deadline,
            )
        except NativeTaskError as exc:
            self.close()
            detail = str(exc)
            if (
                "failed to initialize sqlite state runtime" in detail
                or "failed to clean up stale arg0 temp dirs" in detail
            ):
                raise HostContextRequiredError(
                    "JARVIS_HOST_CONTEXT_REQUIRED: App Server cannot initialize the configured "
                    f"CODEX_HOME {self.config.expected_codex_home!r}; use a supported executor "
                    "with permission to initialize this independent home."
                ) from exc
            raise NativeTaskError(
                f"Codex {self.cli_version} at {self.cli_command!r} failed App Server initialization: {detail}"
            ) from exc
        self.notify("initialized")
        codex_home = str(result.get("codexHome") or "")
        expected = self.config.expected_codex_home
        if expected and Path(codex_home).resolve() != Path(expected).resolve():
            self.close()
            raise NativeTaskError(
                f"Codex home mismatch: expected {expected!r}, observed {codex_home!r}"
            )
        self.initialize_result = result
        return result

    def _read_stdout(self) -> None:
        assert self.process is not None and self.process.stdout is not None
        try:
            for raw in self.process.stdout:
                item = None
                try:
                    item = json.loads(raw)
                    if not isinstance(item, dict):
                        continue
                    if "id" in item:
                        self._queue_response(item)
                    else:
                        self.notifications.put(item)
                except json.JSONDecodeError:
                    continue
                finally:
                    # A reader may block until the next notification for minutes.
                    # The receiving queue owns the message; do not also retain the
                    # last decoded response and raw JSON in this thread's frame.
                    item = None
                    raw = None
        finally:
            # FIFO preserves terminal evidence already read before EOF. A closed
            # transport is not evidence that the remote execution is terminal.
            self.notifications.put(None)

    def _read_stderr(self) -> None:
        assert self.process is not None and self.process.stderr is not None
        for raw in self.process.stderr:
            self.stderr_lines.append(raw.rstrip())
            if len(self.stderr_lines) > 100:
                del self.stderr_lines[:20]

    def _write(self, value: dict[str, Any]) -> None:
        if not self.process or self.process.poll() is not None or not self.process.stdin:
            raise NativeTaskError("Codex app-server is not running")
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        with self._write_lock:
            dispatch_gate = getattr(self, "before_turn_dispatch", None)
            if value.get("method") == "turn/start" and callable(dispatch_gate):
                dispatch_gate(value.get("params") or {})
            self.process.stdin.write(encoded + "\n")
            self.process.stdin.flush()

    def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        value: dict[str, Any] = {"method": method}
        if params is not None:
            value["params"] = params
        self._write(value)

    def _queue_response(self, item: dict[str, Any]) -> None:
        """Drop only the one expired Hold-probe response; preserve other RPCs.

        A timed-out probe cannot dispatch another RPC until this exact response
        arrives. No abandoned-ID collection or late large item page is retained.
        The reader and timeout cleanup use the same lock to cover the enqueue race.
        """
        lock = getattr(self, "_terminal_probe_lock", None)
        if lock is None:
            self.response_queue.put(item)
            return
        with lock:
            if (self._terminal_probe_expired
                    and item.get("id") == self._terminal_probe_pending_id):
                self._terminal_probe_pending_id = None
                self._terminal_probe_expired = False
                return
            self.response_queue.put(item)

    def request(
        self, method: str, params: dict[str, Any], *, deadline: float | None = None,
        _terminal_probe: bool = False,
    ) -> dict[str, Any]:
        request_deadline = time.monotonic() + self.config.request_timeout_seconds
        deadline = min(request_deadline, deadline) if deadline is not None else request_deadline
        if time.monotonic() >= deadline:
            raise NativeTaskError(f"app-server {method} readback deadline exhausted before dispatch")
        self._request_id += 1
        request_id = self._request_id
        if _terminal_probe:
            # Hold calls these sequentially, with control callbacks between RPCs.
            # Never accumulate unanswered probes, even if the server is stalled.
            if not hasattr(self, "_terminal_probe_lock"):
                self._terminal_probe_lock = threading.Lock()
                self._terminal_probe_pending_id = None
                self._terminal_probe_expired = False
            with self._terminal_probe_lock:
                if self._terminal_probe_pending_id is not None:
                    raise NativeTaskError("Hold terminal readback has one unanswered native RPC")
                self._terminal_probe_pending_id = request_id
                self._terminal_probe_expired = False
        deferred: list[dict[str, Any]] = []
        matched = False
        try:
            self._write({"id": request_id, "method": method, "params": params})
            while time.monotonic() < deadline:
                remaining = max(deadline - time.monotonic(), 0.0)
                try:
                    item = self.response_queue.get(timeout=remaining)
                except queue.Empty:
                    break
                if item.get("id") != request_id:
                    deferred.append(item)
                    continue
                matched = True
                if _terminal_probe:
                    with self._terminal_probe_lock:
                        self._terminal_probe_pending_id = None
                        self._terminal_probe_expired = False
                if "error" in item:
                    raise NativeTaskError(
                        f"app-server {method} failed: "
                        + json.dumps(item["error"], ensure_ascii=False)
                    )
                result = item.get("result")
                return result if isinstance(result, dict) else {"result": result}
        finally:
            if _terminal_probe and not matched:
                with self._terminal_probe_lock:
                    self._terminal_probe_expired = True
                    # A reply may have been queued just before expiry. Discard
                    # only its exact ID, preserving all unrelated responses.
                    retained = []
                    for _ in range(self.response_queue.qsize()):
                        try:
                            queued = self.response_queue.get_nowait()
                        except queue.Empty:
                            break
                        if queued.get("id") == request_id:
                            self._terminal_probe_pending_id = None
                            self._terminal_probe_expired = False
                        else:
                            retained.append(queued)
                    for queued in retained:
                        self.response_queue.put(queued)
            for item in deferred:
                self._queue_response(item)
        tail = "\n".join(self.stderr_lines[-10:])
        raise NativeTaskError(f"app-server {method} timed out; stderr={tail}")

    def probe(self) -> dict[str, Any]:
        initialized = self.start()
        listed = self.request(
            "thread/list",
            {"limit": 1, "useStateDbOnly": True},
        )
        models = self.available_models()
        return {
            "initialized": initialized,
            "thread_list_readable": isinstance(listed, dict),
            "available_models": [
                str(item.get("id") or item.get("model") or "")
                for item in models
                if str(item.get("id") or item.get("model") or "").strip()
            ],
        }

    def available_models(self) -> list[dict[str, Any]]:
        self.start()
        result = self.request("model/list", {})
        models = result.get("data") or result.get("models") or []
        if not isinstance(models, list):
            raise NativeTaskError("app-server model/list returned an invalid payload")
        return [item for item in models if isinstance(item, dict)]

    def select_model(self, requested: str | None = None) -> str:
        requested = requested or getattr(self.config, "default_model", None)
        if requested:
            # Explicit IDs belong to the selected upstream; CLI discovery can
            # be stale or unavailable. Retain initialization, never substitute.
            self.start()
            return requested
        models = self.available_models()
        supported: dict[str, dict[str, Any]] = {}
        for item in models:
            model_id = str(item.get("id") or item.get("model") or "").strip()
            if model_id:
                supported[model_id] = item
        if not supported:
            raise NativeTaskError("app-server model/list returned no supported models")
        default = next(
            (
                model_id
                for model_id, item in supported.items()
                if item.get("isDefault") is True
            ),
            None,
        )
        return default or next(iter(supported))

    def start_turn(
        self,
        thread_id: str,
        prompt: str,
        *,
        client_user_message_id: str,
        model: str | None = None,
        reasoning_effort: str | None = None,
    ) -> dict[str, Any]:
        selected_model = self.select_model(model)
        return self._start_turn_with_model(
            thread_id,
            prompt,
            client_user_message_id=client_user_message_id,
            selected_model=selected_model,
            selected_effort=reasoning_effort,
        )

    def start_turn_async(
        self,
        thread_id: str,
        prompt: str,
        *,
        client_user_message_id: str,
        model: str | None = None,
        reasoning_effort: str | None = None,
        input_binding: Mapping[str, Any] | None = None,
        on_phase: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Start one exact existing-thread turn without waiting for its terminal state."""
        _report_phase(on_phase, "app_server_initializing")
        self.start()
        _report_phase(on_phase, "app_server_initialized")
        _report_phase(on_phase, "model_selecting")
        selected_model = self.select_model(model)
        _report_phase(on_phase, "model_selected", model=selected_model)
        _report_phase(on_phase, "turn_starting", thread_id=thread_id)
        started = self._start_turn(
            thread_id,
            append_lane_binding(prompt, input_binding),
            client_user_message_id=client_user_message_id,
            selected_model=selected_model,
            selected_effort=reasoning_effort,
        )
        _report_phase(on_phase, "turn_started", thread_id=thread_id, turn_id=str(started["turn_id"]))
        return started

    def resume_turn_async(
        self,
        thread_id: str,
        prompt: str,
        *,
        client_user_message_id: str,
        model: str | None = None,
        reasoning_effort: str | None = None,
        input_binding: Mapping[str, Any] | None = None,
        on_phase: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Reattach one durable thread in this App Server before starting its turn."""
        self.start()
        _report_phase(on_phase, "thread_resuming", thread_id=thread_id)
        params = {"threadId": thread_id, "excludeTurns": True, **self._runtime_thread_params(model)}
        self.runtime_context = {}
        resumed = self.request("thread/resume", params)
        self._verify_runtime_context(
            resumed, operation="thread/resume", thread_id=thread_id, model=model,
        )
        del resumed
        _report_phase(on_phase, "thread_resumed", thread_id=thread_id)
        return self.start_turn_async(
            thread_id,
            prompt,
            client_user_message_id=client_user_message_id,
            model=model,
            reasoning_effort=reasoning_effort,
            input_binding=input_binding,
            on_phase=on_phase,
        )

    def create_task(
        self,
        request: dict[str, Any],
        *,
        on_phase: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Create one durable thread and return as soon as its first turn starts."""
        project_path = _as_nonempty_string(request.get("project_path"), "project_path")
        title = _as_nonempty_string(request.get("title"), "title")
        prompt = _as_nonempty_string(request.get("prompt"), "prompt")
        request_id = _as_nonempty_string(request.get("request_id"), "request_id")
        _report_phase(on_phase, "app_server_initializing")
        self.start()
        _report_phase(on_phase, "app_server_initialized")
        try:
            _report_phase(on_phase, "thread_starting")
            params = {"cwd": project_path, "ephemeral": False,
                      **self._runtime_thread_params(request.get("model"))}
            if getattr(getattr(self, "config", None), "runtime_environments", None) is not None:
                params["environments"] = []
            self.runtime_context = {}
            started = self.request("thread/start", params)
            thread = started.get("thread")
            thread_id = str(thread.get("id") or "") if isinstance(thread, dict) else ""
            if not thread_id:
                raise NativeTaskCreationError("thread/start response is missing thread.id")
            _report_phase(on_phase, "thread_started", thread_id=thread_id)
            self._verify_runtime_context(
                started, operation="thread/start", thread_id=thread_id,
                model=request.get("model"),
            )
            _report_phase(on_phase, "model_selecting", thread_id=thread_id)
            selected_model = self.select_model(request.get("model"))
            _report_phase(on_phase, "model_selected", thread_id=thread_id, model=selected_model)
            _report_phase(on_phase, "turn_starting", thread_id=thread_id)
            created = self._start_turn(
                thread_id,
                append_lane_binding(prompt, request.get("input_binding")),
                client_user_message_id=request_id,
                selected_model=selected_model,
                selected_effort=request.get("reasoning_effort"),
            )
            _report_phase(on_phase, "turn_started", thread_id=thread_id, turn_id=str(created["turn_id"]))
            return {**created, "thread_id": thread_id, "title": title}
        except NativeTaskCreationError:
            raise
        except Exception as exc:
            raise NativeTaskCreationError(str(exc), thread_id=locals().get("thread_id")) from exc

    def _start_turn_with_model(
        self,
        thread_id: str,
        prompt: str,
        *,
        client_user_message_id: str,
        selected_model: str,
        selected_effort: str | None = None,
    ) -> dict[str, Any]:
        started = self._start_turn(
            thread_id,
            prompt,
            client_user_message_id=client_user_message_id,
            selected_model=selected_model,
            selected_effort=selected_effort,
        )
        turn_id = str(started["turn_id"])
        completed = self.wait_for_turn_terminal(thread_id, turn_id)
        turn_status = str(completed.get("status") or "")
        if turn_status != "completed":
            error = completed.get("error")
            detail = (
                json.dumps(error, ensure_ascii=False)
                if error is not None
                else "no error detail"
            )
            raise NativeTaskCreationError(
                f"native task turn ended with status {turn_status!r}: {detail}",
                thread_id=thread_id, turn_id=turn_id, terminal_status=turn_status,
            )
        final_message = self.wait_for_turn_readback(thread_id, turn_id)
        return {**started, "turn_status": turn_status, "final_message": final_message}

    def _start_turn(
        self,
        thread_id: str,
        prompt: str,
        *,
        client_user_message_id: str,
        selected_model: str,
        selected_effort: str | None = None,
        output_schema: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        config = getattr(self, "config", None)
        selected_effort = selected_effort or getattr(config, "default_reasoning_effort", None)
        constrained = getattr(config, "runtime_constraints", None) is not None
        no_environments = getattr(config, "runtime_environments", None) is not None
        if constrained or no_environments:
            context = getattr(self, "runtime_context", {})
            if ((constrained and context.get("constraints_verified") is not True)
                    or (no_environments and (context.get("environments_verified") is not True
                        or context.get("environments") != []))
                    or context.get("thread_id") != thread_id
                    or context.get("model") != selected_model):
                raise NativeTaskCreationError(
                    "turn/start requires matching verified runtime context",
                    thread_id=thread_id,
                )
        turn_params: dict[str, Any] = {
            "threadId": thread_id,
            "clientUserMessageId": client_user_message_id,
            "input": [{"type": "text", "text": prompt}],
            "model": selected_model,
        }
        if selected_effort is not None:
            turn_params["effort"] = selected_effort
        if no_environments:
            turn_params["environments"] = []
        if output_schema is not None:
            turn_params["outputSchema"] = dict(output_schema)
        turn_started = self.request(
            "turn/start",
            turn_params,
        )
        turn = turn_started.get("turn")
        turn_id = str(turn.get("id") or "") if isinstance(turn, dict) else ""
        if not turn_id:
            raise NativeTaskCreationError(
                "turn/start response is missing turn.id",
                thread_id=thread_id,
            )
        return {
            "thread_id": thread_id,
            "turn_id": turn_id,
            "turn_status": str(turn.get("status") or "inProgress"),
            "model": selected_model,
            "reasoning_effort": selected_effort,
        }

    def _read_final_message_pagewise(
        self, thread_id: str, turn_id: str, *, require_final_answer: bool,
        deadline: float,
    ) -> str:
        """Read only the exact turn, newest items first, without hydrating history.

        The supported alpha.7 paginated endpoint preserves turn identity on every
        entry. There is deliberately no full-history fallback on protocol errors.
        """
        cursor = None
        seen_cursors: set[str] = set()
        newest_message: str | None = None
        while True:
            if time.monotonic() >= deadline:
                return ""
            params: dict[str, Any] = {
                "threadId": thread_id, "turnId": turn_id,
                "sortDirection": "desc", "limit": 50,
            }
            if cursor is not None:
                params["cursor"] = cursor
            page = self.request("thread/items/list", params, deadline=deadline)
            entries = page.get("data")
            if not isinstance(entries, list):
                raise NativeTaskError("Exact-turn item readback is missing data")
            for entry in entries:
                if not isinstance(entry, dict) or entry.get("turnId") != turn_id:
                    raise NativeTaskError("Exact-turn item readback identity mismatch")
                item = entry.get("item")
                if not isinstance(item, dict) or item.get("type") != "agentMessage":
                    continue
                text = str(item.get("text") or "")
                if newest_message is None:
                    newest_message = text
                if item.get("phase") == "final_answer":
                    return text if require_final_answer else text.strip()
            next_cursor = page.get("nextCursor")
            # Release potentially large tool-result pages before waiting on I/O.
            del page, entries
            item = entry = None
            if next_cursor is None:
                return "" if require_final_answer else (newest_message or "").strip()
            if not isinstance(next_cursor, str) or not next_cursor or next_cursor in seen_cursors:
                raise NativeTaskError("Exact-turn item readback has an invalid/repeated cursor")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
            if time.monotonic() >= deadline:
                return ""  # An incomplete scan never licenses commentary fallback.

    def read_exact_turn(
        self, thread_id: str, turn_id: str, *, deadline: float,
        _terminal_only: bool = False,
        _control_poll: Callable[[], None] | None = None,
        _terminal_probe: bool = False,
    ) -> dict[str, Any]:
        """Fresh native metadata and an explicit final-only projection of one turn.

        Exhaust every item page before returning counts or scan completeness. Tool
        payloads are released pagewise; full evidence stays available through the
        native paginated endpoint. No SQL, latest-turn or full-history fallback.
        """
        if not isinstance(thread_id, str) or not thread_id.strip():
            raise NativeTaskError("Exact-turn read requires a non-empty thread_id")
        if not isinstance(turn_id, str) or not turn_id.strip():
            raise NativeTaskError("Exact-turn read requires a non-empty turn_id")

        def check_deadline(*, poll: bool = True) -> None:
            if poll and _control_poll is not None:
                _control_poll()
            if time.monotonic() >= deadline:
                raise NativeTaskError("Exact-turn readback deadline exhausted; scan is incomplete")

        def read_page(method: str, params: dict[str, Any]) -> dict[str, Any]:
            options = {"_terminal_probe": True} if _terminal_probe else {}
            return self.request(method, params, deadline=deadline, **options)

        def next_page(page: dict[str, Any], seen: set[str]) -> str | None:
            cursor = page.get("nextCursor")
            if cursor is None:
                return None
            if not isinstance(cursor, str) or not cursor or cursor in seen:
                raise NativeTaskError("Exact-turn readback has an invalid/repeated cursor")
            seen.add(cursor)
            return cursor

        check_deadline()
        metadata = read_page(
            "thread/read", {"threadId": thread_id, "includeTurns": False},
        )
        check_deadline(poll=False)
        thread = metadata.get("thread")
        if not isinstance(thread, dict) or thread.get("id") != thread_id:
            raise NativeTaskError("Exact-thread metadata readback identity mismatch")
        thread_status = thread.get("status")
        del metadata, thread

        cursor = None
        seen_cursors: set[str] = set()
        selected = None
        while selected is None:
            check_deadline()
            params: dict[str, Any] = {
                "threadId": thread_id, "itemsView": "notLoaded",
                "sortDirection": "desc", "limit": 50,
            }
            if cursor is not None:
                params["cursor"] = cursor
            page = read_page("thread/turns/list", params)
            check_deadline(poll=False)
            turns = page.get("data")
            if not isinstance(turns, list):
                raise NativeTaskError("Exact-turn metadata readback is missing data")
            for turn in turns:
                if (not isinstance(turn, dict) or not isinstance(turn.get("id"), str)
                        or not turn["id"] or turn.get("status") not in
                        {"completed", "interrupted", "failed", "inProgress"}
                        or turn.get("itemsView") != "notLoaded" or turn.get("items") != []):
                    raise NativeTaskError("Exact-turn metadata readback has an invalid/notLoaded shape")
                error = turn.get("error")
                if error is not None and (not isinstance(error, dict)
                        or not isinstance(error.get("message"), str) or turn["status"] != "failed"):
                    raise NativeTaskError("Exact-turn metadata readback has an invalid/conflicting error")
                if turn["id"] == turn_id:
                    if selected is not None:
                        raise NativeTaskError("Exact-turn metadata readback has duplicate target identity")
                    selected = {"turn_id": turn_id, "status": turn["status"],
                                "error": str(turn["error"]) if turn.get("error") is not None else None,
                                "native_error": turn.get("error")}
            cursor = next_page(page, seen_cursors)
            del page, turns
            turn = error = None
            if selected is None and cursor is None:
                raise NativeTaskError("Requested exact turn was not found in native readback")

        cursor = None
        seen_cursors.clear()
        if _terminal_only and selected["status"] == "inProgress":
            return {"thread_id": thread_id, "status": thread_status, "turns": [selected],
                    "read_source": "native_exact_turn", "turn_id": turn_id,
                    "turns_complete": False, "turns_projection": "exact_turn"}
        final = None
        item_count = web_search_count = 0
        while True:
            check_deadline()
            params = {"threadId": thread_id, "turnId": turn_id,
                      "sortDirection": "desc", "limit": 50}
            if cursor is not None:
                params["cursor"] = cursor
            page = read_page("thread/items/list", params)
            check_deadline(poll=False)
            entries = page.get("data")
            if not isinstance(entries, list):
                raise NativeTaskError("Exact-turn item readback is missing data")
            for entry in entries:
                if not isinstance(entry, dict) or entry.get("turnId") != turn_id:
                    raise NativeTaskError("Exact-turn item readback identity mismatch")
                item = entry.get("item")
                if (not isinstance(item, dict) or not isinstance(item.get("id"), str)
                        or not item["id"] or not isinstance(item.get("type"), str) or not item["type"]):
                    raise NativeTaskError("Exact-turn item readback has an invalid item shape")
                item_count += 1
                web_search_count += item["type"] == "webSearch"
                if item["type"] == "agentMessage" and item.get("phase") == "final_answer":
                    if not isinstance(item.get("text"), str):
                        raise NativeTaskError("Exact-turn final answer has an invalid text shape")
                    if final is None:
                        final = {key: item[key] for key in ("id", "type", "phase", "text") if key in item}
            cursor = next_page(page, seen_cursors)
            # Do not pin a large tool page while issuing the next native request.
            del page, entries
            item = entry = None
            if cursor is None:
                break
        check_deadline()
        selected.update(
            items=[final] if final is not None else [],
            items_projection="final_answer", items_complete=False,
            items_scan_complete=True, item_count=item_count, web_search_count=web_search_count,
            final_answer_present=final is not None,
            final_answer_nonempty=final is not None and bool(final["text"].strip()),
        )
        return {"thread_id": thread_id, "status": thread_status, "turns": [selected],
                "read_source": "native_exact_turn", "turn_id": turn_id,
                "turns_complete": False, "turns_projection": "exact_turn"}

    def wait_for_turn_readback(self, thread_id: str, turn_id: str, *, require_final_answer: bool = False) -> str:
        """Wait for the exact completed turn's final answer to be materialized.

        Read one bounded item page at a time. Reusing a durable thread must not
        deserialize every prior turn at each new Hold completion.
        """
        deadline = time.monotonic() + self.config.turn_readback_timeout_seconds
        metadata = self.request(
            "thread/read", {"threadId": thread_id, "includeTurns": False}, deadline=deadline,
        )
        thread = metadata.get("thread")
        if not isinstance(thread, dict) or thread.get("id") != thread_id:
            raise NativeTaskError("Exact-thread metadata readback identity mismatch")
        del metadata, thread
        while True:
            final_message = self._read_final_message_pagewise(
                thread_id, turn_id, require_final_answer=require_final_answer,
                deadline=deadline,
            )
            if final_message:
                return final_message
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return ""
            time.sleep(min(self.config.poll_seconds, remaining))

    def thread_operation_lock_path(self, thread_id: str) -> Path:
        """Return a stable cross-process lock path for one exact target thread."""
        digest = hashlib.sha256(thread_id.encode("utf-8")).hexdigest()[:32]
        return self.config.path.parent / "thread_turn_locks" / f"{digest}.lock"

    @staticmethod
    def final_message_for_turn(thread: Any, turn_id: str, *, require_final_answer: bool = False) -> str:
        if not isinstance(thread, dict):
            return ""
        for turn in reversed(thread.get("turns") or []):
            if not isinstance(turn, dict) or str(turn.get("id") or "") != turn_id:
                continue
            messages = [
                item
                for item in turn.get("items") or []
                if isinstance(item, dict) and item.get("type") == "agentMessage"
            ]
            final_messages = [
                item for item in messages if item.get("phase") == "final_answer"
            ]
            selected = final_messages[-1] if final_messages else (messages[-1] if messages and not require_final_answer else None)
            text = str(selected.get("text") or "") if selected else ""
            return text if require_final_answer else text.strip()
        return ""

    def run_existing_task(
        self,
        thread_id: str,
        request: dict[str, Any],
        *,
        client_user_message_id: str,
        model: str | None = None,
        reasoning_effort: str | None = None,
    ) -> dict[str, Any]:
        self.start()
        try:
            with ProcessLock(
                self.thread_operation_lock_path(thread_id),
                timeout_seconds=self.config.thread_operation_lock_timeout_seconds,
                stale_seconds=(
                    self.config.turn_completion_timeout_seconds
                    + self.config.turn_readback_timeout_seconds
                    + 60
                ),
            ):
                selected_model = self.select_model(model or request.get("model"))
                self.runtime_context = {}
                resumed = self.request("thread/resume", {"threadId": thread_id, "excludeTurns": True, **self._runtime_thread_params(selected_model)})
                self._verify_runtime_context(resumed, operation="thread/resume", thread_id=thread_id, model=selected_model)
                del resumed
                return self._start_turn_with_model(
                    thread_id,
                    append_lane_binding(request["prompt"], request.get("input_binding")),
                    client_user_message_id=client_user_message_id,
                    selected_model=selected_model,
                    selected_effort=(
                        reasoning_effort
                        if reasoning_effort is not None
                        else request.get("reasoning_effort")
                    ),
                )
        except Exception as exc:
            if isinstance(exc, NativeTaskCreationError):
                raise
            raise NativeTaskCreationError(str(exc), thread_id=thread_id) from exc

    def wait_for_turn_terminal(
        self,
        thread_id: str,
        turn_id: str,
        *,
        wait_forever: bool = False,
        control_poll: Callable[[], None] | None = None,
        terminal_readback: bool = False,
    ) -> dict[str, Any]:
        deadline = None if wait_forever else (
            time.monotonic() + self.config.turn_completion_timeout_seconds
        )
        # This capability is enabled only by Hold, on its actual unrestricted
        # AppServerClient. Restricted-role/unknown subclasses do not inherit it.
        enabled = terminal_readback and type(self) is AppServerClient
        self._held_terminal_readback_diagnostic = {
            "state": "waiting" if enabled else "disabled",
            "reason": "Hold opt-in" if enabled else (
                "unsupported client role" if terminal_readback else "not enabled"),
        }
        next_probe = time.monotonic() + 30.0

        def checked_control_poll() -> None:
            if control_poll is not None:
                try:
                    control_poll()
                except Exception as exc:
                    raise _TerminalReadbackControlError(exc) from exc

        while deadline is None or time.monotonic() < deadline:
            if control_poll is not None:
                control_poll()
            remaining = (
                self.config.poll_seconds
                if deadline is None
                else max(deadline - time.monotonic(), 0.05)
            )
            if control_poll is not None:
                remaining = min(remaining, 0.25)
            if enabled:
                remaining = min(remaining, max(next_probe - time.monotonic(), 0.0))
            try:
                item = self.notifications.get(timeout=remaining)
            except queue.Empty:
                item = {}  # Still check due probes when the queue is empty.
            if item is None:
                raise NativeTaskCreationError(
                    "App Server disconnected before exact turn/completed; execution terminal is unknown",
                    thread_id=thread_id,
                )
            if item.get("method") == "turn/completed":
                params = item.get("params")
                turn = params.get("turn") if isinstance(params, dict) else None
                if (isinstance(turn, dict) and params.get("threadId") == thread_id
                        and turn.get("id") == turn_id):
                    return turn
            # Unrelated notification storms cannot starve the monotonic probe.
            if enabled and time.monotonic() >= next_probe:
                # Limit from the *end* as well, including failed/partial probes.
                probe_deadline = time.monotonic() + min(self.config.turn_readback_timeout_seconds, 5.0)
                if deadline is not None:
                    probe_deadline = min(probe_deadline, deadline)
                try:
                    data = self.read_exact_turn(
                        thread_id, turn_id, deadline=probe_deadline,
                        _terminal_only=True, _control_poll=checked_control_poll,
                        _terminal_probe=True,
                    )
                    selected = data["turns"][0]
                    if (selected["status"] in {"completed", "failed", "interrupted"}
                            and selected.get("items_scan_complete") is True
                            and (selected["status"] != "completed"
                                 or selected.get("final_answer_nonempty") is True)):
                        # The complete final-only projection is consumed once by
                        # Hold. Do not begin another readback with a fresh budget.
                        final = selected["items"][0]["text"] if selected["items"] else ""
                        checked_control_poll()
                        if time.monotonic() >= probe_deadline:
                            raise NativeTaskError("Hold terminal readback deadline exhausted after control poll")
                        self._held_terminal_readback_diagnostic = {"state": "terminal", "reason": "exact native readback"}
                        return OwnerExactTerminalReadback({"id": turn_id, "status": selected["status"],
                                "error": selected["native_error"],
                                "_owner_terminal_evidence": "owner_exact_terminal_readback",
                                "_owner_final_message": final,
                                "_owner_thread_id": thread_id})
                    self._held_terminal_readback_diagnostic = {"state": "holding", "reason": "running or final unavailable"}
                except _TerminalReadbackControlError as exc:
                    raise exc.error
                except Exception as exc:
                    # Identity, protocol, partial scans and RPC errors license
                    # neither owner results nor non-controlled Host cleanup.
                    self._held_terminal_readback_diagnostic = {"state": "holding", "reason": str(exc)[:512]}
                finally:
                    # Keep only the fixed-size diagnostic while holding. Even
                    # a huge blank final projection is not cached across polls.
                    data = selected = final = None
                    next_probe = time.monotonic() + 30.0
        raise NativeTaskCreationError(
            "timed out waiting for turn/completed",
            thread_id=thread_id,
        )

    def close(self) -> None:
        process = self.process
        self.process = None
        lock_fd = getattr(self, "_instance_lock_fd", None)
        self._instance_lock_fd = None
        if not process:
            release_instance_lock(lock_fd)
            return
        try:
            if process.stdin:
                process.stdin.close()
            process.wait(timeout=3)
        except (OSError, subprocess.TimeoutExpired):
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        finally:
            release_instance_lock(lock_fd)

    def __enter__(self) -> "AppServerClient":
        self.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


def build_direct_request(
    event: dict[str, Any],
    *,
    dispatcher_thread_id: str,
) -> dict[str, Any]:
    parsed = parse_direct_task(str(event.get("content") or ""))
    if parsed is None:
        raise NativeTaskError("event is not a direct task")
    event_key = str(
        event.get("correlation_id")
        or event.get("event_id")
        or event.get("message_id")
        or ""
    ).strip()
    if not event_key:
        raise NativeTaskError("direct task event has no stable id")
    safe_event_key = re.sub(r"[^A-Za-z0-9._:-]", "-", event_key)[:160]
    return {
        "request_id": f"native:{safe_event_key}",
        "correlation_id": f"native:{safe_event_key}",
        "origin": "cooper_direct",
        "route": "direct_task",
        "source_channel": str(event.get("source_channel") or "unknown"),
        "source_thread_id": str(
            event.get("conversation_id")
            or event.get("chat_id")
            or dispatcher_thread_id
        ),
        "source_message_id": str(
            event.get("source_message_id") or event.get("message_id") or ""
        ),
        "actor_id": str(event.get("sender_id") or event.get("sender_open_id") or ""),
        **parsed,
    }


def load_request(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise NativeTaskError("request file must contain a JSON object")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    sub = parser.add_subparsers(dest="command", required=True)

    submit = sub.add_parser("submit")
    submit.add_argument("--request", type=Path, required=True)

    run_once = sub.add_parser("run-once")
    run_once.add_argument("--execute", action="store_true")

    recover = sub.add_parser("recover")
    recover.add_argument("--request-id", required=True)
    recover.add_argument("--model")
    recover.add_argument("--reasoning-effort", choices=sorted(REASONING_EFFORTS))
    recover.add_argument("--execute", action="store_true")

    serve = sub.add_parser("serve")
    serve.add_argument("--execute", action="store_true")

    sub.add_parser("status")
    sub.add_parser("probe")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        config = NativeTaskLauncherConfig(args.config)
        task_queue = NativeTaskQueue(args.root, config)
        if args.command == "submit":
            result = task_queue.submit(load_request(args.request))
        elif args.command == "status":
            result = {
                "live_creation_enabled": config.live_creation_enabled,
                "pending_count": len(task_queue.pending()),
                "requests_path": str(task_queue.requests_path),
                "results_path": str(task_queue.results_path),
            }
        elif args.command == "probe":
            with AppServerClient(config) as client:
                result = client.probe()
        elif args.command == "run-once":
            with ProcessLock(task_queue.service_lock_path, timeout_seconds=1):
                if args.execute:
                    with AppServerClient(config) as client:
                        result = task_queue.process_one(client, execute=True)
                else:
                    result = task_queue.process_one(
                        AppServerClient(config),
                        execute=False,
                    )
        elif args.command == "recover":
            if not args.execute:
                raise NativeTaskError("recover requires --execute")
            with ProcessLock(task_queue.service_lock_path, timeout_seconds=1):
                with AppServerClient(config) as client:
                    result = task_queue.recover_partial(
                        client,
                        args.request_id,
                        model=args.model,
                        reasoning_effort=args.reasoning_effort,
                    )
        elif args.command == "serve":
            if args.execute and not config.live_creation_enabled:
                raise NativeTaskError("live native task creation is disabled in config")
            with ProcessLock(
                task_queue.service_lock_path,
                timeout_seconds=1,
                stale_seconds=300,
            ):
                if args.execute:
                    with AppServerClient(config) as client:
                        while True:
                            result = task_queue.process_one(client, execute=True)
                            if result is None:
                                time.sleep(config.poll_seconds)
                else:
                    result = task_queue.process_one(
                        AppServerClient(config),
                        execute=False,
                    )
        else:
            raise NativeTaskError(f"unsupported command: {args.command}")
    except (NativeTaskError, DispatcherError, OSError, json.JSONDecodeError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps({"ok": True, "result": result}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
