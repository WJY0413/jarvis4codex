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


class NativeTaskCreationError(NativeTaskError):
    def __init__(self, message: str, *, thread_id: str | None = None):
        super().__init__(message)
        self.thread_id = thread_id


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
        self.codex_cli = str(raw.get("codex_cli") or "auto")
        self.profile = str(raw.get("profile") or "").strip()
        self.expected_codex_home = str(raw.get("expected_codex_home") or "").strip()
        self.live_creation_enabled = bool(raw.get("live_creation_enabled", False))
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
        self.allowed_projects = {
            str(name).strip(): str(path_value).strip()
            for name, path_value in projects.items()
            if str(name).strip() and str(path_value).strip()
        }

    def resolve_project(self, value: str) -> tuple[str, Path]:
        requested = _as_nonempty_string(value, "project")
        if requested in self.allowed_projects:
            name = requested
            path = Path(self.allowed_projects[requested]).resolve()
        else:
            requested_path = Path(requested).resolve()
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
        self.notifications: queue.Queue[dict[str, Any]] = queue.Queue()
        self.stderr_lines: list[str] = []
        self._request_id = 0
        self._write_lock = threading.Lock()
        self.initialize_result: dict[str, Any] | None = None

    def start(self) -> dict[str, Any]:
        if self.process and self.process.poll() is None:
            return self.initialize_result or {}
        env = os.environ.copy()
        env["PYTHONUTF8"] = "1"
        # The launcher may itself run under a sandboxed controller process.
        # Pin the child App Server to the configured real Codex profile so
        # native tasks are created in the intended visible task space.
        if self.config.expected_codex_home:
            env["CODEX_HOME"] = str(self.config.expected_codex_home)
        self.process = subprocess.Popen(
            [*self.cli_command, "app-server", "--stdio"],
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
                    f"CODEX_HOME {self.config.expected_codex_home!r}; run hold from the normal "
                    "Windows user host, not the MCP sandbox."
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
        for raw in self.process.stdout:
            try:
                item = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(item, dict):
                continue
            if "id" in item:
                self.response_queue.put(item)
            else:
                self.notifications.put(item)

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
            self.process.stdin.write(encoded + "\n")
            self.process.stdin.flush()

    def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        value: dict[str, Any] = {"method": method}
        if params is not None:
            value["params"] = params
        self._write(value)

    def request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        self._request_id += 1
        request_id = self._request_id
        self._write({"id": request_id, "method": method, "params": params})
        deadline = time.monotonic() + self.config.request_timeout_seconds
        deferred: list[dict[str, Any]] = []
        try:
            while time.monotonic() < deadline:
                remaining = max(deadline - time.monotonic(), 0.05)
                try:
                    item = self.response_queue.get(timeout=remaining)
                except queue.Empty:
                    break
                if item.get("id") != request_id:
                    deferred.append(item)
                    continue
                if "error" in item:
                    raise NativeTaskError(
                        f"app-server {method} failed: "
                        + json.dumps(item["error"], ensure_ascii=False)
                    )
                result = item.get("result")
                return result if isinstance(result, dict) else {"result": result}
        finally:
            for item in deferred:
                self.response_queue.put(item)
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
        models = self.available_models()
        supported: dict[str, dict[str, Any]] = {}
        for item in models:
            model_id = str(item.get("id") or item.get("model") or "").strip()
            if model_id:
                supported[model_id] = item
        if not supported:
            raise NativeTaskError("app-server model/list returned no supported models")
        if requested:
            if requested not in supported:
                raise NativeTaskError(
                    f"requested model is not supported by this CLI: {requested}; "
                    f"available={sorted(supported)}"
                )
            return requested
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
        self.request("thread/resume", {"threadId": thread_id})
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
            started = self.request("thread/start", {"cwd": project_path, "ephemeral": False})
            thread = started.get("thread")
            thread_id = str(thread.get("id") or "") if isinstance(thread, dict) else ""
            if not thread_id:
                raise NativeTaskCreationError("thread/start response is missing thread.id")
            _report_phase(on_phase, "thread_started", thread_id=thread_id)
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
                thread_id=thread_id,
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
    ) -> dict[str, Any]:
        turn_params: dict[str, Any] = {
            "threadId": thread_id,
            "clientUserMessageId": client_user_message_id,
            "input": [{"type": "text", "text": prompt}],
            "model": selected_model,
        }
        if selected_effort is not None:
            turn_params["effort"] = selected_effort
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

    def wait_for_turn_readback(self, thread_id: str, turn_id: str, *, require_final_answer: bool = False) -> str:
        """Wait until the exact completed turn is materialized in thread/read.

        App Server can deliver ``turn/completed`` before the corresponding
        messages are visible in ``thread/read``.  A blank immediate readback is
        therefore not completion proof and must never be treated as a final
        answer by a bridge or monitor.
        """
        deadline = time.monotonic() + self.config.turn_readback_timeout_seconds
        while True:
            read_result = self.request(
                "thread/read",
                {"threadId": thread_id, "includeTurns": True},
            )
            final_message = self.final_message_for_turn(
                read_result.get("thread"),
                turn_id,
                require_final_answer=require_final_answer,
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
                self.request("thread/resume", {"threadId": thread_id})
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
    ) -> dict[str, Any]:
        deadline = None if wait_forever else (
            time.monotonic() + self.config.turn_completion_timeout_seconds
        )
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
            try:
                item = self.notifications.get(timeout=remaining)
            except queue.Empty:
                continue
            if item.get("method") != "turn/completed":
                continue
            params = item.get("params")
            if not isinstance(params, dict):
                continue
            turn = params.get("turn")
            if not isinstance(turn, dict):
                continue
            if (
                str(params.get("threadId") or "") == thread_id
                and str(turn.get("id") or "") == turn_id
            ):
                return turn
        raise NativeTaskCreationError(
            "timed out waiting for turn/completed",
            thread_id=thread_id,
        )

    def close(self) -> None:
        process = self.process
        self.process = None
        if not process:
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
