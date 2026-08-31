"""MCP tool registry and protocol adapter for the Jarvis control facade."""

from __future__ import annotations

import json
from typing import Annotated, Any, Literal

from mcp.server import MCPServer
from mcp.types import CallToolResult, TextContent, ToolAnnotations
from pydantic import Field

from jarvis_control import JarvisControl


class JarvisMcpServer:
    """Register the public Jarvis control tools on the official MCP SDK."""

    def __init__(self, control: JarvisControl) -> None:
        self.control = control
        self.mcp = MCPServer(
            "jarvis-control",
            title="Jarvis Control Plane",
            version="0.1.9",
            instructions=(
                "Use jarvis_read before a state-changing call when you need capability or thread context. "
                "Use jarvis_hold for a managed lifecycle: Hold executes turns and Monitor issues a verified "
                "CONTINUE or STOP command only after exact terminal and content readback. "
                "Use jarvis_read subject=hold for lifecycle state and jarvis_monitor only for status or notification delivery."
            ),
        )
        self._register_tools()

    def run_stdio(self) -> None:
        """Run the already-wired server on local standard input/output."""
        self.mcp.run("stdio")

    def _register_tools(self) -> None:
        @self.mcp.tool(
            name="jarvis_create",
            description="Ask Jarvis to create and hold a task. Return only after hold owns the exact first turn.",
            annotations=ToolAnnotations(destructiveHint=False, idempotentHint=False, openWorldHint=False),
        )
        def jarvis_create(
            project: str,
            title: str,
            prompt: str,
            request_id: str,
            source_ref: str = "mcp:jarvis_create",
            model: str | None = None,
            reasoning_effort: str | None = None,
            max_turns: int = 1,
            auto_continue: bool = False,
            continue_prompt: str = "继续",
            hold_id: str | None = None,
            notifications: dict[str, Any] | None = None,
        ) -> CallToolResult:
            return _tool_result(self.control.create(
                request_id=request_id,
                project=project,
                title=title,
                prompt=prompt,
                source_ref=source_ref,
                model=model,
                reasoning_effort=reasoning_effort,
                max_turns=max_turns,
                auto_continue=auto_continue,
                continue_prompt=continue_prompt,
                hold_id=hold_id,
                notifications=notifications,
            ))

        @self.mcp.tool(
            name="jarvis_hold",
            description="Start one managed lifecycle. Hold executes turns; Monitor issues CONTINUE or STOP only after exact terminal and content readback.",
            annotations=ToolAnnotations(destructiveHint=False, idempotentHint=False, openWorldHint=False),
        )
        def jarvis_hold(
            request_id: str,
            prompt: str,
            source_ref: str = "mcp:jarvis_hold",
            task_id: str | None = None,
            project: str | None = None,
            title: str | None = None,
            hold_id: str | None = None,
            model: str | None = None,
            reasoning_effort: str | None = None,
            max_turns: int = 1,
            auto_continue: bool = False,
            continue_prompt: str = "继续",
            notifications: dict[str, Any] | None = None,
        ) -> CallToolResult:
            return _tool_result(self.control.hold(
                request_id=request_id,
                prompt=prompt,
                source_ref=source_ref,
                task_id=task_id,
                project=project,
                title=title,
                hold_id=hold_id,
                model=model,
                reasoning_effort=reasoning_effort,
                max_turns=max_turns,
                auto_continue=auto_continue,
                continue_prompt=continue_prompt,
                notifications=notifications,
            ))

        @self.mcp.tool(
            name="jarvis_loop",
            description="Preflight, start, inspect, or stop one bounded Jarvis loop. action=preflight is read-only; action=start requires prompt and business_skill; controller_skill defaults to jarvis-run-controller.",
            annotations=ToolAnnotations(destructiveHint=False, idempotentHint=False, openWorldHint=False),
        )
        def jarvis_loop(
            action: Literal["preflight", "start", "status", "stop"] = "start",
            loop_id: str | None = None,
            request_id: str | None = None,
            project: str | None = None,
            title: str | None = None,
            prompt: Annotated[str | None, Field(description="Required when action=start.")] = None,
            business_skill: Annotated[str | None, Field(description="Required when action=start.")] = None,
            controller_skill: Annotated[str | None, Field(description="Optional when action=start; defaults to jarvis-run-controller.")] = None,
            target_thread_count: int | None = None,
            threads: list[dict[str, Any]] | None = None,
            max_rounds: int | None = None,
            max_turns: int | None = None,
            auto_continue: bool | None = None,
            interval_seconds: int | None = None,
            expires_at: str | None = None,
            model: str | None = None,
            reasoning_effort: str | None = None,
            notifications: dict[str, Any] | bool | None = None,
        ) -> CallToolResult:
            return _tool_result(self.control.loop(
                action=action, loop_id=loop_id, request_id=request_id, project=project,
                title=title, prompt=prompt, business_skill=business_skill, controller_skill=controller_skill, target_thread_count=target_thread_count,
                threads=threads, max_rounds=max_rounds, max_turns=max_turns,
                auto_continue=auto_continue, interval_seconds=interval_seconds,
                expires_at=expires_at, model=model, reasoning_effort=reasoning_effort,
                notifications=notifications,
            ))

        @self.mcp.tool(
            name="jarvis_read",
            description="Read Jarvis capability availability or a known existing task. This tool does not change state.",
            annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
        )
        def jarvis_read(
            subject: Literal["capabilities", "thread", "hold"],
            task_id: str | None = None,
            hold_id: str | None = None,
        ) -> CallToolResult:
            return _tool_result(self.control.read(subject=subject, task_id=task_id, hold_id=hold_id))

        @self.mcp.tool(
            name="jarvis_resume",
            description="Ask Jarvis to resume and hold one existing task. Return only after hold owns the resumed turn.",
            annotations=ToolAnnotations(idempotentHint=True, openWorldHint=False),
        )
        def jarvis_resume(
            task_id: str,
            prompt: str,
            request_id: str,
            source_ref: str = "mcp:jarvis_resume",
            model: str | None = None,
            reasoning_effort: str | None = None,
            hold_with_monitor: bool = True,
            monitor_id: str | None = None,
            hold_id: str | None = None,
            max_turns: int = 1,
            auto_continue: bool = False,
            continue_prompt: str = "继续",
            notifications: dict[str, Any] | None = None,
        ) -> CallToolResult:
            return _tool_result(self.control.resume(
                request_id=request_id, task_id=task_id, prompt=prompt, source_ref=source_ref,
                model=model, reasoning_effort=reasoning_effort,
                hold_with_monitor=hold_with_monitor, monitor_id=monitor_id, hold_id=hold_id,
                max_turns=max_turns, auto_continue=auto_continue,
                continue_prompt=continue_prompt, notifications=notifications,
            ))

        @self.mcp.tool(
            name="jarvis_monitor",
            description="Observe a known task, or resume a configured target only after a newly observed completed terminal state.",
            annotations=ToolAnnotations(idempotentHint=True, openWorldHint=False),
        )
        def jarvis_monitor(
            action: Literal["observe", "terminal_resume", "status", "deliver_hold_notifications"],
            request_id: str,
            source_ref: str = "mcp:jarvis_monitor",
            monitor_id: str | None = None,
            observed_task_id: str | None = None,
            receipt_task_id: str | None = None,
            resume_task_id: str | None = None,
            prompt: str | None = None,
            model: str | None = None,
            reasoning_effort: str | None = None,
            hold_id: str | None = None,
        ) -> CallToolResult:
            return _tool_result(self.control.monitor(
                action=action, request_id=request_id, monitor_id=monitor_id,
                source_ref=source_ref, observed_task_id=observed_task_id,
                receipt_task_id=receipt_task_id, resume_task_id=resume_task_id, prompt=prompt,
                model=model, reasoning_effort=reasoning_effort, hold_id=hold_id,
            ))

        @self.mcp.tool(
            name="jarvis_heartbeat",
            description="Create, update, cancel, or check a scheduler-owned Jarvis heartbeat through its configured scheduler adapter.",
            annotations=ToolAnnotations(idempotentHint=True, openWorldHint=False),
        )
        def jarvis_heartbeat(
            action: Literal["create", "update", "cancel", "health"],
            request_id: str,
            source_ref: str = "mcp:jarvis_heartbeat",
            heartbeat_id: str | None = None,
            options: dict[str, Any] | None = None,
        ) -> CallToolResult:
            return _tool_result(self.control.heartbeat(
                action=action, request_id=request_id, source_ref=source_ref,
                heartbeat_id=heartbeat_id, options=options,
            ))

        @self.mcp.tool(
            name="jarvis_notify",
            description="Deliver a Jarvis notification through its configured verified notification adapter.",
            annotations=ToolAnnotations(destructiveHint=False, idempotentHint=False, openWorldHint=False),
        )
        def jarvis_notify(
            message: str,
            request_id: str = "mcp:jarvis_notify",
            source_ref: str = "mcp:jarvis_notify",
        ) -> CallToolResult:
            return _tool_result(self.control.notify(
                request_id=request_id, source_ref=source_ref, message=message,
            ))


def _tool_result(receipt: dict[str, Any]) -> CallToolResult:
    is_error = receipt["status"] in {"invalid_request", "unsupported", "failed", "partial", "requires_readback"}
    summary = json.dumps(receipt, ensure_ascii=False, sort_keys=True)
    return CallToolResult(
        content=[TextContent(type="text", text=summary)],
        structuredContent=receipt,
        isError=is_error,
    )
