"""MCP tool registry and protocol adapter for the Jarvis control facade."""

from __future__ import annotations

import json
from typing import Any, Literal

from mcp.server import MCPServer
from mcp.types import CallToolResult, TextContent, ToolAnnotations

from jarvis_control import JarvisControl


class JarvisMcpServer:
    """Register the six first-priority Jarvis tools on the official MCP SDK."""

    def __init__(self, control: JarvisControl) -> None:
        self.control = control
        self.mcp = MCPServer(
            "jarvis-control",
            title="Jarvis Control Plane",
            version="0.1.4",
            instructions=(
                "Use jarvis_read before a state-changing call when you need capability or thread context. "
                "Treat every receipt status other than completed as not executed or not verified."
            ),
        )
        self._register_tools()

    def run_stdio(self) -> None:
        """Run the already-wired server on local standard input/output."""
        self.mcp.run("stdio")

    def _register_tools(self) -> None:
        @self.mcp.tool(
            name="jarvis_create",
            description="Create a Jarvis task. Currently reports unsupported until a verified task-creation adapter is supplied.",
            annotations=ToolAnnotations(destructiveHint=False, idempotentHint=False, openWorldHint=False),
        )
        def jarvis_create(project: str, title: str, prompt: str) -> CallToolResult:
            del project, title, prompt
            return _tool_result(self.control.unsupported(
                tool="jarvis_create", reason="no task-creation adapter is configured"
            ))

        @self.mcp.tool(
            name="jarvis_read",
            description="Read Jarvis capability availability or a known existing task. This tool does not change state.",
            annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
        )
        def jarvis_read(subject: Literal["capabilities", "thread"], task_id: str | None = None) -> CallToolResult:
            return _tool_result(self.control.read(subject=subject, task_id=task_id))

        @self.mcp.tool(
            name="jarvis_resume",
            description="Resume one existing Jarvis-managed task and return its exact post-turn readback receipt.",
            annotations=ToolAnnotations(idempotentHint=True, openWorldHint=False),
        )
        def jarvis_resume(
            task_id: str,
            prompt: str,
            request_id: str,
            source_ref: str = "mcp:jarvis_resume",
            model: str | None = None,
            reasoning_effort: str | None = None,
        ) -> CallToolResult:
            return _tool_result(self.control.resume(
                request_id=request_id, task_id=task_id, prompt=prompt, source_ref=source_ref,
                model=model, reasoning_effort=reasoning_effort,
            ))

        @self.mcp.tool(
            name="jarvis_monitor",
            description="Observe a known task, or resume a configured target only after a newly observed completed terminal state.",
            annotations=ToolAnnotations(idempotentHint=True, openWorldHint=False),
        )
        def jarvis_monitor(
            action: Literal["observe", "terminal_resume"],
            request_id: str,
            monitor_id: str,
            observed_task_id: str,
            receipt_task_id: str,
            source_ref: str = "mcp:jarvis_monitor",
            resume_task_id: str | None = None,
            prompt: str | None = None,
            model: str | None = None,
            reasoning_effort: str | None = None,
        ) -> CallToolResult:
            return _tool_result(self.control.monitor(
                action=action, request_id=request_id, monitor_id=monitor_id,
                observed_task_id=observed_task_id, receipt_task_id=receipt_task_id,
                source_ref=source_ref, resume_task_id=resume_task_id, prompt=prompt,
                model=model, reasoning_effort=reasoning_effort,
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
            description="Deliver a Jarvis notification. Currently reports unsupported until a verified notification adapter is supplied.",
            annotations=ToolAnnotations(destructiveHint=False, idempotentHint=False, openWorldHint=False),
        )
        def jarvis_notify(message: str) -> CallToolResult:
            del message
            return _tool_result(self.control.unsupported(
                tool="jarvis_notify", reason="no notification adapter is configured"
            ))


def _tool_result(receipt: dict[str, Any]) -> CallToolResult:
    is_error = receipt["status"] in {"invalid_request", "unsupported", "failed"}
    summary = json.dumps(receipt, ensure_ascii=False, sort_keys=True)
    return CallToolResult(
        content=[TextContent(type="text", text=summary)],
        structuredContent=receipt,
        isError=is_error,
    )
