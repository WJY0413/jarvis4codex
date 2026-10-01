"""MCP tool registry and protocol adapter for the Jarvis control facade."""

from __future__ import annotations

import json
import threading
from typing import Annotated, Any, Literal

from mcp.server import MCPServer
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import CallToolResult, TextContent, ToolAnnotations
from pydantic import Field

from jarvis_control import JarvisControl


class JarvisMcpServer:
    """Register the public Jarvis control tools on the official MCP SDK."""

    def __init__(self, control: JarvisControl) -> None:
        self.control = control
        # Wired App Server adapters and receipt stores are shared, not per-client.
        self._control_lock = threading.RLock()
        self.mcp = MCPServer(
            "jarvis-control",
            title="Jarvis dot",
            version="0.2.5",
            instructions=(
                "Use jarvis_read before a state-changing call when you need capability or thread context. "
                "Use jarvis_hold for a managed lifecycle: Hold executes turns and Monitor issues a verified "
                "CONTINUE or STOP command only after exact terminal and content readback. "
                "Use jarvis_read subject=hold for lifecycle state and jarvis_monitor only for status or notification delivery."
                " Use jarvis_close to gracefully stop an exact Loop or standalone Hold; closing is not confirmed closure."
            ),
        )
        self._register_tools()
        self.control._registry_reader = lambda: [{"name": tool.name, "inputSchema": tool.parameters, "description": tool.description} for tool in self.mcp._tool_manager.list_tools()]

    def run_stdio(self) -> None:
        """Run the already-wired server on local standard input/output."""
        self.mcp.run("stdio")

    def run_http(self, *, port: int) -> None:
        """Explicit single-process, loopback-only service; no per-client sessions."""
        if not 1 <= port <= 65535:
            raise ValueError("HTTP port must be between 1 and 65535")
        self.mcp.run(
            "streamable-http", host="127.0.0.1", port=port,
            stateless_http=True, json_response=True,
            transport_security=TransportSecuritySettings(
                enable_dns_rebinding_protection=True,
                allowed_hosts=[f"127.0.0.1:{port}"],
                allowed_origins=[f"http://127.0.0.1:{port}"],
            ),
        )

    def _invoke(self, method: str, **kwargs: Any) -> dict[str, Any]:
        # One adapter call at a time protects shared request/response queues and
        # read-modify-write state. A failed call releases the lock as well.
        with self._control_lock:
            return getattr(self.control, method)(**kwargs)

    def _register_tools(self) -> None:
        @self.mcp.tool(name="jarvis_contract_check", description="Evaluate the declared controller contract against the actual running build, registry and effective deployment; persist exact terminal check evidence.", annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=False))
        def jarvis_contract_check(request_id: str, contract_id: str, expected_code_sha256: str, expected_config_sha256: str) -> CallToolResult:
            return _tool_result(self._invoke("contract_check", request_id=request_id, contract_id=contract_id, expected_code_sha256=expected_code_sha256, expected_config_sha256=expected_config_sha256))

        @self.mcp.tool(name="jarvis_receipt", description="Read an exact durable operation receipt and its source/target evidence. Never dispatch a new action.", annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False))
        def jarvis_receipt(request_id: str) -> CallToolResult:
            return _tool_result(self._invoke("operation_read", request_id=request_id))

        @self.mcp.tool(name="jarvis_callback", description="Deliver a verified exact source-completion callback to a declared eligible TEST target and retain both identities/readbacks.", annotations=ToolAnnotations(destructiveHint=False, idempotentHint=True, openWorldHint=False))
        def jarvis_callback(request_id: str, run_id: str, source_thread_id: str, source_turn_id: str, target_thread_id: str, target_input: str, source_ref: str) -> CallToolResult:
            return _tool_result(self._invoke("route_output", kind="callback", request_id=request_id, run_id=run_id, source_thread_id=source_thread_id, source_turn_id=source_turn_id, target_thread_id=target_thread_id, target_input=target_input, source_ref=source_ref))

        @self.mcp.tool(name="jarvis_relay", description="Read one completed source's raw final output and relay it unchanged to an eligible TEST target. Completion requires exact target input and output equality.", annotations=ToolAnnotations(destructiveHint=False, idempotentHint=True, openWorldHint=False))
        def jarvis_relay(request_id: str, run_id: str, source_thread_id: str, source_turn_id: str, target_thread_id: str, source_ref: str) -> CallToolResult:
            return _tool_result(self._invoke("route_output", kind="relay", request_id=request_id, run_id=run_id, source_thread_id=source_thread_id, source_turn_id=source_turn_id, target_thread_id=target_thread_id, source_ref=source_ref))

        @self.mcp.tool(name="jarvis_read_delivery", description="Read a real configured receiver's stored message, recipient, exact body and hash by message or request identity.", annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False))
        def jarvis_read_delivery(message_id: str | None = None, request_id: str | None = None) -> CallToolResult:
            return _tool_result(self._invoke("read_delivery", message_id=message_id, request_id=request_id))

        @self.mcp.tool(name="jarvis_capacity", description="Run exactly the declared bounded concurrent TEST inputs; Jarvis selects its implementation and returns separate child identities.", annotations=ToolAnnotations(destructiveHint=False, idempotentHint=False, openWorldHint=False))
        def jarvis_capacity(request_id: str, run_id: str, project: str, inputs: list[str]) -> CallToolResult:
            return _tool_result(self._invoke("capacity", request_id=request_id, run_id=run_id, project=project, inputs=inputs))

        @self.mcp.tool(name="jarvis_dispatch", description="Manage a normal owned, durable dispatch intent for an eligible TEST target. A paused owner can be stopped at its real pre-dispatch boundary; recovery never dispatches a replacement.", annotations=ToolAnnotations(destructiveHint=False, idempotentHint=True, openWorldHint=False))
        def jarvis_dispatch(action: Literal["start", "read", "release", "stop", "recover"], request_id: str, run_id: str,
                            target_thread_id: str | None = None, prompt: str | None = None, source_ref: str | None = None) -> CallToolResult:
            return _tool_result(self._invoke("dispatch", action=action, request_id=request_id, run_id=run_id,
                target_thread_id=target_thread_id, prompt=prompt, source_ref=source_ref))

        @self.mcp.tool(
            name="jarvis_create",
            description="Ask Jarvis to create and hold a task. Return only after hold owns the exact first turn.",
            annotations=ToolAnnotations(destructiveHint=False, idempotentHint=False, openWorldHint=False),
        )
        def jarvis_create(
            project: str,
            prompt: str,
            request_id: str,
            title: str | None = None,
            source_ref: str = "mcp:jarvis_create",
            model: str | None = None,
            reasoning_effort: str | None = None,
            max_turns: int = 1,
            auto_continue: bool = False,
            continue_prompt: str = "继续",
            hold_id: str | None = None,
            notifications: dict[str, Any] | None = None,
            run_id: str | None = None,
            role: str | None = None,
            test_only: bool = False,
        ) -> CallToolResult:
            return _tool_result(self._invoke("create",
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
                notifications=notifications, run_id=run_id, role=role, test_only=test_only,
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
            return _tool_result(self._invoke("hold",
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
            description="Preflight, start, inspect, or stop one bounded Jarvis loop. action=preflight is read-only; action=start requires a prompt. controller_skill and business_skill are optional.",
            annotations=ToolAnnotations(destructiveHint=False, idempotentHint=False, openWorldHint=False),
        )
        def jarvis_loop(
            action: Literal["preflight", "start", "status", "stop"] = "start",
            loop_id: str | None = None,
            request_id: str | None = None,
            project: str | None = None,
            title: str | None = None,
            prompt: Annotated[str | None, Field(description="Required when action=start.")] = None,
            business_skill: Annotated[str | None, Field(description="Optional business Skill to inject into the Worker prompt.")] = None,
            controller_skill: Annotated[str | None, Field(description="Optional controller Skill to inject into the Worker prompt.")] = None,
            target_thread_count: int | None = None,
            threads: Annotated[list[dict[str, Any]] | None, Field(
                description="Optional finite-item lanes; omit lanes for open-ended tasks. batch_size defaults to 1. result_verification.mode defaults to file_receipt; final_answer_json requires batch_size=1, separate receipt_paths and terminal_statuses=['received']; Holder saves the exact final answer and payload with review_needed=true, never business QA approval. output_schema uses Draft 2020-12 with document-local refs and no $id; in final_answer_json mode mismatches are recorded for review, not rejected.",
                json_schema_extra={"anyOf": [{"type": "array", "items": {"type": "object", "properties": {
                    "lane": {"type": "object", "properties": {
                        "batch_size": {"type": "integer", "minimum": 1, "default": 1},
                        "result_verification": {"type": "object", "properties": {
                            "mode": {"type": "string", "enum": ["file_receipt", "final_answer_json"], "default": "file_receipt"},
                            "output_schema": {"type": ["object", "boolean"]},
                        }},
                    }},
                }}}, {"type": "null"}]},
            )] = None,
            max_rounds: int | None = None,
            max_turns: int | None = None,
            turns_per_thread: Annotated[int | None, Field(description="Optional positive turn quota per thread, including its first turn. Rotate to a new thread in the same seat; max_rounds remains the total per seat.")] = None,
            continue_prompt: Annotated[str | None, Field(description="Optional continuation task prompt. New threads use prompt; omitted continuation reuses prompt.")] = None,
            auto_continue: bool | None = None,
            interval_seconds: int | None = None,
            expires_at: str | None = None,
            model: str | None = None,
            reasoning_effort: str | None = None,
            notifications: dict[str, Any] | bool | None = None,
        ) -> CallToolResult:
            return _tool_result(self._invoke("loop",
                action=action, loop_id=loop_id, request_id=request_id, project=project,
                title=title, prompt=prompt, business_skill=business_skill, controller_skill=controller_skill, target_thread_count=target_thread_count,
                threads=threads, max_rounds=max_rounds, max_turns=max_turns,
                turns_per_thread=turns_per_thread, continue_prompt=continue_prompt,
                auto_continue=auto_continue, interval_seconds=interval_seconds,
                expires_at=expires_at, model=model, reasoning_effort=reasoning_effort,
                notifications=notifications,
            ))

        @self.mcp.tool(
            name="jarvis_close",
            description="Durably cancel management of exactly one loop_id or standalone hold_id even when its owner is unknown. Cancel queued work, auto-continuation and resume; request interruption only through a proven exact owner. A dispatch already committed before cancellation may remain in flight. No turn_id required. closed requires terminal/release evidence; closed_unconfirmed means local management stopped but external execution is unknown, never released or replaced automatically. Records a closure report; no shared Host kill, history deletion or chat archival. Prefer loop_id for Loop-managed work.",
            annotations=ToolAnnotations(destructiveHint=True, idempotentHint=True, openWorldHint=False),
        )
        def jarvis_close(
            hold_id: str | None = None,
            loop_id: str | None = None,
            request_id: str | None = None,
        ) -> CallToolResult:
            return _tool_result(self._invoke("jarvis_close",
                hold_id=hold_id, loop_id=loop_id, request_id=request_id,
            ))

        @self.mcp.tool(
            name="jarvis_read",
            description="Read Jarvis capability availability or a known existing task. This tool does not change state.",
            annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
        )
        def jarvis_read(
            subject: Literal["capabilities", "thread", "hold", "history"],
            task_id: str | None = None,
            hold_id: str | None = None,
            thread_id: str | None = None,
            turn_id: str | None = None,
        ) -> CallToolResult:
            return _tool_result(self._invoke("read",
                subject=subject, task_id=task_id, hold_id=hold_id,
                thread_id=thread_id, turn_id=turn_id,
            ))

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
            return _tool_result(self._invoke("resume",
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
            return _tool_result(self._invoke("monitor",
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
            return _tool_result(self._invoke("heartbeat",
                action=action, request_id=request_id, source_ref=source_ref,
                heartbeat_id=heartbeat_id, options=options,
            ))

        @self.mcp.tool(
            name="jarvis_update",
            description="Check or activate Jarvis automatic Codex Desktop runtime selection. check is read-only; apply changes only the launcher policy when no Hold is active.",
            annotations=ToolAnnotations(idempotentHint=True, openWorldHint=False),
        )
        def jarvis_update(
            action: Literal["check", "apply"] = "check",
            request_id: str = "mcp:jarvis_update",
        ) -> CallToolResult:
            return _tool_result(self._invoke("update",
                action=action, request_id=request_id,
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
            return _tool_result(self._invoke("notify",
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
