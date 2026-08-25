"""A small, harness-neutral facade around the current Jarvis capability port."""

from __future__ import annotations

from typing import Any, Mapping

from jarvis_codex_bridge import CapabilityRequest, ExistingThreadBridge, JarvisCapabilityPort


JARVIS_MCP_RECEIPT_SCHEMA = "jarvis-mcp-receipt/v1"


class JarvisControl:
    """Expose only supported Jarvis operations through stable receipt envelopes.

    This class deliberately does not create tasks or deliver external messages.  Those
    operations have no verified adapter in the current product source, so callers get
    an explicit unsupported receipt instead of a guessed implementation.
    """

    def __init__(self, capabilities: JarvisCapabilityPort, bridge: ExistingThreadBridge) -> None:
        self._capabilities = capabilities
        self._bridge = bridge

    def resume(
        self,
        *,
        request_id: str,
        task_id: str,
        prompt: str,
        source_ref: str,
        model: str | None = None,
        reasoning_effort: str | None = None,
    ) -> dict[str, Any]:
        return self._invoke(
            "jarvis_resume",
            "resume.existing",
            request_id=request_id,
            source_ref=source_ref,
            arguments={
                "thread_id": task_id,
                "prompt": prompt,
                "model": model,
                "reasoning_effort": reasoning_effort,
            },
        )

    def read(self, *, subject: str, task_id: str | None = None) -> dict[str, Any]:
        if subject == "capabilities":
            return self._receipt(
                "jarvis_read",
                "completed",
                data={
                    "jarvis_create": {"available": False, "reason": "no task-creation adapter is configured"},
                    "jarvis_read": {"available": True, "read_only": True},
                    "jarvis_resume": {"available": True},
                    "jarvis_monitor": {"available": True},
                    "jarvis_heartbeat": {
                        "available": self._capabilities.heartbeat_available,
                        "requires_scheduler": True,
                    },
                    "jarvis_notify": {"available": False, "reason": "no notification adapter is configured"},
                },
            )
        if subject == "thread":
            if not task_id or not task_id.strip():
                return self._receipt("jarvis_read", "invalid_request", reason="task_id is required for subject=thread")
            try:
                state = self._bridge.observe_thread(task_id)
            except Exception as exc:
                return self._receipt(
                    "jarvis_read", "failed", target_thread_id=task_id, reason=str(exc)
                )
            return self._receipt(
                "jarvis_read",
                "completed",
                target_thread_id=state.thread_id,
                data={
                    "thread_id": state.thread_id,
                    "status": state.status,
                    "turns": [
                        {"turn_id": turn.turn_id, "status": turn.status, "error": turn.error}
                        for turn in state.turns
                    ],
                },
            )
        return self._receipt("jarvis_read", "invalid_request", reason="subject must be capabilities or thread")

    def monitor(
        self,
        *,
        action: str,
        request_id: str,
        monitor_id: str,
        observed_task_id: str,
        receipt_task_id: str,
        source_ref: str,
        resume_task_id: str | None = None,
        prompt: str | None = None,
        model: str | None = None,
        reasoning_effort: str | None = None,
    ) -> dict[str, Any]:
        capability = {"observe": "monitor.observe", "terminal_resume": "monitor.terminal_resume"}.get(action)
        if capability is None:
            return self._receipt("jarvis_monitor", "invalid_request", request_id=request_id, reason="action must be observe or terminal_resume")
        return self._invoke(
            "jarvis_monitor",
            capability,
            request_id=request_id,
            source_ref=source_ref,
            arguments={
                "monitor_id": monitor_id,
                "observed_thread_id": observed_task_id,
                "receipt_target_thread_id": receipt_task_id,
                "resume_target_thread_id": resume_task_id,
                "prompt": prompt,
                "model": model,
                "reasoning_effort": reasoning_effort,
            },
        )

    def heartbeat(
        self,
        *,
        action: str,
        request_id: str,
        source_ref: str,
        heartbeat_id: str | None = None,
        options: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        capability = {
            "create": "heartbeat.create",
            "update": "heartbeat.update",
            "cancel": "heartbeat.cancel",
            "health": "heartbeat.health",
        }.get(action)
        if capability is None:
            return self._receipt("jarvis_heartbeat", "invalid_request", request_id=request_id, reason="unknown heartbeat action")
        if not self._capabilities.heartbeat_available:
            return self._receipt(
                "jarvis_heartbeat",
                "unsupported",
                request_id=request_id,
                reason="no scheduler-owned heartbeat adapter is configured",
            )
        arguments = dict(options or {})
        if heartbeat_id is not None:
            arguments["heartbeat_id"] = heartbeat_id
        return self._invoke(
            "jarvis_heartbeat", capability, request_id=request_id, source_ref=source_ref, arguments=arguments
        )

    def unsupported(self, *, tool: str, reason: str) -> dict[str, Any]:
        return self._receipt(tool, "unsupported", reason=reason)

    def _invoke(
        self,
        tool: str,
        capability: str,
        *,
        request_id: str,
        source_ref: str,
        arguments: Mapping[str, Any],
    ) -> dict[str, Any]:
        try:
            capability_receipt = self._capabilities.invoke(
                CapabilityRequest(
                    request_id=request_id,
                    capability=capability,  # type: ignore[arg-type]
                    source_ref=source_ref,
                    arguments=arguments,
                )
            )
        except (RuntimeError, ValueError) as exc:
            return self._receipt(tool, "invalid_request", request_id=request_id, reason=str(exc))
        return self._receipt(
            tool,
            capability_receipt.status,
            request_id=capability_receipt.request_id,
            target_thread_id=capability_receipt.target_thread_id,
            turn_id=capability_receipt.turn_id,
            reason=capability_receipt.reason,
            data=capability_receipt.data,
            readback={"verified": capability_receipt.status == "completed"},
        )

    @staticmethod
    def _receipt(
        tool: str,
        status: str,
        *,
        request_id: str | None = None,
        target_thread_id: str | None = None,
        turn_id: str | None = None,
        reason: str | None = None,
        data: Mapping[str, Any] | None = None,
        readback: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "schema": JARVIS_MCP_RECEIPT_SCHEMA,
            "tool": tool,
            "status": status,
            "request_id": request_id,
            "target_thread_id": target_thread_id,
            "turn_id": turn_id,
            "reason": reason,
            "data": dict(data) if data is not None else None,
            "readback": dict(readback or {"verified": False}),
        }
