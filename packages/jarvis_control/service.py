"""A small, harness-neutral facade around the current Jarvis capability port."""

from __future__ import annotations

from typing import Any, Mapping, Protocol

from jarvis_codex_bridge import CapabilityRequest, ExistingThreadBridge, JarvisCapabilityPort

from .loop import LoopController
from .provisioning import TaskMonitorResumeRequest, TaskProvisionRequest, TaskProvisioningPort


JARVIS_MCP_RECEIPT_SCHEMA = "jarvis-mcp-receipt/v1"


class NotificationPort(Protocol):
    def notify(self, *, request_id: str, source_ref: str, message: str) -> Mapping[str, Any]: ...


class JarvisControl:
    """Expose only supported Jarvis operations through stable receipt envelopes.

    External notification remains unavailable unless a verified adapter is supplied.
    """

    def __init__(
        self,
        capabilities: JarvisCapabilityPort,
        bridge: ExistingThreadBridge,
        provisioner: TaskProvisioningPort | None = None,
        notifier: NotificationPort | None = None,
        loop_controller: LoopController | None = None,
    ) -> None:
        self._capabilities = capabilities
        self._bridge = bridge
        self._provisioner = provisioner
        self._notifier = notifier
        self._loop_controller = loop_controller

    def loop(self, *, action: str, loop_id: str | None = None, **options: Any) -> dict[str, Any]:
        """Use the existing Holder and Monitor ports as one bounded loop."""
        if self._loop_controller is None:
            return self.unsupported(tool="jarvis_loop", reason="no loop controller is configured")
        if action == "preflight":
            project_reader = getattr(self._provisioner, "preflight_projects", None)
            if not callable(project_reader):
                return self.unsupported(tool="jarvis_loop", reason="no loop preflight adapter is configured")
            try:
                projects = project_reader()
            except Exception as exc:
                return self._receipt("jarvis_loop", "failed", request_id=options.get("request_id"), reason=str(exc))
            return self._receipt(
                "jarvis_loop", "completed", request_id=options.get("request_id"),
                data=self._loop_controller.preflight_contract(allowed_projects=list(projects)),
                readback={"verified": True, "terminal": False},
            )
        if action == "start":
            health_reader = getattr(self._provisioner, "hold_host_health", None)
            health = health_reader() if callable(health_reader) else {"status": "host_not_ready", "reason": "no HoldHost health adapter is configured"}
            if health.get("status") != "ready":
                return self._receipt(
                    "jarvis_loop", "host_not_ready", request_id=options.get("request_id"),
                    reason=str(health.get("reason") or "HoldHost is not ready"), data=health,
                    readback={"verified": False, "terminal": False},
                )
            result = self._loop_controller.start(self, **options)
        elif action == "tick":
            return self._receipt(
                "jarvis_loop", "invalid_request", request_id=options.get("request_id"),
                reason="loop tick is disabled; Holder and Monitor own continuation",
                readback={"verified": False, "terminal": False},
            )
        elif action == "reconcile":
            result = self._loop_controller.reconcile(self)
        elif action == "status":
            result = self._loop_controller.status(self, loop_id=str(loop_id or ""))
        elif action == "stop":
            result = self._loop_controller.stop(self, loop_id=str(loop_id or ""))
        else:
            return self._receipt("jarvis_loop", "invalid_request", reason="action must be preflight, start, status, or stop")
        return self._receipt(
            "jarvis_loop", result.status, request_id=options.get("request_id"),
            reason=result.reason, data=result.data,
            readback={"verified": result.status not in {"invalid_request", "blocked"}, "terminal": result.status in {"completed", "stopped", "expired"}},
        )

    def create(
        self,
        *,
        request_id: str,
        project: str,
        title: str,
        prompt: str,
        source_ref: str,
        model: str | None = None,
        reasoning_effort: str | None = None,
        max_turns: int = 1,
        auto_continue: bool = False,
        continue_prompt: str = "继续",
        hold_id: str | None = None,
        notifications: Mapping[str, Any] | None = None,
        input_binding: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self._provisioner is None:
            return self.unsupported(
                tool="jarvis_create", reason="no task-creation adapter is configured"
            )
        try:
            provision = self._provisioner.provision(TaskProvisionRequest(
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
                input_binding=input_binding,
            ))
        except ValueError as exc:
            return self._receipt("jarvis_create", "invalid_request", request_id=request_id, reason=str(exc))
        return self._receipt(
            "jarvis_create",
            provision.status,
            request_id=provision.request_id,
            target_thread_id=provision.thread_id,
            turn_id=provision.turn_id,
            reason=provision.reason,
            data=provision.as_dict(),
            readback={
                "verified": provision.status in {"holding", "running", "completed"},
                "terminal": provision.status == "completed",
            },
        )

    def hold(
        self,
        *,
        request_id: str,
        prompt: str,
        source_ref: str,
        task_id: str | None = None,
        project: str | None = None,
        title: str | None = None,
        hold_id: str | None = None,
        model: str | None = None,
        reasoning_effort: str | None = None,
        max_turns: int = 1,
        auto_continue: bool = False,
        continue_prompt: str = "继续",
        notifications: Mapping[str, Any] | None = None,
        input_binding: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Start or resume a lifecycle: Hold executes and Monitor controls continuation."""
        if task_id:
            receipt = self.resume(
                request_id=request_id,
                task_id=task_id,
                prompt=prompt,
                source_ref=source_ref,
                model=model,
                reasoning_effort=reasoning_effort,
                hold_id=hold_id,
                max_turns=max_turns,
                auto_continue=auto_continue,
                continue_prompt=continue_prompt,
                notifications=notifications,
                input_binding=input_binding,
            )
            receipt["tool"] = "jarvis_hold"
            return receipt
        if project is None or title is None:
            return self._receipt(
                "jarvis_hold", "invalid_request", request_id=request_id,
                reason="project and title are required when task_id is omitted",
            )
        receipt = self.create(
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
            input_binding=input_binding,
        )
        receipt["tool"] = "jarvis_hold"
        return receipt

    def resume(
        self,
        *,
        request_id: str,
        task_id: str,
        prompt: str,
        source_ref: str,
        model: str | None = None,
        reasoning_effort: str | None = None,
        hold_with_monitor: bool = False,
        monitor_id: str | None = None,
        hold_id: str | None = None,
        max_turns: int = 1,
        auto_continue: bool = False,
        continue_prompt: str = "继续",
        notifications: Mapping[str, Any] | None = None,
        input_binding: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        # Kept only for wire compatibility. All public resume calls are now monitor-owned.
        del hold_with_monitor
        try:
            request = TaskMonitorResumeRequest(
                request_id=request_id, task_id=task_id, prompt=prompt, source_ref=source_ref,
                monitor_id=monitor_id, hold_id=hold_id, max_turns=max_turns, model=model,
                reasoning_effort=reasoning_effort, auto_continue=auto_continue,
                continue_prompt=continue_prompt, notifications=notifications,
                input_binding=input_binding,
            )
        except ValueError as exc:
            return self._receipt("jarvis_resume", "invalid_request", request_id=request_id, reason=str(exc))
        starter = getattr(self._provisioner, "resume_with_monitor", None)
        if not callable(starter):
            return self.unsupported(tool="jarvis_resume", reason="no monitor-owned resume adapter is configured")
        try:
            provision = starter(request)
        except ValueError as exc:
            return self._receipt("jarvis_resume", "invalid_request", request_id=request_id, reason=str(exc))
        return self._receipt(
            "jarvis_resume", provision.status, request_id=provision.request_id,
            target_thread_id=provision.thread_id, turn_id=provision.turn_id,
            reason=provision.reason, data=provision.as_dict(),
                readback={"verified": provision.status in {"holding", "running"}, "terminal": False},
        )

    def read(
        self, *, subject: str, task_id: str | None = None, hold_id: str | None = None,
        thread_id: str | None = None, turn_id: str | None = None,
    ) -> dict[str, Any]:
        if subject == "capabilities":
            return self._receipt(
                "jarvis_read",
                "completed",
                data={
                    "jarvis_create": {
                        "available": self._provisioner is not None,
                        "reason": None if self._provisioner is not None else "no task-creation adapter is configured",
                    },
                    "jarvis_hold": {
                        "available": self._provisioner is not None,
                        "owns_execution": True,
                        "monitor_controls_continuation": True,
                    },
                    "jarvis_loop": {
                        "available": self._loop_controller is not None,
                        "requires_hold_monitor": True,
                    },
                    "jarvis_read": {"available": True, "read_only": True},
                    "jarvis_resume": {
                        "available": callable(getattr(self._provisioner, "resume_with_monitor", None)),
                        "requires_monitor": True,
                    },
                    "jarvis_monitor": {"available": True},
                    "jarvis_heartbeat": {
                        "available": self._capabilities.heartbeat_available,
                        "requires_scheduler": True,
                    },
                    "jarvis_notify": {
                        "available": self._notifier is not None,
                        "reason": None if self._notifier is not None else "no notification adapter is configured",
                    },
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
        if subject == "hold":
            if not hold_id or not hold_id.strip():
                return self._receipt("jarvis_read", "invalid_request", reason="hold_id is required for subject=hold")
            status_reader = getattr(self._provisioner, "hold_status", None)
            if not callable(status_reader):
                return self._receipt("jarvis_read", "unsupported", reason="no managed-hold status adapter is configured")
            try:
                data = status_reader(hold_id)
            except Exception as exc:
                return self._receipt("jarvis_read", "failed", reason=str(exc))
            return self._receipt("jarvis_read", "completed", data=data)
        if subject == "history":
            reader = getattr(self._provisioner, "read_turn_history", None)
            if not callable(reader):
                return self._receipt("jarvis_read", "unsupported", reason="no managed-hold history adapter is configured")
            try:
                turns = reader(task_id=task_id, hold_id=hold_id, thread_id=thread_id, turn_id=turn_id)
            except (RuntimeError, ValueError) as exc:
                return self._receipt("jarvis_read", "invalid_request", reason=str(exc))
            return self._receipt("jarvis_read", "completed", data={"turns": turns})
        return self._receipt("jarvis_read", "invalid_request", reason="subject must be capabilities, thread, hold, or history")

    def monitor(
        self,
        *,
        action: str,
        request_id: str,
        monitor_id: str | None = None,
        source_ref: str,
        observed_task_id: str | None = None,
        receipt_task_id: str | None = None,
        resume_task_id: str | None = None,
        prompt: str | None = None,
        model: str | None = None,
        reasoning_effort: str | None = None,
        hold_id: str | None = None,
    ) -> dict[str, Any]:
        if action == "status":
            status_reader = getattr(self._provisioner, "hold_status", None)
            if not callable(status_reader):
                status_reader = getattr(self._provisioner, "monitor_status", None)
            if not callable(status_reader):
                return self._receipt("jarvis_monitor", "unsupported", request_id=request_id, reason="no task monitor status adapter is configured")
            try:
                target_hold_id = hold_id or monitor_id
                if not target_hold_id:
                    return self._receipt(
                        "jarvis_monitor", "invalid_request", request_id=request_id,
                        reason="hold_id is required for action=status",
                    )
                data = status_reader(target_hold_id)
            except Exception as exc:
                return self._receipt("jarvis_monitor", "failed", request_id=request_id, reason=str(exc))
            return self._receipt("jarvis_monitor", "completed", request_id=request_id, data=data)
        if action == "deliver_hold_notifications":
            if not hold_id:
                return self._receipt(
                    "jarvis_monitor", "invalid_request", request_id=request_id,
                    reason="hold_id is required for action=deliver_hold_notifications",
                )
            if self._notifier is None:
                return self.unsupported(
                    tool="jarvis_monitor", reason="no verified notification adapter is configured"
                )
            pending_reader = getattr(self._provisioner, "pending_hold_notifications", None)
            recorder = getattr(self._provisioner, "record_hold_notification_delivery", None)
            if not callable(pending_reader) or not callable(recorder):
                return self._receipt(
                    "jarvis_monitor", "unsupported", request_id=request_id,
                    reason="no managed-hold notification event adapter is configured",
                )
            deliveries: list[dict[str, Any]] = []
            for event in pending_reader(hold_id):
                event_id = str(event.get("event_id") or "").strip()
                if not event_id:
                    continue
                delivery = dict(self._notifier.notify(
                    request_id=f"{request_id}:{event_id}",
                    source_ref=source_ref,
                    message=str(event.get("message") or ""),
                ))
                recorder(hold_id, event_id, delivery)
                deliveries.append({"event_id": event_id, **delivery})
            verified = bool(deliveries) and all(
                item.get("delivery_status") == "delivered" and item.get("message_id")
                for item in deliveries
            )
            return self._receipt(
                "jarvis_monitor", "completed" if verified else "requires_readback",
                request_id=request_id,
                data={"hold_id": hold_id, "deliveries": deliveries},
                readback={"verified": verified, "terminal": False},
            )
        capability = {"observe": "monitor.observe", "terminal_resume": "monitor.terminal_resume"}.get(action)
        if capability is None:
            return self._receipt("jarvis_monitor", "invalid_request", request_id=request_id, reason="action must be observe, terminal_resume, status, or deliver_hold_notifications")
        if not monitor_id or not monitor_id.strip():
            return self._receipt("jarvis_monitor", "invalid_request", request_id=request_id, reason="monitor_id is required for action=" + action)
        if not observed_task_id or not receipt_task_id:
            return self._receipt("jarvis_monitor", "invalid_request", request_id=request_id, reason="observed_task_id and receipt_task_id are required")
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

    def notify(self, *, request_id: str, source_ref: str, message: str) -> dict[str, Any]:
        if self._notifier is None:
            return self.unsupported(tool="jarvis_notify", reason="no notification adapter is configured")
        try:
            result = dict(self._notifier.notify(request_id=request_id, source_ref=source_ref, message=message))
        except Exception as exc:
            return self._receipt("jarvis_notify", "failed", request_id=request_id, reason=str(exc))
        return self._receipt(
            "jarvis_notify", str(result.get("status") or "failed"), request_id=request_id,
            reason=result.get("reason"), data=result,
            readback={"verified": result.get("delivery_status") == "delivered" and bool(result.get("message_id"))},
        )

    def unsupported(self, *, tool: str, reason: str) -> dict[str, Any]:
        return self._receipt(tool, "unsupported", reason=reason)

    def close(self) -> None:
        close = getattr(self._provisioner, "close", None)
        if callable(close):
            close()

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
