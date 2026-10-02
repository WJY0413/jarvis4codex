"""A small, harness-neutral facade around the current Jarvis capability port."""

from __future__ import annotations

from contextlib import nullcontext
from typing import Any, Mapping, Protocol
from pathlib import Path
from datetime import datetime, timezone, timedelta
from .operations import ControllerOperations

from jarvis_codex_bridge import CapabilityRequest, ExistingThreadBridge, JarvisCapabilityPort

from .loop import LoopController, _validate_start
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
        operation_root: Path | None = None,
        dispatch_manager: Any = None,
    ) -> None:
        self._capabilities = capabilities
        self._bridge = bridge
        self._provisioner = provisioner
        self._notifier = notifier
        self._loop_controller = loop_controller
        self._operations = ControllerOperations(self, operation_root) if operation_root is not None else None
        self._dispatch_manager = dispatch_manager
        self._registry_reader = lambda: []
        self._deployment_paths = {}

    def contract_check(self, *, request_id: str, contract_id: str, expected_code_sha256: str, expected_config_sha256: str) -> dict[str, Any]:
        if self._operations is None:
            return self.unsupported(tool="jarvis_contract_check", reason="durable controller operations are not configured")
        try:
            return self._operations.contract_check(request_id, contract_id, expected_code_sha256, expected_config_sha256)
        except Exception as exc:
            return self._receipt("jarvis_contract_check", "failed", request_id=request_id, reason=str(exc))

    def operation_read(self, *, request_id: str) -> dict[str, Any]:
        if self._operations is None:
            return self.unsupported(tool="jarvis_receipt", reason="durable controller operations are not configured")
        try:
            return self._operations.read(request_id)
        except Exception as exc:
            return self._receipt("jarvis_receipt", "requires_readback", request_id=request_id, reason=str(exc))

    def route_output(self, *, kind: str, request_id: str, run_id: str, source_thread_id: str,
                     source_turn_id: str, target_thread_id: str, source_ref: str,
                     target_input: str | None = None) -> dict[str, Any]:
        if self._operations is None:
            return self.unsupported(tool="jarvis_" + kind, reason="durable controller operations are not configured")
        try:
            return self._operations.route(kind=kind, request_id=request_id, run_id=run_id,
                source_thread_id=source_thread_id, source_turn_id=source_turn_id,
                target_thread_id=target_thread_id, source_ref=source_ref, target_input=target_input)
        except Exception as exc:
            return self._receipt("jarvis_" + kind, "blocked", request_id=request_id, reason=str(exc))

    def read_delivery(self, *, message_id: str | None = None, request_id: str | None = None) -> dict[str, Any]:
        reader = getattr(self._notifier, "read_delivery", None)
        if not callable(reader):
            return self.unsupported(tool="jarvis_read_delivery", reason="receiver-side delivery readback is not configured")
        try:
            data = dict(reader(message_id=message_id, request_id=request_id))
            verified = data.get("delivery_status") == "delivered" and bool(data.get("message_id"))
            return self._receipt("jarvis_read_delivery", "completed" if verified else "requires_readback",
                request_id=request_id, data=data, readback={"verified": verified, "terminal": verified})
        except Exception as exc:
            return self._receipt("jarvis_read_delivery", "requires_readback", request_id=request_id, reason=str(exc))

    def dispatch(self, *, action: str, request_id: str, run_id: str, target_thread_id: str | None = None,
                 prompt: str | None = None, source_ref: str | None = None) -> dict[str, Any]:
        if self._dispatch_manager is None or self._operations is None:
            return self.unsupported(tool="jarvis_dispatch", reason="owned dispatch lifecycle is not configured")
        try:
            if action == "start":
                self._operations.ensure_test_target(run_id, target_thread_id)
                if not callable(getattr(self._provisioner, "creation_enabled", None)) or not self._provisioner.creation_enabled():
                    raise ValueError("live creation is disabled")
                from jarvis_codex_bridge import ResumeRequest
                request = ResumeRequest(request_id=request_id, thread_id=target_thread_id,
                    prompt=prompt or "", source_ref=source_ref or "")
                data = self._dispatch_manager.start(request, owner_scope=run_id, pause_before_dispatch=True)
            elif action in {"read", "release", "stop", "recover"}:
                if action == "release" and not self._provisioner.creation_enabled():
                    raise ValueError("live creation is disabled")
                data = getattr(self._dispatch_manager, action)(request_id, owner_scope=run_id)
            else:
                raise ValueError("unknown owned dispatch action")
            status = str(data.get("status") or "requires_readback")
            return self._receipt("jarvis_dispatch", status, request_id=request_id,
                target_thread_id=data.get("thread_id"), turn_id=data.get("turn_id"), reason=data.get("reason"), data=data,
                readback={"verified": True, "terminal": status in {"completed", "failed", "requires_readback", "owner_stopped"}})
        except Exception as exc:
            return self._receipt("jarvis_dispatch", "blocked", request_id=request_id, reason=str(exc))

    def capacity(self, *, request_id: str, run_id: str, project: str, inputs: list[str]) -> dict[str, Any]:
        """A bounded concurrent batch routed through the existing Host/Holder."""
        if not run_id or not isinstance(inputs, list) or not 1 <= len(inputs) <= 64 or any(not isinstance(x, str) or not x.strip() for x in inputs):
            return self._receipt("jarvis_capacity", "invalid_request", request_id=request_id, reason="run_id and 1..64 declared nonempty TEST inputs required")
        if self._provisioner is None or not self._provisioner.creation_enabled():
            return self._receipt("jarvis_capacity", "blocked", request_id=request_id, reason="live creation unavailable")
        if self._operations is None:
            return self.unsupported(tool="jarvis_capacity", reason="durable capacity receipts unavailable")
        try:
            return self._operations.capacity(request_id=request_id, run_id=run_id, project=project, inputs=inputs)
        except Exception as exc:
            return self._receipt("jarvis_capacity", "blocked", request_id=request_id, reason=str(exc))

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
            try:
                enabled = getattr(self._provisioner, "creation_enabled", None)
                if callable(enabled) and not enabled():
                    raise ValueError("live creation is disabled for this deployment")
                _validate_start(options)
            except ValueError as exc:
                return self._receipt("jarvis_loop", "invalid_request", request_id=options.get("request_id"), reason=str(exc))
            requested_workers = options.get("target_thread_count")
            required_workers = requested_workers if isinstance(requested_workers, int) and requested_workers > 0 else 1
            if self._provisioner is None:
                health = {"status": "host_not_ready", "reason": "no HoldHost health adapter is configured"}
            else:
                health = self._provisioner.ensure_hold_host_ready(required_workers=required_workers)
            if health.get("status") != "ready":
                return self._receipt(
                    "jarvis_loop", "host_not_ready", request_id=options.get("request_id"),
                    reason=str(health.get("reason") or "HoldHost is not ready"), data=health,
                    readback={"verified": False, "terminal": False},
                )
            result = self._loop_controller.start(self, **options)
        elif action == "tick":
            result = self._loop_controller.tick(self, loop_id=str(loop_id or ""))
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

    def stop_hold(self, hold_id: str) -> dict[str, Any]:
        """Internal Loop-to-provisioner boundary; no new public MCP action."""
        stopper = getattr(self._provisioner, "request_hold_stop", None)
        if not callable(stopper):
            return {"status": "unsupported", "reason": "persistent Hold stop is unavailable"}
        try:
            return stopper(hold_id)
        except Exception as exc:
            return {"status": "failed", "reason": str(exc)}

    def jarvis_close(
        self, *, hold_id: str | None = None, loop_id: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Persist closure and stop exact owned work; unknown execution stays explicit."""
        if bool(hold_id) == bool(loop_id):
            return self._receipt("jarvis_close", "invalid_request", request_id=request_id,
                                 reason="provide exactly one of hold_id or loop_id")
        try:
            if loop_id:
                if self._loop_controller is None:
                    return self.unsupported(tool="jarvis_close", reason="no loop controller is configured")
                result = self._loop_controller.close(self, loop_id=loop_id, request_id=request_id)
                data = result.data
                if result.status in {"invalid_request", "failed"}:
                    return self._receipt("jarvis_close", result.status, request_id=request_id, reason=result.reason, data=data)
                if (data.get("close") or {}).get("report_status") == "failed":
                    return self._receipt("jarvis_close", "failed", request_id=request_id,
                        reason=data["close"].get("reason"), data=data, readback={"verified": False, "terminal": False})
                closed = (data.get("cleanup") or {}).get("status") == "completed"
                unconfirmed = result.status == "closed_unconfirmed"
            else:
                data = self.close_hold(hold_id, request_id=request_id)
                if data.get("status") in {"failed", "unsupported", "invalid_request"}:
                    return self._receipt("jarvis_close", data["status"], request_id=request_id,
                        reason=data.get("reason"), data=data, readback={"verified": False, "terminal": False})
                closed = data.get("status") == "closed"
                unconfirmed = data.get("status") == "closed_unconfirmed"
            return self._receipt("jarvis_close", "closed" if closed else "closed_unconfirmed" if unconfirmed else "closing",
                request_id=request_id, data={**data, "close_target": {
                    "loop_id": loop_id} if loop_id else {"hold_id": hold_id}},
                reason=None if closed else "local management closed; external execution remains unconfirmed" if unconfirmed else "stop requested; awaiting terminal evidence and resource release",
                readback={"verified": True, "terminal": closed})
        except Exception as exc:
            return self._receipt("jarvis_close", "failed", request_id=request_id, reason=str(exc))

    def close_hold(self, hold_id: str, *, request_id: str | None = None, parent_close: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Internal generic lifecycle port; MCP exposes only jarvis_close."""
        closer = getattr(self._provisioner, "close_hold", None)
        if not callable(closer):
            raise RuntimeError("durable Hold closure is unavailable")
        return closer(hold_id, request_id=request_id, parent_close=parent_close)

    def create(
        self,
        *,
        request_id: str,
        project: str,
        title: str | None = None,
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
        run_id: str | None = None,
        role: str | None = None,
        test_only: bool = False,
    ) -> dict[str, Any]:
        if self._provisioner is None:
            return self.unsupported(
                tool="jarvis_create", reason="no task-creation adapter is configured"
            )
        try:
            if test_only and (self._operations is None or not run_id or not role):
                raise ValueError("TEST provisioning requires run_id, role and durable operation receipts")
            if test_only:
                previous = self._operations.receipts.read("target:" + run_id + ":" + role)
                if previous:
                    if previous.get("provision_request_id") != request_id:
                        raise ValueError("TEST run role already belongs to another request")
                    return previous["provision_receipt"]
            config_check = getattr(self._provisioner, "creation_enabled", None)
            if callable(config_check) and not config_check():
                raise ValueError("live creation is disabled for this deployment")
            readiness = self._provisioner.ensure_hold_host_ready(required_workers=1)
            if readiness.get("status") != "ready":
                return self._receipt("jarvis_create", "blocked", request_id=request_id, reason=readiness.get("reason"), data=readiness)
            provision = self._provisioner.provision(TaskProvisionRequest(
                request_id=request_id,
                project=project,
                title=title or ("Jarvis " + (role or "task") + " " + request_id[:64]),
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
        receipt = self._receipt(
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
        if test_only:
            self._operations.register_test_target(run_id=run_id, role=role, request_id=request_id, provision=receipt)
        return receipt

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
            enabled = getattr(self._provisioner, "creation_enabled", None)
            if callable(enabled) and not enabled():
                raise ValueError("live creation is disabled for this deployment")
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
                    "jarvis_contract_check": {"available": self._operations is not None},
                    "jarvis_callback": {"available": self._operations is not None},
                    "jarvis_relay": {"available": self._operations is not None},
                    "jarvis_dispatch": {"available": self._dispatch_manager is not None},
                    "jarvis_read_delivery": {"available": callable(getattr(self._notifier, "read_delivery", None))},
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
                    "jarvis_close": {"available": callable(getattr(self._provisioner, "close_hold", None)),
                        "durable_management_cancellation": getattr(self._provisioner, "management_cancellation_version", None) == "jarvis-management-cancellation/v1",
                        "cancellation_version": getattr(self._provisioner, "management_cancellation_version", None),
                        "unknown_execution_status": "closed_unconfirmed",
                        "terminal_requires_owner_evidence": True, "automatic_replacement": False},
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
                    "jarvis_update": {
                        "available": callable(getattr(self._provisioner, "update_runtime", None)),
                        "scope": "codex_runtime",
                    },
                    "jarvis_notify": {
                        "available": self._notifier is not None,
                        "reason": None if self._notifier is not None else "no notification adapter is configured",
                        "target": getattr(getattr(self._notifier, "config", None), "recipient", None),
                        "receiver_readback_available": callable(getattr(self._notifier, "read_delivery", None)),
                    },
                },
            )
        if subject == "thread":
            if not task_id or not task_id.strip():
                return self._receipt("jarvis_read", "invalid_request", reason="task_id is required for subject=thread")
            if turn_id is not None:
                if not isinstance(turn_id, str) or not turn_id.strip():
                    return self._receipt("jarvis_read", "invalid_request", target_thread_id=task_id,
                                         reason="a non-empty turn_id is required for exact-turn read")
                if thread_id is not None and thread_id != task_id:
                    return self._receipt("jarvis_read", "invalid_request", target_thread_id=task_id,
                                         turn_id=turn_id, reason="thread_id conflicts with task_id")
                reader = getattr(self._bridge, "observe_turn", None)
                if not callable(reader):
                    return self._receipt("jarvis_read", "unsupported", target_thread_id=task_id,
                                         turn_id=turn_id, reason="bridge has no exact-turn read capability")
                try:
                    data = reader(task_id, turn_id)
                    turns = data.get("turns") if isinstance(data, dict) else None
                    if (not isinstance(data, dict) or data.get("thread_id") != task_id
                            or data.get("turn_id") != turn_id or not isinstance(turns, list)
                            or len(turns) != 1 or turns[0].get("turn_id") != turn_id
                            or turns[0].get("items_scan_complete") is not True):
                        raise RuntimeError("exact-turn readback identity or completeness mismatch")
                except NotImplementedError as exc:
                    return self._receipt("jarvis_read", "unsupported", target_thread_id=task_id,
                                         turn_id=turn_id, reason=str(exc))
                except Exception as exc:
                    return self._receipt("jarvis_read", "failed", target_thread_id=task_id,
                                         turn_id=turn_id, reason=str(exc))
                return self._receipt(
                    "jarvis_read", "completed", target_thread_id=task_id, turn_id=turn_id, data=data,
                    readback={"verified": True,
                              "native_terminal": turns[0]["status"] in {"completed", "failed", "interrupted"},
                              "terminal": data.get("execution_status") in
                                  {"completed", "failed", "interrupted", "cancelled", "canceled", "blocked"},
                              "final_answer_present": turns[0].get("final_answer_present") is True,
                              "final_answer_nonempty": turns[0].get("final_answer_nonempty") is True},
                )
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
                    "read_source": state.read_source,
                    "execution_status": state.effective_status,
                    "execution_source": state.execution_source,
                    "execution_evidence": state.execution_evidence,
                    "turns": [
                        {"turn_id": turn.turn_id, "status": turn.status, "error": turn.error,
                         "execution_status": turn.effective_status, "execution_source": turn.execution_source,
                         "items": [dict(item) for item in turn.items]}
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

    def update(self, *, action: str, request_id: str) -> dict[str, Any]:
        updater = getattr(self._provisioner, "update_runtime", None)
        if not callable(updater):
            return self.unsupported(
                tool="jarvis_update", reason="no Codex runtime update adapter is configured"
            )
        try:
            result = dict(updater(action=action, request_id=request_id))
        except ValueError as exc:
            return self._receipt(
                "jarvis_update", "invalid_request", request_id=request_id, reason=str(exc)
            )
        except Exception as exc:
            return self._receipt(
                "jarvis_update", "failed", request_id=request_id, reason=str(exc)
            )
        status = str(result.get("status") or "failed")
        return self._receipt(
            "jarvis_update", status, request_id=request_id,
            reason=result.get("reason"), data=result.get("data"),
            readback={"verified": status == "completed", "terminal": True},
        )

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
        notification_event_type: str | None = None,
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
            lifecycle = str(data.get("lifecycle_status") or data.get("status") or "").lower()
            verified = (
                lifecycle in {"completed", "failed", "interrupted", "cancelled", "canceled", "turn_limit_reached", "blocked"}
                and bool(str(data.get("thread_id") or "").strip())
                and bool(str(data.get("turn_id") or "").strip())
                and data.get("terminal_confirmed", True) is True
            )
            return self._receipt(
                "jarvis_monitor", "completed", request_id=request_id, data=data,
                readback={"verified": verified, "terminal": verified},
            )
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
            lock_factory = getattr(self._provisioner, "hold_notification_delivery_lock", None)
            try:
                delivery_lock = lock_factory(hold_id) if callable(lock_factory) else nullcontext()
                with delivery_lock:
                    deliveries: list[dict[str, Any]] = []
                    for event in pending_reader(hold_id):
                        if (
                            notification_event_type is not None
                            and str(event.get("event_type") or "") != notification_event_type
                        ):
                            continue
                        event_id = str(event.get("event_id") or "").strip()
                        if not event_id:
                            continue
                        try:
                            delivery = dict(self._notifier.notify(
                                request_id=f"{request_id}:{event_id}",
                                source_ref=source_ref,
                                message=str(event.get("message") or ""),
                            ))
                        except Exception as exc:
                            delivery = {"delivery_status": "failed", "reason": str(exc)}
                        try:
                            recorder(hold_id, event_id, delivery)
                        except Exception as exc:
                            delivery = {"delivery_status": "failed", "reason": str(exc)}
                        deliveries.append({"event_id": event_id, **delivery})
            except Exception as exc:
                return self._receipt(
                    "jarvis_monitor", "failed", request_id=request_id, reason=str(exc),
                    readback={"verified": False, "terminal": False},
                )
            verified = all(
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

    def deliver_pending_hold_notifications(
        self, *, request_id: str, source_ref: str,
    ) -> dict[str, Any]:
        """Drain durable terminal Hold notification events through the configured bridge."""
        if self._notifier is None:
            return self.unsupported(
                tool="jarvis_monitor", reason="no verified notification adapter is configured"
            )
        hold_reader = getattr(self._provisioner, "pending_hold_notification_holds", None)
        if not callable(hold_reader):
            return self._receipt(
                "jarvis_monitor", "unsupported", request_id=request_id,
                reason="no managed-hold notification discovery adapter is configured",
            )
        try:
            hold_ids = list(hold_reader(event_type="terminal"))
        except Exception as exc:
            return self._receipt("jarvis_monitor", "failed", request_id=request_id, reason=str(exc))
        deliveries: list[dict[str, Any]] = []
        verified = True
        for hold_id in hold_ids:
            try:
                receipt = self.monitor(
                    action="deliver_hold_notifications",
                    request_id=f"{request_id}:{hold_id}", source_ref=source_ref,
                    hold_id=str(hold_id), notification_event_type="terminal",
                )
            except Exception as exc:
                receipt = self._receipt(
                    "jarvis_monitor", "failed", request_id=f"{request_id}:{hold_id}", reason=str(exc),
                    readback={"verified": False, "terminal": False},
                )
            data = receipt.get("data") or {}
            for delivery in data.get("deliveries") or []:
                deliveries.append({"hold_id": str(hold_id), **dict(delivery)})
            verified = verified and bool((receipt.get("readback") or {}).get("verified"))
        return self._receipt(
            "jarvis_monitor", "completed" if verified else "requires_readback",
            request_id=request_id,
            data={"hold_ids": hold_ids, "deliveries": deliveries},
            readback={"verified": verified, "terminal": False},
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
