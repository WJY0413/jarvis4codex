"""Versioned input port for Jarvis monitor, resume, and heartbeat capabilities.

The port is intentionally harness-neutral.  A caller supplies the concrete
Codex transport and (only for heartbeat operations) its scheduler adapter.
No HostBridge implementation is included here.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Literal, Mapping, Protocol

from .contracts import BridgeReceipt, MonitorReceipt, ReceiptRoute, ResumeRequest, TerminalContinuationRule
from .monitor import ThreadTerminalMonitor
from .service import ExistingThreadBridge


CAPABILITY_REQUEST_SCHEMA = "jarvis-capability-request/v1"
CAPABILITY_RECEIPT_SCHEMA = "jarvis-capability-receipt/v1"

CapabilityName = Literal[
    "monitor.observe",
    "resume.existing",
    "monitor.terminal_resume",
    "heartbeat.create",
    "heartbeat.update",
    "heartbeat.cancel",
    "heartbeat.health",
]


@dataclass(frozen=True)
class CapabilityRequest:
    """One caller-owned request with explicit IDs, target, and prompt fields."""

    request_id: str
    capability: CapabilityName
    source_ref: str
    arguments: Mapping[str, Any]
    schema: str = CAPABILITY_REQUEST_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != CAPABILITY_REQUEST_SCHEMA:
            raise ValueError(f"unsupported capability request schema: {self.schema}")
        if not self.request_id.strip():
            raise ValueError("request_id is required")
        if not self.source_ref.strip():
            raise ValueError("source_ref is required")
        if not isinstance(self.arguments, Mapping):
            raise ValueError("arguments must be a mapping")
        required = {
            "resume.existing": ("thread_id", "prompt"),
            "monitor.observe": ("monitor_id", "observed_thread_id", "receipt_target_thread_id"),
            "monitor.terminal_resume": (
                "monitor_id",
                "observed_thread_id",
                "receipt_target_thread_id",
                "resume_target_thread_id",
                "prompt",
            ),
            "heartbeat.create": ("heartbeat_id",),
            "heartbeat.update": ("heartbeat_id",),
            "heartbeat.cancel": ("heartbeat_id",),
            "heartbeat.health": (),
        }[self.capability]
        missing = [name for name in required if not str(self.arguments.get(name, "")).strip()]
        if missing:
            raise ValueError(
                f"{self.capability} requires: {', '.join(missing)}"
            )

    @property
    def target_thread_id(self) -> str | None:
        return self.arguments.get("resume_target_thread_id") or self.arguments.get("thread_id")


@dataclass(frozen=True)
class CapabilityReceipt:
    """Stable result envelope, suitable for a caller's own transport adapter."""

    request_id: str
    capability: CapabilityName
    status: str
    observed_at: datetime
    target_thread_id: str | None = None
    turn_id: str | None = None
    reason: str | None = None
    data: Mapping[str, Any] | None = None
    schema: str = CAPABILITY_RECEIPT_SCHEMA

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["observed_at"] = self.observed_at.isoformat()
        return value


class HeartbeatControlPort(Protocol):
    """Scheduler boundary supplied by the Jarvis runtime, never by HostBridge."""

    def invoke_heartbeat(
        self,
        request_id: str,
        capability: str,
        source_ref: str,
        arguments: Mapping[str, Any],
    ) -> Mapping[str, Any]: ...


class JarvisCapabilityPort:
    """Execute monitor/resume calls and route scheduler calls to its supplied port."""

    def __init__(
        self,
        bridge: ExistingThreadBridge,
        monitor: ThreadTerminalMonitor,
        heartbeat: HeartbeatControlPort | None = None,
    ) -> None:
        self._bridge = bridge
        self._monitor = monitor
        self._heartbeat = heartbeat

    def invoke(self, request: CapabilityRequest) -> CapabilityReceipt:
        if request.capability == "resume.existing":
            return self._bridge_receipt(request, self._resume_request(request))
        if request.capability == "monitor.observe":
            monitor_receipt = self._observe(request)
            return self._monitor_receipt(request, monitor_receipt)
        if request.capability == "monitor.terminal_resume":
            monitor_receipt = self._observe(request)
            if monitor_receipt.state != "terminal_changed":
                return self._monitor_receipt(request, monitor_receipt)
            rule = TerminalContinuationRule(
                monitor_id=str(request.arguments["monitor_id"]),
                observed_thread_id=str(request.arguments["observed_thread_id"]),
                resume_target_thread_id=str(request.arguments["resume_target_thread_id"]),
                prompt=str(request.arguments["prompt"]),
                source_ref=request.source_ref,
                model=_optional_text(request.arguments.get("model")),
                reasoning_effort=_optional_text(request.arguments.get("reasoning_effort")),
            )
            return self._bridge_receipt(request, rule.resume_request(monitor_receipt), monitor_receipt)
        if self._heartbeat is None:
            raise RuntimeError("heartbeat capability requires a supplied scheduler port")
        data = self._heartbeat.invoke_heartbeat(
            request.request_id,
            request.capability,
            request.source_ref,
            request.arguments,
        )
        return CapabilityReceipt(
            request_id=request.request_id,
            capability=request.capability,
            status=str(data.get("status", "requires_readback")),
            observed_at=datetime.now().astimezone(),
            reason=_optional_text(data.get("reason")),
            data=dict(data),
        )

    def _observe(self, request: CapabilityRequest) -> MonitorReceipt:
        return self._monitor.observe(
            str(request.arguments["monitor_id"]),
            ReceiptRoute(
                str(request.arguments["observed_thread_id"]),
                str(request.arguments["receipt_target_thread_id"]),
            ),
        )

    def _resume_request(self, request: CapabilityRequest) -> ResumeRequest:
        return ResumeRequest(
            request_id=request.request_id,
            thread_id=str(request.arguments["thread_id"]),
            prompt=str(request.arguments["prompt"]),
            source_ref=request.source_ref,
            model=_optional_text(request.arguments.get("model")),
            reasoning_effort=_optional_text(request.arguments.get("reasoning_effort")),
        )

    @staticmethod
    def _monitor_receipt(
        request: CapabilityRequest, receipt: MonitorReceipt
    ) -> CapabilityReceipt:
        return CapabilityReceipt(
            request_id=request.request_id,
            capability=request.capability,
            status=receipt.state,
            observed_at=receipt.observed_at,
            target_thread_id=receipt.receipt_target_thread_id,
            reason=receipt.reason,
            data=receipt.as_dict(),
        )

    def _bridge_receipt(
        self,
        request: CapabilityRequest,
        receipt_request: ResumeRequest,
        monitor_receipt: MonitorReceipt | None = None,
    ) -> CapabilityReceipt:
        bridge_receipt = self._bridge.resume_existing(receipt_request)
        data = bridge_receipt.as_dict()
        if monitor_receipt is not None:
            data["monitor"] = monitor_receipt.as_dict()
        return CapabilityReceipt(
            request_id=request.request_id,
            capability=request.capability,
            status=bridge_receipt.status,
            observed_at=bridge_receipt.observed_at,
            target_thread_id=bridge_receipt.thread_id,
            turn_id=bridge_receipt.turn_id,
            reason=bridge_receipt.reason,
            data=data,
        )


def _optional_text(value: Any) -> str | None:
    return str(value) if value is not None else None
