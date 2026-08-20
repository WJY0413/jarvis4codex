"""Read-only terminal monitor; scheduling remains the caller's responsibility."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .contracts import MonitorReceipt, ReceiptRoute, ResumeRequest, ThreadState, utc_now
from .transport import ExistingThreadTransport


_TERMINAL = {"completed", "failed", "interrupted", "cancelled", "canceled"}


class ThreadTerminalMonitor:
    """Reports a new terminal transition, never a pre-existing terminal snapshot."""

    def __init__(self, transport: ExistingThreadTransport, state_path: Path):
        self.transport = transport
        self.state_path = state_path

    @staticmethod
    def _fingerprint(state: ThreadState) -> str:
        latest = state.turns[-1] if state.turns else None
        value = {
            "thread_id": state.thread_id,
            "thread_status": state.status,
            "turn_id": latest.turn_id if latest else None,
            "turn_status": latest.status if latest else None,
        }
        raw = json.dumps(value, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def observe(self, monitor_id: str, route: ReceiptRoute) -> MonitorReceipt:
        thread_id = route.observed_thread_id
        state = self.transport.read_thread(thread_id)
        if state.thread_id != thread_id:
            raise RuntimeError("transport readback thread id does not match the receipt route")
        latest = state.turns[-1] if state.turns else None
        status = (latest.status if latest else state.status).strip().lower()
        fingerprint = self._fingerprint(state)
        previous = {}
        if self.state_path.exists():
            previous = json.loads(self.state_path.read_text(encoding="utf-8-sig"))
        if not previous:
            # A monitor can be attached to an already completed/idle task.
            # That state is a baseline only: no receipt consumer may treat it
            # as a newly completed unit of work.
            event = "baseline_terminal" if status in _TERMINAL else "baseline_active"
        elif previous.get("fingerprint") == fingerprint:
            event = "no_change"
        elif status in _TERMINAL:
            event = "terminal_changed"
        else:
            event = "active_or_unknown_changed"
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(
            json.dumps(
                {"version": 2, "fingerprint": fingerprint, "event": event},
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        return MonitorReceipt(
            monitor_id=monitor_id,
            thread_id=thread_id,
            receipt_target_thread_id=route.receipt_target_thread_id,
            state=event,
            observed_at=utc_now(),
            fingerprint=fingerprint,
            reason=status,
        )

    @staticmethod
    def receipt_delivery_request(receipt: MonitorReceipt, *, source_ref: str) -> ResumeRequest:
        """Build, but do not execute, the exact receipt delivery turn.

        The scheduler/controller owns execution.  This keeps receipt routing
        explicit and makes an observed-thread mismatch impossible to conceal.
        """
        prompt = "JARVIS_THREAD_MONITOR_RECEIPT_V1\n" + json.dumps(
            receipt.as_dict(), ensure_ascii=False, sort_keys=True
        )
        return ResumeRequest(
            request_id=f"monitor-receipt:{receipt.monitor_id}:{receipt.fingerprint}",
            thread_id=receipt.receipt_target_thread_id,
            prompt=prompt,
            source_ref=source_ref,
        )
