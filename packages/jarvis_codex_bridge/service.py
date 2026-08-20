"""Deep module that makes one monitored terminal-to-continuation decision."""

from __future__ import annotations

from dataclasses import replace

from .contracts import BridgeReceipt, ResumeRequest, ThreadState, utc_now
from .journal import ReceiptJournal
from .transport import ExistingThreadTransport


_ACTIVE = {"active", "running", "inprogress", "in_progress", "pending"}


def final_output(state: ThreadState, turn_id: str) -> str:
    for turn in reversed(state.turns):
        if turn.turn_id != turn_id:
            continue
        messages = [item for item in turn.items if item.get("type") == "agentMessage"]
        finals = [item for item in messages if item.get("phase") == "final_answer"]
        selected = finals[-1] if finals else (messages[-1] if messages else None)
        return str(selected.get("text") or "").strip() if selected else ""
    return ""


class ExistingThreadBridge:
    """Resume one existing thread and retain an authoritative receipt.

    The interface is intentionally small: callers provide a target, a prompt and
    an idempotency key.  The implementation owns busy checks, exact-id checks,
    post-turn readback and the no-output stop rule.
    """

    def __init__(self, transport: ExistingThreadTransport, journal: ReceiptJournal):
        self.transport = transport
        self.journal = journal

    def health(self) -> dict[str, object]:
        return {"package": "jarvis-codex-bridge", **self.transport.health()}

    def observe_thread(self, thread_id: str) -> ThreadState:
        return self.transport.read_thread(thread_id)

    def resume_existing(self, request: ResumeRequest) -> BridgeReceipt:
        previous = self.journal.find_terminal(request.request_id)
        if previous is not None:
            return replace(previous, replayed=True)

        before = self.observe_thread(request.thread_id)
        if before.status.strip().lower() in _ACTIVE or any(
            turn.status.strip().lower() in _ACTIVE for turn in before.turns[-1:]
        ):
            return BridgeReceipt(
                request_id=request.request_id,
                thread_id=request.thread_id,
                status="busy",
                observed_at=utc_now(),
                source_ref=request.source_ref,
                reason="thread has an active turn",
            )

        try:
            started = self.transport.resume_existing(request)
            if started.thread_id != request.thread_id or not started.turn_id:
                raise RuntimeError("transport returned an ambiguous thread or turn identity")
            after = self.observe_thread(request.thread_id)
            output = final_output(after, started.turn_id)
            if not output:
                receipt = BridgeReceipt(
                    request_id=request.request_id,
                    thread_id=request.thread_id,
                    turn_id=started.turn_id,
                    status="requires_readback",
                    observed_at=utc_now(),
                    source_ref=request.source_ref,
                    reason="completed turn has no exact final output in thread readback",
                )
            else:
                receipt = BridgeReceipt(
                    request_id=request.request_id,
                    thread_id=request.thread_id,
                    turn_id=started.turn_id,
                    status="completed",
                    observed_at=utc_now(),
                    source_ref=request.source_ref,
                    output=output,
                )
        except Exception as exc:
            receipt = BridgeReceipt(
                request_id=request.request_id,
                thread_id=request.thread_id,
                status="failed",
                observed_at=utc_now(),
                source_ref=request.source_ref,
                reason=str(exc),
            )
        self.journal.append(receipt)
        return receipt
