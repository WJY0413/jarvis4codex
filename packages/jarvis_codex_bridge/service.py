"""One idempotent dispatch with durable ownership and exact terminal readback."""
from __future__ import annotations

from dataclasses import asdict, replace
import hashlib
import json
from typing import Callable
import uuid

from .contracts import BridgeReceipt, ResumeRequest, ThreadState, utc_now
from .journal import ClaimBusy, ReceiptJournal, owner_alive, process_identity
from .transport import ExistingThreadTransport

_ACTIVE = {"active", "running", "inprogress", "in_progress", "pending"}
_COMPLETED = {"completed", "complete", "succeeded"}
_FAILED = {"failed", "error"}
_INTERRUPTED = {"interrupted", "cancelled", "canceled"}


def request_digest(request: ResumeRequest) -> str:
    return hashlib.sha256(json.dumps(asdict(request), ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def final_output(state: ThreadState, turn_id: str) -> str:
    """Preserve the exact stored string, including leading/trailing whitespace."""
    for turn in reversed(state.turns):
        if turn.turn_id != turn_id:
            continue
        messages = [item for item in turn.items if item.get("type") == "agentMessage"]
        finals = [item for item in messages if item.get("phase") == "final_answer"]
        selected = finals[-1] if finals else (messages[-1] if messages else None)
        text = selected.get("text") if selected else None
        return text if isinstance(text, str) else ""
    return ""


class ExistingThreadBridge:
    """Own the write-ahead intent, claim, dispatch, and exact-id readback boundary."""

    def __init__(self, transport: ExistingThreadTransport, journal: ReceiptJournal):
        self.transport = transport
        self.journal = journal

    def health(self) -> dict[str, object]:
        return {"package": "jarvis-codex-bridge", **self.transport.health()}

    def observe_thread(self, thread_id: str) -> ThreadState:
        return self.transport.read_thread(thread_id)

    def observe_turn(self, thread_id: str, turn_id: str) -> dict[str, object]:
        """Use an optional fresh exact-turn reader; never fall back to history."""
        reader = getattr(self.transport, "read_turn", None)
        if not callable(reader):
            raise NotImplementedError("configured transport has no exact-turn read capability")
        return reader(thread_id, turn_id)

    def read_dispatch(self, request_id: str) -> BridgeReceipt | None:
        return self.journal.find_latest(request_id)

    def recover_dispatch(self, request_id: str) -> BridgeReceipt:
        """Resolve only the same orphaned intent, never dispatch or choose a target."""
        previous = self.journal.find_latest(request_id)
        if previous is None:
            raise ValueError("no persisted dispatch exists for this request_id")
        try:
            with self.journal.claim(request_id):
                previous = self.journal.find_latest(request_id)
                if previous is None:
                    raise ValueError("persisted dispatch disappeared")
                if previous.status != "starting":
                    return replace(previous, replayed=True)
                alive = owner_alive(previous.owner)
                if alive is not False:
                    return replace(previous, status="busy", observed_at=utc_now(),
                                   reason="dispatch owner is live or its exit cannot be verified")
                receipt = replace(previous, status="requires_readback", observed_at=utc_now(),
                                  reason="persisted starting intent lost its owner; dispatch outcome is uncertain; no replacement was created")
                self.journal.append(receipt)
                return receipt
        except ClaimBusy:
            return replace(previous, status="busy", observed_at=utc_now(),
                           reason="live dispatch owner holds the original claim; recovery did not mutate it")

    @staticmethod
    def _preflight(state: ThreadState, request: ResumeRequest) -> tuple[str, str] | None:
        if state.thread_id != request.thread_id:
            return "requires_readback", "thread readback identity does not match requested target"
        statuses = {state.effective_status.strip().lower(), state.status.strip().lower()}
        if "unknown" in statuses:
            return "requires_readback", "execution state is unknown; observer status cannot authorize a new turn"
        if statuses & _ACTIVE:
            return "busy", "thread has an active turn"
        return None

    def resume_existing(
        self, request: ResumeRequest, *, owner: dict[str, object] | None = None,
        dispatch_boundary: Callable[[BridgeReceipt], None] | None = None,
    ) -> BridgeReceipt:
        digest = request_digest(request)
        base = BridgeReceipt(request_id=request.request_id, thread_id=request.thread_id,
                             status="requires_readback", observed_at=utc_now(),
                             source_ref=request.source_ref, request_digest=digest)
        try:
            with self.journal.claim(request.request_id):
                previous = self.journal.find_latest(request.request_id)
                if previous is not None:
                    if (previous.thread_id != request.thread_id or previous.source_ref != request.source_ref
                            or previous.request_digest not in (None, digest)):
                        return replace(base, status="failed", reason="request_id is already bound to a different dispatch")
                    if previous.status == "starting":
                        return replace(previous, status="requires_readback", replayed=True,
                                       reason="persisted starting intent exists; explicit same-record recovery is required")
                    return replace(previous, replayed=True)
                try:
                    before = self.observe_thread(request.thread_id)
                except Exception as exc:
                    return replace(base, reason=f"pre-dispatch target readback failed: {exc}")
                rejected = self._preflight(before, request)
                if rejected:
                    return replace(base, status=rejected[0], reason=rejected[1])
                identity = {**(owner or {}), **process_identity()}
                identity.setdefault("owner_id", str(uuid.uuid4()))
                starting = replace(base, status="starting", owner=identity,
                                   reason="durable intent claimed before transport dispatch")
                self.journal.append(starting)  # fsync completes BEFORE any turn dispatch.
                started = None
                try:
                    if dispatch_boundary is not None:
                        dispatch_boundary(starting)
                        # A deferred target can change while paused. Never blindly dispatch.
                        rejected = self._preflight(self.observe_thread(request.thread_id), request)
                        if rejected:
                            receipt = replace(starting, status="requires_readback", observed_at=utc_now(),
                                              reason="deferred dispatch target is no longer eligible: " + rejected[1])
                            self.journal.append(receipt)
                            return receipt
                    started = self.transport.resume_existing(request)
                    if started.thread_id != request.thread_id or not started.turn_id:
                        raise RuntimeError("transport returned an ambiguous thread or turn identity")
                    # Persist exact identity even when subsequent readback fails.
                    starting = replace(starting, turn_id=started.turn_id, observed_at=utc_now(),
                                       reason="transport returned this exact turn; terminal readback is pending")
                    self.journal.append(starting)
                    after = self.observe_thread(request.thread_id)
                    receipt = self._terminal_readback(starting, after, started.status)
                except Exception as exc:
                    # No exact terminal proof exists. Transport exceptions are not failure proof.
                    receipt = replace(starting, status="requires_readback", observed_at=utc_now(),
                                      reason=f"dispatch/readback outcome uncertain: {exc}")
                self.journal.append(receipt)
                return receipt
        except ClaimBusy:
            return replace(base, status="busy", reason="the same request already has a live dispatch owner")

    @staticmethod
    def _terminal_readback(starting: BridgeReceipt, state: ThreadState, dispatch_status: str) -> BridgeReceipt:
        receipt = replace(starting, status="requires_readback", observed_at=utc_now())
        if state.thread_id != starting.thread_id:
            return replace(receipt, reason="post-dispatch thread readback identity mismatch")
        matches = [turn for turn in state.turns if turn.turn_id == starting.turn_id]
        if len(matches) != 1:
            return replace(receipt, reason="exact returned turn is missing or ambiguous in readback")
        turn = matches[0]
        native = turn.status.strip().lower()
        effective = turn.effective_status.strip().lower()
        claimed = dispatch_status.strip().lower()
        if effective in _FAILED and native in _FAILED:
            return replace(receipt, status="failed", reason=turn.error or "exact target turn failed")
        if effective in _INTERRUPTED and native in _INTERRUPTED:
            return replace(receipt, status="interrupted", reason=turn.error or "exact target turn was interrupted")
        if not (native in _COMPLETED and effective in _COMPLETED and claimed in _COMPLETED):
            return replace(receipt, reason=f"exact turn is not consistently terminal completed: native={native}, execution={effective}, dispatch={claimed}")
        output = final_output(state, starting.turn_id or "")
        if not output.strip():
            return replace(receipt, reason="completed exact turn has no required final output in readback")
        return replace(receipt, status="completed", output=output, reason=None)
