"""Independent holder for one Jarvis App Server task turn.

This process is intentionally detached from the MCP stdio process. It writes
an ownership acknowledgement immediately after the exact turn starts, then
holds the App Server client until the turn has reached a terminal state.
"""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from urllib.parse import unquote
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from jarvis_monitor import HoldTurnMonitor, HoldTurnRequest, NotificationPolicy
from jarvis_runtime.jarvis_native_task_launcher import AppServerClient, NativeTaskLauncherConfig
from adapters.codex_app_server.jarvis_connection import route_for_model
from adapters.codex_app_server.task_provisioning_adapter import append_terminal_turn_history, _count_scheduler_failed_items, verify_candidate_output, receive_candidate_final_answer, _pid_is_alive
from jarvis_runtime.coo_dispatcher_store import ProcessLock
from jarvis_runtime.cancellation import cancellation_path, commit_dispatch, DispatchCancelledBeforeSend, StateGateBusy, generation_guard
from jarvis_control.provisioning import lane_batch_ids, lane_batch_size, final_answer_mode
from jarvis_control.provisioning import validate_prediction_options
from adapters.codex_app_server.prediction import prepare_prediction_prompt


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _client_transport_contiguous(client) -> bool:
    """Owner event evidence needs an unbroken transport, not an owned Popen."""
    health = getattr(client, "transport_health", None)
    if callable(health):
        try:
            state = health()
            if (isinstance(state, dict)
                    and state.get("schema") == "jarvis-client-transport-health/v1"
                    and state.get("mode") == "stdio"):
                # Keep the existing stdio recorder guard unchanged. Protocol
                # failures are separately recorded by its native event path.
                process = getattr(client, "process", None)
                return process is not None and process.poll() is None
            return (isinstance(state, dict)
                    and state.get("schema") == "jarvis-client-transport-health/v1"
                    and state.get("connected") is True
                    and state.get("contiguous") is True
                    and state.get("failure") is None)
        except Exception:
            return False
    # Preserve the pre-interface stdio adapter contract.
    process = getattr(client, "process", None)
    return process is not None and process.poll() is None


def _client_observer_stopped(client, owned_process) -> bool:
    """A closed attachment proves only its local observer stopped."""
    health = getattr(client, "transport_health", None)
    if callable(health):
        try:
            state = health()
            return (isinstance(state, dict)
                    and state.get("schema") == "jarvis-client-transport-health/v1"
                    and state.get("local_connection_closed") is True)
        except Exception:
            return False
    return owned_process is None or owned_process.poll() is not None


class _HolderStopUnknown(RuntimeError):
    """Owner is exiting, but this is not evidence that the model turn ended."""


class _RecoveredTurnTerminal(RuntimeError):
    """Exact terminal readback; release still requires the holder's own exit."""

    def __init__(self, status: str) -> None:
        super().__init__("Recovering holder read exact terminal; own exit confirmation pending")
        self.status = status


def _publish_generation(path: Path, value: dict[str, Any], request: dict[str, Any], *, on_busy=None) -> bool:
    from jarvis_runtime.cancellation import generation_guard, GenerationRetired
    if (not isinstance(request, dict) or value.get("request_id") != request.get("request_id")
            or ((request.get("hold_id") or request.get("monitor_id")) is not None
                and (value.get("hold_id") or value.get("monitor_id")) !=
                    (request.get("hold_id") or request.get("monitor_id")))):
        return False
    while True:
        try:
            with generation_guard(path.parent, request):
                _write_json(path, value)
            return True
        except StateGateBusy:
            # Receipt contention is not execution failure. Retain the owning client;
            # retry only publication, acquiring all gates and rechecking generation.
            # No deadline may turn missing terminal proof into owner release.
            if on_busy is not None:
                on_busy()
            time.sleep(0.05)
        except GenerationRetired:
            return False  # Preserve the successor and the old owner's historical evidence.


def _write_json(path: Path, value: dict[str, Any], *, durable: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps(value, ensure_ascii=False))
            if durable:
                handle.flush()
                os.fsync(handle.fileno())
        for attempt in range(20):
            try:
                temporary.replace(path)
                break
            except PermissionError:
                if attempt == 19:
                    raise
                time.sleep(min(0.05 * (attempt + 1), 0.25))
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except PermissionError:
            pass  # A temp cleanup denial must not mask the replace outcome.


def _read_json(path: Path) -> dict[str, Any]:
    from jarvis_runtime.cancellation import _read_binding
    value = _read_binding(path)
    if not isinstance(value, dict):
        raise RuntimeError("task-monitor request must be a JSON object")
    return value


def _notification_policy(value: object) -> NotificationPolicy:
    raw = value if isinstance(value, dict) else {}
    milestones = raw.get("milestones") or []
    if not isinstance(milestones, list):
        raise RuntimeError("notifications.milestones must be a list")
    try:
        normalized = tuple(sorted({int(item) for item in milestones}))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("notifications.milestones must contain integers") from exc
    terminal = raw.get("terminal", False)
    if not isinstance(terminal, bool):
        raise RuntimeError("notifications.terminal must be a boolean")
    return NotificationPolicy(
        milestone_turns=normalized,
        terminal=terminal,
    )


def _append_monitor_events(path: Path, decision: object) -> None:
    events = getattr(decision, "notification_events", ())
    if not events:
        return
    with path.open("a", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps({
                "event_id": event.event_id,
                "event_type": event.event_type,
                "hold_id": event.hold_id,
                "thread_id": event.thread_id,
                "turn_id": event.turn_id,
                "turn_count": event.turn_count,
                "status": event.status,
                "message": event.message,
                "observed_at": _now(),
            }, ensure_ascii=False) + "\n")


def _validate_monitor_command(decision: object, *, hold_id: str, turn_id: str) -> None:
    if getattr(decision, "hold_id", None) != hold_id:
        raise RuntimeError("monitor command hold_id does not match active hold")
    if getattr(decision, "expected_turn_id", None) != turn_id:
        raise RuntimeError("monitor command expected_turn_id does not match active turn")
    if not str(getattr(decision, "command_id", "")).strip():
        raise RuntimeError("monitor command_id is required")


def _turn_input_binding(request: dict[str, Any], total_turn_count: int) -> dict[str, Any]:
    """Keep a full lane private to Holder while exposing only the current slice per Worker turn."""
    binding = dict(request.get("input_binding") or {})
    if "lane_item_count" not in binding:
        return binding
    pocket = final_answer_mode(binding)
    binding["candidate_ids"] = lane_batch_ids(binding, total_turn_count)
    contract = binding.get("result_verification")
    if isinstance(contract, dict):
        if pocket:
            binding["result_verification"] = {
                "mode": "final_answer_json",
                **({"output_schema": contract["output_schema"]} if "output_schema" in contract else {}),
            }
            return binding
        candidate = binding["candidate_ids"][0]
        binding["result_verification"] = {
            **({"output_schema": contract["output_schema"]} if "output_schema" in contract else {}),
            **({"receipt_path": contract["receipt_paths"][str(candidate)]} if lane_batch_size(binding) == 1 else
               {"receipt_paths": {str(value): contract["receipt_paths"][str(value)] for value in binding["candidate_ids"]}}),
            "terminal_statuses": contract["terminal_statuses"],
            "request_id": request["request_id"], "turn_number": total_turn_count,
        }
    return binding


def _task_id(request: dict[str, Any], hold_id: str) -> str:
    source = request.get("source_ref") or ""
    if source.startswith("jarvis_loop_task:"):
        return unquote(source.removeprefix("jarvis_loop_task:"))
    return hold_id.rsplit(":", 1)[0] if hold_id.startswith("loop-") and ":" in hold_id else hold_id


def _candidate_id(request: dict[str, Any], total_turn_count: int) -> int | None:
    binding = request.get("input_binding") or {}
    if "lane_item_count" in binding and lane_batch_size(binding) > 1:
        return None
    value = _turn_input_binding(request, total_turn_count).get("candidate_ids")
    return value[0] if isinstance(value, list) and len(value) == 1 and isinstance(value[0], int) else None


def hold_task(
    launcher_config_path: Path, request_path: Path, ack_path: Path, result_path: Path,
    *, monitor: HoldTurnMonitor | None = None, diagnostic_sink=None,
) -> int:
    """Legacy by default; opted-in tasks keep the fence through local cleanup."""
    from jarvis_runtime.owner_control_ledger import ControlLease, ControlUnavailable, control_requested
    request = _read_json(request_path)
    if not control_requested(request) and (request_path.parent / "owner-control.json").exists():
        raise ControlUnavailable("existing owner control ledger cannot downgrade to legacy dispatch")
    from types import SimpleNamespace
    from jarvis_runtime.business_quarantine_gate import require_not_abandoned
    raw_config = _read_json(launcher_config_path)
    if not isinstance(raw_config, dict):
        raise ValueError("launcher metadata must be a JSON object")
    require_not_abandoned(SimpleNamespace(business_quarantine_file=raw_config.get("business_quarantine_file"), scheduling_generation_id=raw_config.get("scheduling_generation_id")),
                          request_path.parent.parents[1], request)
    if not control_requested(request):
        if (request_path.parent / "owner-control.json").exists():
            raise ControlUnavailable("existing owner control ledger cannot downgrade to legacy dispatch")
        return _hold_task(launcher_config_path, request_path, ack_path, result_path,
                          monitor=monitor, diagnostic_sink=diagnostic_sink)
    config = NativeTaskLauncherConfig(launcher_config_path)
    with ControlLease(request_path.parent) as lease:
        # Existing ledger is NOT a new attempt, even if no ACK was published.
        lease.create(request, config)
        code = _hold_task(launcher_config_path, request_path, ack_path, result_path,
                         monitor=monitor, diagnostic_sink=diagnostic_sink,
                         owner_control_lease=lease, frozen_launcher_config=config)
        saved = _read_json(result_path) if result_path.is_file() else {}
        if saved.get("local_observer_stopped") is True and lease._terminal_valid():
            lease.settle()
        return code


def _hold_task(
    launcher_config_path: Path,
    request_path: Path,
    ack_path: Path,
    result_path: Path,
    *,
    monitor: HoldTurnMonitor | None = None,
    diagnostic_sink=None,
    owner_control_lease=None,
    frozen_launcher_config=None,
) -> int:
    request = _read_json(request_path)
    from types import SimpleNamespace
    from jarvis_runtime.business_quarantine_gate import dispatch_scope
    raw_config = _read_json(launcher_config_path)
    if not isinstance(raw_config, dict):
        raise ValueError("launcher metadata must be a JSON object")
    with dispatch_scope(SimpleNamespace(business_quarantine_file=raw_config.get("business_quarantine_file"), scheduling_generation_id=raw_config.get("scheduling_generation_id")),
                        request_path.parent.parents[1], request):
        pass  # Before report/lock/result publication: isolation is not task failure.
    request_id = str(request.get("request_id") or "").strip()
    max_turns = max(int(request.get("max_turns") or 1), 1)
    auto_continue = bool(request.get("auto_continue", False))
    continue_prompt = str(request.get("continue_prompt") or "继续").strip() or "继续"
    hold_id = str(request.get("hold_id") or request.get("monitor_id") or f"hold-{request_id}")
    initial_turn_count = max(int(request.get("initial_turn_count") or 1), 1)
    initial_total_turn_count = max(int(request.get("initial_total_turn_count") or initial_turn_count), 1)
    client: AppServerClient | None = None
    transport_health = None
    phase = "hold_started"
    terminal_confirmed = request.get("mode") != "recover"
    thread_id = str(request.get("thread_id") or "").strip()
    turn_id = str(request.get("turn_id") or "").strip() if request.get("mode") == "recover" else ""
    turn_count, total_turn_count = initial_turn_count, initial_total_turn_count
    verification: dict[str, Any] = {"status": "not_checked"}
    scheduler_failed_items = 0
    interrupt_at: float | None = None
    host_stop_selected = False
    dispatch_evidence = "unknown" if request.get("mode") == "recover" else "not_sent"
    pre_helper_rejection = None

    def runtime_fields() -> dict[str, Any]:
        # Legacy/mocked clients need not implement this optional contract.
        context = getattr(client, "runtime_context", None)
        if not isinstance(context, dict) or not context:
            return {}
        allowed = {"model", "modelProvider", "sandbox", "approvalPolicy",
                   "operation", "thread_id", "constraints_verified",
                   "environments", "environments_verified", "reasoningEffort", "codex_home", "codex_home_verified"}
        return {"runtime_context": {key: value for key, value in context.items() if key in allowed}}

    def control_poll(*, publish_interrupt: bool = True) -> None:
        nonlocal interrupt_at, host_stop_selected
        try:
            current = _read_json(request_path)
            control = current.get("host_stop")
            # An explicit exact stop must not wait for unrelated alias snapshots.
            cancelled = control is None and cancellation_path(request_path.parent, current) is not None
        except (PermissionError, StateGateBusy):
            # A busy alias gate or Windows sharing denial is not a stop signal.
            # Windows atomic replacement can briefly deny an alias ack read too.
            # Retry the whole observation next poll; do not interrupt or release.
            return
        control = current.get("host_stop")
        if cancelled and control is None:
            # This holder owns this client and these exact runtime IDs. No OS PID lookup.
            control = {"request_id": request_id, "hold_id": hold_id,
                       "thread_id": thread_id, "turn_id": turn_id}
        if control is None:
            if not (current.get("mode") == "recover" and current.get("stop_requested") is True):
                return  # Preserve the ordinary stop-after-current-turn contract.
            control = {key: current.get(key) for key in ("request_id", "hold_id", "thread_id", "turn_id")}
        expected = {"request_id": request_id, "hold_id": hold_id, "thread_id": thread_id, "turn_id": turn_id}
        if (current.get("request_id") != request_id or not isinstance(control, dict)
                or any(control.get(key) != value for key, value in expected.items())):
            if not publish_interrupt:
                return  # Defer the invalid stop; never lose the owner during publication.
            raise _HolderStopUnknown("Host stop identity conflict; no execution interrupt sent")
        host_stop_selected = True
        if transport_health is not None:
            try:
                transport_health.stop(cancelled=cancelled)
            except Exception:
                pass
        if request.get("mode") == "recover":
            readback = client.request("thread/read", {"threadId": thread_id, "includeTurns": True})
            thread = readback.get("thread") or {}
            turns = thread.get("turns") or []
            exact = [turn for turn in turns if isinstance(turn, dict) and turn.get("id") == turn_id]
            if len(exact) == 1:
                from adapters.codex_app_server.native_execution_evidence import classify_native_turn
                authority = classify_native_turn(thread_id, turn_id, exact[0],
                    observed_thread_id=thread.get("id"),
                    protocol_version=getattr(client, "cli_version", None))
                if authority.get("terminal_confirmed") is True:
                    raise _RecoveredTurnTerminal(exact[0]["status"])
            raise _HolderStopUnknown("Recovered observer stopped; original execution terminal remains unknown")
        if interrupt_at is None:
            # Latch before sending: a timeout/ambiguous response must not resend.
            interrupt_at = time.monotonic()
            # Send once before the optional receipt: a contended receipt must not
            # prevent an already authorized exact-owner interrupt.
            try:
                if owner_control_lease is not None:
                    owner_control_lease.interrupt(client)
                else:
                    client.request("turn/interrupt", {"threadId": thread_id, "turnId": turn_id})
            except Exception:
                if publish_interrupt:
                    raise
                # An ambiguous interrupt is latched, never resent. Keep ownership
                # until normal exact-turn observation resumes after contention.
                return
            if publish_interrupt:
                report("host_stop_interrupting", {"host_stop": control})
        elif publish_interrupt and time.monotonic() - interrupt_at >= 5:
            raise _HolderStopUnknown("Owner stop timed out without exact turn/completed evidence")

    def stop_requested() -> bool:
        while True:
            try:
                current = _read_json(request_path)
                if current.get("request_id") != request.get("request_id"):
                    raise RuntimeError("Hold request changed while a turn was owned")
                close_path = request_path.with_name("close.json")
                close = _read_json(close_path) if close_path.exists() else {}
                return (cancellation_path(request_path.parent, current) is not None
                        or current.get("stop_requested") is True or close.get("request_id") == request_id)
            except StateGateBusy:
                # An inconclusive cancellation snapshot cannot authorize continuation.
                time.sleep(0.05)

    def verify_output() -> dict[str, Any]:
        nonlocal verification
        verification = verify_candidate_output(request, total_turn_count)
        return verification

    def receive_final_answer(raw: str) -> dict[str, Any]:
        nonlocal verification
        verification = receive_candidate_final_answer(request, total_turn_count,
            thread_id=thread_id, turn_id=turn_id, raw=raw)
        return verification

    def publish(path: Path, value: dict[str, Any]) -> None:
        from jarvis_runtime.cancellation import GenerationRetired
        nonlocal interrupt_at
        if owner_control_lease is not None:
            owner_control_lease.check()
        waited = False
        def poll_while_busy():
            nonlocal waited
            waited = True
            if (client is not None and thread_id and turn_id and not terminal_confirmed
                    and request.get("mode") != "recover"):
                # Never recursively publish while waiting to publish a receipt.
                control_poll(publish_interrupt=False)
        published = _publish_generation(path, value, request, on_busy=poll_while_busy)
        if waited and interrupt_at is not None:
            # The client could not consume terminal notifications while publishing.
            # Start its observation budget now; retain the send-once latch.
            interrupt_at = time.monotonic()
        if not published:
            raise GenerationRetired("old holder publication was fenced by a paused/successor request_id")

    def report(next_phase: str, details: dict[str, Any] | None = None) -> None:
        nonlocal phase, thread_id, turn_id, dispatch_evidence
        phase = next_phase
        if phase == "turn_started":
            dispatch_evidence = "sent"
        # Capture dispatch identity before ack replacement can fail. Do not recover
        # a turn by guessing the latest turn on a reused thread.
        if details and details.get("thread_id"):
            thread_id = str(details["thread_id"])
        if details and details.get("turn_id"):
            turn_id = str(details["turn_id"])
        if owner_control_lease is not None and phase in {"thread_started", "turn_started"}:
            owner_control_lease.bind(thread_id=thread_id,
                                    turn_id=turn_id if phase == "turn_started" else None)
        publish(ack_path, {
            "request_id": request_id,
            "status": "accepted",
            "phase": phase,
            "dispatch_evidence": dispatch_evidence,
            "pid": os.getpid(),
            "hold_id": hold_id,
            "thread_id": thread_id,
            "turn_id": turn_id,
            "turn_count": initial_turn_count,
            "session_turn_count": initial_turn_count,
            "total_turn_count": initial_total_turn_count,
            "max_turns": max_turns,
            "observed_at": _now(),
            **runtime_fields(),
            **(details or {}),
        })

    try:
        prediction_options = validate_prediction_options(request.get("prediction_options"))
        pocket = final_answer_mode(request.get("input_binding") or {})
        if pocket and "lane_item_count" not in request["input_binding"]:
            raise ValueError("final_answer_json requires a finite lane binding")
        report("hold_started")
        with ProcessLock(request_path.with_suffix(".lock"), owner_alive=_pid_is_alive):
            if stop_requested() and request.get("mode") != "recover":
                cancelled = {"request_id": request_id, "hold_id": hold_id, "status": "cancelled",
                             "phase": "stopped_before_dispatch", "terminal_confirmed": True,
                             "total_turn_count": 0, "observed_at": _now()}
                publish(result_path, cancelled)
                publish(ack_path, cancelled)
                return 0
            launcher_config = frozen_launcher_config or NativeTaskLauncherConfig(launcher_config_path)
            launcher_config.connection_base_url_override = route_for_model(
                getattr(launcher_config, "jarvis_connection", None),
                request.get("model"),
                allow_start=str(request.get("mode") or "create") != "recover",
            )
            client = AppServerClient(launcher_config)
            client.business_quarantine_context = dict(request)
            if owner_control_lease is not None:
                client.owner_control_rpc_guard = owner_control_lease.rpc_guard
                client.owner_control_wire_scope = owner_control_lease.wire_scope
                client.owner_control_terminal = lambda observed_thread, native_turn: (
                    owner_control_lease.event_terminal(client, observed_thread, native_turn))
            from jarvis_runtime.holder_transport_release import configured_unix_owner_binding
            binding = configured_unix_owner_binding(client, request)
            if binding is not None:
                # Durable before any native dispatch; reconnect never substitutes
                # another owner for this generation.
                with generation_guard(request_path.parent, request):
                    current = _read_json(request_path)
                    if (current.get("request_id") != request_id
                            or current.get("hold_id") != hold_id
                            or current.get("owner_transport_binding") not in (None, binding)):
                        raise RuntimeError("Hold generation or pinned Unix owner binding changed")
                    _write_json(request_path, {**current, "owner_transport_binding": binding}, durable=True)
                request = {**current, "owner_transport_binding": binding}
            elif "owner_transport_binding" in request:
                raise RuntimeError("Pinned Unix Hold cannot fall back to a stdio client")
            if diagnostic_sink is not None:
                client._terminal_diagnostic_sink = lambda value: diagnostic_sink({**value,
                    "request_id": request_id, "hold_id": hold_id, "thread_id": thread_id,
                    "turn_id": turn_id, "model": str(request.get("model") or "")})
            if request.get("mode") != "recover" and (request_path.parent / ".user-host-claim/owner.json").is_file():
                from jarvis_runtime.owner_transport_health import OwnerTransportHealthRecorder
                transport_health = OwnerTransportHealthRecorder(request_path.parent, request_id,
                    is_connected=lambda: _client_transport_contiguous(client))
                client.owner_transport_health_recorder = transport_health
            def before_turn_dispatch(params):
                nonlocal dispatch_evidence
                try:
                    commit_dispatch(request_path.parent, request, params)
                except DispatchCancelledBeforeSend:
                    dispatch_evidence = "not_sent"
                    raise
                except Exception:
                    dispatch_evidence = "unknown"  # Existing/uncertain commit is never proof of non-dispatch.
                    raise
                dispatch_evidence = "possibly_sent"
                if owner_control_lease is not None:
                    owner_control_lease.dispatch(params)
            client.before_turn_dispatch = before_turn_dispatch
            report("launcher_config_loaded")
            input_binding = _turn_input_binding(request, initial_total_turn_count)
            mode = str(request.get("mode") or "create")
            if mode == "recover":
                thread_id = str(request.get("thread_id") or "").strip()
                turn_id = str(request.get("turn_id") or "").strip()
                if not thread_id or not turn_id:
                    raise RuntimeError("hold recovery requires thread_id and turn_id")
                client.read_only = True  # Attach/read the original exact turn; no new model work.
                client.start()
                report("recovery_attached", {"thread_id": thread_id, "turn_id": turn_id})
            elif mode == "resume":
                thread_id = str(request.get("thread_id") or "").strip()
                if not thread_id:
                    raise RuntimeError("hold resume requires thread_id")
                first_prompt = str(request.get("prompt") or "")
                if prediction_options:
                    report("waiting_for_prediction_source")
                    predicted = prepare_prediction_prompt(client, thread_id=thread_id,
                        request_id=request_id + ":prediction:first", options=prediction_options,
                        receipt_dir=request_path.parent / "predictions", stop_requested=stop_requested, model=request.get("model"))
                    publish(request_path.with_name("prediction-first.json"),
                            {"request_id":request_id, "hold_id":hold_id, "prediction":predicted})
                    if predicted.get("status") != "completed":
                        witness = predicted.get("pre_helper_rejection")
                        if (isinstance(witness, dict)
                                and witness.get("schema") == "jarvis-prediction-pre-helper-rejection/v1"
                                and witness.get("request_id") == request_id + ":prediction:first"
                                and witness.get("source_thread_id") == thread_id
                                and witness.get("source_turn_id") == predicted.get("source_turn_id")
                                and witness.get("model") == request.get("model")
                                and all(witness.get(key) is False for key in (
                                    "helper_fork_attempted", "helper_dispatch_attempted", "research_dispatch_attempted"))
                                and dispatch_evidence == "not_sent" and not turn_id):
                            pre_helper_rejection = witness
                            terminal_confirmed = False
                        if predicted.get("cleanup_completed") is False:
                            terminal_confirmed = False
                        raise RuntimeError("prediction continuation stopped: " + str(predicted.get("status")))
                    if stop_requested():
                        raise DispatchCancelledBeforeSend("stop requested before predictive resume")
                    first_prompt = predicted["prompt"]
                terminal_confirmed = False
                created = client.resume_turn_async(
                    thread_id,
                    first_prompt,
                    client_user_message_id=request_id,
                    model=request.get("model"),
                    reasoning_effort=request.get("reasoning_effort"),
                    input_binding=input_binding,
                    on_phase=report,
                )
            else:
                terminal_confirmed = False
                created = client.create_task({**request, "input_binding": input_binding}, on_phase=report)
                thread_id = str(created.get("thread_id") or "").strip()
                turn_id = str(created.get("turn_id") or "").strip()
            if mode == "resume":
                turn_id = str(created.get("turn_id") or "").strip()
            if not thread_id or not turn_id:
                raise RuntimeError("App Server task creation did not return thread_id and turn_id")
        if transport_health is not None:
            try:
                transport_health.bind(thread_id, turn_id, getattr(client, "runtime_context", {}))
            except Exception:
                try:
                    transport_health.invalidate()
                except Exception:
                    pass
        publish(ack_path, {
            "request_id": request_id,
            "status": "holding",
            "phase": "turn_holding",
            "pid": os.getpid(),
            "thread_id": thread_id,
            "turn_id": turn_id,
            "hold_id": hold_id,
            "turn_count": initial_turn_count,
            "session_turn_count": initial_turn_count,
            "total_turn_count": initial_total_turn_count,
            "max_turns": max_turns,
            "observed_at": _now(),
            **runtime_fields(),
        })
        turn_count = initial_turn_count
        total_turn_count = initial_total_turn_count
        turn_monitor = monitor or HoldTurnMonitor()
        policy = _notification_policy(request.get("notifications"))
        events_path = result_path.with_name("monitor-events.jsonl")
        while True:
            verification = {"status": "not_checked"}
            monitor_request = HoldTurnRequest(
                hold_id=hold_id,
                thread_id=thread_id,
                turn_id=turn_id,
                turn_count=turn_count,
                max_turns=max_turns,
                continuation_enabled=auto_continue,
                continue_prompt=continue_prompt,
                notification_policy=policy,
                verify_output=verify_output,
                stop_requested=stop_requested,
                control_poll=control_poll,
                receive_final_answer=receive_final_answer if pocket else None,
                completion_marker=prediction_options.get("completion_marker"),
            )
            decision = turn_monitor.observe(client, monitor_request)
            terminal_confirmed = decision.result_status in {
                "holding", "completed", "turn_limit_reached", "failed", "blocked",
                "cancelled", "canceled", "interrupted",
            }
            with ProcessLock(request_path.with_suffix(".lock"), owner_alive=_pid_is_alive):
                _validate_monitor_command(decision, hold_id=hold_id, turn_id=turn_id)
                if decision.action == "CONTINUE" and stop_requested():
                    decision = turn_monitor._stop(monitor_request, decision.command_id, "cancelled",
                                                  decision.final_message, "stop_requested",
                                                  terminal_evidence=getattr(decision, "terminal_evidence", "owner_turn_completed"))
                _append_monitor_events(events_path, decision)
                final_message = decision.final_message
                terminal_status = decision.result_status
                if not terminal_confirmed:
                    break  # Unknown execution is not an item terminal record or continuation permit.
                phase = "history_recording"
                history_path = Path(str(request.get("turn_history_path") or result_path.with_name("turn-history.sqlite")))
                binding = request.get("input_binding") or {}
                is_batch_lane = "lane_item_count" in binding and lane_batch_size(binding) > 1
                append_terminal_turn_history(
                    history_path, task_id=_task_id(request, hold_id), hold_id=hold_id,
                    request_id=request_id, thread_id=thread_id, turn_id=turn_id,
                    turn_number=total_turn_count, candidate_id=_candidate_id(request, total_turn_count),
                    status="failed" if verification.get("status") == "review" else terminal_status,
                    final_answer=final_message, completed_at=_now(),
                    candidate_ids=(lane_batch_ids(request["input_binding"], total_turn_count)
                                   if is_batch_lane else None),
                    output_verification=verification if "lane_item_count" in binding else None,
                )
                scheduler_failed_items = _count_scheduler_failed_items(history_path, hold_id=hold_id)
                publish(ack_path, {
                    "request_id": request_id,
                    "status": "holding" if decision.action == "CONTINUE" else terminal_status,
                    "phase": "monitor_decision",
                    "pid": os.getpid(),
                    "hold_id": hold_id,
                    "thread_id": thread_id,
                    "turn_id": turn_id,
                    "turn_count": turn_count,
                    "session_turn_count": turn_count,
                    "total_turn_count": total_turn_count,
                    "max_turns": max_turns,
                    "monitor_command": {
                        "action": decision.action,
                        "command_id": decision.command_id,
                        "expected_turn_id": decision.expected_turn_id,
                        "reason": decision.reason,
                    },
                    "reason": decision.reason,
                    "error": decision.error,
                    "observed_at": _now(),
                    **runtime_fields(),
                })
                if decision.action != "CONTINUE":
                    break
                client.read_only = False  # Validated continuation still passes native dispatch admission.
                next_prompt = str(decision.continue_prompt or continue_prompt)
                if prediction_options:
                    predicted = prepare_prediction_prompt(client, thread_id=thread_id,
                        source_turn_id=turn_id, request_id=decision.command_id + ":prediction",
                        options=prediction_options, receipt_dir=request_path.parent / "predictions", stop_requested=stop_requested, model=request.get("model"))
                    publish(request_path.with_name(f"prediction-{total_turn_count + 1}.json"),
                            {"request_id":request_id, "hold_id":hold_id, "prediction":predicted})
                    if predicted.get("status") != "completed":
                        if predicted.get("cleanup_completed") is False:
                            terminal_confirmed = False
                        terminal_status = "blocked"
                        decision = turn_monitor._stop(monitor_request, decision.command_id, terminal_status,
                            final_message, "prediction_" + str(predicted.get("status")))
                        break
                    if stop_requested():
                        terminal_status = "cancelled"
                        decision = turn_monitor._stop(monitor_request, decision.command_id, terminal_status,
                            final_message, "stop_requested")
                        break
                    next_prompt = predicted["prompt"]
                terminal_confirmed = False
                turn_id = ""  # A failed next dispatch must not reuse the previous terminal turn.
                dispatch_evidence = "not_sent"
                started = client.start_turn_async(
                    thread_id,
                    next_prompt,
                    client_user_message_id=decision.command_id,
                    model=request.get("model"),
                    reasoning_effort=request.get("reasoning_effort"),
                    input_binding=_turn_input_binding(request, total_turn_count + 1),
                    on_phase=report,
                )
                turn_id = str(started.get("turn_id") or "").strip()
                if not turn_id:
                    raise RuntimeError("monitor continuation command did not return turn_id")
                turn_count += 1
                total_turn_count += 1
                publish(ack_path, {
                    "request_id": request_id,
                    "status": "holding",
                    "phase": "turn_holding",
                    "pid": os.getpid(),
                    "thread_id": thread_id,
                    "turn_id": turn_id,
                    "hold_id": hold_id,
                    "turn_count": turn_count,
                    "session_turn_count": turn_count,
                    "total_turn_count": total_turn_count,
                    "max_turns": max_turns,
                    "observed_at": _now(),
                    **runtime_fields(),
                })
                if mode == "recover":
                    # This request is an exact recovery binding, not just an
                    # initial thread hint. Advance it from the actual dispatch
                    # response under the same request lock as continuation.
                    current = _read_json(request_path)
                    if current.get("request_id") != request_id:
                        raise RuntimeError("Hold request changed during continuation")
                    publish(request_path, {**current, "thread_id": thread_id, "turn_id": turn_id})
        publish(result_path, {
            "request_id": request_id,
            "status": terminal_status,
            "phase": "turn_terminal",
            "pid": os.getpid(),
            "thread_id": thread_id,
            "turn_id": turn_id,
            "hold_id": hold_id,
            "turn_count": turn_count,
            "session_turn_count": turn_count,
            "total_turn_count": total_turn_count,
            "max_turns": max_turns,
            "final_message": final_message,
            "reason": decision.reason,
            "error": decision.error,
            "output_verification": verification,
            "scheduler_failed_items": scheduler_failed_items,
            "terminal_confirmed": terminal_confirmed,
            "host_stop": interrupt_at is not None,
            "terminal_evidence": getattr(decision, "terminal_evidence", "owner_turn_completed"),
            "observed_at": _now(),
            **runtime_fields(),
        })
        return 0
    except Exception as exc:
        from jarvis_runtime.cancellation import GenerationRetired
        if isinstance(exc, GenerationRetired):
            return 1
        failure = {
            "request_id": request_id, "hold_id": hold_id, "status": "failed", "phase": phase,
            "pid": os.getpid(), "error_code": getattr(exc, "code", None),
            "thread_id": thread_id, "turn_id": turn_id,
            "turn_count": turn_count, "total_turn_count": total_turn_count,
            "max_turns": max_turns,
            "terminal_confirmed": terminal_confirmed, "reason": str(exc), "observed_at": _now(),
            "dispatch_evidence": dispatch_evidence,
            **runtime_fields(),
        }
        if pre_helper_rejection is not None:
            failure["pre_helper_rejection"] = pre_helper_rejection
            failure["terminal_confirmed"] = False
        if host_stop_selected or interrupt_at is not None:
            failure.update(host_stop=True, execution_evidence="requires_owner_terminal")
        if isinstance(exc, _RecoveredTurnTerminal):
            failure.update(status="cancelled" if exc.status == "completed" else exc.status,
                           phase="host_stop_exit_pending", terminal_confirmed=False,
                           host_stop=True, execution_evidence="holder_exact_terminal_readback",
                           owner_terminal_readback={"thread_id": thread_id, "turn_id": turn_id,
                                                    "status": exc.status, "observed_at": _now()})
        if isinstance(exc, _HolderStopUnknown):
            failure.update(status="unknown", phase="host_stop_unknown", terminal_confirmed=False,
                           execution_evidence="requires_owner_terminal", host_stop=True)
        publish(result_path, failure)
        publish(ack_path, failure)
        return 1
    finally:
        if transport_health is not None:
            try:
                transport_health.stop()
            except Exception:
                pass
        observer_stopped = client is None
        if client is not None:
            owned_process = getattr(client, "process", None)
            client.close()
            observer_stopped = _client_observer_stopped(client, owned_process)
            if owner_control_lease is not None and observer_stopped and owner_control_lease.state.get("connection"):
                owner_control_lease.observer_closed(client)
            if result_path.is_file():
                saved = _read_json(result_path)
                if saved.get("request_id") == request_id and saved.get("host_stop"):
                    closed = {**saved, "holder_client_exit_confirmed": (
                        owned_process is not None and owned_process.poll() is not None)}
                    if owned_process is None and observer_stopped:
                        from jarvis_runtime.holder_transport_release import make_unix_attachment_release
                        proof = make_unix_attachment_release(client, closed, _read_json(request_path))
                        if proof is not None:
                            closed.update(holder_transport_release=proof, local_observer_stopped=True)
                    publish(result_path, closed)
        if observer_stopped and result_path.is_file():
            saved = _read_json(result_path)
            current = _read_json(request_path)
            if saved.get("request_id") == current.get("request_id") == request_id:
                publish(result_path, {**saved, "local_observer_stopped": True})
                from adapters.codex_app_server.task_provisioning_adapter import refresh_close_report
                refresh_close_report(result_path.parent, {**saved, "local_observer_stopped": True,
                    "hold_released": saved.get("terminal_confirmed") is True
                        and not (result_path.parent / ".user-host-claim").exists()})


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--launcher-config", type=Path, required=True)
    parser.add_argument("--request-path", type=Path, required=True)
    parser.add_argument("--ack-path", type=Path, required=True)
    parser.add_argument("--result-path", type=Path, required=True)
    args = parser.parse_args()
    return hold_task(
        args.launcher_config,
        args.request_path,
        args.ack_path,
        args.result_path,
    )


if __name__ == "__main__":
    raise SystemExit(main())
