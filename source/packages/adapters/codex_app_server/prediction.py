"""Native composer prediction protocol, isolated behind the Codex adapter."""
from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
import urllib.request
import urllib.error

from jarvis_runtime.coo_dispatcher_store import ProcessLock
from jarvis_control.provisioning import compose_resume_prompt


def official_prediction_config(codex_home: Path) -> dict:
    """Use the existing signed-in account; never persist or return credentials."""
    try:
        tokens = json.loads((codex_home / "auth.json").read_text(encoding="utf-8"))["tokens"]
        request = urllib.request.Request("https://chatgpt.com/backend-api/wham/predictions/config", headers={
            "Authorization": "Bearer " + tokens["access_token"],
            "ChatGPT-Account-ID": tokens["account_id"], "originator": "Codex Desktop",
        })
        with urllib.request.urlopen(request, timeout=25) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"official prediction configuration HTTP {exc.code}") from None
    except Exception:
        raise RuntimeError("official prediction configuration unavailable for this Codex Home") from None


def _save(path, value):
    from adapters.codex_app_server.jarvis_task_hold_host import _write_json
    from jarvis_codex_bridge.journal import fsync_directory
    _write_json(path, value, durable=True)
    fsync_directory(path.parent)


def latest_turn(client, thread_id, *, deadline=None):
    if deadline is None:
        deadline = time.monotonic() + client.config.turn_completion_timeout_seconds
    thread = client.request("thread/read", {"threadId":thread_id, "includeTurns":False}, deadline=deadline).get("thread", {})
    if thread.get("id") != thread_id:
        raise ValueError("source thread identity mismatch")
    page = client.request("thread/turns/list", {"threadId":thread_id, "itemsView":"notLoaded",
        "sortDirection":"desc", "limit":1}, deadline=deadline)
    turns = page.get("data")
    if not isinstance(turns, list) or len(turns) != 1 or not isinstance(turns[0], dict) or not turns[0].get("id"):
        raise ValueError("source has no exact turn")
    turn = turns[0]
    if turn.get("itemsView") != "notLoaded" or turn.get("items") != []:
        raise ValueError("source metadata contains unexpected item payload")
    return turn


class NativePredictor:
    def __init__(self, client, receipt_dir: Path, config_loader):
        self.client, self.root, self.config_loader = client, receipt_dir, config_loader

    def predict(self, *, thread_id, request_id, source_turn_id=None, model=None):
        if not isinstance(request_id, str) or not request_id.strip() or not thread_id:
            return {"status":"invalid_request", "reason":"thread_id and request_id are required"}
        path = self.root / (hashlib.sha256(request_id.encode()).hexdigest() + ".json")
        binding = {"thread_id":thread_id, "source_turn_id":source_turn_id, "model":model}
        path.parent.mkdir(parents=True, exist_ok=True)
        with ProcessLock(path.with_suffix(".lock")):
            if path.exists():
                saved = json.loads(path.read_text(encoding="utf-8"))
                if saved.get("binding") != binding:
                    return {"status":"conflict", "reason":"request_id already bound to different inputs"}
                return saved["result"]
            result = {"schema":"jarvis-prediction/v1", "status":"requires_readback", "request_id":request_id,
                      "source_thread_id":thread_id, "source_turn_id":source_turn_id,
                      "native_prediction_mode":True, "cleanup_completed":False,
                      "parent_archived":False, "prediction_terminal_confirmed":False}
            record = {"binding":binding, "result":result}
            parent_id = None
            dispatch_attempted = False
            rpc_context = {"phase": "source_read", "thread_id": thread_id, "method": None}
            def request(method, params, **kwargs):
                rpc_context.update(phase="rpc", method=method, thread_id=params.get("threadId"))
                return self.client.request(method, params, **kwargs)
            def error_binding(source):
                return {"request_id": request_id, "source_thread_id": thread_id,
                        "source_turn_id": result.get("source_turn_id"),
                        "prediction_thread_id": result.get("prediction_thread_id"),
                        "prediction_turn_id": result.get("prediction_turn_id"),
                        "model": result.get("requested_model"), "source": source}
            try:
                source = latest_turn(self.client, thread_id)
                result["source_turn_id"] = source["id"]
                if source_turn_id is not None and source["id"] != source_turn_id:
                    return {**result, "status":"stale"}
                if source.get("status") != "completed":
                    return {**result, "status":"waiting" if source.get("status") in {"inProgress","running"} else "source_not_completed"}
                # Never inherit the helper server's default model. A task's explicit
                # model wins; standalone prediction requires source metadata proof.
                expected_model = model
                if expected_model is None:
                    source_thread = request("thread/read", {
                        "threadId":thread_id, "includeTurns":False}).get("thread", {})
                    if source_thread.get("id") != thread_id:
                        raise ValueError("source thread identity mismatch")
                    expected_model = source_thread.get("model")
                if not isinstance(expected_model, str) or not expected_model.strip():
                    raise ValueError("prediction model is unknown; specify the task model or read back source model metadata")
                result["requested_model"] = expected_model
                config = self.config_loader()
                def reject_before_helper(status, reason):
                    # This branch precedes the first fork and every helper/research send.
                    config_digest = hashlib.sha256(json.dumps(config, sort_keys=True,
                        separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
                    result.update(status=status, reason=reason, pre_helper_rejection={
                        "schema": "jarvis-prediction-pre-helper-rejection/v1",
                        "request_id": request_id, "source_thread_id": thread_id,
                        "source_turn_id": source["id"], "model": expected_model,
                        "config_sha256": config_digest, "prompt_version": config.get("prompt_version"),
                        "rejection": status, "helper_fork_attempted": False,
                        "helper_dispatch_attempted": False, "research_dispatch_attempted": False})
                    _save(path, record)
                    return result
                if config.get("is_enabled") is not True:
                    return reject_before_helper("disabled", "official prediction configuration disabled")
                if not config.get("prompt") or not config.get("prediction_reasoning_effort") or not config.get("prompt_version"):
                    return reject_before_helper("failed", "official prediction configuration is incomplete")
                if expected_model in config.get("unsupported_models", []):
                    return reject_before_helper("unsupported", "requested prediction model is unsupported")
                _save(path, record)  # Crash/timeout after this point is not permission to regenerate.
                parent = request("thread/fork", {"threadId":thread_id, "lastTurnId":source["id"],
                    "ephemeral":False, "deferGoalContinuation":True, "threadSource":"composer_predictions",
                    "excludeTurns":True, "sandbox":"read-only", "approvalPolicy":"never",
                    "model":expected_model, "modelProvider":"openai"})
                parent_id = parent["thread"]["id"]
                result["parent_thread_id"] = parent_id
                _save(path, record)
                if parent.get("model") != expected_model or parent.get("modelProvider") != "openai":
                    raise ValueError("prediction parent model/provider mismatch; no helper turn dispatched")
                fork = request("thread/fork", {"threadId":parent_id, "ephemeral":True,
                    "experimentalPredictionMode":True, "threadSource":"composer_predictions", "excludeTurns":True,
                    "model":expected_model, "modelProvider":"openai"})
                prediction_id = fork["thread"]["id"]
                result.update(prediction_thread_id=prediction_id, model=fork.get("model"),
                              model_provider=fork.get("modelProvider"), prompt_version=config["prompt_version"])
                if (fork.get("modelProvider") != "openai" or fork.get("model") != expected_model
                        or fork.get("model") in config.get("unsupported_models", [])
                        or fork["thread"].get("ephemeral") is not True):
                    result.update(status="unsupported", reason="prediction helper model/provider or ephemeral identity mismatch; no helper turn dispatched")
                else:
                    schema = {"type":"object", "properties":{"suggestion":{"type":["string","null"]}},
                              "required":["suggestion"], "additionalProperties":False}
                    # The source Hold's dispatch journal must never bind a helper turn.
                    original_gate = getattr(self.client, "before_turn_dispatch", None)
                    def prediction_gate(params):
                        if params.get("threadId") != prediction_id or params.get("model") != expected_model:
                            raise RuntimeError("prediction dispatch target mismatch")
                    self.client.before_turn_dispatch = prediction_gate
                    try:
                        dispatch_attempted = True
                        started = request("turn/start", {"threadId":prediction_id, "model":expected_model,
                            "input":[{"type":"text", "text":config["prompt"] + "\n\nReturn only JSON matching this schema: " + json.dumps(schema), "text_elements":[]}],
                            "effort":config["prediction_reasoning_effort"], "outputSchema":schema})
                    finally:
                        self.client.before_turn_dispatch = original_gate
                    turn_id = started["turn"]["id"]
                    result["prediction_turn_id"] = turn_id
                    _save(path, record)
                    rpc_context.update(phase="owner_terminal_wait", method=None, thread_id=prediction_id)
                    terminal = self.client.wait_for_turn_terminal(prediction_id, turn_id)
                    result["prediction_terminal_confirmed"] = (terminal.get("id") == turn_id
                        and terminal.get("status") in {"completed", "failed", "interrupted"})
                    if result["prediction_terminal_confirmed"]:
                        result["prediction_terminal_status"] = terminal["status"]
                        if terminal["status"] in {"failed", "interrupted"} and isinstance(terminal.get("error"), dict):
                            result["native_error"] = json.loads(json.dumps(terminal["error"]))
                            result["error_binding"] = error_binding("owner_turn_completed")
                    if terminal.get("id") != turn_id or terminal.get("status") != "completed":
                        raise RuntimeError("prediction turn completion unverified")
                    messages = [item.get("text", "") for item in terminal.get("items", [])
                                if item.get("type") == "agentMessage" and item.get("phase") == "final_answer"]
                    # Ephemeral predictions already carry the final answer in turn/completed.
                    rpc_context.update(phase="helper_readback", method=None, thread_id=prediction_id)
                    raw = messages[-1] if messages else self.client.wait_for_turn_readback(prediction_id, turn_id)
                    output = json.loads(raw)
                    if not isinstance(output, dict) or set(output) != {"suggestion"} or not (output["suggestion"] is None or isinstance(output["suggestion"], str)):
                        raise ValueError("invalid native prediction output")
                    result["suggestion"] = output["suggestion"]
                    rpc_context.update(phase="source_read", method=None, thread_id=thread_id)
                    current = latest_turn(self.client, thread_id)
                    result["source_unchanged"] = current == source
                    result["status"] = ("stale" if not result["source_unchanged"] else
                                        "completed" if output["suggestion"] and output["suggestion"].strip() else "no_prediction")
            except Exception as exc:
                rpc_error = getattr(exc, "rpc_error", None)
                if isinstance(rpc_error, dict):
                    result["rpc_error"] = json.loads(json.dumps(rpc_error))
                    result["rpc_error_binding"] = {**error_binding("native_rpc_error"), **rpc_context}
                unsupported = isinstance(rpc_error, dict) and rpc_error.get("code") == -32601
                result.update(status="requires_readback" if dispatch_attempted and not result["prediction_terminal_confirmed"]
                    else "unsupported" if unsupported else "failed", reason=str(exc))
            finally:
                if parent_id:
                    try:
                        request("thread/archive", {"threadId":parent_id})
                        result["parent_archived"] = True
                        result["cleanup_completed"] = not dispatch_attempted or result["prediction_terminal_confirmed"]
                    except Exception:
                        result.update(status="cleanup_failed", reason="prediction helper archive unconfirmed")
                if path.exists():
                    _save(path, record)
            return result


def prepare_prediction_prompt(client, *, thread_id, request_id, options, receipt_dir,
                              source_turn_id=None, stop_requested=None, model=None):
    client.start()
    if source_turn_id is None:
        deadline = time.monotonic() + client.config.turn_completion_timeout_seconds
        source = latest_turn(client, thread_id, deadline=deadline)
        source_turn_id = source["id"]
        # A new app-server does not receive another client's completion events.
        while source.get("status") in {"inProgress", "running"}:
            if stop_requested and stop_requested():
                return {"status":"cancelled", "reason":"stop requested while awaiting source"}
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return {"status":"requires_readback", "reason":"source turn wait deadline exhausted"}
            time.sleep(min(1, remaining))
            source = latest_turn(client, thread_id, deadline=deadline)
            if source["id"] != source_turn_id:
                return {"status":"stale", "reason":"source changed while waiting"}
        if source.get("status") != "completed":
            return {"status":"source_not_completed", "reason":"source turn did not complete"}
    if stop_requested and stop_requested():
        return {"status":"cancelled"}
    home = Path(client.config.expected_codex_home)
    result = NativePredictor(client, Path(receipt_dir), lambda: official_prediction_config(home)).predict(
        thread_id=thread_id, request_id=request_id, source_turn_id=source_turn_id, model=model)
    if result["status"] == "completed":
        source = latest_turn(client, thread_id)
        if source.get("id") != source_turn_id or source.get("status") != "completed":
            return {**result, "status":"stale", "source_unchanged":False}
        result = {**result, "prompt":compose_resume_prompt(options["mode"], options.get("fixed_prompt", ""), result["suggestion"])}
    return result
