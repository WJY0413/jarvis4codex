"""Opt-in live onboarding check for the reproduced Jean /alpha/search 404.

No Jarvis runtime state is opened. Only a new ephemeral Codex thread is used.
Ordinary suite runs skip this paid/network check; see JARVIS_CONNECTION.md.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import queue
import subprocess
import tempfile
import threading
import time
import tomllib
import unittest
from urllib.parse import urlsplit


@unittest.skipUnless(os.environ.get("MODEL_WEB_ACCEPTANCE_MODELS"), "explicit live onboarding only")
class ModelWebAcceptance(unittest.TestCase):
    def test_registered_models_execute_builtin_web(self):
        executable = Path(os.environ["MODEL_WEB_ACCEPTANCE_CODEX"])
        base_url = os.environ["MODEL_WEB_ACCEPTANCE_BASE_URL"]
        destination = urlsplit(base_url)
        self.assertIn(destination.hostname, {"127.0.0.1", "localhost", "::1"})
        self.assertEqual(destination.scheme, "http")
        self.assertEqual(destination.path, "/v1")
        self.assertIsNone(destination.username)
        evidence = Path(os.environ["MODEL_WEB_ACCEPTANCE_OUTPUT"]).resolve()
        evidence.mkdir(parents=True, exist_ok=True)
        for model in os.environ["MODEL_WEB_ACCEPTANCE_MODELS"].split(","):
            with self.subTest(model=model):
                receipt = self._probe(executable, base_url, model.strip())
                safe_name = model.replace("/", "_")
                (evidence / f"{safe_name}.json").write_text(
                    json.dumps(receipt, ensure_ascii=False, indent=2), encoding="utf-8"
                )
                self.assertTrue(receipt["passed"], json.dumps(receipt, ensure_ascii=False))

    def _probe(self, executable: Path, base_url: str, model: str) -> dict:
        receipt = {"model": model, "base_url": base_url, "started_at": time.time(),
                   "ephemeral": True, "events": [], "passed": False}
        process_base = os.environ.get("MODEL_WEB_ACCEPTANCE_PROCESS_BASE_URL", base_url)
        receipt["process_default_base_url"] = process_base
        args = [str(executable), "-c", f"openai_base_url={json.dumps(process_base)}",
                "-c", 'web_search="live"', "-c", 'model_reasoning_effort="low"',
                "-c", "features.shell_tool=false", "-c", "features.code_mode=false"]
        # Do not start unrelated MCP servers in a synthetic search-only probe.
        codex_root = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))
        config_path = codex_root / "config.toml"
        if config_path.is_file():
            config = tomllib.loads(config_path.read_text(encoding="utf-8"))
            for server in config.get("mcp_servers", {}):
                if not all(c.isalnum() or c in "_-" for c in server):
                    raise ValueError("MCP server name cannot be addressed by a dotted CLI override")
                args += ["-c", f"mcp_servers.{server}.enabled=false"]
        args += ["app-server", "--stdio"]
        events: queue.Queue = queue.Queue()
        counter = 0
        with tempfile.TemporaryDirectory(prefix="model-web-acceptance-") as working:
            diagnostic = tempfile.TemporaryFile(mode="w+", encoding="utf-8")
            proc = subprocess.Popen(
                args, cwd=working, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=diagnostic, text=True, encoding="utf-8",
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            receipt["probe_pid"] = proc.pid

            def reader():
                for line in proc.stdout:
                    try:
                        events.put(json.loads(line))
                    except ValueError:
                        pass
                events.put({"probe_process_exited": True})

            threading.Thread(target=reader, daemon=True).start()

            def send(value):
                proc.stdin.write(json.dumps(value) + "\n")
                proc.stdin.flush()

            def consume(event):
                if event.get("probe_process_exited"):
                    raise RuntimeError("probe app-server exited before turn completion")
                method = event.get("method", "")
                if "id" in event and method:
                    # No approvals or external dynamic tools are allowed in this probe.
                    send({"id": event["id"], "error": {"code": -32601, "message": "Search-only probe"}})
                    receipt.setdefault("unexpected_requests", []).append(method)
                if method in {"item/started", "item/completed", "turn/completed", "error", "thread/tokenUsage/updated"}:
                    receipt["events"].append(event)

            def rpc(method, params):
                nonlocal counter
                counter += 1
                send({"id": counter, "method": method, "params": params})
                deadline = time.monotonic() + 45
                while time.monotonic() < deadline:
                    event = events.get(timeout=max(.1, deadline - time.monotonic()))
                    if event.get("id") == counter and not event.get("method"):
                        if "error" in event:
                            raise RuntimeError(json.dumps(event["error"]))
                        return event["result"]
                    consume(event)
                raise TimeoutError(method)

            try:
                rpc("initialize", {"clientInfo": {"name": "model-web-acceptance", "version": "1.0"},
                                   "capabilities": {"experimentalApi": True}})
                send({"method": "initialized", "params": {}})
                started = rpc("thread/start", {
                    "model": model, "ephemeral": True, "cwd": working,
                    "config": {"openai_base_url": base_url},
                    "allowProviderModelFallback": False, "approvalPolicy": "never", "sandbox": "read-only",
                    "baseInstructions": "You are running a search-only connection check. Use the builtin web tool exactly as requested. Never run commands, read local files, use MCP, or modify anything. If web fails, state its actual error; never invent evidence.",
                })
                receipt["actual_model"] = started.get("model")
                receipt["provider"] = started.get("modelProvider")
                receipt["thread_id"] = started["thread"]["id"]
                turn = rpc("turn/start", {
                    "threadId": receipt["thread_id"],
                    "input": [{"type": "text", "text_elements": [], "text":
                        'Use builtin web search now, exactly once: search query site:iana.org/help/example-domains "documentation". From the returned result, state in one sentence what IANA example domains are for, with its source link. Do not substitute time, direct HTTP, shell, or a memory answer.'}],
                })
                receipt["turn_id"] = turn["turn"]["id"]
                deadline = time.monotonic() + 180
                while time.monotonic() < deadline:
                    event = events.get(timeout=max(.1, deadline - time.monotonic()))
                    consume(event)
                    if event.get("method") == "turn/completed":
                        receipt["terminal"] = event["params"]["turn"]
                        break
                completed = [e["params"]["item"] for e in receipt["events"] if e.get("method") == "item/completed"]
                searches = [i for i in completed if i.get("type") == "webSearch" and i.get("action", {}).get("type") == "search"]
                messages = [i.get("text", "") for i in completed if i.get("type") == "agentMessage"]
                receipt["searches"] = searches
                receipt["messages"] = messages
                source_results = [r for search in searches for r in search.get("results", [])
                                  if r.get("url") == "https://www.iana.org/help/example-domains"
                                  and r.get("ref_id") and "documentation" in r.get("snippet", "").lower()]
                receipt["passed"] = (
                    receipt["actual_model"] == model
                    and receipt.get("terminal", {}).get("status") == "completed"
                    and len(searches) >= 1
                    and bool(source_results)
                    and any("iana.org" in m.lower() and "documentation" in m.lower() for m in messages)
                    and not any(e.get("method") == "error" for e in receipt["events"])
                    and not receipt.get("unexpected_requests")
                    and not any(word in "\n".join(messages).lower() for word in ["404", "stream disconnected", "search failed"])
                )
            except Exception as exc:
                receipt["failure"] = f"{type(exc).__name__}: {exc}"
            finally:
                # Terminate only the child created here, never a desktop/production process.
                proc.terminate()
                proc.wait(timeout=15)
                proc.stdin.close()
                proc.stdout.close()
                if not receipt["passed"]:
                    diagnostic.seek(0)
                    receipt["diagnostic_tail"] = diagnostic.read()[-3000:]
                diagnostic.close()
                receipt["finished_at"] = time.time()
        return receipt
