"""Verified Jarvis notification adapter backed by the existing Feishu bridge.

The adapter owns no bot credentials.  It enqueues one outbox record through
the established dispatcher and asks that bridge to deliver *that exact* record.
Only a delivery-log entry containing a Feishu ``message_id`` is a success.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import subprocess
import tempfile
from typing import Any, Callable, Mapping


Runner = Callable[..., subprocess.CompletedProcess[str]]


@dataclass(frozen=True)
class FeishuOutboxNotificationConfig:
    python_executable: Path
    dispatcher_store_script: Path
    bridge_script: Path
    bridge_config: Path
    dispatcher_root: Path
    recipient: str

    @classmethod
    def load(cls, path: Path) -> "FeishuOutboxNotificationConfig":
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
        base = path.resolve().parent

        def file(key: str) -> Path:
            value = Path(str(raw.get(key) or ""))
            if not str(value):
                raise ValueError(f"notification config requires {key}")
            return value.resolve() if value.is_absolute() else (base / value).resolve()

        recipient = str(raw.get("recipient") or "").strip()
        if not recipient:
            raise ValueError("notification config requires recipient")
        return cls(
            python_executable=file("python_executable"),
            dispatcher_store_script=file("dispatcher_store_script"),
            bridge_script=file("bridge_script"),
            bridge_config=file("bridge_config"),
            dispatcher_root=file("dispatcher_root"),
            recipient=recipient,
        )


class FeishuOutboxNotificationPort:
    """A notification succeeds only after exact delivery readback."""

    def __init__(self, config: FeishuOutboxNotificationConfig, *, runner: Runner | None = None) -> None:
        self.config = config
        self._runner = runner or subprocess.run

    def notify(self, *, request_id: str, source_ref: str, message: str) -> Mapping[str, Any]:
        content = str(message).strip()
        if not content:
            return {"status": "invalid_request", "reason": "message is required"}
        request = {
            "task_id": f"jarvis-notify-{request_id}",
            "source_thread_id": source_ref,
            "status": "notification",
            "recipient": self.config.recipient,
            "content": content,
            "message_kind": "jarvis_notification",
            "interaction_mode": "notify",
        }
        try:
            queued = self._enqueue(request)
            record = dict(queued["result"]["record"])
            outbox_id = str(record["outbox_id"])
            self._run([
                str(self.config.python_executable), str(self.config.bridge_script),
                "--config", str(self.config.bridge_config), "--send-outbox-id", outbox_id,
            ])
            delivery = self._delivery(outbox_id)
        except (OSError, ValueError, KeyError, subprocess.SubprocessError) as exc:
            return {"status": "failed", "reason": str(exc)}
        if delivery and delivery.get("delivery_status") == "delivered" and delivery.get("message_id"):
            return {
                "status": "completed", "outbox_id": outbox_id,
                "message_id": delivery["message_id"], "delivery_status": "delivered",
            }
        return {
            "status": "failed", "outbox_id": outbox_id,
            "reason": str((delivery or {}).get("error") or "Feishu delivery has no confirmed message_id"),
            "delivery_status": (delivery or {}).get("delivery_status"),
        }

    def _enqueue(self, request: Mapping[str, Any]) -> Mapping[str, Any]:
        self.config.dispatcher_root.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".json", prefix="jarvis-notify-",
            dir=self.config.dispatcher_root, delete=False,
        ) as handle:
            request_path = Path(handle.name)
            json.dump(dict(request), handle, ensure_ascii=False)
        try:
            return self._run([
                str(self.config.python_executable), str(self.config.dispatcher_store_script),
                "--root", str(self.config.dispatcher_root), "enqueue-outbox", "--request", str(request_path),
            ])
        finally:
            request_path.unlink(missing_ok=True)

    def _run(self, command: list[str]) -> Mapping[str, Any]:
        result = self._runner(command, text=True, capture_output=True, check=False)
        if result.returncode != 0:
            raise RuntimeError((result.stderr or result.stdout or "notification command failed").strip())
        try:
            parsed = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError("notification command did not return JSON") from exc
        if not parsed.get("ok", True):
            raise RuntimeError(str(parsed.get("error") or "notification command failed"))
        return parsed

    def _delivery(self, outbox_id: str) -> Mapping[str, Any] | None:
        path = self.config.dispatcher_root / "delivery_log.jsonl"
        if not path.exists():
            return None
        latest: dict[str, Any] | None = None
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if value.get("outbox_id") == outbox_id:
                latest = value
        return latest
