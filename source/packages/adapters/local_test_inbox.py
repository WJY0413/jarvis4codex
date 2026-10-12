"""NotificationPort for an actual private, local TEST inbox.

An outbox record is only an intent. A separate receiver process must durably
store the exact UTF-8 message and return a matching acknowledgement. The
adapter then independently reads that receiver's store before claiming
delivery. There is no network, automatic resend, credential, or service.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import math
import os
from pathlib import Path
import stat
import subprocess
import sys
from typing import Any, Mapping

from .local_test_inbox_receiver import (
    MESSAGE_ID, PROTOCOL, check_envelope, check_receipt, decode_json, encode_json,
    publish_record, read_record, relative_parts, request_key, storage_fd,
    validate_settings, workspace_fd,
)


@dataclass(frozen=True)
class LocalTestInboxNotificationConfig:
    workspace_root: Path
    storage_dir: str
    recipient: str
    scope: str = "TEST"
    max_body_bytes: int = 65536
    delivery_wait_seconds: float = 10.0

    def __post_init__(self) -> None:
        if not isinstance(self.workspace_root, Path):
            raise ValueError("workspace_root must be a Path")
        validate_settings(self.workspace_root, self.storage_dir, self.recipient,
                          self.scope, self.max_body_bytes)
        if (isinstance(self.delivery_wait_seconds, bool)
                or not isinstance(self.delivery_wait_seconds, (int, float))
                or not math.isfinite(self.delivery_wait_seconds)
                or not 0.1 <= self.delivery_wait_seconds <= 60):
            raise ValueError("delivery_wait_seconds must be between 0.1 and 60")
        # The configured private storage must exist. Never chmod or expand access.
        with storage_fd(self.workspace_root, self.storage_dir):
            pass

    @classmethod
    def load(cls, path: Path, *, workspace_root: Path) -> "LocalTestInboxNotificationConfig":
        """Load a bounded config inside the explicit workspace, without symlinks.

        Required JSON fields: scope="TEST", storage_dir, recipient. Optional:
        max_body_bytes and delivery_wait_seconds. Paths are workspace-relative.
        The trusted integration supplies workspace_root, never the JSON file.
        """
        path = Path(path)
        if path.is_absolute():
            try:
                relative = path.relative_to(workspace_root).as_posix()
            except ValueError as exc:
                raise ValueError("notification config is outside the workspace") from exc
        else:
            relative = path.as_posix()
        parts = relative_parts(relative)
        with workspace_fd(workspace_root) as root_fd:
            fd = os.dup(root_fd)
            try:
                for part in parts[:-1]:
                    next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW |
                                      os.O_CLOEXEC, dir_fd=fd)
                    os.close(fd)
                    fd = next_fd
                # Configuration contains no secrets and need not be mode 0600.
                handle = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK |
                                 os.O_CLOEXEC, dir_fd=fd)
                try:
                    info = os.fstat(handle)
                    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 16384:
                        raise ValueError("notification config must be a bounded regular file")
                    raw = os.read(handle, 16385)
                    if len(raw) > 16384:
                        raise ValueError("notification config is too large")
                finally:
                    os.close(handle)
            finally:
                os.close(fd)
        value = decode_json(raw)
        required = {"scope", "storage_dir", "recipient"}
        allowed = required | {"max_body_bytes", "delivery_wait_seconds"}
        if not required.issubset(value) or set(value) - allowed:
            raise ValueError("notification config has missing or unsupported fields")
        return cls(workspace_root=workspace_root, **value)


class LocalTestInboxNotificationPort:
    """TEST-only NotificationPort with exact receiver-backed delivery readback."""

    def __init__(self, config: LocalTestInboxNotificationConfig) -> None:
        self.config = config

    def _envelope(self, *, request_id: str, source_ref: str, message: str) -> dict[str, Any]:
        if not isinstance(message, str) or len(message) > self.config.max_body_bytes:
            raise ValueError("message must be a bounded UTF-8 string")
        body = message.encode("utf-8", errors="strict")  # Deliberately never strip or normalize.
        value = {
            "protocol": PROTOCOL, "scope": "TEST", "request_id": request_id,
            "source_ref": source_ref, "recipient": self.config.recipient,
            "body_b64": base64.b64encode(body).decode("ascii"),
            "body_sha256": hashlib.sha256(body).hexdigest(), "body_bytes": len(body),
        }
        check_envelope(value, self.config.recipient, self.config.max_body_bytes)
        return value

    def _receiver(self, request: Mapping[str, Any]) -> dict[str, Any]:
        script = Path(__file__).with_name("local_test_inbox_receiver.py")
        if script.is_symlink() or not script.is_file():
            raise ValueError("trusted local receiver script is missing or a symlink")
        command = [sys.executable, "-I", "-B", str(script),
                   "--workspace-root", str(self.config.workspace_root),
                   "--storage-dir", self.config.storage_dir,
                   "--recipient", self.config.recipient, "--scope", self.config.scope,
                   "--max-body-bytes", str(self.config.max_body_bytes)]
        # The trusted, bounded receiver writes one bounded JSON response. There is
        # no shell, network transport, credential forwarding, or injectable runner.
        with subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, env={"LANG": "C.UTF-8"}) as process:
            try:
                stdout, _stderr = process.communicate(encode_json(request),
                                                     timeout=self.config.delivery_wait_seconds)
            except subprocess.TimeoutExpired as exc:
                process.kill()
                process.communicate()
                raise TimeoutError("receiver timed out; delivery is unverified, no resend") from exc
            if len(stdout) > self.config.max_body_bytes * 2 + 16384:
                raise ValueError("receiver acknowledgement exceeds protocol bound")
            result = decode_json(stdout)
            if result.get("process_pid") != process.pid or process.pid == os.getpid():
                raise ValueError("receiver process acknowledgement identity mismatch")
            if process.returncode != 0 or result.get("ok") is not True:
                raise ValueError(str(result.get("reason") or "receiver did not acknowledge delivery"))
            if result.get("operation") != request["action"]:
                raise ValueError("receiver acknowledgement operation mismatch")
            receipt = result.get("receipt")
            if not isinstance(receipt, dict):
                raise ValueError("receiver acknowledgement is missing")
            check_receipt(receipt, self.config.recipient, self.config.max_body_bytes)
            if receipt["receiver_pid"] == os.getpid():
                raise ValueError("notification was not received by an independent process")
            return result

    def _unverified(self, reason: str, **identities: Any) -> dict[str, Any]:
        return {"status": "requires_readback", "delivery_status": "unverified",
                "scope": "TEST", "recipient": self.config.recipient,
                "reason": reason, "automatic_resend": False, **identities}

    def notify(self, *, request_id: str, source_ref: str, message: str) -> Mapping[str, Any]:
        try:
            envelope = self._envelope(request_id=request_id, source_ref=source_ref, message=message)
            key = request_key(request_id)
        except (ValueError, TypeError, KeyError) as exc:
            return {"status": "invalid_request", "delivery_status": "unverified", "reason": str(exc)}
        try:
            with storage_fd(self.config.workspace_root, self.config.storage_dir, "outbox", create=True) as fd:
                created = publish_record(fd, key + ".json", envelope)
                stored = read_record(fd, key + ".json", self.config.max_body_bytes * 2 + 16384)
                if stored != envelope:
                    return {"status": "invalid_request", "delivery_status": "unverified",
                            "request_id": request_id, "reason": "duplicate request has different body or identity",
                            "automatic_resend": False}
            if not created:
                # Includes uncertain previous calls: read only; never resend.
                return self.read_delivery(request_id=request_id)
            acknowledgement = self._receiver({"action": "receive", "envelope": envelope})
            receipt = acknowledgement["receipt"]
            if receipt["envelope"] != envelope:
                raise ValueError("receiver acknowledgement differs from the exact outbox intent")
            result = dict(self.read_delivery(request_id=request_id, message_id=receipt["message_id"]))
            if result.get("delivery_status") == "delivered" and result.get("receiver_receipt") != receipt:
                raise ValueError("receiver acknowledgement and independent persisted readback differ")
            return result
        except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError) as exc:
            return self._unverified(str(exc), request_id=request_id)

    def read_delivery(self, *, message_id: str | None = None,
                      request_id: str | None = None) -> Mapping[str, Any]:
        """Read the real recipient store; never enqueue, send, or replace a message.

        Either identity is sufficient; when both are given they must agree.
        A missing intent, acknowledgement, or exact body/identity match remains
        unverified. The result includes original text and receiver evidence.
        """
        try:
            if request_id is not None:
                request_key(request_id)
            if message_id is not None and (not isinstance(message_id, str)
                                           or MESSAGE_ID.fullmatch(message_id) is None):
                raise ValueError("invalid message_id")
            if request_id is None and message_id is None:
                raise ValueError("read_delivery requires request_id or message_id")
            response = self._receiver({"action": "read", "message_id": message_id, "request_id": request_id})
            receipt = response["receipt"]
            received = check_receipt(receipt, self.config.recipient, self.config.max_body_bytes)
            if request_id is not None and received["request_id"] != request_id:
                raise ValueError("receiver request identity mismatch")
            if message_id is not None and receipt["message_id"] != message_id:
                raise ValueError("receiver message identity mismatch")
            key = request_key(received["request_id"])
            with storage_fd(self.config.workspace_root, self.config.storage_dir, "outbox") as fd:
                intent = read_record(fd, key + ".json", self.config.max_body_bytes * 2 + 16384)
            body = check_envelope(intent, self.config.recipient, self.config.max_body_bytes)
            if received != intent:
                raise ValueError("receiver body/hash/recipient/source/request differs from outbox intent")
            receiver_ref = self.config.storage_dir + "/receiver/" + key + ".json"
            if response.get("receiver_ref") != receiver_ref:
                raise ValueError("receiver evidence reference mismatch")
            return {"status": "completed", "delivery_status": "delivered", "verified": True,
                    "scope": "TEST", "request_id": received["request_id"],
                    "message_id": receipt["message_id"], "recipient": self.config.recipient,
                    "source_ref": received["source_ref"], "body": body.decode("utf-8"),
                    "body_sha256": received["body_sha256"], "body_bytes": len(body),
                    "ack": receipt["ack"], "receiver_pid": receipt["receiver_pid"],
                    "readback_pid": response["process_pid"], "receiver_ref": receiver_ref,
                    "outbox_ref": self.config.storage_dir + "/outbox/" + key + ".json",
                    "receiver_receipt": receipt, "automatic_resend": False}
        except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError) as exc:
            return self._unverified(str(exc), request_id=request_id, message_id=message_id)
