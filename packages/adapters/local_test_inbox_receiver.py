"""One-shot, pipe-only TEST inbox receiver. No network or background service.

The sender may create an outbox intent, but only this separate process writes
the recipient's inbox. Acknowledgements follow durable receipt publication.
This module is product code, not an acceptance-test runner.
"""

from __future__ import annotations

import argparse
import base64
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
import uuid
from typing import Any, Iterator, Mapping


PROTOCOL = "jarvis.local-test-inbox.v1"
MAX_RECORDS = 4096
MESSAGE_ID = re.compile(r"local-test-[0-9a-f]{32}\Z")
RECIPIENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{7,127}\Z")
_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_FILE_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK


def relative_parts(value: str) -> tuple[str, ...]:
    if not isinstance(value, str) or not value or "\\" in value or ":" in value:
        raise ValueError("storage paths must be nonempty workspace-relative POSIX paths")
    parts = value.split("/")
    if PurePosixPath(value).is_absolute() or any(part in {"", ".", ".."} for part in parts):
        raise ValueError("absolute, empty, and traversal path components are forbidden")
    return tuple(parts)


def text_field(value: Any, name: str, maximum: int = 1024) -> str:
    if (not isinstance(value, str) or not value or len(value) > maximum
            or len(value.encode("utf-8")) > maximum):
        raise ValueError(f"{name} must be a nonempty bounded UTF-8 string")
    return value


def validate_settings(workspace_root: Path, storage_dir: str, recipient: str, scope: str,
                      max_body_bytes: int) -> None:
    if scope != "TEST":
        raise ValueError("local inbox is restricted to explicit TEST scope")
    if not workspace_root.is_absolute() or ".." in workspace_root.parts or workspace_root == Path("/"):
        raise ValueError("workspace_root must be an existing absolute workspace anchor")
    relative_parts(storage_dir)
    if not isinstance(recipient, str) or RECIPIENT.fullmatch(recipient) is None:
        raise ValueError("recipient must be a fixed opaque TEST identifier, not an address or path")
    if type(max_body_bytes) is not int or not 1 <= max_body_bytes <= 1048576:
        raise ValueError("max_body_bytes must be between 1 and 1048576")


def _private(fd: int) -> None:
    info = os.fstat(fd)
    if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077:
        raise ValueError("TEST storage must already be private to the current workspace user")


@contextmanager
def workspace_fd(workspace_root: Path) -> Iterator[int]:
    """Open each component without following symlinks, including the anchor."""
    if not workspace_root.is_absolute() or ".." in workspace_root.parts:
        raise ValueError("invalid workspace anchor")
    fd = os.open("/", _DIR_FLAGS)
    try:
        for part in workspace_root.parts[1:]:
            next_fd = os.open(part, _DIR_FLAGS, dir_fd=fd)
            os.close(fd)
            fd = next_fd
        yield fd
    finally:
        os.close(fd)


@contextmanager
def storage_fd(workspace_root: Path, storage_dir: str, child: str | None = None,
               *, create: bool = False) -> Iterator[int]:
    with workspace_fd(workspace_root) as root_fd:
        fd = os.dup(root_fd)
        try:
            for part in relative_parts(storage_dir):
                next_fd = os.open(part, _DIR_FLAGS, dir_fd=fd)
                os.close(fd)
                fd = next_fd
            _private(fd)
            if child is not None:
                if child not in {"outbox", "receiver"}:
                    raise ValueError("unknown storage role")
                if create:
                    try:
                        os.mkdir(child, mode=0o700, dir_fd=fd)
                        os.fsync(fd)
                    except FileExistsError:
                        pass
                next_fd = os.open(child, _DIR_FLAGS, dir_fd=fd)
                os.close(fd)
                fd = next_fd
                _private(fd)
            yield fd
        finally:
            os.close(fd)


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def decode_json(raw: bytes) -> dict[str, Any]:
    value = json.loads(raw.decode("utf-8"), object_pairs_hook=_no_duplicate_keys)
    if not isinstance(value, dict):
        raise ValueError("protocol record must be an object")
    return value


def encode_json(value: Mapping[str, Any]) -> bytes:
    return json.dumps(dict(value), ensure_ascii=False, separators=(",", ":"),
                      sort_keys=True, allow_nan=False).encode("utf-8")


def read_record(fd: int, name: str, limit: int) -> dict[str, Any]:
    if "/" in name or "\\" in name or name in {"", ".", ".."}:
        raise ValueError("invalid record name")
    handle = os.open(name, _FILE_FLAGS, dir_fd=fd)
    try:
        info = os.fstat(handle)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValueError("record must be a regular non-linked file")
        _private(handle)
        if info.st_size > limit:
            raise ValueError("record exceeds bounded protocol size")
        chunks: list[bytes] = []
        remaining = limit + 1
        while remaining:
            chunk = os.read(handle, min(remaining, 65536))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        if len(raw) > limit:
            raise ValueError("record exceeds bounded protocol size")
        return decode_json(raw)
    finally:
        os.close(handle)


def publish_record(fd: int, name: str, value: Mapping[str, Any]) -> bool:
    """Publish an immutable, fsynced record; never overwrite an existing name."""
    if "/" in name or "\\" in name or name in {"", ".", ".."}:
        raise ValueError("invalid record name")
    if len(os.listdir(fd)) >= MAX_RECORDS:
        raise ValueError("bounded TEST inbox capacity reached")
    temporary = ".pending-" + uuid.uuid4().hex
    handle = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                     os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=fd)
    try:
        try:
            raw = encode_json(value)
            offset = 0
            while offset < len(raw):
                written = os.write(handle, raw[offset:])
                if written <= 0:
                    raise OSError("incomplete record write")
                offset += written
            os.fsync(handle)
        finally:
            os.close(handle)
        try:
            os.link(temporary, name, src_dir_fd=fd, dst_dir_fd=fd, follow_symlinks=False)
            created = True
        except FileExistsError:
            created = False
    finally:
        os.unlink(temporary, dir_fd=fd)
        os.fsync(fd)
    return created


def request_key(request_id: str) -> str:
    return hashlib.sha256(text_field(request_id, "request_id", 256).encode("utf-8")).hexdigest()


def check_envelope(value: Mapping[str, Any], recipient: str, max_body_bytes: int) -> bytes:
    required = {"protocol", "scope", "request_id", "source_ref", "recipient",
                "body_b64", "body_sha256", "body_bytes"}
    if set(value) != required or value["protocol"] != PROTOCOL or value["scope"] != "TEST":
        raise ValueError("invalid TEST notification envelope")
    request_key(value["request_id"])
    text_field(value["source_ref"], "source_ref")
    if value["recipient"] != recipient:
        raise ValueError("notification recipient does not match the fixed TEST recipient")
    encoded = value["body_b64"]
    if not isinstance(encoded, str) or len(encoded) > 4 * ((max_body_bytes + 2) // 3):
        raise ValueError("body exceeds bounded protocol size")
    body = base64.b64decode(encoded, validate=True)
    body.decode("utf-8", errors="strict")
    if not body or len(body) > max_body_bytes:
        raise ValueError("message must contain between 1 and max_body_bytes UTF-8 bytes")
    if (type(value["body_bytes"]) is not int or value["body_bytes"] != len(body)
            or value["body_sha256"] != hashlib.sha256(body).hexdigest()
            or base64.b64encode(body).decode("ascii") != encoded):
        raise ValueError("notification body bytes or hash mismatch")
    return body


def check_receipt(value: Mapping[str, Any], recipient: str, max_body_bytes: int) -> dict[str, Any]:
    if (value.get("protocol") != PROTOCOL or value.get("scope") != "TEST"
            or value.get("ack") != "durably_received"
            or value.get("delivery_status") != "delivered"
            or not isinstance(value.get("message_id"), str)
            or MESSAGE_ID.fullmatch(value["message_id"]) is None
            or type(value.get("receiver_pid")) is not int
            or value["receiver_pid"] <= 0):
        raise ValueError("invalid durable receiver acknowledgement")
    envelope = value.get("envelope")
    if not isinstance(envelope, dict):
        raise ValueError("receiver acknowledgement has no envelope")
    check_envelope(envelope, recipient, max_body_bytes)
    return dict(envelope)


def receive_or_read(args: argparse.Namespace, request: Mapping[str, Any]) -> dict[str, Any]:
    validate_settings(args.workspace_root, args.storage_dir, args.recipient, args.scope,
                      args.max_body_bytes)
    limit = args.max_body_bytes * 2 + 16384
    action = request.get("action")
    if action not in {"receive", "read"}:
        raise ValueError("receiver supports only receive or read")
    with storage_fd(args.workspace_root, args.storage_dir, "receiver", create=action == "receive") as fd:
        if action == "receive":
            envelope = request.get("envelope")
            if not isinstance(envelope, dict):
                raise ValueError("notification envelope missing")
            check_envelope(envelope, args.recipient, args.max_body_bytes)
            key = request_key(envelope["request_id"])
            record = {
                "protocol": PROTOCOL, "scope": "TEST", "ack": "durably_received",
                "delivery_status": "delivered", "message_id": "local-test-" + uuid.uuid4().hex,
                "receiver_pid": os.getpid(), "sender_pid": os.getppid(),
                "received_at": datetime.now(timezone.utc).isoformat(), "envelope": envelope,
            }
            if not publish_record(fd, key + ".json", record):
                record = read_record(fd, key + ".json", limit)
                if check_receipt(record, args.recipient, args.max_body_bytes) != envelope:
                    raise ValueError("duplicate request has a different notification envelope")
        else:
            request_id = request.get("request_id")
            message_id = request.get("message_id")
            if request_id is not None:
                key = request_key(request_id)
                record = read_record(fd, key + ".json", limit)
            elif isinstance(message_id, str) and MESSAGE_ID.fullmatch(message_id):
                names = os.listdir(fd)
                if len(names) > MAX_RECORDS:
                    raise ValueError("bounded TEST inbox capacity exceeded")
                matches = []
                for name in names:
                    if re.fullmatch(r"[0-9a-f]{64}\.json", name):
                        candidate = read_record(fd, name, limit)
                        if candidate.get("message_id") == message_id:
                            matches.append(candidate)
                if len(matches) != 1:
                    raise ValueError("message_id must identify exactly one received message")
                record = matches[0]
            else:
                raise ValueError("read requires an exact request_id or message_id")
            envelope = check_receipt(record, args.recipient, args.max_body_bytes)
            key = request_key(envelope["request_id"])
            if request_id is not None and envelope["request_id"] != request_id:
                raise ValueError("request identity mismatch")
            if message_id is not None and record["message_id"] != message_id:
                raise ValueError("message identity mismatch")
        # Acknowledge only after durable receiver-side data and directory sync.
        handle = os.open(key + ".json", _FILE_FLAGS, dir_fd=fd)
        try:
            os.fsync(handle)
        finally:
            os.close(handle)
        os.fsync(fd)
        readback = read_record(fd, key + ".json", limit)
        if readback != record:
            raise ValueError("receiver persisted readback mismatch")
        check_receipt(readback, args.recipient, args.max_body_bytes)
        return {"ok": True, "operation": action, "process_pid": os.getpid(),
                "receipt": readback, "receiver_ref": args.storage_dir + "/receiver/" + key + ".json"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace-root", required=True, type=Path)
    parser.add_argument("--storage-dir", required=True)
    parser.add_argument("--recipient", required=True)
    parser.add_argument("--scope", required=True, choices=["TEST"])
    parser.add_argument("--max-body-bytes", required=True, type=int)
    args = parser.parse_args()
    try:
        validate_settings(args.workspace_root, args.storage_dir, args.recipient, args.scope,
                          args.max_body_bytes)
        limit = args.max_body_bytes * 2 + 16384
        raw = sys.stdin.buffer.read(limit + 1)
        if len(raw) > limit:
            raise ValueError("request exceeds bounded protocol size")
        response = receive_or_read(args, decode_json(raw))
    except (OSError, ValueError, TypeError, KeyError) as exc:
        response = {"ok": False, "reason": str(exc), "process_pid": os.getpid()}
    sys.stdout.buffer.write(encode_json(response) + b"\n")
    sys.stdout.buffer.flush()
    return 0 if response["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
