"""Fsync-backed append-only dispatch receipts and non-stealable local claims."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import fields
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
from typing import Iterator, Protocol

from .contracts import BridgeReceipt, utc_now


class ClaimBusy(RuntimeError):
    """A live dispatcher or recovery reader owns this exact request."""


@contextmanager
def file_lock(path: Path, *, blocking: bool = True) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        if os.name == "nt":
            import msvcrt
            if os.fstat(fd).st_size == 0:
                os.write(fd, b"\0")
            os.lseek(fd, 0, os.SEEK_SET)
            try:
                msvcrt.locking(fd, msvcrt.LK_LOCK if blocking else msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                raise ClaimBusy("dispatch claim is held by a live owner") from exc
        else:
            import fcntl
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
            except BlockingIOError as exc:
                raise ClaimBusy("dispatch claim is held by a live owner") from exc
        yield
    finally:
        os.close(fd)


def fsync_directory(path: Path) -> None:
    if os.name != "nt":
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def process_identity(pid: int | None = None) -> dict[str, object]:
    pid = pid or os.getpid()
    identity: dict[str, object] = {"pid": pid}
    if os.name == "posix" and Path("/proc").is_dir():
        try:
            # Field 22 is starttime; comm may contain spaces and parentheses.
            stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
            identity["start_ticks"] = stat[19]
            identity["boot_id"] = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        except (OSError, IndexError):
            pass
    return identity


def owner_alive(owner: dict[str, object] | None) -> bool | None:
    """False means proven gone; None means recovery cannot safely claim death."""
    if not owner:
        return None
    try:
        pid = int(owner.get("pid") or 0)
    except (TypeError, ValueError):
        return None
    if pid <= 0:
        return None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return None
    if os.name == "posix" and Path("/proc").is_dir():
        try:
            state = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
            if state in {"Z", "X"}:
                return False  # The process exited and released all claims, even if not reaped.
        except FileNotFoundError:
            return False
        except (OSError, IndexError):
            return None
    identity = process_identity(pid)
    if owner.get("start_ticks") is not None and identity.get("start_ticks") is not None:
        return (identity["start_ticks"] == owner["start_ticks"]
                and identity.get("boot_id") == owner.get("boot_id"))
    return True


class ReceiptJournal(Protocol):
    def find_terminal(self, request_id: str) -> BridgeReceipt | None: ...
    def find_latest(self, request_id: str) -> BridgeReceipt | None: ...
    def append(self, receipt: BridgeReceipt) -> None: ...
    def claim(self, request_id: str): ...


class JsonlReceiptJournal:
    """Portable local receipt journal. Never removes or rewrites old evidence."""

    _terminal = {"completed", "failed", "interrupted", "requires_readback"}

    def __init__(self, path: Path):
        self.path = Path(path)

    def claim(self, request_id: str):
        digest = hashlib.sha256(request_id.encode("utf-8")).hexdigest()
        return file_lock(self.path.with_suffix(self.path.suffix + ".claims") / (digest + ".lock"), blocking=False)

    def _find(self, request_id: str, terminal_only: bool) -> BridgeReceipt | None:
        with file_lock(self.path.with_suffix(self.path.suffix + ".lock")):
            if not self.path.exists():
                return None
            found = None
            names = {field.name for field in fields(BridgeReceipt)}
            for raw in self.path.read_text(encoding="utf-8-sig").splitlines():
                if not raw.strip():
                    continue
                value = json.loads(raw)
                if value.get("request_id") != request_id:
                    continue
                if terminal_only and value.get("status") not in self._terminal:
                    continue
                selected = {name: value[name] for name in names if name in value}
                observed = selected.get("observed_at")
                selected["observed_at"] = datetime.fromisoformat(observed) if observed else utc_now()
                found = BridgeReceipt(**selected)
            return found

    def find_terminal(self, request_id: str) -> BridgeReceipt | None:
        return self._find(request_id, True)

    def find_latest(self, request_id: str) -> BridgeReceipt | None:
        return self._find(request_id, False)

    def append(self, receipt: BridgeReceipt) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with file_lock(self.path.with_suffix(self.path.suffix + ".lock")):
            # A torn tail is evidence of uncertainty, not permission to repair history.
            if self.path.exists() and self.path.stat().st_size:
                with self.path.open("rb") as reader:
                    reader.seek(-1, os.SEEK_END)
                    if reader.read(1) != b"\n":
                        raise RuntimeError("receipt journal has a torn tail; manual readback required")
            fd = os.open(self.path, os.O_CREAT | os.O_APPEND | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0), 0o600)
            try:
                data = (json.dumps(receipt.as_dict(), ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8")
                remaining = memoryview(data)
                while remaining:
                    count = os.write(fd, remaining)
                    if count <= 0:
                        raise OSError("receipt journal write did not progress")
                    remaining = remaining[count:]
                os.fsync(fd)
            finally:
                os.close(fd)
            fsync_directory(self.path.parent)
            fsync_directory(self.path.parent.parent)
