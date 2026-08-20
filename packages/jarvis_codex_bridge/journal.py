"""Small append-only receipt journal used for continuation idempotency."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Protocol

from .contracts import BridgeReceipt, utc_now


class ReceiptJournal(Protocol):
    def find_terminal(self, request_id: str) -> BridgeReceipt | None: ...

    def append(self, receipt: BridgeReceipt) -> None: ...


class JsonlReceiptJournal:
    """A portable journal; the caller owns lifecycle and retention."""

    _terminal = {"completed", "failed", "requires_readback"}

    def __init__(self, path: Path):
        self.path = path

    def find_terminal(self, request_id: str) -> BridgeReceipt | None:
        if not self.path.exists():
            return None
        found: BridgeReceipt | None = None
        for raw in self.path.read_text(encoding="utf-8-sig").splitlines():
            if not raw.strip():
                continue
            value = json.loads(raw)
            if value.get("request_id") != request_id:
                continue
            if value.get("status") not in self._terminal:
                continue
            observed = value.get("observed_at")
            found = BridgeReceipt(
                request_id=str(value["request_id"]),
                thread_id=str(value["thread_id"]),
                status=value["status"],
                observed_at=(
                    __import__("datetime").datetime.fromisoformat(observed)
                    if observed
                    else utc_now()
                ),
                source_ref=str(value.get("source_ref") or ""),
                turn_id=value.get("turn_id"),
                output=value.get("output"),
                reason=value.get("reason"),
            )
        return found

    def append(self, receipt: BridgeReceipt) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(receipt.as_dict(), ensure_ascii=False, sort_keys=True))
            handle.write("\n")
