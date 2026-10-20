"""Bounded, reasoning-free receipts for the existing exact-turn native scan.

Hashes cover canonical public projections, not undisclosed native reasoning or
wire bytes. A complete receipt documents every item page, including empty pages.
It is provenance for independent QA, never execution/owner-release authority.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

SCHEMA = "jarvis-native-public-scan/v1"
MAX_RECEIPT_BYTES = 4 * 1024 * 1024
MAX_PAGES = 256
MAX_ITEMS = 10000
# Preserve completion/error metadata at the native item envelope boundary. These
# fields are required to distinguish a completed search from mere tool presence.
ENVELOPE_FIELDS = ("turnId", "startedAtMs", "completedAtMs", "status", "error", "type")
# Only protocol-declared public output/tool fields cross this boundary. Unknown
# item types, user inputs, plans, commentary and reasoning are metadata-only.
PUBLIC_FIELDS = {
    "agentMessage": ("id", "type", "phase", "text"),
    "webSearch": ("id", "type", "query", "action", "results"),
    "functionCallOutput": ("id", "type", "name", "namespace", "output"),
    "commandExecution": ("id", "type", "command", "cwd", "commandActions", "status", "aggregatedOutput", "exitCode", "durationMs"),
    "fileChange": ("id", "type", "changes", "status"),
    "mcpToolCall": ("id", "type", "server", "tool", "arguments", "status", "result", "error", "durationMs"),
    "dynamicToolCall": ("id", "type", "namespace", "tool", "arguments", "status", "success", "contentItems", "durationMs"),
}


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _require(value, message):
    if not value:
        raise ValueError("native scan receipt: " + message)


def _public_item(item):
    kind = item["type"]
    fields = PUBLIC_FIELDS.get(kind)
    if fields is None or kind == "agentMessage" and item.get("phase") != "final_answer":
        return None
    return {key: item[key] for key in (*fields, "status", "error") if key in item}


def _page(index, cursor, next_cursor, entries):
    body = {"index": index, "cursor": cursor, "next_cursor": next_cursor, "entries": entries}
    return {**body, "sha256": sha256(body)}


class PublicScanReceiptBuilder:
    """Record one native scan without keeping private or unbounded payloads.

    A size/shape failure only disables this additional receipt. Existing native
    read semantics remain unchanged, while checkpoint consumers must fail closed.
    """
    def __init__(self, thread_id, turn_id, *, model_provider, protocol_version):
        self.value = {"schema": SCHEMA, "thread_id": thread_id, "turn_id": turn_id,
            "model_provider": model_provider, "protocol_version": protocol_version,
            "hash_scope": "canonical_public_projection", "complete": False,
            "turn_lookup": {"target_found": False, "history_exhausted": False, "pages": []},
            "items": {"scan_complete": False, "exhausted": False, "sort_direction": "desc", "pages": []},
            "item_count": 0, "web_search_count": 0, "omitted_count": 0, "final_envelope_sha256": None}
        self.failure = None
        self.bytes = len(canonical_json(self.value).encode())
        self.item_ids = set()

    def _append(self, section, page):
        encoded = canonical_json(page).encode()
        if len(self.value[section]["pages"]) >= MAX_PAGES or self.bytes + len(encoded) > MAX_RECEIPT_BYTES - 65536:
            self._fail("public scan receipt size/page bound exceeded")
            return
        self.value[section]["pages"].append(page)
        self.bytes += len(encoded)

    def _fail(self, reason):
        self.failure = reason
        # Never return a partial raw evidence set as if it were independently
        # complete. The selected final/count projection remains separate.
        self.value["turn_lookup"]["pages"] = []
        self.value["items"]["pages"] = []
        self.item_ids.clear()

    def add_turn_page(self, cursor, page):
        if self.failure:
            return
        try:
            entries = [{"id": row["id"]} for row in page["data"]]
            self._append("turn_lookup", _page(len(self.value["turn_lookup"]["pages"]), cursor, page.get("nextCursor"), entries))
            self.value["turn_lookup"].update(target_found=any(x["id"] == self.value["turn_id"] for x in entries),
                history_exhausted=page.get("nextCursor") is None)
        except (KeyError, TypeError, ValueError):
            self._fail("invalid public turn-page projection")

    def add_item_page(self, cursor, page):
        if self.failure:
            return
        try:
            entries = []
            for row in page["data"]:
                item = row["item"]
                _require(row["turnId"] == self.value["turn_id"], "item turn identity mismatch")
                _require(item["id"] not in self.item_ids, "duplicate item identity")
                self.item_ids.add(item["id"])
                _require(len(self.item_ids) <= MAX_ITEMS, "item bound exceeded")
                projected = _public_item(item)
                entry = {"id": item["id"], "type": item["type"]}
                if projected is None:
                    entry["omitted"] = True
                    self.value["omitted_count"] += 1
                else:
                    envelope = {key: row[key] for key in ENVELOPE_FIELDS if key in row}
                    envelope["item"] = projected
                    entry.update(envelope=envelope, sha256=sha256(envelope))
                    if item["type"] == "agentMessage" and self.value["final_envelope_sha256"] is None:
                        self.value["final_envelope_sha256"] = entry["sha256"]
                self.value["item_count"] += 1
                self.value["web_search_count"] += item["type"] == "webSearch"
                entries.append(entry)
            self._append("items", _page(len(self.value["items"]["pages"]), cursor, page.get("nextCursor"), entries))
            self.value["items"].update(scan_complete=page.get("nextCursor") is None, exhausted=page.get("nextCursor") is None)
        except (KeyError, TypeError, ValueError):
            self._fail("invalid, duplicate or oversized public item-page projection")

    def finish(self, selected):
        # The exact selected native error is public protocol output, preserved
        # verbatim as structured JSON, never extracted from model/page text.
        self.value["native_turn"] = {key: selected.get(key) for key in
            ("turn_id", "status", "native_error", "startedAt", "completedAt")}
        self.value["complete"] = (not self.failure and self.value["turn_lookup"]["target_found"]
                                  and self.value["items"]["exhausted"])
        if self.failure:
            return {"schema": SCHEMA, "complete": False, "thread_id": self.value["thread_id"],
                    "turn_id": self.value["turn_id"], "reason": self.failure}
        try:
            if len(canonical_json(self.value).encode()) > MAX_RECEIPT_BYTES:
                self._fail("public scan receipt size bound exceeded")
                # The ordinary exact read still preserves its native error.
                return {"schema": SCHEMA, "complete": False, "thread_id": self.value["thread_id"],
                        "turn_id": self.value["turn_id"], "reason": self.failure}
            return json.loads(canonical_json(self.value))
        except (TypeError, ValueError):
            return {"schema": SCHEMA, "complete": False, "thread_id": self.value["thread_id"],
                    "turn_id": self.value["turn_id"], "reason": "non-JSON public scan receipt"}


def validate_public_scan(receipt, *, thread_id, turn, model_provider="openai"):
    """Check exact binding, every cursor/page/envelope hash, totals and final.

    This checks the native reader's self-contained provenance, not provider
    authenticity against a remote server or the correctness of research claims.
    """
    _require(isinstance(receipt, dict), "missing receipt")
    _require(len(canonical_json(receipt).encode()) <= MAX_RECEIPT_BYTES, "receipt size bound exceeded")
    _require(set(receipt) == {"schema", "thread_id", "turn_id", "model_provider", "protocol_version", "hash_scope",
             "complete", "turn_lookup", "items", "item_count", "web_search_count", "omitted_count",
             "final_envelope_sha256", "native_turn"}, "unexpected receipt fields")
    _require(receipt.get("schema") == SCHEMA and receipt.get("complete") is True
             and receipt.get("hash_scope") == "canonical_public_projection", "incomplete/unsupported receipt")
    _require(receipt.get("thread_id") == thread_id and receipt.get("turn_id") == turn.get("turn_id")
             and receipt.get("model_provider") == model_provider, "identity/provider mismatch")
    _require(receipt.get("native_turn") == {key: turn.get(key) for key in
             ("turn_id", "status", "native_error", "startedAt", "completedAt")}, "native turn/error mismatch")
    lookup, items = receipt.get("turn_lookup"), receipt.get("items")
    _require(isinstance(lookup, dict) and set(lookup) == {"target_found", "history_exhausted", "pages"}
             and isinstance(items, dict) and set(items) == {"scan_complete", "exhausted", "sort_direction", "pages"}, "page sections missing/unknown")
    _require(lookup.get("target_found") is True and items.get("scan_complete") is True
             and items.get("exhausted") is True and items.get("sort_direction") == "desc"
             and turn.get("items_scan_complete") is True, "pagination incomplete")
    count = web = omitted = 0
    final = None
    ids = set()
    for section, value in (("turn_lookup", lookup), ("items", items)):
        pages = value.get("pages")
        _require(isinstance(pages, list) and 1 <= len(pages) <= MAX_PAGES, "invalid page list")
        cursor, cursors = None, set()
        for index, page in enumerate(pages):
            _require(isinstance(page, dict) and set(page) == {"index", "cursor", "next_cursor", "entries", "sha256"}, "invalid page fields")
            _require(type(page["index"]) is int and page["index"] == index and page["cursor"] == cursor, "broken cursor chain")
            _require(page["sha256"] == sha256({k: v for k, v in page.items() if k != "sha256"}), "page hash mismatch")
            entries = page["entries"]
            _require(isinstance(entries, list), "invalid page entries")
            if section == "turn_lookup":
                _require(all(isinstance(x, dict) and set(x) == {"id"} and isinstance(x["id"], str) and x["id"] for x in entries), "invalid turn lookup projection")
                found = sum(x["id"] == turn["turn_id"] for x in entries)
                _require(found == (1 if index == len(pages) - 1 else 0), "turn lookup target mismatch")
            else:
                for entry in entries:
                    _require(isinstance(entry, dict) and isinstance(entry.get("id"), str) and entry["id"]
                        and isinstance(entry.get("type"), str) and entry["type"] and entry["id"] not in ids, "invalid/duplicate item identity")
                    ids.add(entry["id"]); count += 1
                    _require(count <= MAX_ITEMS, "item bound exceeded")
                    web += entry["type"] == "webSearch"
                    if entry.get("omitted") is True:
                        _require(set(entry) == {"id", "type", "omitted"}
                                 and entry["type"] not in set(PUBLIC_FIELDS) - {"agentMessage"}, "public tool envelope omitted")
                        omitted += 1
                        continue
                    _require(set(entry) == {"id", "type", "envelope", "sha256"}, "invalid public entry fields")
                    envelope = entry["envelope"]
                    _require(isinstance(envelope, dict) and {"turnId", "item"} <= set(envelope)
                             and set(envelope) <= set(ENVELOPE_FIELDS) | {"item"}
                             and envelope["turnId"] == turn["turn_id"], "envelope turn/field mismatch")
                    item = envelope["item"]
                    _require(isinstance(item, dict) and item.get("id") == entry["id"] and item.get("type") == entry["type"]
                             and _public_item(item) == item, "non-public envelope fields")
                    _require(entry["sha256"] == sha256(envelope), "envelope hash mismatch")
                    if entry["type"] == "agentMessage" and final is None:
                        _require(isinstance(item.get("text"), str), "invalid final text")
                        final = entry
            cursor = page["next_cursor"]
            if cursor is not None:
                _require(isinstance(cursor, str) and cursor and cursor not in cursors, "invalid/repeated cursor")
                cursors.add(cursor)
            _require(index == len(pages) - 1 or cursor is not None, "page after pagination end")
        if section == "items":
            _require(cursor is None, "unconsumed item cursor")
        else:
            _require(lookup.get("history_exhausted") is (cursor is None), "lookup exhaustion mismatch")
    _require(all(type(receipt.get(k)) is int for k in ("item_count", "web_search_count", "omitted_count"))
             and (receipt["item_count"], receipt["web_search_count"], receipt["omitted_count"]) == (count, web, omitted), "count mismatch")
    _require(type(turn.get("item_count")) is int and turn["item_count"] == count
             and type(turn.get("web_search_count")) is int and turn["web_search_count"] == web, "native count mismatch")
    _require(receipt.get("final_envelope_sha256") == (final["sha256"] if final else None)
             and turn.get("items") == ([{k: v for k, v in final["envelope"]["item"].items()
                 if k in PUBLIC_FIELDS["agentMessage"]}] if final else []), "selected final envelope mismatch")
    return receipt
