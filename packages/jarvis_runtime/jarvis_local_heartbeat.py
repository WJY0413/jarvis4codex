"""Local-only heartbeat scheduler behind ``JarvisControl.heartbeat``.

The scheduler owns computer time, counters, lifecycle and receipts.  It never
constructs a prompt and never imports a Codex/App Server implementation.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Callable, Mapping


ACTIVE = "ACTIVE"
TERMINAL = {"COMPLETED", "CANCELLED", "FAILED", "RETIRED"}
ALLOWED_FUNCTIONS = {
    "JarvisControl.monitor",
    "JarvisControl.resume",
    "JarvisControl.notify",
    "JarvisControl.loop_tick",
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def computer_time() -> dict[str, str]:
    local = datetime.now().astimezone()
    return {
        "local_time": local.isoformat(),
        "utc_time": local.astimezone(timezone.utc).isoformat(),
        "timezone": local.tzname() or "unknown",
        "utc_offset": local.strftime("%z"),
    }


def _parse_time(value: object, name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO timestamp") from exc
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class LocalHeartbeatConfig:
    db_path: Path
    health_path: Path
    poll_seconds: float
    min_interval_seconds: int
    max_interval_seconds: int

    @classmethod
    def load(cls, path: Path) -> "LocalHeartbeatConfig":
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
        base = path.resolve().parent

        def resolve(key: str, default: str) -> Path:
            value = Path(str(raw.get(key) or default))
            return value.resolve() if value.is_absolute() else (base / value).resolve()

        return cls(
            db_path=resolve("db_path", "jarvis-heartbeats.sqlite"),
            health_path=resolve("health_path", "jarvis-heartbeat-health.json"),
            poll_seconds=max(float(raw.get("poll_seconds", 5)), 1.0),
            min_interval_seconds=max(int(raw.get("min_interval_seconds", 30)), 1),
            max_interval_seconds=max(int(raw.get("max_interval_seconds", 2_592_000)), 1),
        )


class LocalHeartbeatStore:
    """Persistent local schedule state; legacy prompt schedules are never read."""

    def __init__(self, config: LocalHeartbeatConfig) -> None:
        self.config = config
        self.config.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.config.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    @contextmanager
    def _session(self):
        connection = self._connect()
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._session() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS jarvis_local_heartbeats (
                    heartbeat_id TEXT PRIMARY KEY, name TEXT NOT NULL,
                    function_name TEXT NOT NULL, arguments_json TEXT NOT NULL,
                    interval_seconds INTEGER NOT NULL, status TEXT NOT NULL,
                    next_run_epoch REAL, run_count INTEGER NOT NULL DEFAULT 0,
                    max_runs INTEGER NOT NULL, expires_at TEXT NOT NULL,
                    failure_count INTEGER NOT NULL DEFAULT 0, last_error TEXT,
                    source_event_key TEXT NOT NULL, confirmation_evidence TEXT NOT NULL,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS jarvis_local_heartbeat_runs (
                    run_id INTEGER PRIMARY KEY AUTOINCREMENT, heartbeat_id TEXT NOT NULL,
                    started_at TEXT NOT NULL, completed_at TEXT NOT NULL,
                    outcome TEXT NOT NULL, receipt_json TEXT, error TEXT NOT NULL DEFAULT ''
                );
            """)

    def _validate(self, request: Mapping[str, Any]) -> dict[str, Any]:
        if "prompt" in request:
            raise ValueError("prompt is not supported by local heartbeat")
        heartbeat_id = str(request.get("heartbeat_id") or "").strip()
        name = str(request.get("name") or "").strip()
        function = str(request.get("function") or "").strip()
        arguments = request.get("arguments", {})
        if not heartbeat_id or not name:
            raise ValueError("heartbeat_id and name are required")
        if function not in ALLOWED_FUNCTIONS:
            raise ValueError("function must be a supported JarvisControl function")
        if not isinstance(arguments, Mapping):
            raise ValueError("arguments must be an object")
        interval = int(request.get("interval_seconds") or 0)
        if not self.config.min_interval_seconds <= interval <= self.config.max_interval_seconds:
            raise ValueError("interval_seconds is outside configured limits")
        max_runs = int(request.get("max_runs") or 0)
        if max_runs < 1:
            raise ValueError("max_runs must be positive")
        expires = _parse_time(request.get("expires_at"), "expires_at")
        now = _now()
        if expires <= now:
            raise ValueError("expires_at must be in the future")
        start = _parse_time(request["start_at"], "start_at") if request.get("start_at") else (
            now if request.get("start_immediately") else datetime.fromtimestamp(now.timestamp() + interval, timezone.utc)
        )
        source = str(request.get("source_event_key") or "").strip()
        confirmation = str(request.get("confirmation_evidence") or "").strip()
        if not source or not confirmation:
            raise ValueError("source_event_key and confirmation_evidence are required")
        return {
            "heartbeat_id": heartbeat_id, "name": name[:160], "function_name": function,
            "arguments_json": json.dumps(dict(arguments), ensure_ascii=False, sort_keys=True),
            "interval_seconds": interval, "next_run_epoch": start.timestamp(), "max_runs": max_runs,
            "expires_at": expires.isoformat(), "source_event_key": source,
            "confirmation_evidence": confirmation[:500],
        }

    def create(self, request: Mapping[str, Any]) -> dict[str, Any]:
        value = self._validate(request)
        now = _now().isoformat()
        with self._session() as db:
            existing = db.execute("SELECT * FROM jarvis_local_heartbeats WHERE heartbeat_id=?", (value["heartbeat_id"],)).fetchone()
            if existing is not None:
                return {"created": False, "heartbeat": dict(existing)}
            db.execute("""INSERT INTO jarvis_local_heartbeats(
                heartbeat_id,name,function_name,arguments_json,interval_seconds,status,next_run_epoch,max_runs,expires_at,source_event_key,confirmation_evidence,created_at,updated_at
            ) VALUES (?,?,?,?,?,'ACTIVE',?,?,?,?,?,?,?)""", (
                value["heartbeat_id"], value["name"], value["function_name"], value["arguments_json"], value["interval_seconds"],
                value["next_run_epoch"], value["max_runs"], value["expires_at"], value["source_event_key"], value["confirmation_evidence"], now, now,
            ))
        return {"created": True, "heartbeat": self.get(value["heartbeat_id"])}

    def get(self, heartbeat_id: str) -> dict[str, Any]:
        with self._session() as db:
            row = db.execute("SELECT * FROM jarvis_local_heartbeats WHERE heartbeat_id=?", (heartbeat_id,)).fetchone()
        if row is None:
            raise ValueError("heartbeat not found")
        return dict(row)

    def list(self) -> list[dict[str, Any]]:
        with self._session() as db:
            return [dict(row) for row in db.execute("SELECT * FROM jarvis_local_heartbeats ORDER BY heartbeat_id")]

    def cancel(self, heartbeat_id: str) -> dict[str, Any]:
        with self._session() as db:
            changed = db.execute("UPDATE jarvis_local_heartbeats SET status='CANCELLED',next_run_epoch=NULL,updated_at=? WHERE heartbeat_id=? AND status='ACTIVE'", (_now().isoformat(), heartbeat_id)).rowcount
        if changed != 1:
            raise ValueError("active heartbeat not found")
        return self.get(heartbeat_id)

    def claim_due(self) -> list[dict[str, Any]]:
        now = _now()
        with self._session() as db:
            rows = db.execute("SELECT * FROM jarvis_local_heartbeats WHERE status='ACTIVE' AND next_run_epoch<=? AND run_count<max_runs", (now.timestamp(),)).fetchall()
            claimed = [dict(row) for row in rows if _parse_time(row["expires_at"], "expires_at") > now]
            for item in claimed:
                db.execute("UPDATE jarvis_local_heartbeats SET next_run_epoch=?,updated_at=? WHERE heartbeat_id=?", (now.timestamp() + int(item["interval_seconds"]), now.isoformat(), item["heartbeat_id"]))
        return claimed

    def record(self, heartbeat: Mapping[str, Any], result: Mapping[str, Any]) -> dict[str, Any]:
        success = str(result.get("status") or "") in {"completed", "holding", "running", "active", "accepted"}
        now = _now().isoformat()
        with self._session() as db:
            current = self.get(str(heartbeat["heartbeat_id"]))
            run_count = int(current["run_count"]) + (1 if success else 0)
            failures = 0 if success else int(current["failure_count"]) + 1
            status = "COMPLETED" if success and run_count >= int(current["max_runs"]) else ("FAILED" if failures >= 3 else current["status"])
            next_epoch = None if status in TERMINAL else current["next_run_epoch"]
            outcome = "function_completed" if success else "function_failed"
            db.execute("INSERT INTO jarvis_local_heartbeat_runs(heartbeat_id,started_at,completed_at,outcome,receipt_json,error) VALUES (?,?,?,?,?,?)", (heartbeat["heartbeat_id"], now, now, outcome, json.dumps(dict(result), ensure_ascii=False), str(result.get("error") or "")))
            db.execute("UPDATE jarvis_local_heartbeats SET run_count=?,failure_count=?,status=?,next_run_epoch=?,last_error=?,updated_at=? WHERE heartbeat_id=?", (run_count, failures, status, next_epoch, result.get("error"), now, heartbeat["heartbeat_id"]))
        return {"heartbeat_id": heartbeat["heartbeat_id"], "outcome": outcome, "receipt": dict(result)}

    def summary(self) -> dict[str, int]:
        with self._session() as db:
            active = db.execute("SELECT COUNT(*) FROM jarvis_local_heartbeats WHERE status='ACTIVE'").fetchone()[0]
        return {"active_count": int(active), "total_count": len(self.list())}


FunctionRunner = Callable[[str, Mapping[str, Any], Mapping[str, Any]], Mapping[str, Any]]


class HeartbeatService:
    def __init__(self, config: LocalHeartbeatConfig, *, store: LocalHeartbeatStore | None = None, function_runner: FunctionRunner | None = None) -> None:
        self.config = config
        self.store = store or LocalHeartbeatStore(config)
        self.function_runner = function_runner or self._unconfigured_runner

    @staticmethod
    def _unconfigured_runner(_function: str, _arguments: Mapping[str, Any], _context: Mapping[str, Any]) -> Mapping[str, Any]:
        return {"status": "failed", "error": "no JarvisControl function runner is configured"}

    def health(self) -> dict[str, Any]:
        return {"status": "running", **computer_time(), **self.store.summary()}

    def run_once(self) -> dict[str, Any]:
        results = []
        for heartbeat in self.store.claim_due():
            receipt = self.function_runner(heartbeat["function_name"], json.loads(heartbeat["arguments_json"]), {
                "heartbeat_id": heartbeat["heartbeat_id"],
                "run_number": int(heartbeat["run_count"]) + 1,
                **computer_time(),
            })
            results.append(self.store.record(heartbeat, receipt))
        health = self.health()
        self.config.health_path.parent.mkdir(parents=True, exist_ok=True)
        self.config.health_path.write_text(json.dumps(health, ensure_ascii=False, indent=2), encoding="utf-8")
        return {"results": results, "health": health}

    def run_forever(self) -> int:
        try:
            while True:
                self.run_once()
                time.sleep(self.config.poll_seconds)
        except KeyboardInterrupt:
            return 0


class JarvisControlHeartbeat:
    """Concrete scheduler port used by ``JarvisControl.heartbeat``."""

    def __init__(self, config_path: Path) -> None:
        self.store = LocalHeartbeatStore(LocalHeartbeatConfig.load(config_path))

    def invoke_heartbeat(self, _request_id: str, capability: str, _source_ref: str, arguments: Mapping[str, Any]) -> dict[str, Any]:
        if capability == "heartbeat.health":
            return {"status": "completed", **computer_time(), **self.store.summary()}
        if capability == "heartbeat.create":
            created = self.store.create(arguments)
            return {"status": "active", "heartbeat_id": arguments["heartbeat_id"], **created}
        if capability == "heartbeat.update":
            raise ValueError("local heartbeat update is not implemented; cancel then create a new bounded schedule")
        if capability == "heartbeat.cancel":
            heartbeat = self.store.cancel(str(arguments.get("heartbeat_id") or ""))
            return {"status": "cancelled", "heartbeat_id": heartbeat["heartbeat_id"], "heartbeat": heartbeat}
        raise ValueError("unsupported heartbeat capability")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("command", choices=("run-once", "run-forever", "health-check"))
    args = parser.parse_args()
    service = HeartbeatService(LocalHeartbeatConfig.load(args.config))
    if args.command == "run-forever":
        return service.run_forever()
    payload = service.health() if args.command == "health-check" else service.run_once()
    print(json.dumps(payload, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
