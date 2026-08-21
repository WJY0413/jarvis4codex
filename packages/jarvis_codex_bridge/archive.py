"""SQLite-backed archival and reconstruction of observed Codex threads."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .contracts import ThreadState, TurnState
from .service import utf8_text


_SCHEMA_VERSION = 1


def _json_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


class SQLiteThreadArchive:
    """Store complete thread snapshots and rebuild them without code-page conversion."""

    def __init__(self, path: Path):
        self.path = path

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path)
        try:
            connection.execute("PRAGMA foreign_keys = ON")
            self._migrate(connection)
        except BaseException:
            connection.close()
            raise
        return connection

    @staticmethod
    def _migrate(connection: sqlite3.Connection) -> None:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        if version > _SCHEMA_VERSION:
            raise RuntimeError("thread archive schema is newer than this package")
        if version == _SCHEMA_VERSION:
            return
        connection.executescript(
            """
            CREATE TABLE threads (
                thread_id TEXT PRIMARY KEY,
                status TEXT NOT NULL
            );
            CREATE TABLE turns (
                thread_id TEXT NOT NULL,
                turn_id TEXT NOT NULL,
                turn_index INTEGER NOT NULL,
                status TEXT NOT NULL,
                error TEXT,
                PRIMARY KEY (thread_id, turn_id),
                UNIQUE (thread_id, turn_index),
                FOREIGN KEY (thread_id) REFERENCES threads(thread_id) ON DELETE CASCADE
            );
            CREATE TABLE thread_items (
                thread_id TEXT NOT NULL,
                turn_id TEXT NOT NULL,
                item_index INTEGER NOT NULL,
                item_type TEXT,
                phase TEXT,
                message_text TEXT,
                raw_json TEXT NOT NULL,
                PRIMARY KEY (thread_id, turn_id, item_index),
                FOREIGN KEY (thread_id, turn_id) REFERENCES turns(thread_id, turn_id)
                    ON DELETE CASCADE
            );
            """
        )
        connection.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")

    def store(self, state: ThreadState) -> None:
        connection = self._connect()
        try:
            with connection:
                connection.execute(
                    "INSERT INTO threads(thread_id, status) VALUES (?, ?) "
                    "ON CONFLICT(thread_id) DO UPDATE SET status = excluded.status",
                    (state.thread_id, state.status),
                )
                for turn_index, turn in enumerate(state.turns):
                    connection.execute(
                        "INSERT INTO turns(thread_id, turn_id, turn_index, status, error) "
                        "VALUES (?, ?, ?, ?, ?) "
                        "ON CONFLICT(thread_id, turn_id) DO UPDATE SET "
                        "turn_index = excluded.turn_index, status = excluded.status, error = excluded.error",
                        (state.thread_id, turn.turn_id, turn_index, turn.status, turn.error),
                    )
                    connection.execute(
                        "DELETE FROM thread_items WHERE thread_id = ? AND turn_id = ?",
                        (state.thread_id, turn.turn_id),
                    )
                    for item_index, item in enumerate(turn.items):
                        normalized = _json_value(item)
                        connection.execute(
                            "INSERT INTO thread_items("
                            "thread_id, turn_id, item_index, item_type, phase, message_text, raw_json"
                            ") VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (
                                state.thread_id,
                                turn.turn_id,
                                item_index,
                                normalized.get("type"),
                                normalized.get("phase"),
                                utf8_text(normalized.get("text") or ""),
                                json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                            ),
                        )
        finally:
            connection.close()

    def rebuild_thread(self, thread_id: str) -> ThreadState | None:
        connection = self._connect()
        try:
            thread = connection.execute(
                "SELECT status FROM threads WHERE thread_id = ?", (thread_id,)
            ).fetchone()
            if thread is None:
                return None
            turns = []
            for turn_id, status, error in connection.execute(
                "SELECT turn_id, status, error FROM turns WHERE thread_id = ? "
                "ORDER BY turn_index",
                (thread_id,),
            ):
                items = tuple(
                    json.loads(raw_json)
                    for (raw_json,) in connection.execute(
                        "SELECT raw_json FROM thread_items WHERE thread_id = ? AND turn_id = ? "
                        "ORDER BY item_index",
                        (thread_id, turn_id),
                    )
                )
                turns.append(TurnState(turn_id, status, items, error))
            return ThreadState(thread_id, thread[0], tuple(turns))
        finally:
            connection.close()
