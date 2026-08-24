"""Durable, host-scoped Codex Desktop project catalog for Jarvis.

The catalog deliberately stores the Desktop Host snapshot rather than trying to
discover projects from a remote App Server.  A Host integration supplies
``list_projects``; failed or empty refreshes leave the last good snapshot
untouched so the UI can simply keep showing its last observed timestamp.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import re
from typing import Any, Callable, Iterable

from jarvis_runtime.coo_dispatcher_store import ProcessLock, atomic_write_json, read_json, utc_now


CATALOG_VERSION = 1
_WHITESPACE = re.compile(r"\s+")


class ProjectCatalogError(RuntimeError):
    """Raised only for a caller that needs a deterministic resolution result."""


def normalize_project_name(value: Any) -> str:
    return _WHITESPACE.sub(" ", str(value or "").strip()).casefold()


@dataclass(frozen=True)
class ProjectRecord:
    project_id: str
    label: str
    path: str
    host_id: str
    is_git_repository: bool
    aliases: tuple[str, ...] = ()

    @classmethod
    def from_host_project(cls, value: dict[str, Any], *, host_id: str) -> "ProjectRecord | None":
        if str(value.get("projectKind") or "local") != "local":
            return None
        project_id = str(value.get("projectId") or "").strip()
        label = str(value.get("label") or "").strip()
        path = str(value.get("path") or "").strip()
        item_host_id = str(value.get("hostId") or host_id).strip()
        if not project_id or not label or not path or item_host_id != host_id:
            return None
        aliases = value.get("aliases") or ()
        if not isinstance(aliases, (list, tuple)):
            aliases = ()
        normalized_aliases = tuple(
            text for text in (str(item).strip() for item in aliases) if text
        )
        return cls(
            project_id=project_id,
            label=label,
            path=path,
            host_id=host_id,
            is_git_repository=bool(value.get("isGitRepository", False)),
            aliases=normalized_aliases,
        )

    def matches(self, name: str) -> bool:
        wanted = normalize_project_name(name)
        return wanted in {normalize_project_name(self.label), *(normalize_project_name(item) for item in self.aliases)}


@dataclass(frozen=True)
class ProjectResolution:
    project: ProjectRecord
    observed_at: str


@dataclass(frozen=True)
class RefreshReceipt:
    updated: bool
    host_id: str
    observed_at: str | None
    project_count: int

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class ProjectCatalog:
    """One JSON snapshot containing independent project catalogs per Desktop Host."""

    def __init__(self, path: Path):
        self.path = path.resolve()
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"version": CATALOG_VERSION, "hosts": {}}
        payload = read_json(self.path)
        if int(payload.get("version", 0)) != CATALOG_VERSION:
            raise ProjectCatalogError("unsupported project catalog version")
        if not isinstance(payload.get("hosts"), dict):
            raise ProjectCatalogError("project catalog hosts must be an object")
        return payload

    @staticmethod
    def _records(projects: Iterable[dict[str, Any]], *, host_id: str) -> list[ProjectRecord]:
        records: list[ProjectRecord] = []
        seen_ids: set[str] = set()
        for value in projects:
            if not isinstance(value, dict):
                continue
            record = ProjectRecord.from_host_project(value, host_id=host_id)
            if record is None or record.project_id in seen_ids:
                continue
            seen_ids.add(record.project_id)
            records.append(record)
        return records

    def refresh(self, host_id: str, projects: Iterable[dict[str, Any]]) -> RefreshReceipt:
        """Persist a non-empty Host snapshot; never replace a good one with no data."""
        target_host = str(host_id or "").strip()
        if not target_host:
            return RefreshReceipt(False, "", None, 0)
        records = self._records(projects, host_id=target_host)
        if not records:
            current = self.host_snapshot(target_host)
            return RefreshReceipt(
                False,
                target_host,
                str(current.get("observed_at") or "") or None,
                len(current.get("projects") or []),
            )
        observed_at = utc_now()
        with ProcessLock(self.lock_path):
            payload = self._load()
            hosts = dict(payload["hosts"])
            hosts[target_host] = {
                "host_id": target_host,
                "observed_at": observed_at,
                "projects": [asdict(record) for record in records],
            }
            atomic_write_json(self.path, {"version": CATALOG_VERSION, "hosts": hosts})
        return RefreshReceipt(True, target_host, observed_at, len(records))

    def refresh_silently(
        self,
        host_id: str,
        provider: Callable[[], Iterable[dict[str, Any]] | dict[str, Any]],
    ) -> RefreshReceipt:
        """Best-effort startup/manual refresh that intentionally exposes no provider error."""
        try:
            raw = provider()
            projects = raw.get("projects", []) if isinstance(raw, dict) else raw
            return self.refresh(host_id, projects)
        except Exception:
            current = self.host_snapshot(host_id)
            return RefreshReceipt(
                False,
                str(host_id or "").strip(),
                str(current.get("observed_at") or "") or None,
                len(current.get("projects") or []),
            )

    def host_snapshot(self, host_id: str) -> dict[str, Any]:
        try:
            snapshot = self._load()["hosts"].get(str(host_id or "").strip())
        # A broken/missing local snapshot is deliberately not a frontend error.
        # The next successful Host refresh will replace it with a valid snapshot.
        except Exception:
            return {}
        return dict(snapshot) if isinstance(snapshot, dict) else {}

    def resolve(self, host_id: str, project_name: str) -> ProjectResolution:
        requested = str(project_name or "").strip()
        if not requested:
            raise ProjectCatalogError("project is required")
        snapshot = self.host_snapshot(host_id)
        observed_at = str(snapshot.get("observed_at") or "").strip()
        if not observed_at:
            raise ProjectCatalogError("project catalog is unavailable for this host")
        matches: list[ProjectRecord] = []
        for value in snapshot.get("projects") or []:
            if not isinstance(value, dict):
                continue
            try:
                record = ProjectRecord(
                    project_id=str(value["project_id"]),
                    label=str(value["label"]),
                    path=str(value["path"]),
                    host_id=str(value["host_id"]),
                    is_git_repository=bool(value.get("is_git_repository", False)),
                    aliases=tuple(str(item) for item in value.get("aliases") or ()),
                )
            except (KeyError, TypeError):
                continue
            if record.matches(requested):
                matches.append(record)
        if not matches:
            raise ProjectCatalogError(f"project is not in the current host catalog: {requested}")
        if len(matches) != 1:
            raise ProjectCatalogError(f"project name is ambiguous in the current host catalog: {requested}")
        return ProjectResolution(matches[0], observed_at)
