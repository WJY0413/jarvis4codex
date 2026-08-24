"""Desktop Host adapter for refreshing Jarvis project catalogs."""

from __future__ import annotations

from typing import Any, Callable, Iterable

from jarvis_runtime.project_catalog import ProjectCatalog, RefreshReceipt


class DesktopHostProjectCatalogAdapter:
    """Runs inside a Desktop-capable frontend and never calls a model."""

    name = "desktop-host-project-catalog"

    def __init__(
        self,
        catalog: ProjectCatalog,
        *,
        host_id: str,
        list_projects: Callable[[], Iterable[dict[str, Any]] | dict[str, Any]],
    ):
        self.catalog = catalog
        self.host_id = host_id
        self._list_projects = list_projects

    def refresh_now(self) -> RefreshReceipt:
        return self.catalog.refresh_silently(self.host_id, self._list_projects)

    def refresh_on_start(self) -> RefreshReceipt:
        """Lightweight startup refresh; unavailable Host means unchanged catalog."""
        return self.refresh_now()
