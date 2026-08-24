from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from adapters.desktop_host.project_catalog_adapter import DesktopHostProjectCatalogAdapter
from jarvis_runtime.project_catalog import ProjectCatalog, ProjectCatalogError


def project(project_id: str, label: str, path: str, *, aliases=()):
    return {
        "projectKind": "local",
        "projectId": project_id,
        "label": label,
        "path": path,
        "hostId": "local",
        "isGitRepository": True,
        "aliases": list(aliases),
    }


class ProjectCatalogTests(unittest.TestCase):
    def test_refresh_persists_host_snapshot_timestamp_and_resolves_exact_name(self):
        with tempfile.TemporaryDirectory() as temp:
            catalog = ProjectCatalog(Path(temp) / "host-project-catalog.json")
            receipt = catalog.refresh(
                "local",
                [project("project-chief", "Chief of Staff", r"C:\\Chief", aliases=("chief",))],
            )
            self.assertTrue(receipt.updated)
            self.assertTrue(receipt.observed_at)
            resolved = catalog.resolve("local", " CHIEF ")
            self.assertEqual(resolved.project.project_id, "project-chief")
            self.assertEqual(resolved.project.path, r"C:\\Chief")
            self.assertEqual(resolved.observed_at, receipt.observed_at)

    def test_empty_or_failed_refresh_keeps_last_good_snapshot_without_error(self):
        with tempfile.TemporaryDirectory() as temp:
            catalog = ProjectCatalog(Path(temp) / "host-project-catalog.json")
            first = catalog.refresh("local", [project("project-chief", "Chief of Staff", r"C:\\Chief")])
            empty = catalog.refresh("local", [])
            failed = catalog.refresh_silently("local", lambda: (_ for _ in ()).throw(RuntimeError("offline")))
            self.assertFalse(empty.updated)
            self.assertFalse(failed.updated)
            self.assertEqual(empty.observed_at, first.observed_at)
            self.assertEqual(failed.observed_at, first.observed_at)
            self.assertEqual(catalog.resolve("local", "Chief of Staff").project.project_id, "project-chief")

    def test_unknown_or_ambiguous_name_is_rejected_not_defaulted_to_null(self):
        with tempfile.TemporaryDirectory() as temp:
            catalog = ProjectCatalog(Path(temp) / "host-project-catalog.json")
            catalog.refresh(
                "local",
                [
                    project("project-1", "Chief A", r"C:\\A", aliases=("chief",)),
                    project("project-2", "Chief B", r"C:\\B", aliases=("chief",)),
                ],
            )
            with self.assertRaisesRegex(ProjectCatalogError, "ambiguous"):
                catalog.resolve("local", "chief")
            with self.assertRaisesRegex(ProjectCatalogError, "not in"):
                catalog.resolve("local", "missing")

    def test_frontend_adapter_refreshes_on_start_and_keeps_snapshot_when_host_unavailable(self):
        with tempfile.TemporaryDirectory() as temp:
            catalog = ProjectCatalog(Path(temp) / "host-project-catalog.json")
            adapter = DesktopHostProjectCatalogAdapter(
                catalog,
                host_id="local",
                list_projects=lambda: {"projects": [project("project-chief", "Chief of Staff", r"C:\\Chief")]},
            )
            first = adapter.refresh_on_start()
            self.assertTrue(first.updated)
            unavailable = DesktopHostProjectCatalogAdapter(
                catalog,
                host_id="local",
                list_projects=lambda: (_ for _ in ()).throw(OSError("host unavailable")),
            ).refresh_on_start()
            self.assertFalse(unavailable.updated)
            self.assertEqual(unavailable.observed_at, first.observed_at)


if __name__ == "__main__":
    unittest.main()
