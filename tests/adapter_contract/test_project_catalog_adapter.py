from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from adapters.desktop_host.project_catalog_adapter import DesktopHostProjectCatalogAdapter
from jarvis_runtime.project_catalog import ProjectCatalog


class ProjectCatalogAdapterContractTests(unittest.TestCase):
    def test_host_adapter_uses_list_projects_and_exposes_a_silent_startup_refresh(self):
        with tempfile.TemporaryDirectory() as temp:
            calls = []

            def list_projects():
                calls.append(True)
                return {
                    "projects": [{
                        "projectKind": "local",
                        "projectId": "desktop-project-1",
                        "label": "Project One",
                        "path": temp,
                        "hostId": "desktop-a",
                    }]
                }

            adapter = DesktopHostProjectCatalogAdapter(
                ProjectCatalog(Path(temp) / "catalog.json"),
                host_id="desktop-a",
                list_projects=list_projects,
            )
            receipt = adapter.refresh_on_start()
            self.assertEqual(adapter.name, "desktop-host-project-catalog")
            self.assertEqual(len(calls), 1)
            self.assertTrue(receipt.updated)
            self.assertTrue(receipt.observed_at)

    def test_host_failure_is_an_unchanged_receipt_not_an_adapter_exception(self):
        with tempfile.TemporaryDirectory() as temp:
            adapter = DesktopHostProjectCatalogAdapter(
                ProjectCatalog(Path(temp) / "catalog.json"),
                host_id="desktop-a",
                list_projects=lambda: (_ for _ in ()).throw(ConnectionError("offline")),
            )
            receipt = adapter.refresh_now()
            self.assertFalse(receipt.updated)
            self.assertIsNone(receipt.observed_at)
            self.assertEqual(receipt.project_count, 0)


if __name__ == "__main__":
    unittest.main()
