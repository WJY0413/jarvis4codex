"""The secure TEST inbox must not prevent importing the normal MCP entry."""
import os
import json
from pathlib import Path
import subprocess
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]


class WindowsSecureInboxGateTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "nt", "Windows import regression")
    def test_mcp_entry_imports_without_posix_directory_flags(self):
        env = dict(os.environ, PYTHONPATH=str(ROOT / "packages"))
        completed = subprocess.run(
            [sys.executable, "-B", "-c", "import jarvis_mcp.entrypoint"],
            env=env, capture_output=True, text=True, timeout=20,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    @unittest.skipUnless(os.name == "nt", "Windows capability gate")
    def test_secure_inbox_fails_closed_before_opening_storage(self):
        from adapters.local_test_inbox_receiver import workspace_fd
        with self.assertRaisesRegex(OSError, "secure TEST inbox storage is unavailable"):
            with workspace_fd(ROOT):
                self.fail("insecure storage was opened")

    @unittest.skipUnless(os.name == "nt", "Windows-only Linux-task boundary")
    def test_linux_only_task_clis_import_but_refuse_windows_execution(self):
        env = dict(os.environ, PYTHONPATH=str(ROOT / "packages"))
        for module in ("jarvis_control.file_task", "jarvis_control.contact_task"):
            with self.subTest(module=module):
                version = subprocess.run(
                    [sys.executable, "-B", "-m", module, "--version"],
                    env=env, capture_output=True, text=True, timeout=20,
                )
                self.assertEqual(version.returncode, 0, version.stderr)
                self.assertIn("0.3.1", version.stdout)
                refused = subprocess.run(
                    [sys.executable, "-B", "-m", module, "run", "--packet", "missing-packet.json"],
                    env=env, capture_output=True, text=True, timeout=20,
                )
                self.assertEqual(refused.returncode, 1, refused.stderr)
                receipt = json.loads(refused.stdout)
                self.assertEqual(receipt["status"], "error")
                self.assertEqual(receipt["error_type"], "FileTaskError")
                self.assertEqual(receipt["error"], "Linux-only task workflow is unavailable on Windows")
