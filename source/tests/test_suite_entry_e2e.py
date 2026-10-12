"""CLI E2E authored before run_suite.py. Basis: SUITE_FAILURE_MODES.md."""
import ast
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

HERE = Path(__file__).resolve().parent


class SuiteEntryE2E(unittest.TestCase):
    def setUp(self):
        artifact = os.environ.get("JARVIS_CASE_ARTIFACT_DIR")
        if artifact:
            self.root = Path(artifact) / "entry-fixtures"
            self.root.mkdir()
        else:
            self.temp = tempfile.TemporaryDirectory(prefix="jarvis-entry-e2e-")
            self.addCleanup(self.temp.cleanup)
            self.root = Path(self.temp.name)
        self.source = Path(os.environ["JARVIS_TEST_SOURCE"])
        self.expected_hash = os.environ["JARVIS_TEST_EXPECTED_CODE_SHA256"]
        self.expected_version = os.environ["JARVIS_TEST_EXPECTED_VERSION"]

    def command(self, directory, *extra):
        return [sys.executable, "-B", str(HERE / "run_suite.py"),
                "--source", str(self.source), "--receipt-dir", str(directory),
                "--expected-code-sha256", self.expected_hash,
                "--expected-version", self.expected_version,
                "--deadline", "15", *extra]

    def run_cli(self, *extra):
        directory = self.root / ("run-" + str(len(list(self.root.iterdir()))))
        proc = subprocess.run(self.command(directory, *extra), capture_output=True,
                              text=True, timeout=25)
        self.assertTrue((directory / "receipt.json").is_file(), proc.stderr)
        receipt = json.loads((directory / "receipt.json").read_text())
        self.assertEqual(receipt["exit_code"], proc.returncode)
        self.assertTrue((directory / "report.md").is_file())
        return receipt, proc

    def test_nested_discovery_and_eight_block_filter(self):
        receipt, proc = self.run_cli("--list", "--block", "B05")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(receipt["source"]["version"], self.expected_version)
        manifest = json.loads((HERE / 'coverage_manifest.json').read_text())
        legacy = set(manifest.get('bundled_legacy_files', []))
        methods = sum(1 for path in (self.source / "tests").rglob("test*.py")
            if path.name not in manifest['suite_files']
            and path.relative_to(self.source / 'tests').as_posix() not in legacy
            for cls in ast.parse(path.read_text(encoding="utf-8-sig")).body
            if isinstance(cls, ast.ClassDef) for method in cls.body
            if isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef))
            and method.name.startswith("test_"))
        self.assertEqual(receipt["catalog"]["source_methods"], methods)
        self.assertEqual(len(receipt["catalog"]["blocks"]), 8)
        self.assertTrue(any("test_hold_host_recovery" in c["selector"]
                            for c in receipt["catalog"]["selected"]))
        self.assertEqual(receipt["counts"]["methods_run"], 0)

    def test_exact_loaded_source_and_passing_fixture(self):
        receipt, proc = self.run_cli("--select", "fixture::EntryCases.test_pass")
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(receipt["counts"]["passed"], 1)
        self.assertTrue(receipt["source"]["loaded_identity_verified"])
        self.assertEqual(receipt["source"]["loaded_version"], "0.3.0")
        self.assertTrue(receipt["cleanup"]["confirmed"])

    def test_wrong_version_or_hash_rejects_with_report(self):
        for flag, value in (("--expected-version", "0.2.4"),
                            ("--expected-code-sha256", "0" * 64)):
            with self.subTest(flag=flag):
                receipt, proc = self.run_cli(flag, value, "--select", "fixture::EntryCases.test_pass")
                self.assertNotEqual(proc.returncode, 0)
                self.assertEqual(receipt["counts"]["methods_run"], 0)
                self.assertEqual(receipt["status"], "source_rejected")

    def test_method_and_subtest_statistics_preserve_failure(self):
        receipt, proc = self.run_cli("--select", "fixture::EntryCases.test_pass",
            "--select", "fixture::EntryCases.test_fail", "--select", "fixture::EntryCases.test_error",
            "--select", "fixture::EntryCases.test_skip", "--select", "fixture::EntryCases.test_partial",
            "--select", "fixture::EntryCases.test_subtests")
        self.assertEqual(proc.returncode, 1)
        count = receipt["counts"]
        self.assertEqual((count["methods_run"], count["passed"], count["failed"],
                          count["error"], count["skipped"], count["partial"]), (6, 1, 1, 2, 1, 1))
        self.assertEqual((count["failure_records"], count["error_records"],
                          count["method_skip_records"], count["subtest_skip_records"]), (3, 2, 1, 2))
        self.assertTrue(any(e.get("subtest_id") for c in receipt["cases"] for e in c["events"]))

    def test_missing_evidence_is_not_run_and_incomplete(self):
        receipt, proc = self.run_cli("--select", "gap::MODEL_SCHEMA_TERMINAL")
        self.assertEqual(proc.returncode, 2)
        self.assertEqual(receipt["counts"]["not_run"], 1)
        self.assertEqual(receipt["counts"]["passed"], 0)
        self.assertEqual(receipt["status"], "incomplete")

    def test_deadline_reaps_owned_descendant_and_saves_report(self):
        receipt, proc = self.run_cli("--deadline", "3", "--select", "fixture::EntryCases.test_spawn_timeout")
        self.assertEqual(proc.returncode, 124)
        self.assertEqual(receipt["status"], "deadline")
        self.assertTrue(receipt["cleanup"]["confirmed"])
        self.assertGreaterEqual(len(receipt["cleanup"]["owned_pids"]), 2)
        self.assertEqual(receipt["cleanup"]["residual_pids"], [])
        self.assertTrue(receipt["resources"]["samples"])

    def test_cancel_reaps_owned_descendant_and_saves_report(self):
        directory, cancel = self.root / "cancel-run", self.root / "cancel"
        proc = subprocess.Popen(self.command(directory, "--cancel-file", str(cancel),
            "--select", "fixture::EntryCases.test_spawn_timeout"),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            limit = time.monotonic() + 10
            while not (directory / "fixture-child.json").exists():
                self.assertLess(time.monotonic(), limit)
                self.assertIsNone(proc.poll())
                time.sleep(.05)
            cancel.write_text("cancel this test run only")
            stdout, stderr = proc.communicate(timeout=10)
            receipt = json.loads((directory / "receipt.json").read_text())
            self.assertEqual(proc.returncode, 130, stdout + stderr)
            self.assertEqual(receipt["status"], "interrupted")
            self.assertTrue(receipt["cleanup"]["confirmed"])
            self.assertEqual(receipt["cleanup"]["residual_pids"], [])
        finally:
            if proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=5)
