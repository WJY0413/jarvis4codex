"""End-to-end source identity across an installed-like package layout.

Failure reproduced before the fix: the same Jarvis files hash differently when
an unrelated Python dependency lives beside them in site-packages.
"""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

from jarvis_control.operations import source_identity


ROOT = Path(__file__).resolve().parents[1] / "packages"
JARVIS_PACKAGES = (
    "adapters", "jarvis_codex_bridge", "jarvis_contracts", "jarvis_control",
    "jarvis_engine", "jarvis_harness_sdk", "jarvis_mcp", "jarvis_monitor",
    "jarvis_runtime", "jarvis_schema",
)


class DistributionIdentityE2E(unittest.TestCase):
    def test_unrelated_installed_dependency_does_not_change_product_identity(self):
        original = source_identity()
        with tempfile.TemporaryDirectory() as temp:
            stage = Path(temp)
            for package in JARVIS_PACKAGES:
                shutil.copytree(ROOT / package, stage / package, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
            (stage / "unrelated_dependency.py").write_text("VALUE = 'not Jarvis'\n", encoding="utf-8")
            # Stage takes precedence, while declared third-party dependencies
            # remain available from the documented isolated test environment.
            dependency_paths = [str(Path(entry).resolve()) for entry in
                os.environ.get("PYTHONPATH", "").split(os.pathsep) if entry]
            env = dict(os.environ, PYTHONPATH=os.pathsep.join((str(stage), *dependency_paths)),
                       PYTHONDONTWRITEBYTECODE="1")
            observed = subprocess.run(
                [sys.executable, "-B", "-c", "import json; from jarvis_control.operations import source_identity; "
                 "value=source_identity(); print(json.dumps({'hash': value['code_sha256'], 'count': len(value['files'])}))"],
                cwd=stage, env=env, capture_output=True, text=True, timeout=30,
            )
            self.assertEqual(observed.returncode, 0, observed.stderr)
            result = json.loads(observed.stdout)
            self.assertEqual(result["count"], len(original["files"]))
            self.assertEqual(result["hash"], original["code_sha256"])


if __name__ == "__main__":
    unittest.main()
