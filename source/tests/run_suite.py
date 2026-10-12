"""Single internal canonical entry. Explicit immutable source; owned TEST processes only."""
from __future__ import annotations

import argparse
import ast
import ctypes
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import tomllib
import traceback
import unittest
from datetime import datetime, timezone

HERE = Path(__file__).resolve().parent
VERSION = "0.3.0"
CODE_SHA = "621c3db30dac2adcbcd71a05a72428350cc44f445cf6ca29792d6e2b51a6a226"
PRODUCT_PACKAGES = ("adapters", "jarvis_codex_bridge", "jarvis_contracts", "jarvis_control",
                    "jarvis_engine", "jarvis_harness_sdk", "jarvis_mcp", "jarvis_monitor",
                    "jarvis_runtime", "jarvis_schema")


def utc():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def process_sample(pid):
    """Non-secret measurements of a specified owned PID; no global inventory saved."""
    if os.name != "nt":
        try:
            stat = Path(f"/proc/{pid}/stat").read_text().split(")", 1)[1].split()
            status = Path(f"/proc/{pid}/status").read_text().splitlines()
            memory = {s.split(":")[0]: int(s.split()[1]) * 1024 for s in status if s.startswith(("VmRSS:", "VmHWM:"))}
            return {"pid": pid, "birth": stat[19], "rss_bytes": memory.get("VmRSS"),
                    "peak_rss_bytes": memory.get("VmHWM"), "cpu_seconds": (int(stat[11]) + int(stat[12])) / os.sysconf("SC_CLK_TCK"),
                    "handles": len(list(Path(f"/proc/{pid}/fd").iterdir())), "private_bytes": None}
        except (OSError, ValueError):
            return None
    from ctypes import wintypes as w
    k, ps = ctypes.WinDLL("kernel32", use_last_error=True), ctypes.WinDLL("psapi", use_last_error=True)
    k.OpenProcess.argtypes, k.OpenProcess.restype = [w.DWORD, w.BOOL, w.DWORD], w.HANDLE
    k.CloseHandle.argtypes = [w.HANDLE]
    k.GetProcessTimes.argtypes = [w.HANDLE, ctypes.POINTER(w.FILETIME), ctypes.POINTER(w.FILETIME), ctypes.POINTER(w.FILETIME), ctypes.POINTER(w.FILETIME)]
    k.GetProcessHandleCount.argtypes = [w.HANDLE, ctypes.POINTER(w.DWORD)]
    class Memory(ctypes.Structure):
        _fields_ = [("cb", w.DWORD), ("faults", w.DWORD)] + [(n, ctypes.c_size_t) for n in
            ("peak_rss", "rss", "peak_paged", "paged", "peak_nonpaged", "nonpaged", "pagefile", "peak_pagefile", "private")]
    ps.GetProcessMemoryInfo.argtypes = [w.HANDLE, ctypes.POINTER(Memory), w.DWORD]
    handle = k.OpenProcess(0x1000 | 0x10, False, pid)
    if not handle:
        return None
    try:
        created, exited, kernel, user = (w.FILETIME() for _ in range(4))
        if not k.GetProcessTimes(handle, ctypes.byref(created), ctypes.byref(exited), ctypes.byref(kernel), ctypes.byref(user)):
            return None
        def ticks(t):
            return (t.dwHighDateTime << 32) | t.dwLowDateTime
        if ticks(exited):
            return None
        memory, handles = Memory(), w.DWORD()
        memory.cb = ctypes.sizeof(memory)
        ok = ps.GetProcessMemoryInfo(handle, ctypes.byref(memory), memory.cb)
        k.GetProcessHandleCount(handle, ctypes.byref(handles))
        return {"pid": pid, "birth": str(ticks(created)), "rss_bytes": memory.rss if ok else None,
                "peak_rss_bytes": memory.peak_rss if ok else None, "private_bytes": memory.private if ok else None,
                "cpu_seconds": (ticks(kernel) + ticks(user)) / 1e7, "handles": handles.value}
    finally:
        k.CloseHandle(handle)


def children_of(pid):
    if os.name != "nt":
        rows = {}
        for path in Path("/proc").glob("[0-9]*/stat"):
            try:
                parts = path.read_text().split(")", 1)[1].split()
                rows[int(path.parent.name)] = int(parts[1])
            except (OSError, ValueError):
                pass
    else:
        from ctypes import wintypes as w
        k = ctypes.WinDLL("kernel32", use_last_error=True)
        class Entry(ctypes.Structure):
            _fields_ = [("dwSize", w.DWORD), ("cntUsage", w.DWORD), ("pid", w.DWORD), ("heap", ctypes.c_size_t),
                ("module", w.DWORD), ("threads", w.DWORD), ("parent", w.DWORD), ("priority", w.LONG),
                ("flags", w.DWORD), ("exe", w.WCHAR * 260)]
        k.CreateToolhelp32Snapshot.argtypes, k.CreateToolhelp32Snapshot.restype = [w.DWORD, w.DWORD], w.HANDLE
        k.Process32FirstW.argtypes = k.Process32NextW.argtypes = [w.HANDLE, ctypes.POINTER(Entry)]
        k.CloseHandle.argtypes = [w.HANDLE]
        snapshot = k.CreateToolhelp32Snapshot(2, 0)
        if snapshot == ctypes.c_void_p(-1).value:
            raise OSError(ctypes.get_last_error(), "cannot inspect owned process ancestry")
        rows, entry = {}, Entry()
        entry.dwSize = ctypes.sizeof(entry)
        try:
            present = k.Process32FirstW(snapshot, ctypes.byref(entry))
            while present:
                rows[entry.pid] = entry.parent
                present = k.Process32NextW(snapshot, ctypes.byref(entry))
        finally:
            k.CloseHandle(snapshot)
    owned = {pid}
    while True:
        more = {p for p, parent in rows.items() if parent in owned}
        if more.issubset(owned):
            return owned
        owned.update(more)


class OwnedTree:
    """Windows kill-on-close Job / POSIX private group. Admission precedes tests."""
    def __init__(self, process, worker_pid):
        self.process, self.worker_pid = process, worker_pid
        self.observed, self.samples, self.job = {}, [], None
        ancestry = children_of(process.pid)
        if worker_pid not in ancestry:
            raise RuntimeError("worker readiness PID is not a descendant of the created process")
        if os.name == "nt":
            from ctypes import wintypes as w
            k = self.k = ctypes.WinDLL("kernel32", use_last_error=True)
            k.CreateJobObjectW.argtypes, k.CreateJobObjectW.restype = [ctypes.c_void_p, w.LPCWSTR], w.HANDLE
            k.SetInformationJobObject.argtypes = [w.HANDLE, ctypes.c_int, ctypes.c_void_p, w.DWORD]
            k.QueryInformationJobObject.argtypes = [w.HANDLE, ctypes.c_int, ctypes.c_void_p, w.DWORD, ctypes.c_void_p]
            k.AssignProcessToJobObject.argtypes = [w.HANDLE, w.HANDLE]
            k.IsProcessInJob.argtypes = [w.HANDLE, w.HANDLE, ctypes.POINTER(w.BOOL)]
            k.OpenProcess.argtypes, k.OpenProcess.restype = [w.DWORD, w.BOOL, w.DWORD], w.HANDLE
            k.CloseHandle.argtypes = [w.HANDLE]
            class Basic(ctypes.Structure):
                _fields_ = [("process_time", ctypes.c_int64), ("job_time", ctypes.c_int64), ("flags", w.DWORD),
                    ("min_ws", ctypes.c_size_t), ("max_ws", ctypes.c_size_t), ("active", w.DWORD),
                    ("affinity", ctypes.c_size_t), ("priority", w.DWORD), ("scheduling", w.DWORD)]
            class Extended(ctypes.Structure):
                _fields_ = [("basic", Basic), ("io", ctypes.c_uint64 * 6), ("process_mem", ctypes.c_size_t),
                    ("job_mem", ctypes.c_size_t), ("peak_process_mem", ctypes.c_size_t), ("peak_job_mem", ctypes.c_size_t)]
            limits = Extended()
            limits.basic.flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            self.job = k.CreateJobObjectW(None, None)
            if not self.job or not k.SetInformationJobObject(self.job, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
                raise OSError(ctypes.get_last_error(), "cannot establish owned TEST Job")
            try:
                # Bind the parent before existing venv children. Nested Job
                # membership has an ordered hierarchy, not an unordered PID set.
                for pid in [process.pid, *sorted(ancestry - {process.pid})]:
                    handle = k.OpenProcess(0x400 | 0x1000 | 0x100 | 1, False, pid)
                    try:
                        already = w.BOOL()
                        queried = bool(handle and k.IsProcessInJob(handle, self.job, ctypes.byref(already)))
                        if queried and already.value:
                            continue
                        if not handle or not k.AssignProcessToJobObject(self.job, handle):
                            raise OSError(ctypes.get_last_error(), f"cannot bind owned TEST process {pid}; launcher={process.pid}, membership_query={queried}, already={already.value}")
                    finally:
                        if handle:
                            k.CloseHandle(handle)
            except BaseException:
                k.CloseHandle(self.job)
                self.job = None
                raise
        self.sample("admitted-idle")

    def pids(self):
        if os.name != "nt":
            return children_of(self.process.pid) | {self.worker_pid}
        buffer = ctypes.create_string_buffer(65536)
        if not self.k.QueryInformationJobObject(self.job, 3, buffer, len(buffer), None):
            raise OSError(ctypes.get_last_error(), "owned Job PID readback failed")
        count = ctypes.c_uint32.from_buffer(buffer, 4).value
        values = (ctypes.c_size_t * count).from_buffer(buffer, 8)
        return set(values)

    def sample(self, phase):
        rows = []
        for pid in self.pids():
            row = process_sample(pid)
            if row:
                rows.append(row)
                self.observed[pid] = row["birth"]
        self.samples.append({"phase": phase, "monotonic": time.monotonic(), "processes": rows,
            "rss_bytes": sum(r["rss_bytes"] or 0 for r in rows),
            "private_bytes": sum(r["private_bytes"] or 0 for r in rows),
            "cpu_seconds": sum(r["cpu_seconds"] for r in rows), "handles": sum(r["handles"] for r in rows)})

    def close(self):
        self.sample("before-owned-cleanup")
        if os.name == "nt":
            self.k.CloseHandle(self.job)
            self.job = None
        else:
            try:
                os.killpg(self.process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            if os.name != "nt":
                os.killpg(self.process.pid, signal.SIGKILL)
            self.process.wait(timeout=5)
        deadline = time.monotonic() + 3
        while True:
            residual = []
            for pid, birth in self.observed.items():
                sample = process_sample(pid)
                if sample and sample["birth"] == birth:
                    residual.append(pid)
            if not residual or time.monotonic() >= deadline:
                break
            time.sleep(.05)
        self.samples.append({"phase": "after-owned-cleanup", "monotonic": time.monotonic(),
            "processes": [process_sample(pid) for pid in residual], "residual_pids": residual})
        return {"confirmed": not residual, "owned_pids": sorted(self.observed),
                "owned_births": self.observed, "residual_pids": residual,
                "mechanism": "Windows private kill-on-close Job" if os.name == "nt" else "private POSIX process group"}


def start_owned(command, **kwargs):
    """Admit the newly created launcher before it can create venv children."""
    process = subprocess.Popen(command, **kwargs,
        creationflags=(0x4 | getattr(subprocess, "CREATE_NO_WINDOW", 0)) if os.name == "nt" else 0,
        start_new_session=os.name != "nt")
    tree = None
    try:
        tree = OwnedTree(process, process.pid)
        if os.name == "nt":
            from ctypes import wintypes as w
            k = ctypes.WinDLL("kernel32", use_last_error=True)
            class Thread(ctypes.Structure):
                _fields_ = [("size", w.DWORD), ("usage", w.DWORD), ("tid", w.DWORD),
                            ("owner", w.DWORD), ("base_priority", w.LONG),
                            ("delta_priority", w.LONG), ("flags", w.DWORD)]
            k.CreateToolhelp32Snapshot.argtypes, k.CreateToolhelp32Snapshot.restype = [w.DWORD, w.DWORD], w.HANDLE
            k.Thread32First.argtypes = k.Thread32Next.argtypes = [w.HANDLE, ctypes.POINTER(Thread)]
            k.OpenThread.argtypes, k.OpenThread.restype = [w.DWORD, w.BOOL, w.DWORD], w.HANDLE
            k.ResumeThread.argtypes, k.ResumeThread.restype = [w.HANDLE], w.DWORD
            k.CloseHandle.argtypes = [w.HANDLE]
            snapshot = k.CreateToolhelp32Snapshot(4, 0)
            if snapshot == ctypes.c_void_p(-1).value:
                raise OSError(ctypes.get_last_error(), "cannot inspect suspended TEST launcher thread")
            threads, row = [], Thread()
            row.size = ctypes.sizeof(row)
            try:
                present = k.Thread32First(snapshot, ctypes.byref(row))
                while present:
                    if row.owner == process.pid:
                        threads.append(row.tid)
                    present = k.Thread32Next(snapshot, ctypes.byref(row))
            finally:
                k.CloseHandle(snapshot)
            if len(threads) != 1:
                raise RuntimeError("new suspended launcher must have exactly one owned initial thread")
            handle = k.OpenThread(2, False, threads[0])
            try:
                if not handle or k.ResumeThread(handle) != 1:
                    raise OSError(ctypes.get_last_error(), "cannot resume admitted TEST launcher")
            finally:
                if handle:
                    k.CloseHandle(handle)
        return process, tree
    except BaseException:
        if tree:
            tree.close()
        else:
            process.terminate()
            process.wait(timeout=5)
        raise


def discover(source, manifest):
    catalog = []
    bundled_legacy = set(manifest.get("bundled_legacy_files", []))
    primary = {p.relative_to(source / "tests").as_posix() for p in (source / "tests").rglob("test*.py")}
    files = [("legacy" if p.relative_to(source / "tests").as_posix() in bundled_legacy else "source",
              p, p.relative_to(source / "tests").as_posix())
             for p in sorted((source / "tests").rglob("test*.py")) if p.name not in manifest["suite_files"]]
    files += [("legacy", p, p.relative_to(HERE).as_posix()) for p in sorted(HERE.rglob("test*.py"))
              if p.relative_to(HERE).as_posix() not in primary and p.name not in manifest["suite_files"]]
    files += [("suite", HERE / name, name) for name in manifest["suite_files"]]
    files += [("fixture", HERE / "suite_fixtures.py", "suite_fixtures.py")]
    for origin, path, relative in files:
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        for cls in (n for n in tree.body if isinstance(n, ast.ClassDef)):
            for method in cls.body:
                if not isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef)) or not method.name.startswith("test_"):
                    continue
                local = f"{cls.name}.{method.name}"
                selector = f"{origin}::{local}" if origin == "fixture" else f"{origin}::{path.stem}.{local}"
                blocks = manifest["file_blocks"].get(relative, list(manifest["blocks"]))
                blocked = None
                if path.name == "test_model_web_acceptance.py":
                    blocked = "live model/network onboarding and real Codex config access are outside this suite authorization"
                catalog.append({"selector": selector, "case_id": "CASE-" + hashlib.sha256(selector.encode()).hexdigest()[:16],
                    "origin": origin, "path": str(path), "relative_path": relative, "method": local,
                    "line": method.lineno, "file_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                    "assertion_source": f"{relative}:{method.lineno} ({local})",
                    "preconditions": "explicit verified source; existing test-owned fixtures; runtime platform skips retained",
                    "effects": "isolated TEST artifacts and owned test processes",
                    "cleanup_contract": "existing fixture cleanup plus owned-tree exit readback",
                    "blocks": blocks, "evidence_level": manifest["evidence_levels"].get(origin, "protocol_isolated"),
                    "blocked_reason": blocked, "basis": "existing canonical contract" if origin in {"source", "legacy"} else "SUITE_FAILURE_MODES.md and explicit 2026-10-08 requirements"})
    for gap in manifest["gaps"]:
        catalog.append({**gap, "selector": "gap::" + gap["id"], "case_id": gap["id"], "origin": "gap", "path": None,
                        "evidence_level": "real_product_e2e", "blocked_reason": gap["reason"], "basis": "confirmed eight-block acceptance scope"})
    if len({c["selector"] for c in catalog}) != len(catalog):
        raise ValueError("duplicate canonical selector")
    return catalog


def summarize(cases):
    counts = {s: sum(c["status"] == s for c in cases) for s in ("passed", "failed", "error", "skipped", "partial", "not_run", "running", "not_implemented")}
    counts["methods_run"] = sum(c.get("started", False) for c in cases)
    for field, kinds in {"failure_records": {"failure", "subtest_failure"}, "error_records": {"error", "subtest_error"},
            "method_skip_records": {"skip"}, "subtest_skip_records": {"subtest_skip"}, "subtest_pass_records": {"subtest_pass"}}.items():
        counts[field] = sum(e["kind"] in kinds for c in cases for e in c["events"])
    return counts


def worker(plan_path):
    plan = json.loads(Path(plan_path).read_text())
    directory, source = Path(plan["directory"]), Path(plan["source"])
    write_json(directory / "worker-ready.json", {"pid": os.getpid()})
    deadline = time.monotonic() + 15
    while not (directory / "gate").exists():
        if time.monotonic() >= deadline:
            return 2
        time.sleep(.02)
    source_paths = [source / "packages", source / "packages/jarvis_runtime", source / "tests"]
    source_paths += sorted({p.parent for p in (source / "tests").rglob("test*.py")})
    sys.path[:0] = [str(p) for p in source_paths] + [str(HERE), str(HERE.parent)]
    os.environ.update(JARVIS_TEST_SOURCE=str(source), JARVIS_SUITE_RUN_DIR=str(directory), PYTHONDONTWRITEBYTECODE="1")
    rows = plan["cases"]
    progress = {"cases": rows, "source": {}}
    def save():
        write_json(directory / "progress.json", progress)
    def assert_origins():
        bare = {p.stem for p in (source / "packages/jarvis_runtime").glob("*.py")}
        wrong = []
        for name, module in tuple(sys.modules.items()):
            if name.split(".")[0] not in (*PRODUCT_PACKAGES, *bare):
                continue
            file = getattr(module, "__file__", None)
            if file and not Path(file).resolve().is_relative_to(source / "packages"):
                wrong.append(name)
        if wrong:
            raise RuntimeError("product modules loaded outside explicit source: " + ", ".join(wrong))
    try:
        from jarvis_control.operations import source_identity
        from jarvis_mcp.server import JarvisMcpServer
        from types import SimpleNamespace
        identity = source_identity()
        loaded = JarvisMcpServer(SimpleNamespace()).mcp._lowlevel_server.version
        assert_origins()
        progress["source"] = {"loaded_version": loaded, "code_sha256": identity["code_sha256"],
            "loaded_path": str(Path(sys.modules["jarvis_control"].__file__).resolve()),
            "loaded_identity_verified": loaded == plan["expected_version"] and identity["code_sha256"] == plan["expected_hash"],
            "product_python_files": len(identity["files"])}
        save()
        if not progress["source"]["loaded_identity_verified"]:
            progress["source_error"] = "actual loaded version/hash does not match explicit expected identity"
            save()
            return 3
        # Nested entry E2Es must keep the caller's verified pin, not silently
        # fall back to the historical rev7 defaults after a product repair.
        os.environ["JARVIS_TEST_EXPECTED_CODE_SHA256"] = plan["expected_hash"]
        os.environ["JARVIS_TEST_EXPECTED_VERSION"] = plan["expected_version"]
        if plan["list_only"]:
            return 0
        class Recorder(unittest.TextTestResult):
            def startTest(self, test):
                super().startTest(test)
                self.row = current
                self.row.update(status="running", started=True, started_at_utc=utc(), reason=None)
                self.started = time.monotonic()
                case_dir = directory / "case-artifacts" / self.row["case_id"]
                case_dir.mkdir(parents=True, exist_ok=True)
                os.environ["JARVIS_CASE_ARTIFACT_DIR"] = str(case_dir)
                save()

            def event(self, kind, test, error=None, reason=None):
                event = {"kind": kind}
                if kind.startswith("subtest_"):
                    event["subtest_id"] = test.id()
                if error:
                    event["detail"] = self._exc_info_to_string(error, test)
                if reason:
                    event["reason"] = reason
                self.row["events"].append(event)
                save()

            def addSuccess(self, test):
                super().addSuccess(test)
                self.row["status"] = "partial" if any(e["kind"] == "subtest_skip" for e in self.row["events"]) else "passed"

            def addFailure(self, test, error):
                super().addFailure(test, error)
                if self.row["status"] != "error":
                    self.row["status"] = "failed"
                self.event("failure", test, error)

            def addError(self, test, error):
                super().addError(test, error)
                self.row["status"] = "error"
                self.event("error", test, error)

            def addSkip(self, test, reason):
                super().addSkip(test, reason)
                sub = isinstance(test, unittest.case._SubTest)
                if not sub:
                    self.row["status"] = "skipped"
                self.event("subtest_skip" if sub else "skip", test, reason=reason)

            def addSubTest(self, test, subtest, error):
                super().addSubTest(test, subtest, error)
                if error:
                    kind = "subtest_failure" if issubclass(error[0], test.failureException) else "subtest_error"
                    if kind == "subtest_error" or self.row["status"] != "error":
                        self.row["status"] = "error" if kind == "subtest_error" else "failed"
                    self.event(kind, subtest, error)
                else:
                    self.event("subtest_pass", subtest)

            def stopTest(self, test):
                if self.row["status"] == "running":
                    # unittest does not call addSuccess after a skipped subTest.
                    self.row["status"] = "partial" if any(e["kind"] == "subtest_skip" for e in self.row["events"]) else "error"
                self.row["elapsed_seconds"] = time.monotonic() - self.started
                self.row["ended_at_utc"] = utc()
                save()
                super().stopTest(test)

        for current in rows:
            if current.get("blocked_reason"):
                continue
            try:
                path = Path(current["path"])
                name = path.stem
                module = sys.modules.get(name)
                if module is None or Path(module.__file__).resolve() != path.resolve():
                    spec = importlib.util.spec_from_file_location(name, path)
                    module = importlib.util.module_from_spec(spec)
                    sys.modules[name] = module
                    spec.loader.exec_module(module)
                assert_origins()
                suite = unittest.defaultTestLoader.loadTestsFromName(current["method"], module)
                unittest.TextTestRunner(verbosity=2, resultclass=Recorder).run(suite)
                assert_origins()
            except Exception:
                current.update(status="error")
                current["events"].append({"kind": "error", "detail": traceback.format_exc()})
                save()
        progress["counts"] = summarize(rows)
        save()
        return 0
    except Exception:
        progress["source_error"] = traceback.format_exc()
        save()
        return 3


def finish(directory, receipt):
    for case in receipt["cases"]:
        if case["status"] == "running":
            case.update(status="error", reason="worker ended without a terminal case record")
            case["events"].append({"kind": "error", "reason": case["reason"]})
    receipt["ended_at_utc"] = utc()
    receipt["counts"] = summarize(receipt["cases"])
    receipt["block_counts"] = {block: summarize([c for c in receipt["cases"] if block in c["blocks"]])
                               for block in receipt.get("catalog", {}).get("blocks", {})}
    receipt["artifacts"] = [{"path": p.relative_to(directory).as_posix(), "bytes": p.stat().st_size,
        "sha256": hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(directory.rglob("*"))
        if p.is_file() and p.name not in {"receipt.json", "report.md"}]
    write_json(directory / "receipt.json", receipt)
    lines = ["# Jarvis canonical run", "", f"Status: {receipt['status']}; exit: {receipt['exit_code']}",
        f"Source: {receipt['source'].get('version')} / {receipt['source'].get('code_sha256', 'unverified')}",
        f"Profile: {receipt['profile']}; counts: {json.dumps(receipt['counts'])}",
        "", "This run is the selected scope only. It does not imply complete product acceptance.",
        f"Owned cleanup confirmed: {receipt['cleanup'].get('confirmed')}",
        "Resource samples are measurements; no performance threshold or leak verdict is invented.", "",
        "| Case | Status | Evidence | Reason |", "|---|---|---|---|"]
    for case in receipt["cases"]:
        reason = case.get("blocked_reason") or case.get("reason") or ""
        lines.append(f"| {case['selector']} | {case['status']} | {case['evidence_level']} | {reason} |")
    (directory / "report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps({"status": receipt["status"], "exit_code": receipt["exit_code"],
                      "counts": receipt["counts"], "receipt": str(directory / "receipt.json")}))
    return receipt["exit_code"]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--receipt-dir", required=True, type=Path)
    parser.add_argument("--expected-version", default=VERSION)
    parser.add_argument("--expected-code-sha256", default=CODE_SHA)
    parser.add_argument("--profile", choices=("core", "entry", "protocol", "resources", "all", "acceptance"), default="core")
    parser.add_argument("--block", action="append", choices=[f"B{i:02d}" for i in range(1, 9)])
    parser.add_argument("--select", action="append")
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--deadline", type=float, default=90)
    parser.add_argument("--cancel-file", type=Path, help="Explicit cancellation of this run only; also supports Ctrl-C")
    args = parser.parse_args(argv)
    source, directory = args.source.resolve(), args.receipt_dir.resolve()
    if directory.is_relative_to(source) or (directory.exists() and any(directory.iterdir())):
        parser.error("receipt-dir must be a fresh directory outside the immutable source")
    if args.deadline <= 0:
        parser.error("deadline must be positive")
    directory.mkdir(parents=True, exist_ok=True)
    receipt = {"schema": "jarvis-canonical-test-run/v1", "run_id": directory.name, "started_at_utc": utc(),
        "profile": args.profile, "command": [sys.executable, "-B", str(Path(__file__).resolve()), *(argv or sys.argv[1:])],
        "cwd": str(Path.cwd()), "deadline_seconds": args.deadline, "source": {"path": str(source)},
        "environment": {"python": sys.version, "platform": sys.platform}, "status": "preparing", "exit_code": 2,
        "cases": [], "cleanup": {"confirmed": True, "owned_pids": [], "residual_pids": []},
        "resources": {"samples": [], "acceptance": "measurement_only_no_threshold"},
        "protected_effects": {"model_turns": 0, "external_messages": 0, "production_accessed": False,
            "target_modified": False, "runtime_config_modified": False, "service_restart": False},
        "scope_note": "Explicit selected source and cases only; real-model/99-seat/authoritative recovery remain unaccepted"}
    receipt["suite_inputs"] = [{"path": name, "sha256": hashlib.sha256((HERE / name).read_bytes()).hexdigest()}
                              for name in ("run_suite.py", "suite_fixtures.py", "coverage_manifest.json",
                                           "test_suite_entry_e2e.py", "test_suite_protocol_e2e.py")]
    process, tree = None, None
    start = time.monotonic()
    try:
        version = tomllib.loads((source / "pyproject.toml").read_text())["project"]["version"]
        receipt["source"]["version"] = version
        if version != args.expected_version:
            raise ValueError("source version does not match explicit expected version")
        manifest = json.loads((HERE / "coverage_manifest.json").read_text())
        catalog = discover(source, manifest)
        if args.select:
            requested = set(args.select)
            selected = [c for c in catalog if c["selector"] in requested]
            missing = requested - {c["selector"] for c in selected}
            if missing:
                raise ValueError("unknown canonical selectors: " + ", ".join(sorted(missing)))
        elif args.profile in {"all", "acceptance"}:
            selected = [c for c in catalog if c["origin"] != "fixture" and (args.profile == "acceptance" or c["origin"] != "gap")]
        else:
            requested = set(manifest["profiles"][args.profile])
            selected = [c for c in catalog if c["selector"] in requested]
            if requested - {c["selector"] for c in selected}:
                raise ValueError("profile selector missing from actual discovery")
        if args.block:
            selected = [c for c in selected if set(c["blocks"]) & set(args.block)]
        receipt["catalog"] = {"source_methods": sum(c["origin"] == "source" for c in catalog),
            "legacy_methods": sum(c["origin"] == "legacy" for c in catalog), "blocks": manifest["blocks"],
            "selected": selected, "all_selectors": [c["selector"] for c in catalog]}
        receipt["cases"] = [{**c, "status": "not_implemented" if c.get("product_not_implemented") else "not_run",
            "started": False, "events": [], "reason": c.get("blocked_reason") or "not started"} for c in selected]
        if not selected and not args.list:
            receipt.update(status="incomplete", exit_code=2, reason="selection is empty")
            return finish(directory, receipt)
        plan = {"directory": str(directory), "source": str(source), "cases": receipt["cases"],
            "expected_version": args.expected_version, "expected_hash": args.expected_code_sha256, "list_only": args.list}
        write_json(directory / "plan.json", plan)
        write_json(directory / "receipt.json", receipt)
        with (directory / "worker.log").open("w", encoding="utf-8") as log:
            process, tree = start_owned([sys.executable, "-B", str(Path(__file__).resolve()), "_worker", str(directory / "plan.json")],
                stdout=log, stderr=log, cwd=directory, env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"))
            ready_limit = min(start + args.deadline, time.monotonic() + 10)
            while not (directory / "worker-ready.json").exists():
                if time.monotonic() >= ready_limit or process.poll() is not None:
                    raise RuntimeError("owned worker did not reach pre-execution readiness")
                time.sleep(.02)
            ready = json.loads((directory / "worker-ready.json").read_text())
            if ready["pid"] not in tree.pids():
                raise RuntimeError("actual interpreter is outside the admitted owned tree")
            tree.worker_pid = ready["pid"]
            (directory / "gate").write_text("owned tree admitted")
            receipt.update(status="running")
            while process.poll() is None:
                tree.sample("running")
                if args.cancel_file and args.cancel_file.exists():
                    receipt.update(status="interrupted", exit_code=130)
                    break
                if time.monotonic() - start >= args.deadline:
                    receipt.update(status="deadline", exit_code=124)
                    break
                time.sleep(.1)
        progress_path = directory / "progress.json"
        if progress_path.exists():
            progress = json.loads(progress_path.read_text())
            receipt["source"].update(progress.get("source", {}))
            receipt["cases"] = progress["cases"]
            if progress.get("source_error"):
                receipt.update(status="source_rejected", exit_code=3, reason=progress["source_error"])
        if receipt["status"] == "running":
            count = summarize(receipt["cases"])
            if process.returncode != 0:
                receipt.update(status="worker_error", exit_code=1)
            elif args.list:
                receipt.update(status="listed", exit_code=0)
            elif count["failed"] or count["error"]:
                receipt.update(status="failed", exit_code=1)
            elif count["not_run"] or count["partial"] or count["skipped"] or count["not_implemented"]:
                receipt.update(status="incomplete", exit_code=2)
            else:
                receipt.update(status="passed_selected_scope", exit_code=0)
    except KeyboardInterrupt:
        receipt.update(status="interrupted", exit_code=130)
    except Exception:
        receipt.update(status="source_rejected" if process is None else "runner_error", exit_code=3,
                       reason=traceback.format_exc())
    finally:
        if tree:
            try:
                receipt["cleanup"] = tree.close()
                receipt["resources"]["samples"] = tree.samples
            except Exception:
                receipt["cleanup"] = {"confirmed": False, "reason": traceback.format_exc()}
        elif process and process.poll() is None:
            process.terminate()  # Pre-gate worker: no tests/descendants have been admitted.
            process.wait(timeout=5)
        if receipt["status"] in {"interrupted", "deadline"}:
            path = directory / "progress.json"
            if path.exists():
                progress = json.loads(path.read_text())
                receipt["source"].update(progress.get("source", {}))
                receipt["cases"] = progress["cases"]
            for case in receipt["cases"]:
                if case["status"] == "running":
                    case.update(status="error", reason=receipt["status"])
                    case["events"].append({"kind": "error", "reason": receipt["status"]})
        if not receipt["cleanup"].get("confirmed") and receipt["exit_code"] == 0:
            receipt.update(status="incomplete", exit_code=2)
    return finish(directory, receipt)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "_worker":
        raise SystemExit(worker(sys.argv[2]))
    raise SystemExit(main())
