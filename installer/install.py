"""Install an offline Jarvis bundle into a NEW standalone directory only.

No current pointer, service, system Python, shell profile, or credentials are changed.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import uuid


def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(1048576):
            h.update(chunk)
    return h.hexdigest()


def checked_relative(name):
    p = Path(name)
    if p.is_absolute() or ".." in p.parts or "\\" in name or ":" in name:
        raise ValueError("Unsafe manifest path")
    return p


def verify_bundle(base, manifest):
    for name, entry in manifest["files"].items():
        p = base / checked_relative(name)
        if entry["kind"] == "symlink":
            if not p.is_symlink() or os.readlink(p) != entry["target"]:
                raise ValueError("Link mismatch: " + name)
            if not p.resolve().is_relative_to(base.resolve()):
                raise ValueError("Link escapes bundle: " + name)
        elif p.is_symlink() or not p.is_file() or sha(p) != entry["sha256"]:
            raise ValueError("File hash mismatch: " + name)
    actual = {p.relative_to(base).as_posix() for p in base.rglob("*")
              if p.is_file() or p.is_symlink()}
    expected = set(manifest["files"]) | {"bundle-manifest.json"}
    if actual != expected:
        raise ValueError("Unexpected or missing bundle files: " + repr(sorted(actual ^ expected)))


def inspect_python(executable):
    snippet = ("import json,platform,sys,sysconfig,venv;"
               "print(json.dumps({'version':list(sys.version_info[:3]),"
               "'implementation':sys.implementation.name,'machine':platform.machine(),"
               "'platform':sys.platform,'soabi':sysconfig.get_config_var('SOABI'),"
               "'libc':platform.libc_ver(),'executable':sys.executable,"
               "'prefix':sys.prefix,'base_prefix':sys.base_prefix}))")
    p = subprocess.run([str(executable), "-I", "-B", "-c", snippet],
                       check=True, capture_output=True, text=True, timeout=20)
    result = json.loads(p.stdout)
    if result["implementation"] != "cpython" or result["version"][:2] != [3, 12]:
        raise ValueError("This wheel set requires CPython 3.12")
    if result["platform"] != "linux" or result["machine"] not in ("x86_64", "amd64"):
        raise ValueError("This bundle supports Linux x86_64 only")
    libc, version = result["libc"]
    if libc != "glibc" or tuple(map(int, version.split(".")[:2])) < (2, 34):
        raise ValueError("This wheel set requires glibc >= 2.34; musl is unsupported")
    return result


def safe_target(value):
    target = Path(value).expanduser().absolute()
    if target.resolve() != target or ".." in target.parts:
        raise ValueError("Target must not traverse symlinks")
    if {".codex", ".ssh", ".aws", ".gnupg"}.intersection(target.parts):
        raise ValueError("Credential or assistant configuration directories are refused")
    if target.exists():
        raise ValueError("Target already exists; use a NEW version-specific directory")
    if not target.parent.is_dir() or target.parent == target:
        raise ValueError("Target needs an existing dedicated parent directory")
    if str(target) in ("/etc", "/usr", "/var", "/tmp", "/workspace"):
        raise ValueError("Dedicated target required")
    return target


def main():
    if sys.implementation.name != "cpython" or sys.version_info[:2] != (3, 12):
        raise ValueError("Run install.sh, or invoke this installer with CPython 3.12")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--mode", choices=("auto", "external", "bundled"), default="auto")
    ap.add_argument("--python", help="Exact existing CPython 3.12 interpreter")
    ap.add_argument("--target", required=True)
    ap.add_argument("--profile", choices=("execution", "isolated"), default="execution",
                    help="Default: configure ordinary task creation from a verified existing context")
    ap.add_argument("--context-config",
                    help="Exact existing non-secret launcher.json; no credentials are copied")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--sdk-smoke", action="store_true",
                    help="Bounded synthetic-state, zero-model MCP test; requires existing codex CLI")
    args = ap.parse_args()
    base = Path(__file__).resolve().parent
    manifest = json.loads((base / "bundle-manifest.json").read_text())
    verify_bundle(base, manifest)
    spec = importlib.util.spec_from_file_location("jarvis_installer_execution_setup",
                                                base / "execution_setup.py")
    setup = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(setup)
    target = safe_target(args.target)
    if args.mode == "bundled":
        if not (base / "runtime/python/bin/python3.12").is_file():
            raise ValueError("This is the external-Python bundle; no runtime included")
        python = base / "runtime/python/bin/python3.12"
    else:
        if args.python:
            resolved = shutil.which(args.python) if not Path(args.python).is_absolute() else args.python
            if not resolved:
                raise ValueError("Requested interpreter not found")
            python = Path(resolved).absolute()
        else:
            candidates = [sys.executable]
            candidates += [shutil.which(name) for name in ("python3.12", "python3", "python")]
            python = None
            for candidate in dict.fromkeys(c for c in candidates if c):
                try:
                    inspect_python(Path(candidate))
                    python = Path(candidate).absolute()
                    break
                except (ValueError, OSError, subprocess.SubprocessError, json.JSONDecodeError):
                    continue
            if python is None and args.mode == "auto" and (base / "runtime/python/bin/python3.12").is_file():
                python = base / "runtime/python/bin/python3.12"
                args.mode = "bundled"
            if python is None:
                raise ValueError("No compatible existing CPython 3.12 found; use --python or the bundled variant")
        if args.mode == "auto":
            bundled_python = base / "runtime/python/bin/python3.12"
            args.mode = ("bundled" if bundled_python.exists() and
                         python.resolve() == bundled_python.resolve() else "external")
    interpreter = inspect_python(python)
    configuration_plan = None
    configuration_blockers = []
    if args.profile == "execution":
        try:
            configuration_plan = setup.prepare_configuration(explicit=args.context_config)
        except setup.SetupBlocked as exc:
            configuration_blockers.append({"code": exc.code, "detail": exc.detail})
    plan = {
        "status": "preview", "mode": args.mode, "target": str(target),
        "installer_python": sys.executable,
        "interpreter": interpreter, "release": manifest["release"],
        "dependency_count": 38, "bootstrap_package": "pip==26.2.1",
        "existing_environment_mutated": False, "system_services_changed": False,
        "network": "Python audit denies network; local wheels only; no OS namespace claim",
        "sdk_smoke_requested": args.sdk_smoke, "dry_run": not args.apply,
        "profile": args.profile,
        "default_worker_capacity": setup.DEFAULT_WORKER_CAPACITY,
        "configured_total_capacity": configuration_plan["configured_total_capacity"] if configuration_plan else None,
        "provider_capacity_verified": False,
        "configuration_source": configuration_plan["source"] if configuration_plan else None,
        "configuration_source_sha256": configuration_plan["source_sha256"] if configuration_plan else None,
        "selected_existing_cli": configuration_plan["cli_snapshot"] if configuration_plan else None,
        "configuration_blockers": configuration_blockers,
        "task_started": False, "live_execution_verified": False,
    }
    if not args.apply:
        print(json.dumps(plan, indent=2))
        return
    target.mkdir()
    (target / "logs").mkdir()
    (target / "build-home").mkdir()
    (target / "build-temp").mkdir()
    phase = base / "offline_phase.py"
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(target / "build-home"), "USERPROFILE": str(target / "build-home"),
        "XDG_CONFIG_HOME": str(target / "build-home"),
        "XDG_CACHE_HOME": str(target / "build-home"),
        "TMPDIR": str(target / "build-temp"), "PIP_CONFIG_FILE": os.devnull,
        "PIP_NO_INDEX": "1", "PIP_DISABLE_PIP_VERSION_CHECK": "1",
        "PIP_KEYRING_PROVIDER": "disabled", "PYTHONDONTWRITEBYTECODE": "1",
    }
    receipt = {**plan, "status": "installing", "dry_run": False}

    def save():
        (target / "offline-install-receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")

    def run(name, executable, mode, *arguments):
        audit_log = target / "logs" / (name + ".audit.json")
        cmd = [str(executable), "-I", "-B", str(phase), mode, str(audit_log),
               *map(str, arguments)]
        with (target / "logs" / (name + ".log")).open("w") as out:
            subprocess.run(cmd, env=env, cwd=target, check=True,
                           stdout=out, stderr=subprocess.STDOUT, timeout=180)
        proof = json.loads(audit_log.read_text())
        if not proof["closed"] or proof["blocked_attempts"]:
            raise RuntimeError("Offline audit incomplete: " + name)

    save()
    try:
        for entry in (base / "release").iterdir():
            dest = target / entry.name
            if entry.is_dir():
                shutil.copytree(entry, dest)
            else:
                shutil.copy2(entry, dest)
        if args.mode == "bundled":
            shutil.copytree(base / "runtime/python", target / "python", symlinks=True)
            python = target / "python/bin/python3.12"
        run("create-venv", python, "venv", target / "venv")
        env_python = target / "venv/bin/python"
        pip_wheel = base / manifest["pip_wheel"]
        pip_common = ["--isolated", "--keyring-provider", "disabled", "--no-input",
                      "--disable-pip-version-check"]
        run("install", env_python, "pip", pip_wheel, *pip_common, "install",
            "--no-index", "--no-deps", "--only-binary=:all:", "--require-hashes",
            "--no-cache-dir", "--no-compile", "--find-links", base / "wheelhouse",
            "-r", base / "requirements.hashed.txt")
        run("pip-check", env_python, "pip", pip_wheel, *pip_common, "check")
        run("identity", env_python, "identity")
        identity = json.loads((target / "logs/identity.log").read_text())
        if identity["identity"]["code_sha256"] != manifest["release"]["source_code_sha256"]:
            raise RuntimeError("Installed source identity does not match manifest")
        if identity["runner_source_sha256"] != manifest["release"]["runner_source_sha256"]:
            raise RuntimeError("Installed Runner source pin does not match frozen own-package pin")
        expected = manifest["locked_versions"]
        if any(identity["versions"].get(n) != v for n, v in expected.items()):
            raise RuntimeError("Installed dependency versions do not match lock")
        run("subprocess", env_python, "probe_child")
        if args.sdk_smoke:
            run("sdk-smoke", env_python, "smoke", target)
            mcp_proof = json.loads((target / "logs/mcp.audit.json").read_text())
            if not mcp_proof["closed"] or mcp_proof["blocked_attempts"]:
                raise RuntimeError("MCP subprocess offline audit incomplete")
        runtime = target / "runtime"
        runtime.mkdir()
        (runtime / "state").mkdir()
        readiness = None
        staged_config = runtime / (".config-" + uuid.uuid4().hex)
        if args.profile == "isolated":
            templates = json.loads((target / "deploy/smoke-config.json").read_text())
            (runtime / "isolated-home").mkdir()
            (runtime / "isolated-project").mkdir()
            templates["launcher.json"].update(
                linux_cloud=True, expected_codex_home=str(runtime / "isolated-home"),
                codex_cli=shutil.which("codex") or "auto",
                allowed_projects={"IsolatedAcceptance": str(runtime / "isolated-project")},
                runtime_environments=[], runtime_constraints=setup.CONSTRAINTS,
                live_creation_enabled=False, worker_capacity=setup.DEFAULT_WORKER_CAPACITY,
            )
            readiness = setup.state(profile="isolated", configuration_ready=True)
            origin = "installer_isolated_default"
        elif configuration_plan is not None:
            try:
                setup.assert_unchanged(configuration_plan["snapshots"])
                identity_status = setup.auth_status(configuration_plan["config"],
                                                    configuration_plan["cli_snapshot"])
                if not identity_status["verified"]:
                    raise setup.SetupBlocked("existing_identity_not_verified",
                                             "Existing Codex login status is unavailable or not authenticated; sign in yourself")
                templates = json.loads((target / "deploy/smoke-config.json").read_text())
                templates["launcher.json"] = configuration_plan["config"]
                readiness = setup.state(profile="execution", configuration_ready=True,
                                        identity_verified=True)
                readiness["identity_status"] = identity_status
                origin = ("migrated_verified_installer_default"
                          if configuration_plan["source_creation_origin"] == "installer_isolated_default"
                          else "installer_execution_default"
                          if configuration_plan["source_creation_origin"] == "installer_execution_default"
                          else "inherited_enabled_context")
            except setup.SetupBlocked as exc:
                configuration_blockers.append({"code": exc.code, "detail": exc.detail})
        if readiness is not None:
            templates["transport.json"]["standard_bridge_package_root"] = str(target / "source/packages")
            templates["transport.json"]["native_task_launcher_config"] = "launcher.json"
            staged_config.mkdir()
            for name, value in templates.items():
                (staged_config / name).write_text(json.dumps(value, indent=2) + "\n")
            # Parse installed configuration and exact optional owner pin only.
            # This does not construct an AppServerClient, connect, or dispatch.
            try:
                run("configuration-validation", env_python, "configuration",
                    staged_config / "launcher.json")
                if configuration_plan is not None:
                    setup.assert_unchanged(configuration_plan["snapshots"])
                generated = {
                    "schema": setup.SCHEMA,
                    "launcher_sha256": sha(staged_config / "launcher.json"),
                    "creation_setting_origin": origin,
                    "user_disabled": False,
                    "capacity_setting_origin": (configuration_plan["capacity_setting_origin"]
                                                if configuration_plan else "installer_default"),
                    "capacity_default_value": setup.DEFAULT_WORKER_CAPACITY,
                    "capacity_user_configured": (configuration_plan["capacity_setting_origin"] != "installer_default"
                                                if configuration_plan else False),
                    "source_sha256": configuration_plan["source_sha256"] if configuration_plan else None,
                }
                (staged_config / "configuration-origin.json").write_text(json.dumps(generated, indent=2) + "\n")
                os.replace(staged_config, runtime / "config")
            except (setup.SetupBlocked, OSError, subprocess.SubprocessError) as exc:
                code = exc.code if isinstance(exc, setup.SetupBlocked) else "installed_configuration_validation_failed"
                detail = exc.detail if isinstance(exc, setup.SetupBlocked) else "Installed configuration or pinned policy could not be validated"
                configuration_blockers.append({"code": code, "detail": detail})
                readiness = None
        if readiness is None:
            readiness = setup.state(profile=args.profile, blockers=configuration_blockers)
        readiness["default_worker_capacity"] = setup.DEFAULT_WORKER_CAPACITY
        readiness["configured_total_capacity"] = (templates["launcher.json"]["worker_capacity"]
                                                  if readiness["configuration_ready"] else None)
        readiness["provider_capacity_verified"] = False
        (runtime / "readiness.json").write_text(json.dumps(readiness, indent=2) + "\n")
        bin_dir = target / "bin"
        bin_dir.mkdir()
        for name, module in (("jarvis", "jarvis_deploy.cli"),
                             ("jarvis-mcp", "jarvis_mcp.entrypoint")):
            wrapper = bin_dir / name
            prefix = '#!/bin/sh\nset -eu\nHERE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)\n'
            if name == "jarvis-mcp":
                prefix += ('test -f "$HERE/runtime/config/launcher.json" || { '
                           'echo "Jarvis is installed but not configured; read runtime/readiness.json." >&2; exit 3; }\n')
                extra = (' --config "$HERE/runtime/config/transport.json"'
                         ' --launcher-config "$HERE/runtime/config/launcher.json"'
                         ' --state-dir "$HERE/runtime/state"'
                         ' --local-heartbeat-config "$HERE/runtime/config/local-heartbeat.json"')
            else:
                extra = ""
            wrapper.write_text(prefix + 'exec "$HERE/venv/bin/python" -I -B -m ' + module + extra + ' "$@"\n')
            wrapper.chmod(0o755)
        shutil.copy2(base / "bundle-manifest.json", target / "bundle-origin.json")
        receipt.update(**readiness, installed_python=str(env_python),
                       installed_identity=identity["identity"],
                       installed_runner_source_sha256=identity["runner_source_sha256"],
                       sdk_smoke="passed" if args.sdk_smoke else "not_requested",
                       pointer_switched=False)
        save()
        print(json.dumps(receipt, indent=2))
        if readiness["status"] == "installed_but_not_ready":
            return 3
    except BaseException as exc:
        receipt.update(status="failed_retained_for_diagnosis",
                       error_type=type(exc).__name__, error=str(exc))
        save()
        raise


if __name__ == "__main__":
    raise SystemExit(main())
