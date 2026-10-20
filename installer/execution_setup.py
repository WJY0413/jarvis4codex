"""Non-secret configuration planning for a fresh standalone Jarvis installation.

This module never opens authentication files, discovers credentials, starts an
App Server, captures a new owner pin, or creates a business task.
"""
from pathlib import Path
import copy
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess

SCHEMA = "jarvis-configuration-origin/v1"
DEFAULT_WORKER_CAPACITY = 99
CONSTRAINTS = {"model_provider": "openai", "sandbox": "read-only", "approval_policy": "never"}
SECRET_KEYS = frozenset({
    "password", "api_key", "apikey", "token", "access_token", "refresh_token",
    "client_secret", "authorization", "auth", "credentials", "secret",
})
ALLOWED_KEYS = frozenset({
    "version", "dispatcher_thread_id", "codex_cli", "profile",
    "expected_codex_home", "linux_cloud", "live_creation_enabled",
    "allowed_projects", "runtime_environments", "runtime_constraints",
    "default_model", "default_reasoning_effort", "worker_capacity",
    "poll_seconds", "request_timeout_seconds", "turn_completion_timeout_seconds",
    "turn_readback_timeout_seconds", "thread_operation_lock_timeout_seconds",
    "max_attempts", "memory_admission_enabled", "memory_admission_max_used_percent",
    "cooper_actor_ids", "unix_owner", "unix_initial_dispatch", "owner_control_enabled",
    "business_quarantine_file", "scheduling_generation_id", "jarvis_connection",
    "task_creation_user_disabled",
})


class SetupBlocked(ValueError):
    def __init__(self, code, detail):
        self.code, self.detail = code, detail
        super().__init__(code + ": " + detail)


def sha_bytes(data):
    return hashlib.sha256(data).hexdigest()


def executable_snapshot(value):
    path = Path(value).absolute()
    try:
        link = path.lstat()
        resolved = path.resolve(strict=True)
        with resolved.open("rb") as handle:
            info = os.fstat(handle.fileno())
            if not stat.S_ISREG(info.st_mode) or not os.access(resolved, os.X_OK):
                raise ValueError("Not executable")
            digest = hashlib.file_digest(handle, "sha256").hexdigest()
            after = os.fstat(handle.fileno())
        if (info.st_ino, info.st_size, info.st_mtime_ns) != (
                after.st_ino, after.st_size, after.st_mtime_ns):
            raise ValueError("Changed")
        return {"path": str(path), "resolved_path": str(resolved),
                "alias_device": link.st_dev, "alias_inode": link.st_ino,
                "alias_mtime_ns": link.st_mtime_ns, "device": info.st_dev,
                "inode": info.st_ino, "mtime_ns": info.st_mtime_ns,
                "bytes": info.st_size, "sha256": digest}
    except (OSError, ValueError) as exc:
        raise SetupBlocked("selected_cli_unavailable", "Cannot identify the existing selected executable") from exc


def verify_executable(expected):
    if executable_snapshot(expected["path"]) != expected:
        raise SetupBlocked("selected_cli_changed", "The selected CLI changed after configuration planning")


def pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("Duplicate key")
        result[key] = value
    return result


def safe_json(path, *, credential_paths_forbidden=True, limit=131072):
    """Read one named non-secret configuration, never recurse into a HOME."""
    path = Path(path).expanduser().absolute()
    if path.resolve() != path or ".." in path.parts:
        raise SetupBlocked("unsafe_configuration_path", "Configuration path traverses a link")
    if credential_paths_forbidden and {".codex", ".ssh", ".aws", ".gnupg"}.intersection(path.parts):
        raise SetupBlocked("credential_path_refused", "A credential directory is not a configuration source")
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(fd, "rb") as handle:
            before = os.fstat(handle.fileno())
            if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                    or before.st_uid != os.getuid() or before.st_size > limit):
                raise SetupBlocked("unsafe_configuration_file", "Configuration is not a bounded, user-owned regular file")
            data = handle.read(limit + 1)
            after = os.fstat(handle.fileno())
        if len(data) > limit or (before.st_ino, before.st_size, before.st_mtime_ns) != (
                after.st_ino, after.st_size, after.st_mtime_ns):
            raise SetupBlocked("configuration_changed", "Configuration changed during the read")
        value = json.loads(data, object_pairs_hook=pairs,
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError("Nonfinite JSON")))
        if not isinstance(value, dict):
            raise ValueError("Not an object")
    except SetupBlocked:
        raise
    except (OSError, UnicodeError, ValueError) as exc:
        raise SetupBlocked("configuration_unreadable", "Cannot read the named non-secret configuration") from exc
    snapshot = {"path": str(path), "sha256": sha_bytes(data), "inode": before.st_ino,
                "device": before.st_dev, "size": before.st_size, "mtime_ns": before.st_mtime_ns}
    return value, snapshot


def assert_unchanged(snapshots):
    for expected in snapshots:
        _, actual = safe_json(Path(expected["path"]),
                              credential_paths_forbidden=expected.get("credential_paths_forbidden", True),
                              limit=expected.get("read_limit", 131072))
        comparable = {key: expected[key] for key in actual}
        if actual != comparable:
            raise SetupBlocked("source_context_changed", "Selected configuration or policy changed; select it again")


def secret_key_present(value):
    if isinstance(value, dict):
        for key, child in value.items():
            if key.lower().replace("-", "_") in SECRET_KEYS or secret_key_present(child):
                return True
    elif isinstance(value, list):
        return any(secret_key_present(child) for child in value)
    return False


def exact_directory(value, base, label):
    if not isinstance(value, str) or not value.strip():
        raise SetupBlocked("missing_" + label, label + " must reference an existing directory")
    path = Path(value).expanduser()
    path = (path if path.is_absolute() else base / path).absolute()
    if path.resolve() != path or ".." in path.parts or not path.is_dir():
        raise SetupBlocked("invalid_" + label, label + " must be an existing non-symlink directory")
    if path.parent == path:
        raise SetupBlocked("broad_" + label, "A filesystem root is not an eligible " + label)
    return str(path)


def discover_context(explicit, cwd, environ):
    if explicit:
        paths = [Path(explicit).expanduser().absolute()]
    else:
        paths = []
        configured = environ.get("JARVIS_LAUNCHER_CONFIG")
        if configured:
            paths.append(Path(configured).expanduser().absolute())
        for relative in ("runtime/config/launcher.json", "config/launcher.json"):
            path = Path(cwd).absolute() / relative
            if path.exists() or path.is_symlink():
                paths.append(path)
    paths = list(dict.fromkeys(paths))
    if not paths:
        raise SetupBlocked("execution_context_missing",
                           "No existing Jarvis launcher context was selected; supply --context-config")
    if len(paths) != 1:
        raise SetupBlocked("execution_context_ambiguous",
                           "More than one existing context was found; supply one exact --context-config")
    return paths[0]


def classify_disabled(raw, source, snapshot):
    if raw.get("task_creation_user_disabled") is True:
        return "user_disabled", []
    if "live_creation_enabled" not in raw:
        return "installer_execution_default", []
    if raw.get("live_creation_enabled") is True:
        return "already_enabled", []
    if raw.get("live_creation_enabled") is not False:
        return "unknown", []
    origin_path = source.with_name("configuration-origin.json")
    if not origin_path.exists():
        return "unknown_disabled", []
    origin, origin_snapshot = safe_json(origin_path)
    if (origin.get("schema") == SCHEMA
            and origin.get("launcher_sha256") == snapshot["sha256"]
            and origin.get("creation_setting_origin") == "installer_isolated_default"
            and origin.get("user_disabled") is False):
        return "installer_isolated_default", [origin_snapshot]
    return "user_or_changed_disabled", [origin_snapshot]


def capacity_setting(raw, source, snapshot):
    """Use the new user-configurable ceiling, never provider capacity or a dispatch count."""
    if "worker_capacity" not in raw:
        return DEFAULT_WORKER_CAPACITY, "installer_default", []
    value = raw["worker_capacity"]
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 99:
        raise SetupBlocked("invalid_worker_capacity", "Configured worker_capacity must be an integer between 1 and 99")
    origin_path = source.with_name("configuration-origin.json")
    if not origin_path.exists():
        return value, "existing_explicit", []
    origin, origin_snapshot = safe_json(origin_path)
    default = origin.get("capacity_default_value")
    if (origin.get("schema") == SCHEMA
            and origin.get("launcher_sha256") == snapshot["sha256"]
            and origin.get("capacity_setting_origin") == "installer_default"
            and origin.get("capacity_user_configured") is False
            and isinstance(default, int) and not isinstance(default, bool)
            and default == value):
        return DEFAULT_WORKER_CAPACITY, "installer_default", [origin_snapshot]
    # No matching provenance means an existing explicit value, not an old default.
    return value, "existing_explicit", [origin_snapshot]


def prepare_configuration(*, explicit=None, cwd=None, environ=None):
    environ = dict(os.environ if environ is None else environ)
    source = discover_context(explicit, Path.cwd() if cwd is None else cwd, environ)
    raw, snapshot = safe_json(source)
    if secret_key_present(raw):
        raise SetupBlocked("secret_bearing_context_refused",
                           "Context contains authentication fields; use a non-secret launcher configuration")
    if set(raw) - ALLOWED_KEYS:
        raise SetupBlocked("unsupported_context_fields",
                           "Context has unsupported fields; no unknown setting will be silently dropped")
    origin, extra_snapshots = classify_disabled(raw, source, snapshot)
    if origin in ("user_disabled", "user_or_changed_disabled"):
        raise SetupBlocked("task_creation_disabled_by_user", "Preserve the existing explicit or changed disabled setting")
    if origin in ("unknown", "unknown_disabled"):
        raise SetupBlocked("disabled_origin_unverified",
                           "Creation is disabled without matching installer-origin evidence; do not guess whether it was paused")
    capacity, capacity_origin, capacity_snapshots = capacity_setting(raw, source, snapshot)
    config = copy.deepcopy(raw)
    config["worker_capacity"] = capacity
    config.pop("task_creation_user_disabled", None)
    snapshots = [snapshot, *extra_snapshots, *capacity_snapshots]
    base = source.parent
    if not isinstance(config.get("dispatcher_thread_id"), str) or not config["dispatcher_thread_id"].strip():
        raise SetupBlocked("dispatcher_identity_missing", "The existing context must identify its dispatcher")
    config["expected_codex_home"] = exact_directory(config.get("expected_codex_home"), base, "codex_home")
    projects = config.get("allowed_projects")
    if not isinstance(projects, dict) or not projects or len(projects) > 100:
        raise SetupBlocked("authorized_projects_missing", "Use a non-empty existing authorized project map")
    normalized = {}
    for name, value in projects.items():
        if not isinstance(name, str) or not name.strip():
            raise SetupBlocked("invalid_project_name", "Project names must be non-empty strings")
        project = exact_directory(value, base, "project")
        if {".codex", ".ssh", ".aws", ".gnupg"}.intersection(Path(project).parts):
            raise SetupBlocked("credential_project_refused", "A credential directory is not an authorized project")
        normalized[name] = project
    config["allowed_projects"] = normalized
    configured_cli = config.get("codex_cli")
    if configured_cli in (None, "", "auto"):
        configured_cli = shutil.which("codex", path=environ.get("PATH"))
    elif not Path(str(configured_cli)).is_absolute():
        configured_cli = (str(base / configured_cli) if "/" in configured_cli
                          else shutil.which(configured_cli, path=environ.get("PATH")))
    if not configured_cli:
        raise SetupBlocked("existing_codex_cli_missing", "No existing Codex CLI was found; no software will be downloaded")
    cli = Path(configured_cli).absolute()
    if cli.name not in ("codex", "codex.exe") or not cli.is_file() or not os.access(cli, os.X_OK):
        raise SetupBlocked("existing_codex_cli_invalid", "The existing CLI must be an executable named codex")
    environment_cli = shutil.which("codex", path=environ.get("PATH"))
    if not environment_cli or cli.resolve() != Path(environment_cli).resolve():
        raise SetupBlocked("codex_cli_provenance_conflict",
                           "Context CLI differs from the existing selected environment CLI; do not execute it implicitly")
    config["codex_cli"] = str(cli)
    if config.get("jarvis_connection"):
        raise SetupBlocked("gateway_context_not_supported", "Do not implicitly copy gateway or authentication overrides")
    if config.get("runtime_constraints") != CONSTRAINTS or config.get("runtime_environments") != []:
        raise SetupBlocked("execution_constraints_unverified",
                           "Existing context must explicitly constrain OpenAI, read-only, never-approval, and no extra environments")
    if config.get("linux_cloud") is not True:
        raise SetupBlocked("linux_context_unverified", "This release requires an explicit Linux context")
    if any(key.startswith("CODEX_EXEC_SERVER_NOISE_") for key in environ):
        raise SetupBlocked("inherited_noise_route", "Preserve the original environment guard; do not strip routing conflicts")
    if environ.get("CODEX_EXEC_SERVER_URL") not in (None, "", "none"):
        raise SetupBlocked("inherited_exec_route", "An inherited exec-server route conflicts with this context")
    if config.get("owner_control_enabled", False) is True and (
            not config.get("unix_owner") or config.get("unix_initial_dispatch") is not True):
        raise SetupBlocked("owner_control_pin_missing", "Explicit owner-control mode requires its existing exact pin")
    has_policy = config.get("business_quarantine_file") is not None
    has_generation = config.get("scheduling_generation_id") is not None
    if has_generation and not has_policy:
        raise SetupBlocked("generation_policy_missing", "A scheduling generation cannot lose its policy binding")
    if has_policy:
        policy = Path(str(config["business_quarantine_file"]))
        if not policy.is_absolute():
            raise SetupBlocked("policy_path_not_absolute", "Preserve an exact absolute policy reference")
        _, policy_snapshot = safe_json(policy, limit=8 * 1024 * 1024)
        policy_snapshot["read_limit"] = 8 * 1024 * 1024
        snapshots.append(policy_snapshot)
        if has_generation:
            if not re.fullmatch(r"budget-[a-f0-9]{64}", str(config["scheduling_generation_id"])):
                raise SetupBlocked("invalid_generation_binding", "Scheduling generation identity is malformed")
            _, generation_snapshot = safe_json(policy.parent / "scheduling-budget-generation.json",
                                               limit=8 * 1024 * 1024)
            generation_snapshot["read_limit"] = 8 * 1024 * 1024
            snapshots.append(generation_snapshot)
    config["live_creation_enabled"] = True
    return {"config": config, "snapshots": snapshots, "source": str(source),
            "source_sha256": snapshot["sha256"], "source_creation_origin": origin,
            "capacity_setting_origin": capacity_origin,
            "configured_total_capacity": capacity, "provider_capacity_verified": False,
            "configuration_ready": True, "cli_snapshot": executable_snapshot(cli),
            "credential_content_read_by_installer": False, "credentials_copied": False,
            "policy_binding_preserved": has_policy, "project_scope_changed": False}


def auth_status(config, expected_cli, *, environ=None, runner=subprocess.run):
    """Use the selected existing CLI's own status; never open its auth files.

    A version string establishes compatible protocol, not vendor provenance.
    Trust comes from the explicitly selected existing environment/context.
    """
    original = dict(os.environ if environ is None else environ)
    allowed = ("PATH", "HOME", "LANG", "LC_ALL", "SystemRoot", "SYSTEMROOT", "TEMP", "TMP")
    env = {key: value for key, value in original.items() if key in allowed}
    env["CODEX_HOME"] = config["expected_codex_home"]
    command = [config["codex_cli"]]
    try:
        verify_executable(expected_cli)
        version = runner(command + ["--version"], env=env, capture_output=True, text=True,
                         check=False, timeout=15)
        recognized = version.returncode == 0 and re.fullmatch(
            r"codex-cli \d+\.\d+\.\d+(?:[A-Za-z0-9.+-]*)?\s*", version.stdout.strip()
        )
        del version
        if not recognized:
            return {"verified": False, "status": "unrecognized_existing_cli",
                    "raw_output_retained": False}
        verify_executable(expected_cli)
        result = runner(command + ["login", "status"], env=env, capture_output=True,
                        text=True, check=False, timeout=15)
        text = (result.stdout + "\n" + result.stderr)
        logged_in = result.returncode == 0 and any(
            line.strip().startswith("Logged in using ") for line in text.splitlines()
        )
        del result, text
        verify_executable(expected_cli)
        return {"verified": bool(logged_in),
                "status": "existing_identity_verified" if logged_in else "existing_identity_not_verified",
                "raw_output_retained": False, "cli_protocol_recognized": True,
                "vendor_signature_verified": False,
                "identity_used_by_existing_cli_only": True}
    except SetupBlocked as exc:
        return {"verified": False, "status": exc.code, "raw_output_retained": False}
    except (OSError, subprocess.SubprocessError):
        return {"verified": False, "status": "identity_status_unavailable",
                "raw_output_retained": False}


def state(*, profile, configuration_ready=False, identity_verified=False, blockers=None):
    blockers = list(blockers or [])
    isolated = profile == "isolated"
    enabled = configuration_ready and identity_verified and not isolated and not blockers
    return {
        "profile": profile,
        "status": ("installed_isolated" if isolated and configuration_ready else
                   "configured_for_task_creation" if enabled else "installed_but_not_ready"),
        "installation_ready": True,
        "configuration_ready": bool(configuration_ready),
        "identity_status_verified": bool(identity_verified),
        "task_creation_enabled": bool(enabled),
        "execution_ready": None,
        "execution_verification": "not_run",
        "live_execution_verified": False,
        "task_started": False,
        "business_dispatch_count": 0,
        "services_started": False,
        "credential_content_read_by_installer": False,
        "credentials_copied": False,
        "authentication_modified": False,
        "persistent_authentication_created": False,
        "blockers": blockers,
    }
