"""Opt-in cloud bootstrap. Does not mutate host configuration or credentials."""
from __future__ import annotations

import os
import stat
from pathlib import Path, PureWindowsPath
from typing import Mapping

SAFE_ENV = frozenset("PATH HOME LANG LC_ALL TERM TMPDIR HTTP_PROXY HTTPS_PROXY ALL_PROXY NO_PROXY http_proxy https_proxy all_proxy no_proxy SSL_CERT_FILE SSL_CERT_DIR REQUESTS_CA_BUNDLE CURL_CA_BUNDLE NODE_EXTRA_CA_CERTS".split())


def config_path(value: str, base: Path, *, command: bool = False) -> str:
    """Resolve file paths against their config, never against process cwd."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("path must be a non-empty string")
    value = value.strip()
    if os.name != "nt" and (PureWindowsPath(value).drive or "\\" in value):
        raise ValueError("Windows path requires an explicit Linux mapping")
    if command and "/" not in value and "\\" not in value:
        return value
    path = Path(value).expanduser()
    return str((path if path.is_absolute() else base / path).resolve())


def child_environment(config, source: Mapping[str, str] | None = None) -> dict[str, str]:
    source = os.environ if source is None else source
    noenv = getattr(config, "runtime_environments", None) is not None
    strict = noenv or getattr(config, "linux_cloud", False)
    if strict:
        if not config.expected_codex_home:
            raise ValueError("cloud runtime requires an explicit independent expected_codex_home")
        # Presence, including a dangling symlink, is a conflict. Never read it.
        if os.path.lexists(Path(config.expected_codex_home) / "environments.toml"):
            raise ValueError("environments.toml conflicts with the isolated cloud runtime")
        if any(k.startswith("CODEX_EXEC_SERVER_NOISE_") for k in source):
            raise ValueError("Noise routing variables conflict with the isolated cloud runtime")
        inherited = source.get("CODEX_EXEC_SERVER_URL")
        if inherited not in (None, "", "none"):
            raise ValueError("an inherited exec-server route conflicts with the isolated cloud runtime")
        env = {k: v for k, v in source.items() if k in SAFE_ENV}
        if noenv:
            env["CODEX_EXEC_SERVER_URL"] = "none"
        # Default-local file-service instances intentionally omit this variable.
    else:
        env = dict(source)  # Legacy Windows behavior is not silently migrated.
    env["PYTHONUTF8"] = "1"
    if config.expected_codex_home:
        env["CODEX_HOME"] = config.expected_codex_home
    return env


def acquire_instance_lock(config):
    """Nonblocking advisory lock shared by all opt-in clients of this HOME."""
    if os.name == "nt" or getattr(config, "exclusive_instance", False) is not True:
        return None
    import fcntl
    path = Path(config.expected_codex_home) / ".jarvis-linux-instance.lock"
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        meta = os.fstat(fd)
        if not stat.S_ISREG(meta.st_mode) or meta.st_uid != os.geteuid() or meta.st_nlink != 1:
            raise ValueError("unsafe instance lock")
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BaseException:
        os.close(fd)
        raise ValueError("independent CODEX_HOME already in use or lock unsafe") from None


def release_instance_lock(fd):
    if fd is not None:
        os.close(fd)
