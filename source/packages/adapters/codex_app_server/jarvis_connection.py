"""Optional Jarvis-owned startup boundary for model-bound local gateways."""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit


OFFICIAL_BASE_URL = "https://chatgpt.com/backend-api/codex"
GATEWAY_BASE_URL = "http://127.0.0.1:4003/v1"
GATEWAY_HEALTH_URL = "http://127.0.0.1:4003/healthz"
PRIVATE_MODEL_PREFIXES = ("fjd-", "jean-", "lesli-", "chatgpt-web/")


class JarvisConnectionError(RuntimeError):
    """The requested private route cannot be prepared safely."""


def is_private_model(model: str | None) -> bool:
    return str(model or "").startswith(PRIVATE_MODEL_PREFIXES)


def _probe_health(url: str = GATEWAY_HEALTH_URL) -> dict[str, Any] | None:
    # This loopback probe must not inherit HTTPS_PROXY/HTTP_PROXY.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(url, timeout=2) as response:
            if response.status != 200:
                return None
            payload = json.load(response)
    except (OSError, ValueError, urllib.error.URLError):
        return None
    if not isinstance(payload, dict) or payload.get("service") != "private-additive-router":
        return None
    return payload


def _is_loopback_http_url(value: Any, *, path: str) -> bool:
    if not isinstance(value, str) or not value:
        return False
    try:
        parsed = urlsplit(value)
        return (
            parsed.scheme == "http"
            and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
            and parsed.port is not None
            and parsed.username is None
            and parsed.password is None
            and parsed.path.rstrip("/") == path.rstrip("/")
            and not parsed.query
            and not parsed.fragment
        )
    except ValueError:
        return False


def _connection_for_model(
    settings: Mapping[str, Any], model: str,
) -> tuple[str, str, str | None, list[str] | None, str | None]:
    """Resolve a model to one named local gateway and a non-secret credential reference."""
    model_connections = settings.get("model_connections")
    if model_connections is None:
        return (
            GATEWAY_BASE_URL,
            GATEWAY_HEALTH_URL,
            None,
            settings.get("start_command"),
            settings.get("log_path"),
        )
    if not isinstance(model_connections, Mapping):
        raise JarvisConnectionError("jarvis-connection model_connections must be an object")
    connection_id = model_connections.get(model)
    if not isinstance(connection_id, str) or not connection_id:
        # Named connections are additive: an unmapped private model keeps the
        # legacy 4003 route instead of being rejected by a Jean-only mapping.
        return (
            GATEWAY_BASE_URL,
            GATEWAY_HEALTH_URL,
            None,
            settings.get("start_command"),
            settings.get("log_path"),
        )
    connections = settings.get("connections")
    if not isinstance(connections, Mapping):
        raise JarvisConnectionError("jarvis-connection connections must be an object")
    connection = connections.get(connection_id)
    if not isinstance(connection, Mapping):
        raise JarvisConnectionError(f"jarvis-connection names unknown connection {connection_id!r}")

    base_url = connection.get("base_url")
    health_url = connection.get("health_url")
    credential_ref = connection.get("credential_ref")
    if not _is_loopback_http_url(base_url, path="/v1"):
        raise JarvisConnectionError(f"connection {connection_id!r} base_url must be a loopback /v1 URL")
    if not _is_loopback_http_url(health_url, path="/healthz"):
        raise JarvisConnectionError(f"connection {connection_id!r} health_url must be a loopback /healthz URL")
    base_parts = urlsplit(base_url)
    health_parts = urlsplit(health_url)
    if (base_parts.hostname, base_parts.port) != (health_parts.hostname, health_parts.port):
        raise JarvisConnectionError(f"connection {connection_id!r} base_url and health_url must use the same listener")
    if not isinstance(credential_ref, str) or not re.fullmatch(
        r"(?:dpapi:[A-Za-z0-9][A-Za-z0-9._-]{0,62}|env:[A-Z_][A-Z0-9_]*)",
        credential_ref,
    ):
        raise JarvisConnectionError(
            f"connection {connection_id!r} credential_ref must name a DPAPI profile or environment variable"
        )

    command = connection.get("start_command")
    if command is not None:
        if not isinstance(command, list) or not all(isinstance(part, str) and part for part in command):
            raise JarvisConnectionError(f"connection {connection_id!r} start_command must be a string array")
        command = [part.replace("{credential_ref}", credential_ref) for part in command]
    log_path = connection.get("log_path")
    return base_url, health_url, credential_ref, command, log_path


def _start_gateway(command: list[str], log_path: str) -> None:
    if len(command) < 2 or not all(isinstance(part, str) and part for part in command):
        raise JarvisConnectionError("jarvis-connection start_command must contain an executable and script")
    executable = Path(command[0])
    script_arg = command[1]
    if script_arg == "-File" and len(command) > 2:
        script_arg = command[2]
    script = Path(script_arg)
    if not executable.is_absolute() or not executable.is_file() or not script.is_absolute() or not script.is_file():
        raise JarvisConnectionError("jarvis-connection executable and script must be existing absolute files")
    log = Path(log_path)
    if not log.is_absolute():
        raise JarvisConnectionError("jarvis-connection log_path must be absolute")
    log.parent.mkdir(parents=True, exist_ok=True)
    startupinfo = None
    creationflags = 0
    if os.name == "nt":
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    with log.open("ab") as output:
        subprocess.Popen(
            command, cwd=str(script.parent), stdin=subprocess.DEVNULL,
            stdout=output, stderr=subprocess.STDOUT, shell=False,
            startupinfo=startupinfo, creationflags=creationflags,
        )


def route_for_model(
    settings: Mapping[str, Any] | None,
    model: str | None,
    *,
    allow_start: bool = True,
    probe: Callable[[], dict[str, Any] | None] = _probe_health,
    probe_url: Callable[[str], dict[str, Any] | None] = _probe_health,
    start: Callable[[list[str], str], None] = _start_gateway,
    sleep: Callable[[float], None] = time.sleep,
) -> str | None:
    """Return the per-child Codex base URL, starting only the selected private gateway."""
    if not settings or settings.get("enabled") is not True:
        return None
    model_connections = settings.get("model_connections")
    mapped_model = (isinstance(model, str) and bool(model.strip())
                    and isinstance(model_connections, Mapping) and model in model_connections)
    if not is_private_model(model) and not mapped_model:
        return OFFICIAL_BASE_URL
    if not isinstance(model, str):
        raise JarvisConnectionError("jarvis-connection requires a model name for private routes")
    base_url, health_url, _credential_ref, command, log_path = _connection_for_model(settings, model)
    named_route = settings.get("model_connections") is not None
    check = (lambda: probe_url(health_url)) if named_route else probe
    health = check()
    if health is None:
        if not allow_start:
            raise JarvisConnectionError(f"recovery cannot start unavailable gateway at {health_url}")
        if not isinstance(command, list) or not isinstance(log_path, str) or not log_path:
            raise JarvisConnectionError("jarvis-connection needs start_command and log_path when its gateway is down")
        start(command, log_path)
        deadline = time.monotonic() + max(float(settings.get("startup_timeout_seconds", 10)), 1)
        while health is None and time.monotonic() < deadline:
            sleep(0.2)
            health = check()
    if health is None:
        raise JarvisConnectionError(f"jarvis-connection did not become healthy at {health_url}")
    # Health proves listener identity, not an authoritative model allowlist.
    # The configured route's actual upstream decides model support.
    return base_url
