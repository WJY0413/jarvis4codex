"""Bounded Python-level offline validation. Not an OS network sandbox."""
from pathlib import Path
import atexit
import json
import os
import runpy
import sys

MODE, LOG = sys.argv[1:3]
ARGS = sys.argv[3:]
NETWORK = {
    "socket.connect", "socket.bind", "socket.getaddrinfo",
    "socket.gethostbyname", "socket.gethostbyaddr", "socket.sendto",
}
SPAWN = {"subprocess.Popen", "os.system", "os.posix_spawn",
         "os.posix_spawnp", "os.exec", "os.spawn"}
allowed_child = None
record = {"mode": MODE, "python_audit_network_denial": True,
          "os_network_namespace": False, "blocked_attempts": [], "closed": False}


def save():
    Path(LOG).write_text(json.dumps(record, indent=2) + "\n")


def close():
    record["closed"] = True
    save()
    if record["blocked_attempts"]:
        os._exit(97)


def audit(event, args):
    if event in NETWORK or event in SPAWN:
        if event == "subprocess.Popen" and allowed_child is not None:
            cmd = args[1]
            if isinstance(cmd, (tuple, list)) and list(cmd[:len(allowed_child)]) == allowed_child:
                return
        record["blocked_attempts"].append(event)
        save()
        raise RuntimeError("Offline phase forbids " + event)


save()
atexit.register(close)
sys.addaudithook(audit)

if MODE == "venv":
    import venv
    venv.EnvBuilder(with_pip=False, symlinks=False).create(ARGS[0])
elif MODE == "pip":
    pip_wheel, *pip_args = ARGS
    sys.path.insert(0, pip_wheel)
    # Offline pip needs neither local IPv6 discovery nor platform/compiler
    # user-agent probes. Suppress those optional probes before their imports;
    # do not relax any audit denial or permit helper subprocesses.
    import socket
    socket.has_ipv6 = False
    from pip._internal.network import session as pip_session
    pip_session.user_agent = lambda: "pip/offline-verified-installer"
    sys.argv = ["pip", *pip_args]
    runpy.run_module("pip", run_name="__main__")
elif MODE == "build":
    source, destination = ARGS
    os.chdir(source)
    from setuptools.build_meta import build_wheel
    print(build_wheel(destination))
elif MODE == "identity":
    import importlib.metadata as metadata
    import platform
    import sysconfig
    from jarvis_control.operations import source_identity
    from jarvis_control.sustained_runner import source_sha256 as runner_source_sha256
    result = {
        "python": sys.version.split()[0], "executable": sys.executable,
        "soabi": sysconfig.get_config_var("SOABI"), "libc": platform.libc_ver(),
        "identity": source_identity(), "runner_source_sha256": runner_source_sha256(),
        "versions": {
            d.metadata["Name"].lower().replace("_", "-"): d.version
            for d in metadata.distributions()
        },
    }
    print(json.dumps(result))
elif MODE == "configuration":
    from jarvis_runtime.jarvis_native_task_launcher import NativeTaskLauncherConfig
    config = NativeTaskLauncherConfig(Path(ARGS[0]))
    if config.unix_owner_pin_json is not None:
        from jarvis_runtime.unix_owner_transport import OwnerEndpoint
        OwnerEndpoint.from_dict(json.loads(config.unix_owner_pin_json))
    print(json.dumps({"status": "valid", "live_creation_enabled": config.live_creation_enabled,
                      "project_count": len(config.allowed_projects),
                      "services_started": False, "connections_opened": False,
                      "tasks_started": 0}))
elif MODE == "mcp":
    sys.argv = ["jarvis_mcp.entrypoint", *ARGS]
    runpy.run_module("jarvis_mcp.entrypoint", run_name="__main__")
elif MODE == "smoke":
    import subprocess
    original_popen = subprocess.Popen
    child_log = str(Path(LOG).with_name("mcp.audit.json"))
    allowed_child = [sys.executable, "-I", "-B", str(Path(__file__).resolve()),
                     "mcp", child_log]

    class GuardedPopen(original_popen):
        # Keep Popen a class: MCP evaluates Popen[bytes] in type annotations.
        def __init__(self, args, *a, **kw):
            expected = [sys.executable, "-I", "-B", "-m", "jarvis_mcp.entrypoint"]
            if not isinstance(args, (list, tuple)) or list(args[:len(expected)]) != expected:
                raise RuntimeError("Only the zero-model MCP child is allowed")
            super().__init__(allowed_child + list(args[len(expected):]), *a, **kw)

    subprocess.Popen = GuardedPopen
    sys.argv = ["jarvis_deploy.smoke", "--release", ARGS[0]]
    runpy.run_module("jarvis_deploy.smoke", run_name="__main__")
elif MODE == "probe_child":
    import subprocess
    child_log = str(Path(LOG).with_name("identity-child.audit.json"))
    allowed_child = [sys.executable, "-I", "-B", str(Path(__file__).resolve()),
                     "identity", child_log]
    completed = subprocess.run(allowed_child, check=True, capture_output=True, text=True)
    data = json.loads(completed.stdout)
    assert data["executable"] == sys.executable
    print(json.dumps({"status": "passed", "same_python": True,
                      "child_identity": data["identity"]}))
else:
    raise ValueError("Unknown phase: " + MODE)
