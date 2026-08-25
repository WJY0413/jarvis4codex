"""Stdio process entry point for a locally registered Jarvis MCP server."""

from __future__ import annotations

import argparse
from pathlib import Path

from adapters.codex_app_server.mcp_wiring import build_jarvis_control

from .server import JarvisMcpServer


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True, help="deployed Jarvis heartbeat-service config")
    parser.add_argument(
        "--launcher-config",
        type=Path,
        required=True,
        help="deployed Jarvis native-task-launcher config for jarvis_create",
    )
    parser.add_argument("--state-dir", type=Path, required=True, help="isolated MCP receipt and monitor state directory")
    parser.add_argument("--local-heartbeat-config", type=Path, help="local scheduler config, kept separate from App Server transport")
    parser.add_argument("--notification-config", type=Path, help="verified Jarvis notification adapter config")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    control = build_jarvis_control(
        args.config,
        args.state_dir,
        launcher_config_path=args.launcher_config,
        local_heartbeat_config_path=args.local_heartbeat_config,
        notification_config_path=args.notification_config,
    )
    server = JarvisMcpServer(control)
    try:
        server.run_stdio()
    finally:
        control.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
