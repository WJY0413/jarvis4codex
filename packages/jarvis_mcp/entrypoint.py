"""Local Jarvis MCP entry point: stdio or explicit shared loopback HTTP."""

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
    parser.add_argument("--transport", choices=("stdio", "streamable-http"), default="stdio")
    parser.add_argument("--port", type=int, help="required loopback port for streamable-http")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.transport == "streamable-http" and (args.port is None or not 1 <= args.port <= 65535):
        parser.error("streamable-http requires --port between 1 and 65535")
    if args.transport == "stdio" and args.port is not None:
        parser.error("--port requires --transport streamable-http")
    control = build_jarvis_control(
        args.config,
        args.state_dir,
        launcher_config_path=args.launcher_config,
        local_heartbeat_config_path=args.local_heartbeat_config,
        notification_config_path=args.notification_config,
    )
    server = JarvisMcpServer(control)
    try:
        if args.transport == "streamable-http":
            server.run_http(port=args.port)
        else:
            server.run_stdio()
    finally:
        control.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
