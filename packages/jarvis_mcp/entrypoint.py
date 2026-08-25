"""Stdio process entry point for a locally registered Jarvis MCP server."""

from __future__ import annotations

import argparse
from pathlib import Path

from adapters.codex_app_server.mcp_wiring import build_jarvis_control

from .server import JarvisMcpServer


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True, help="deployed Jarvis heartbeat-service config")
    parser.add_argument("--state-dir", type=Path, required=True, help="isolated MCP receipt and monitor state directory")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    server = JarvisMcpServer(build_jarvis_control(args.config, args.state_dir))
    server.run_stdio()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
