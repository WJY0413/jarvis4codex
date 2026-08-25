"""Start the registered Jarvis MCP process and verify its tool catalogue only."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from mcp import Client, StdioServerParameters


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--config", type=Path, required=True)
    return value


async def verify(config_path: Path) -> dict[str, object]:
    import tomllib

    config = tomllib.loads(config_path.read_text(encoding="utf-8-sig"))
    registered = config["mcp_servers"]["jarvis"]
    parameters = StdioServerParameters(
        command=registered["command"],
        args=registered["args"],
        env=registered.get("env"),
    )
    async with Client(parameters) as client:
        result = await client.list_tools()
    return {"ok": True, "tools": [tool.name for tool in result.tools]}


def main() -> int:
    args = parser().parse_args()
    print(json.dumps(asyncio.run(verify(args.config)), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
