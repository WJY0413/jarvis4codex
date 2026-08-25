"""Invoke one registered Jarvis MCP tool and print its protocol receipt."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from mcp import Client, StdioServerParameters


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--config", type=Path, required=True)
    value.add_argument("--tool", required=True)
    value.add_argument("--arguments-json", required=True)
    value.add_argument("--receipt-path", type=Path)
    return value


async def invoke(config_path: Path, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
    import tomllib

    config = tomllib.loads(config_path.read_text(encoding="utf-8-sig"))
    registered = config["mcp_servers"]["jarvis"]
    parameters = StdioServerParameters(
        command=registered["command"],
        args=registered["args"],
        env=registered.get("env"),
    )
    async with Client(parameters) as client:
        result = await client.call_tool(tool, arguments)
    return result.model_dump(by_alias=True)


def main() -> int:
    args = parser().parse_args()
    arguments = json.loads(args.arguments_json)
    if not isinstance(arguments, dict):
        raise SystemExit("--arguments-json must be an object")
    receipt = json.dumps(asyncio.run(invoke(args.config, args.tool, arguments)), ensure_ascii=False)
    if args.receipt_path:
        args.receipt_path.parent.mkdir(parents=True, exist_ok=True)
        args.receipt_path.write_text(receipt + "\n", encoding="utf-8")
    print(receipt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
