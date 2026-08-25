"""Public control facade used by harness-neutral entry points such as MCP."""

from .service import JARVIS_MCP_RECEIPT_SCHEMA, JarvisControl

__all__ = ["JARVIS_MCP_RECEIPT_SCHEMA", "JarvisControl"]
