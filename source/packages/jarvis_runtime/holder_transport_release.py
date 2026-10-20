"""Local attachment cleanup evidence, distinct from native execution terminality.

Callers must independently require the exact native terminal receipt and the
Holder's return before releasing a controlled Hold. A Unix attachment's closure
never means that its shared owner process has exited.
"""
from __future__ import annotations

import json
import re
from typing import Any


SCHEMA = "jarvis-holder-transport-release/v1"
IDENTITY_KEYS = ("request_id", "hold_id", "thread_id", "turn_id")
_KEYS = {"schema", "mode", "owner_instance_id", "connection_epoch",
         "local_connection_closed", "owner_process_exit_confirmed", *IDENTITY_KEYS}
_BINDING_KEYS = {"schema", "mode", "request_id", "hold_id", "owner_instance_id"}


def _binding_valid(binding, identity) -> bool:
    return (isinstance(binding, dict) and set(binding) == _BINDING_KEYS
            and binding.get("schema") == "jarvis-holder-owner-binding/v1"
            and binding.get("mode") == "unix_owner"
            and isinstance(binding.get("owner_instance_id"), str)
            and re.fullmatch(r"[a-f0-9]{64}", binding["owner_instance_id"]) is not None
            and all(isinstance(identity.get(key), str) and bool(identity[key])
                    and binding.get(key) == identity[key] for key in ("request_id", "hold_id")))


def configured_unix_owner_binding(client, request: dict[str, Any]) -> dict | None:
    """Obtain the expected pin from frozen verified configuration, before dispatch."""
    raw = getattr(getattr(client, "config", None), "unix_owner_pin_json", None)
    if raw is None:
        return None
    pin = json.loads(raw)
    evidence = client.unix_connection_evidence()
    binding = {
        "schema": "jarvis-holder-owner-binding/v1", "mode": "unix_owner",
        "request_id": request.get("request_id"), "hold_id": request.get("hold_id"),
        "owner_instance_id": pin.get("instance_id"),
    }
    if (not _binding_valid(binding, request)
            or evidence.get("schema") != "jarvis-unix-connection/v1"
            or evidence.get("owner_instance_id") != binding["owner_instance_id"]):
        raise RuntimeError("Configured Unix owner differs from client attachment pin")
    return binding


def unix_attachment_release_valid(receipt: dict[str, Any], request: dict | None = None) -> bool:
    """Validate only exact-generation local cleanup, never native terminality."""
    proof = receipt.get("holder_transport_release")
    if not isinstance(proof, dict) or set(proof) != _KEYS:
        return False
    request = request if isinstance(request, dict) else {}
    binding = request.get("owner_transport_binding")
    if (not _binding_valid(binding, request)
            or any(request.get(key) != receipt.get(key) for key in ("request_id", "hold_id"))
            or any(request.get(key) not in (None, "", receipt.get(key))
                   for key in ("thread_id", "turn_id"))
            or proof.get("owner_instance_id") != binding["owner_instance_id"]):
        return False
    stop = request.get("host_stop")
    if stop is not None and (not isinstance(stop, dict)
            or any(stop.get(key) != receipt.get(key) for key in IDENTITY_KEYS)):
        return False
    return (
        proof.get("schema") == SCHEMA
        and proof.get("mode") == "unix_owner"
        and isinstance(proof.get("owner_instance_id"), str)
        and re.fullmatch(r"[a-f0-9]{64}", proof["owner_instance_id"]) is not None
        and type(proof.get("connection_epoch")) is int
        and proof["connection_epoch"] > 0
        and proof.get("local_connection_closed") is True
        and proof.get("owner_process_exit_confirmed") is False
        and receipt.get("local_observer_stopped") is True
        and receipt.get("holder_client_exit_confirmed") is False
        and all(isinstance(receipt.get(key), str) and bool(receipt[key])
                and proof.get(key) == receipt[key] for key in IDENTITY_KEYS)
    )


def holder_transport_cleanup_confirmed(receipt: dict[str, Any], request: dict | None = None) -> bool:
    """Legacy owned-process exit OR strict attach-only local cleanup.

    A present malformed Unix proof must not downgrade to the stdio path.
    Neither branch on its own permits release of a native turn.
    """
    if "holder_transport_release" in receipt:
        return unix_attachment_release_valid(receipt, request)
    if isinstance(request, dict) and "owner_transport_binding" in request:
        return False  # A pinned Unix generation cannot downgrade to stdio proof.
    return receipt.get("holder_client_exit_confirmed") is True


def make_unix_attachment_release(client, receipt: dict[str, Any], request: dict) -> dict | None:
    """Capture read-only close evidence from the exact attached client instance."""
    try:
        health = client.transport_health()
        evidence = client.unix_connection_evidence()
        if (health.get("schema") != "jarvis-client-transport-health/v1"
                or health.get("mode") != "unix_owner"
                or health.get("local_connection_closed") is not True
                or evidence.get("schema") != "jarvis-unix-connection/v1"
                or evidence.get("connected") is not False):
            return None
        proof = {
            "schema": SCHEMA, "mode": "unix_owner",
            **{key: receipt.get(key) for key in IDENTITY_KEYS},
            "owner_instance_id": evidence.get("owner_instance_id"),
            "connection_epoch": evidence.get("epoch"),
            "local_connection_closed": True,
            "owner_process_exit_confirmed": False,
        }
        candidate = {**receipt, "holder_transport_release": proof,
                     "holder_client_exit_confirmed": False, "local_observer_stopped": True}
        return proof if unix_attachment_release_valid(candidate, request) else None
    except Exception:
        return None
