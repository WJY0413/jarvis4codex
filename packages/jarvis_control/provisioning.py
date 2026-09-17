"""Stable provisioning seam used by JarvisControl callers such as MCP."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Literal, Mapping, Protocol

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError
from referencing import Registry, Resource
from referencing.exceptions import Unresolvable
from referencing.jsonschema import DRAFT202012


def _json_pointer(path: Any) -> str:
    return "/" + "/".join(str(part).replace("~", "~0").replace("/", "~1") for part in path) if path else "(root)"


def lane_batch_size(binding: Mapping[str, Any]) -> int:
    size = binding.get("batch_size", 1)
    if type(size) is not int or size < 1:
        raise ValueError("lane.batch_size must be a positive integer")
    return size


def lane_batch_ids(binding: Mapping[str, Any], turn_number: int) -> list[int]:
    ids = binding.get("candidate_ids")
    size = lane_batch_size(binding)
    if (not isinstance(ids, list) or not ids or any(type(value) is not int or value < 1 for value in ids)
            or len(set(ids)) != len(ids)):
        raise ValueError("lane requires unique positive candidate_ids")
    if type(turn_number) is not int or not 1 <= turn_number <= (len(ids) + size - 1) // size:
        raise ValueError("lane binding has no candidates for the current turn")
    start = (turn_number - 1) * size
    return ids[start:start + size]


def output_schema_validator(schema: Any) -> Draft202012Validator:
    """Draft 2020-12 with document-local references and no external retrieval."""
    try:
        Draft202012Validator.check_schema(schema)
        resource = Resource(schema, DRAFT202012)
        registry = Registry()  # Default retrieval always fails; never opens URLs or files.
        resolver = registry.resolver_with_root(resource)
        pending = [resource]
        seen = set()
        while pending:
            current = pending.pop()
            node = current.contents
            if id(node) in seen:
                continue
            seen.add(id(node))
            Draft202012Validator.check_schema(node)
            if isinstance(node, dict):
                if node.get("$schema", Draft202012Validator.META_SCHEMA["$id"]) != Draft202012Validator.META_SCHEMA["$id"]:
                    raise ValueError("output_schema supports only Draft 2020-12")
                if "$id" in node:
                    raise ValueError("output_schema $id is unsupported; references must stay in the root document")
                for keyword in ("$ref", "$dynamicRef"):
                    if keyword in node:
                        ref = node[keyword]
                        if not ref.startswith("#"):
                            raise ValueError(f"output_schema {keyword} must be a local fragment: {ref}")
                        target = resolver.lookup(ref).contents
                        Draft202012Validator.check_schema(target)
                        pending.append(Resource(target, DRAFT202012))
                pending.extend(Resource(child, DRAFT202012) for child in DRAFT202012.subresources_of(node))
        return Draft202012Validator(schema, registry=registry)
    except SchemaError as exc:
        raise ValueError(f"invalid output_schema at {_json_pointer(exc.absolute_path)}: {exc.message}") from exc
    except (Unresolvable, RecursionError) as exc:
        raise ValueError(f"invalid output_schema reference: {exc}") from exc


def validate_saved_output(schema: Any, output: Any) -> None:
    validator = output_schema_validator(schema)
    try:
        validator.validate(output)
    except ValidationError as exc:
        raise ValueError(f"output_schema mismatch at {_json_pointer(exc.absolute_path)} "
                         f"(schema {_json_pointer(exc.absolute_schema_path)}): {exc.message}") from exc
    except (Unresolvable, RecursionError) as exc:
        raise ValueError(f"output_schema reference evaluation failed: {exc}") from exc


ProvisionStatus = Literal["accepted", "holding", "running", "completed", "failed", "requires_readback", "partial"]


@dataclass(frozen=True)
class TaskProvisionRequest:
    request_id: str
    project: str
    title: str
    prompt: str
    source_ref: str
    model: str | None = None
    reasoning_effort: str | None = None
    max_turns: int = 1
    auto_continue: bool = False
    continue_prompt: str = "继续"
    hold_id: str | None = None
    notifications: Mapping[str, Any] | None = None
    input_binding: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        required = {
            "request_id": self.request_id,
            "project": self.project,
            "title": self.title,
            "prompt": self.prompt,
            "source_ref": self.source_ref,
        }
        missing = [name for name, value in required.items() if not value.strip()]
        if missing:
            raise ValueError("required provision fields: " + ", ".join(missing))
        if not isinstance(self.max_turns, int) or self.max_turns < 1:
            raise ValueError("max_turns must be a positive integer")
        if not self.continue_prompt.strip():
            raise ValueError("continue_prompt is required")
        if self.hold_id is not None and not self.hold_id.strip():
            raise ValueError("hold_id cannot be blank")
        notifications = dict(self.notifications or {})
        if self.input_binding is not None and not isinstance(self.input_binding, Mapping):
            raise ValueError("input_binding must be an object")
        milestones = notifications.get("milestones") or []
        if not isinstance(milestones, list):
            raise ValueError("notifications.milestones must be a list")
        try:
            if any(int(turn) < 1 for turn in milestones):
                raise ValueError("notifications.milestones must be positive")
        except (TypeError, ValueError) as exc:
            raise ValueError("notifications.milestones must contain positive integers") from exc
        if "terminal" in notifications and not isinstance(notifications["terminal"], bool):
            raise ValueError("notifications.terminal must be a boolean")


@dataclass(frozen=True)
class TaskMonitorResumeRequest:
    request_id: str
    task_id: str
    prompt: str
    source_ref: str
    monitor_id: str | None = None
    hold_id: str | None = None
    max_turns: int = 1
    model: str | None = None
    reasoning_effort: str | None = None
    auto_continue: bool = False
    continue_prompt: str = "继续"
    notifications: Mapping[str, Any] | None = None
    input_binding: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        required = {
            "request_id": self.request_id,
            "task_id": self.task_id,
            "prompt": self.prompt,
            "source_ref": self.source_ref,
        }
        missing = [name for name, value in required.items() if not value.strip()]
        if missing:
            raise ValueError("required monitor-resume fields: " + ", ".join(missing))
        if not isinstance(self.max_turns, int) or self.max_turns < 1:
            raise ValueError("max_turns must be a positive integer")
        if self.hold_id is not None and not self.hold_id.strip():
            raise ValueError("hold_id cannot be blank")
        if not self.continue_prompt.strip():
            raise ValueError("continue_prompt is required")
        notifications = dict(self.notifications or {})
        if self.input_binding is not None and not isinstance(self.input_binding, Mapping):
            raise ValueError("input_binding must be an object")
        milestones = notifications.get("milestones") or []
        if not isinstance(milestones, list):
            raise ValueError("notifications.milestones must be a list")
        try:
            if any(int(turn) < 1 for turn in milestones):
                raise ValueError("notifications.milestones must be positive")
        except (TypeError, ValueError) as exc:
            raise ValueError("notifications.milestones must contain positive integers") from exc
        if "terminal" in notifications and not isinstance(notifications["terminal"], bool):
            raise ValueError("notifications.terminal must be a boolean")


@dataclass(frozen=True)
class TaskProvisionReceipt:
    request_id: str
    status: ProvisionStatus
    observed_at: datetime
    thread_id: str | None = None
    turn_id: str | None = None
    output: str | None = None
    reason: str | None = None
    error_code: str | None = None
    phase: str | None = None
    monitor_id: str | None = None
    hold_id: str | None = None
    turn_count: int | None = None
    total_turn_count: int | None = None
    max_turns: int | None = None

    def as_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["observed_at"] = self.observed_at.isoformat()
        return value


class TaskProvisioningPort(Protocol):
    """Create one durable thread, start its first turn, and return exact identity."""

    def provision(self, request: TaskProvisionRequest) -> TaskProvisionReceipt: ...

    def ensure_hold_host_ready(self, *, required_workers: int) -> dict[str, str]: ...

    def request_hold_stop(self, hold_id: str) -> dict[str, Any]: ...


def observed_now() -> datetime:
    return datetime.now(timezone.utc)
