from typing import Protocol

from jarvis_contracts import ExecutionReceipt, TargetRef


class HarnessAdapter(Protocol):
    """A stable boundary around a concrete agent harness."""

    name: str

    def health(self) -> dict[str, object]: ...

    def preflight(self, target: TargetRef) -> None: ...

    def trigger(self, target: TargetRef, envelope_ref: str) -> ExecutionReceipt: ...

    def read_run(self, execution_id: str) -> ExecutionReceipt: ...
