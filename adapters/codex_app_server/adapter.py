from jarvis_contracts import ExecutionReceipt, TargetRef


class CodexAppServerAdapter:
    """Reserved compatibility boundary for existing Codex App Server operations."""

    name = "codex-app-server"

    def health(self) -> dict[str, object]:
        return {"adapter": self.name, "status": "not_configured"}

    def preflight(self, target: TargetRef) -> None:
        if target.harness != self.name:
            raise ValueError("target harness does not match adapter")

    def trigger(self, target: TargetRef, envelope_ref: str) -> ExecutionReceipt:
        raise NotImplementedError("Codex App Server transport is not migrated in v0.1.0")

    def read_run(self, execution_id: str) -> ExecutionReceipt:
        raise NotImplementedError("Codex App Server transport is not migrated in v0.1.0")
