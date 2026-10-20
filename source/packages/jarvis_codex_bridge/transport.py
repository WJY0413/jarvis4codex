"""The sole external seam used by the standard Codex Bridge package."""

from __future__ import annotations

from typing import Protocol

from .contracts import ResumeRequest, StartedTurn, ThreadState


class ExistingThreadTransport(Protocol):
    """Transport for a desktop-owned, already-existing Codex thread.

    Creation is deliberately absent.  A caller must provide the real thread id.
    """

    name: str

    def health(self) -> dict[str, object]: ...

    def read_thread(self, thread_id: str) -> ThreadState: ...

    def resume_existing(self, request: ResumeRequest) -> StartedTurn: ...
