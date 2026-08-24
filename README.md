# Jarvis Control Plane

`jarvis0.1.4` is the harness-neutral foundation for governed agent heartbeats and monitoring.

It separates the stable runtime (engine, monitoring, contracts and adapters) from Codex control skills. The first adapter preserves the future integration point for Codex App Server; it does not operate the existing production scheduler.

## Stable input port

`jarvis_codex_bridge.JarvisCapabilityPort` is the versioned integration input.
It accepts `CapabilityRequest` envelopes for:

- `monitor.observe`
- `resume.existing`
- `monitor.terminal_resume` (a monitor-to-resume composition)
- `heartbeat.create`, `heartbeat.update`, `heartbeat.cancel`, and `heartbeat.health`

Every request carries an explicit request ID, source reference, target thread
and prompt where applicable.  Resume and monitor execute through the supplied
standard bridge; heartbeat operations are delegated to the caller-supplied
Jarvis scheduler port.  This package deliberately does not contain a
HostBridge adapter or any external messaging implementation.

No production scheduler, credential, or existing runtime state is migrated by
installing this package.

## Desktop Host project catalog

Desktop-facing integrations can refresh a host-scoped snapshot of native
`list_projects` results through `DesktopHostProjectCatalogAdapter`.  Each
snapshot records its observation time and the exact Desktop project ID, Host ID
and path.  A failed or empty refresh deliberately keeps the last good snapshot.

Native task launchers can require this catalog to resolve a supplied project
name before starting work.  Unknown or ambiguous names are rejected rather
than silently routed to an unassigned project.  See
`updates/current/HOST_PROJECT_CATALOG.md` for the integration contract.
