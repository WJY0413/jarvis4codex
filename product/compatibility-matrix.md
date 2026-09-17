# Compatibility Matrix

| Harness | Adapter | 0.2.2 status |
|---|---|---|
| Codex App Server | `adapters.codex_app_server` | Task Hold/Loop, observer reads, monitoring and local scheduling; requires a configured compatible CLI/profile |
| Isolated test doubles | Existing contract and runtime test fixtures | Validate protocol, state and lifecycle behavior without production state |
| Other harnesses | SDK contract available | No production adapter shipped in this release |

Windows is the primary tested host. Cross-platform behavior and third-party harness adapters require their own validation. Installing the package does not install or authenticate Codex or enable notification delivery.
