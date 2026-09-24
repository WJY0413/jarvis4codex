# Jarvis 0.2.4

## Public interface

- Adds `jarvis_update(action="check"|"apply")` as the ninth public MCP tool.
- `check` reports the configured launcher policy, the newest validated local Codex Desktop command and version, whether a policy update is required, and current HoldHost activity.
- `apply` is idempotent. It activates `desktop_auto` only when no Hold is active, saves the exact prior launcher configuration under the Jarvis state directory, writes atomically, and verifies the resulting runtime selection.

## Runtime behavior

- `desktop_auto` discovers `LOCALAPPDATA/OpenAI/Codex/bin/*/codex.exe` for every new App Server client, validates each candidate with `--version`, and selects the highest valid version.
- If no usable Desktop candidate exists, Jarvis logs the condition and uses the existing npm/PATH auto-discovery path.
- The updater does not download Codex, update the global npm package, change model routing, or restart Codex Desktop.
