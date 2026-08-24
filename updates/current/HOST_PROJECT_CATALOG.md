# Host Project Catalog

Jarvis resolves a supplied project name using a snapshot from the target Desktop Host.  The snapshot contains `projectId`, `label`, `path`, `hostId`, repository flag, and `observed_at` (UTC).  It is intentionally Host-scoped: an install on another computer keeps a separate host entry.

## Desktop startup and manual refresh

The Desktop-facing integration creates `DesktopHostProjectCatalogAdapter` with the native Desktop `list_projects` callback, then calls `refresh_on_start()`.  The same `refresh_now()` method powers a one-click refresh action.  Both methods are lightweight and make no model/App Server call.

If the Host is unavailable, returns no projects, or returns invalid data, the adapter returns an unchanged receipt and does not replace the previous snapshot.  The frontend should show the last `observed_at` when present; it should not surface a refresh error to the user.

## Launcher configuration

Use the catalog as the authoritative resolver for new native tasks:

```json
{
  "project_catalog_path": "host-project-catalog.json",
  "project_catalog_host_id": "local",
  "project_catalog_required": true,
  "allowed_projects": {}
}
```

With `project_catalog_required: true`, a supplied project name must match exactly one normalized label or configured alias in the current Host snapshot.  Jarvis records the resolved `project_id`, `project_host_id`, catalog timestamp, and resolution source with the task request, dry-run plan, registration, and COO callback.  There is no fallback to `null` for an unknown or ambiguous supplied project name.

`projectId: null` remains reserved for an explicitly projectless/unassigned thread; it is not a fallback routing result.
