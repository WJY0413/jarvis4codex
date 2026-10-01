# Packaging, publication and rollback

Candidate distribution version: `0.2.5+dot.5`. Retained MCP API baseline: `0.2.5`. Jarvis schema adapter version: `1.0.0-contact-v25`.

The archive is an allowlisted source overlay, not a runtime backup. It contains reviewed Python modules, package metadata, clean documentation and disabled synthetic examples. It excludes customer/company data, original business work packages, model transcripts, auth, runtime databases/logs, caches, virtual environments, native binaries and private absolute account paths. The matching manifest records every distributed file hash. No repository credential was read or copied.

Publish to a dedicated dot branch of the existing repository. Do not replace the upstream main branch, remove its license/tests, rewrite history or publish local runtime folders. Publication is a separate authorized step; preparation of this archive does not mean a push occurred.

Before deployment, retain the currently installed source/version and configuration. Drain active work, then switch only that instance to the new source copy and refresh persistent clients. Keep previous source and immutable evidence for rollback. Repoint a stopped instance to the preserved source/configuration if needed; never roll back by rewriting native task history, replacing unknown actions, copying credentials or deleting data. No destructive rollback operation is included.

The dot variant is Linux-only. The original Windows implementation remains separately available through main/v0.2.5 and the preserved original input; do not overlay this dot candidate into a Windows runtime. A Windows Python 3.14 import check failed on os.O_DIRECTORY before any test executed. Linux acceptance does not establish Windows compatibility.

Packaged Python code exactly matches the frozen dot.5 original-suite code. Documentation was updated after sealing that result. The label-only patch uses zero context; full functional provenance is in SOURCE_MANIFEST.json. The original imported source contained no LICENSE file, so none is invented here; preserve the target repository's existing license and do not infer a new license grant.
