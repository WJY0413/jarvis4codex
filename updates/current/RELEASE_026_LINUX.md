# Jarvis 0.2.6 Linux dot.1 publication candidate

## Scope and publication status

This independent Linux candidate uses core version **0.2.6**, Linux adapter **dot.1**, and Python distribution **0.2.6+dot.1**. It is based on the existing `jarvis-dot` public commit `17903f679baa8746d0edf3bf65088b9c11aa3934` and overlays the 62 unchanged files from `Jarvis-0.2.6-Linux-dot.1-source.zip`. Unrelated upstream files and history are retained.

This commit is a source-publication candidate, not a completed dual-platform GitHub Release. The latest accepted Windows 0.2.6 release assets are not included or verified in this candidate. No Windows source/package identity or platform-parity claim is made. Existing `main`, `jarvis-dot`, and 0.2.5 tags/releases are not moved by publishing this candidate branch. There is no 0.2.6 tag or downloadable GitHub Release asset set as part of this candidate publication.

The new authoritative Linux source record is `RELEASE-MANIFEST.json`; an identical copy is supplied as `updates/current/Jarvis-0.2.6-Linux-dot.1-manifest.json`. Retained 0.2.5 manifests, validation notes, packaging notes, examples and other prior-version documents are historical records rather than 0.2.6 acceptance evidence.

## Changes and platform boundaries

- Preserve the accepted exact-terminal observer fallback.
- Include reviewed alias cancellation, immutable request replay and owned-dispatch gates.
- Isolate monitor terminal baselines, preserve failed notification outcomes, and serialize bounded heartbeat execution per database.
- Include shared bounded loopback HTTP listener recovery without tool replay; stdio remains the default.
- Require a frozen explicit Linux model and reasoning-effort policy with verified runtime constraints. Windows project-config automatic selection and the Windows thread-cwd LRU cache are not implemented in this Linux adapter.

See `PLATFORM-MATRIX.md` for the platform boundaries. Recorded Linux stdio acceptance does not establish an actual Linux HTTP-listener fault reproduction.

## Recorded acceptance, not a new live run

The unchanged source manifest records the original P0-P7 suite and its distinct required outcomes: P0-P4 and P6 completed, P5 interrupted, and P7 requires_readback. It records 12 distinct native turns, 15 action receipts, maximum TEST concurrency 2, zero final active TEST holds, zero automatic retries and zero replacement actions. Source configuration remained unchanged.

The publisher did not repeat the live suite. Publication checks verified all 51 distributed package files against the accepted manifest and wheel, all 62 source-archive files against the extracted bytes, Python syntax for 58 source/test files, and the distribution metadata version. These packaging checks do not replace runtime or business-quality acceptance.

## Prepared release assets

The following unchanged Linux files are prepared separately; their presence in the release manifest does not mean they have been attached to a GitHub Release:

- `Jarvis-0.2.6-Linux-dot.1-source.zip`: SHA-256 `9401a4299d91a3a3883ee128f47070a17fd1a0b92016b59ae6bf86414e6e89d4`
- `jarvis_control_plane-0.2.6+dot.1-py3-none-any.whl`: SHA-256 `1ad568392618255cf26610a766fa39c6126fe23781bd01972bbb725c5ab6593b`
- `Jarvis-0.2.6-Linux-dot.1-manifest.json`: SHA-256 `2205f9f56a3fd687eb748d67c3ec074a030018a910389cc5300535e130449c36`

`updates/current/SHA256SUMS-Linux-0.2.6.txt` lists these hashes. The source/wheel payload excludes credentials, operational configuration, runtime databases, private source data, original session logs and private deployment/bootstrap assets.
