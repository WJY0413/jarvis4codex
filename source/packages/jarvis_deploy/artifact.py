"""Verified, bounded release ZIP handling. No execution, authentication, or routing.

The caller must obtain the pinned SHA-256 independently from a trusted release
contract. An embedded manifest alone is not proof of publisher authenticity.
Destinations must be new, their parent must already exist, and failures never
overwrite an existing object. Failed extraction may leave its new staging dir.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import tempfile
import time
import unicodedata
from urllib.parse import unquote, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
import zipfile

MANIFEST_NAME = "deployment-manifest.json"
MAX_ARCHIVE_BYTES = 128 * 1024 * 1024
MAX_FILE_BYTES = 32 * 1024 * 1024
MAX_TOTAL_BYTES = 512 * 1024 * 1024
MAX_MANIFEST_BYTES = 4 * 1024 * 1024
MAX_ENTRIES = 20_000
MAX_DOWNLOAD_SECONDS = 120
CHUNK_BYTES = 1024 * 1024
ASSET_HOSTS = frozenset({"release-assets.githubusercontent.com", "objects.githubusercontent.com"})
_WINDOWS_RESERVED = {"con", "prn", "aux", "nul", "clock$", "conin$", "conout$"}
_WINDOWS_RESERVED.update(f"{prefix}{digit}" for prefix in ("com", "lpt") for digit in "123456789¹²³")


class ArtifactError(ValueError):
    """An artifact failed provenance, integrity, path, or resource checks."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ArtifactError(message)


def _inputs(sha256: str, version: str) -> str:
    _require(isinstance(sha256, str) and re.fullmatch(r"[0-9a-fA-F]{64}", sha256) is not None,
             "An external pinned SHA-256 is required")
    _require(isinstance(version, str) and re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.+-]+)?", version) is not None,
             "An explicit exact release version is required")
    return sha256.lower()


def _path(path: str | os.PathLike) -> Path:
    value = Path(path).expanduser().absolute()
    _require(".." not in value.parts and not value.is_symlink() and value.resolve() == value,
             "Filesystem paths must not traverse links or '..'")
    # is_symlink() does not cover Windows directory junctions on all Python versions.
    for item in (value, *value.parents):
        if item.exists():
            meta = item.lstat()
            _require(not (getattr(meta, "st_file_attributes", 0) & 0x400), "Windows reparse points are refused")
    return value


def _fresh(path: str | os.PathLike) -> Path:
    value = _path(path)
    _require(not value.exists(), "Destination already exists")
    _require(value.parent.is_dir(), "Destination parent must already exist")
    return value


def _member_name(name: str, *, directory: bool = False) -> str:
    _require(isinstance(name, str) and bool(name), "Archive names must be nonempty strings")
    value = name[:-1] if directory and name.endswith("/") else name
    _require(bool(value) and len(value) <= 1024 and not value.startswith("/")
             and "\\" not in value and unicodedata.normalize("NFC", value) == value,
             "Unsafe or noncanonical archive path")
    for part in value.split("/"):
        _require(bool(part) and part not in (".", "..") and len(part.encode("utf-8")) <= 240,
                 "Unsafe archive path component")
        _require(not part.endswith((".", " ")) and not any(ord(char) < 32 or char in '<>:"|?*' for char in part),
                 "Archive path is not Windows-safe")
        _require(part.split(".", 1)[0].casefold() not in _WINDOWS_RESERVED, "Windows reserved archive name")
    return value


def _no_duplicate_json(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, "Duplicate manifest JSON key")
        result[key] = value
    return result


def _inspect_open(handle, sha256: str, version: str):
    meta = os.fstat(handle.fileno())
    _require(stat.S_ISREG(meta.st_mode) and meta.st_size <= MAX_ARCHIVE_BYTES,
             "Archive must be a bounded regular file")
    digest = hashlib.sha256()
    bytes_read = 0
    for chunk in iter(lambda: handle.read(CHUNK_BYTES), b""):
        bytes_read += len(chunk)
        _require(bytes_read <= MAX_ARCHIVE_BYTES, "Archive grew beyond its size bound")
        digest.update(chunk)
    _require(digest.hexdigest() == sha256, "Archive SHA-256 mismatch")
    handle.seek(0)
    try:
        archive = zipfile.ZipFile(handle)
    except zipfile.BadZipFile as exc:
        raise ArtifactError("Invalid ZIP structure") from exc
    try:
        entries = archive.infolist()
        _require(0 < len(entries) <= MAX_ENTRIES, "Archive entry limit exceeded or archive empty")
        total = 0
        seen = set()
        nodes = {}
        files = {}
        for info in entries:
            _require(info.orig_filename == info.filename and "\x00" not in info.orig_filename,
                     "NUL in archive filename")
            name = _member_name(info.filename, directory=info.is_dir())
            _require(info.filename not in seen, "Duplicate archive entry")
            seen.add(info.filename)
            _require(not info.flag_bits & 1, "Encrypted ZIP members are unsupported")
            _require(info.compress_type in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED), "Unsupported ZIP compression")
            kind = stat.S_IFMT(info.external_attr >> 16)
            _require(kind in (0, stat.S_IFREG, stat.S_IFDIR)
                     and (kind != stat.S_IFDIR or info.is_dir())
                     and (kind != stat.S_IFREG or not info.is_dir())
                     and not info.external_attr & 0x400, "ZIP links, reparse points, or special files are refused")
            # Legacy Unix/ASi metadata may encode hardlink/symlink targets separately.
            extra = info.extra
            while extra:
                _require(len(extra) >= 4, "Malformed ZIP extra field")
                key = int.from_bytes(extra[:2], "little")
                length = int.from_bytes(extra[2:4], "little")
                _require(len(extra) >= 4 + length and key not in (0x000D, 0x756E),
                         "Malformed or link-capable ZIP extra field")
                extra = extra[4 + length:]
            _require(0 <= info.file_size <= MAX_FILE_BYTES, "ZIP member size limit exceeded")
            _require(not info.is_dir() or info.file_size == 0, "ZIP directory contains a hidden payload")
            total += info.file_size
            _require(total <= MAX_TOTAL_BYTES, "ZIP total expanded size limit exceeded")
            parts = name.split("/")
            for index in range(1, len(parts) + 1):
                node = "/".join(parts[:index])
                is_dir = index < len(parts) or info.is_dir()
                previous = nodes.get(node.casefold())
                _require(previous is None or previous == (node, is_dir), "ZIP case collision or file/directory collision")
                nodes[node.casefold()] = (node, is_dir)
            if not info.is_dir():
                files[name] = info
        _require(MANIFEST_NAME in files, "Release deployment manifest is missing")
        _require(files[MANIFEST_NAME].file_size <= MAX_MANIFEST_BYTES, "Release manifest is too large")
        manifest = json.loads(archive.read(files[MANIFEST_NAME]).decode("utf-8"), object_pairs_hook=_no_duplicate_json)
        _require(isinstance(manifest, dict) and manifest.get("schema") == "jarvis-release/v1", "Unsupported release manifest schema")
        _require(manifest.get("version") == version, "Release manifest version mismatch")
        _require(isinstance(manifest.get("channel"), str) and bool(manifest["channel"]), "Release channel is missing")
        expected = manifest.get("files")
        _require(isinstance(expected, dict) and bool(expected), "Release file manifest is empty or malformed")
        for name, checksum in expected.items():
            _member_name(name)
            _require(isinstance(checksum, str) and re.fullmatch(r"[0-9a-f]{64}", checksum) is not None,
                     "Invalid per-file SHA-256")
        _require(set(files) - {MANIFEST_NAME} == set(expected), "Release file membership mismatch")
        for name, checksum in expected.items():
            digest = hashlib.sha256()
            with archive.open(files[name]) as member:
                for chunk in iter(lambda: member.read(CHUNK_BYTES), b""):
                    digest.update(chunk)
            _require(digest.hexdigest() == checksum, "Release file digest mismatch")
        return archive, entries, manifest
    except BaseException as exc:
        archive.close()
        if isinstance(exc, (zipfile.BadZipFile, UnicodeError, json.JSONDecodeError)):
            raise ArtifactError("Invalid ZIP or manifest structure") from exc
        raise


def inspect_archive(path, sha256: str, version: str) -> dict:
    """Read/hash every member; return its validated manifest without writing."""
    checksum = _inputs(sha256, version)
    with _path(path).open("rb") as handle:
        archive, _, manifest = _inspect_open(handle, checksum, version)
        archive.close()
        return manifest


def extract_verified(path, dest, sha256: str, version: str) -> dict:
    """Verify fully before creating dest, then extract only ordinary source files."""
    checksum = _inputs(sha256, version)
    target = _fresh(dest)
    with _path(path).open("rb") as handle:
        archive, entries, manifest = _inspect_open(handle, checksum, version)
        try:
            target.mkdir(mode=0o700)
            for info in entries:
                name = _member_name(info.filename, directory=info.is_dir())
                output = target.joinpath(*name.split("/"))
                if info.is_dir():
                    output.mkdir(parents=True, exist_ok=True)
                    continue
                output.parent.mkdir(parents=True, exist_ok=True)
                if name == MANIFEST_NAME:
                    # Write the validated in-memory object, not reread mutable manifest bytes.
                    with output.open("x", encoding="utf-8") as sink:
                        json.dump(manifest, sink, ensure_ascii=False, sort_keys=True, indent=2)
                        sink.write("\n")
                    os.chmod(output, 0o644)
                    continue
                digest = hashlib.sha256()
                written = 0
                with archive.open(info) as source, output.open("xb") as sink:
                    for chunk in iter(lambda: source.read(CHUNK_BYTES), b""):
                        written += len(chunk)
                        _require(written <= info.file_size and written <= MAX_FILE_BYTES, "Extracted member exceeded its size bound")
                        digest.update(chunk)
                        sink.write(chunk)
                _require(written == info.file_size, "Extracted member length mismatch")
                expected = manifest["files"].get(name)
                if expected is not None:
                    _require(digest.hexdigest() == expected, "Source changed during extraction")
                os.chmod(output, 0o755 if (info.external_attr >> 16) & 0o111 else 0o644)
            return manifest
        finally:
            archive.close()


def _release_url(url: str, version: str, *, redirect: bool = False) -> str:
    _require(isinstance(url, str) and not any(ord(char) <= 32 for char in url), "URL whitespace/control characters are refused")
    parsed = urlsplit(url)
    _require(parsed.scheme == "https" and parsed.hostname and parsed.username is None
             and parsed.password is None and parsed.port in (None, 443) and not parsed.fragment,
             "Only HTTPS release URLs without credentials/fragments are allowed")
    if redirect and parsed.hostname in ASSET_HOSTS:
        return url
    _require(parsed.hostname == "github.com", "Release URL host is not allowed")
    prefix = f"/WJY0413/jarvis4codex/releases/download/v{version}/"
    path = unquote(parsed.path)
    _require(path.startswith(prefix) and bool(path[len(prefix):]) and "/" not in path[len(prefix):]
             and "\\" not in path and not parsed.query, "URL must select the exact official versioned release asset")
    _member_name(path[len(prefix):])
    return url


class _ReleaseRedirects(HTTPRedirectHandler):
    def __init__(self, version: str):
        super().__init__()
        self.version = version

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        _release_url(newurl, self.version, redirect=True)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def download_verified(url: str, dest, sha256: str, version: str) -> dict:
    """Download an official HTTPS release asset, validate it, and save to new dest.

No credentials are added. Every redirect is checked before opening it. A failed
download/verification removes only its own temporary file. If the final local
copy fails, the newly created partial destination is retained, never overwritten.
"""
    _inputs(sha256, version)
    _release_url(url, version)
    target = _fresh(dest)
    opener = build_opener(_ReleaseRedirects(version))
    descriptor, temporary_name = tempfile.mkstemp(prefix=".jarvis-download-", suffix=".zip", dir=target.parent)
    temporary = Path(temporary_name)
    try:
        started = time.monotonic()
        with os.fdopen(descriptor, "wb") as sink:
            request = Request(url, headers={"User-Agent": "jarvis-release-verifier/1", "Accept": "application/octet-stream"})
            with opener.open(request, timeout=30) as response:
                _release_url(response.geturl(), version, redirect=True)
                _require(response.status == 200, "Release download did not return HTTP 200")
                length = response.headers.get("Content-Length")
                if length is not None:
                    _require(length.isdigit() and int(length) <= MAX_ARCHIVE_BYTES, "Download size limit exceeded")
                total = 0
                for chunk in iter(lambda: response.read(CHUNK_BYTES), b""):
                    _require(time.monotonic() - started <= MAX_DOWNLOAD_SECONDS, "Download time limit exceeded")
                    total += len(chunk)
                    _require(total <= MAX_ARCHIVE_BYTES, "Download size limit exceeded")
                    sink.write(chunk)
        manifest = inspect_archive(temporary, sha256, version)
        with temporary.open("rb") as source, target.open("xb") as sink:
            for chunk in iter(lambda: source.read(CHUNK_BYTES), b""):
                sink.write(chunk)
        return manifest
    finally:
        temporary.unlink(missing_ok=True)
