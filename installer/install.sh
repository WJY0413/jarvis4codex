#!/bin/sh
# No installation occurs without --apply. This bootstrap never downloads.
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
MODE=auto
PYTHON=
EXPECT=
FALLBACK_BUNDLED=false
for ARG in "$@"; do
    if [ "$EXPECT" = mode ]; then MODE=$ARG; EXPECT=; continue; fi
    if [ "$EXPECT" = python ]; then PYTHON=$ARG; EXPECT=; continue; fi
    case "$ARG" in
        --mode) EXPECT=mode ;;
        --mode=*) MODE=${ARG#--mode=} ;;
        --python) EXPECT=python ;;
        --python=*) PYTHON=${ARG#--python=} ;;
    esac
done
if [ "$MODE" = bundled ]; then
    PYTHON="$HERE/runtime/python/bin/python3.12"
fi
COMPATIBLE='import sys; raise SystemExit(0 if sys.implementation.name == "cpython" and sys.version_info[:2] == (3, 12) else 1)'
if [ -n "$PYTHON" ]; then
    if ! "$PYTHON" -I -B -c "$COMPATIBLE" >/dev/null 2>&1; then
        echo "The selected interpreter is not usable CPython 3.12; no installation occurred." >&2
        exit 2
    fi
fi
if [ -z "$PYTHON" ]; then
    for CANDIDATE in python3.12 python3 python; do
        if command -v "$CANDIDATE" >/dev/null 2>&1 &&
           "$CANDIDATE" -I -B -c "$COMPATIBLE" >/dev/null 2>&1; then
            PYTHON=$(command -v "$CANDIDATE"); break
        fi
    done
    if [ -z "$PYTHON" ] && [ "$MODE" = auto ] && [ -x "$HERE/runtime/python/bin/python3.12" ]; then
        if "$HERE/runtime/python/bin/python3.12" -I -B -c "$COMPATIBLE" >/dev/null 2>&1; then
            PYTHON="$HERE/runtime/python/bin/python3.12"
            FALLBACK_BUNDLED=true
        fi
    fi
    if [ -z "$PYTHON" ]; then
        echo "No compatible CPython 3.12 found. Use --python /path/to/python3.12 or the bundled variant." >&2
        exit 2
    fi
fi
if [ "$FALLBACK_BUNDLED" = true ]; then
    exec "$PYTHON" -I -B "$HERE/install.py" "$@" --mode bundled
fi
exec "$PYTHON" -I -B "$HERE/install.py" "$@"
