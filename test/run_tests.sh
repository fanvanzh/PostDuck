#!/usr/bin/env bash
#
# Convenience wrapper that runs the Python integration test suite against a
# freshly-built PostDuck binary. Used from CI and for quick local runs.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

BIN="${POSTDUCK_BIN:-$REPO_DIR/build/postduck}"
if [[ ! -x "$BIN" ]]; then
    echo "postduck binary not found at $BIN" >&2
    echo "Build it first: 'mkdir -p build && cd build && cmake .. && make -j'" >&2
    exit 1
fi

# Ensure Python dependencies are available.
if ! python3 -c "import pytest, psycopg2" 2>/dev/null; then
    echo "Installing test dependencies..."
    python3 -m pip install --quiet --user -r "$REPO_DIR/test/requirements.txt"
fi

export POSTDUCK_BIN="$BIN"
exec python3 -m pytest "$@" "$REPO_DIR/test"
