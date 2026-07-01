#!/usr/bin/env bash
# Build a wheel for this package and publish it to the local private index
# at ~/python-package-index.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INDEX_DIR="$HOME/github/bormanjo/python-package-index"
BUILD_DIR="$(mktemp -d)"
trap 'rm -rf "$BUILD_DIR"' EXIT

pip wheel "$REPO_ROOT" -w "$BUILD_DIR" --no-deps
cp "$BUILD_DIR"/*.whl "$INDEX_DIR/wheels/"
python3 "$INDEX_DIR/build_index.py"
