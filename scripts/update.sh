#!/usr/bin/env bash
# Update this checkout and reinstall Fluxa. Pass --no-pull to rebuild local sources only.
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
NO_PULL=false
if [[ ${1:-} == "--no-pull" ]]; then NO_PULL=true; shift; fi
if [[ $# -ne 0 ]]; then
  printf 'Usage: %s [--no-pull]\n' "$0" >&2
  exit 2
fi

command -v go >/dev/null 2>&1 || { printf 'Fluxa update requires Go.\n' >&2; exit 1; }
command -v make >/dev/null 2>&1 || { printf 'Fluxa update requires Make.\n' >&2; exit 1; }

cd "$ROOT"
if [[ "$NO_PULL" == false ]]; then
  command -v git >/dev/null 2>&1 || { printf 'Updating from remote requires Git; use --no-pull to rebuild local sources.\n' >&2; exit 1; }
  git rev-parse --is-inside-work-tree >/dev/null 2>&1 || { printf 'This source directory is not a Git checkout; use --no-pull.\n' >&2; exit 1; }
  git pull --ff-only
fi

exec "$ROOT/scripts/install.sh"
