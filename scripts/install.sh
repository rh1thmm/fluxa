#!/usr/bin/env bash
# Install Fluxa from this source checkout. Override FLUXA_BIN_DIR to choose a target.
set -euo pipefail

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
BIN_DIR=${FLUXA_BIN_DIR:-"$HOME/.local/bin"}

require() {
  command -v "$1" >/dev/null 2>&1 || {
    printf 'Fluxa install requires %s. Install it and retry.\n' "$1" >&2
    exit 1
  }
}

require go
require make

if ! go version | grep -Eq 'go1\.(2[2-9]|[3-9][0-9])'; then
  printf 'Fluxa requires Go 1.22 or newer; found: %s\n' "$(go version)" >&2
  exit 1
fi

cd "$ROOT"
make build
mkdir -p "$BIN_DIR"
install -m 0755 bin/fluxa "$BIN_DIR/fluxa"

printf 'Installed Fluxa to %s/fluxa\n' "$BIN_DIR"
case ":$PATH:" in
  *":$BIN_DIR:"*) ;;
  *) printf 'Add %s to PATH to run `fluxa` directly.\n' "$BIN_DIR" ;;
esac
