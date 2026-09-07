#!/usr/bin/env bash
# Permanently remove Fluxa, its credentials/state, and registered workspaces.
set -euo pipefail

YES=false
if [[ ${1:-} == "--yes" ]]; then YES=true; shift; fi
if [[ $# -ne 0 ]]; then
  printf 'Usage: %s [--yes]\n' "$0" >&2
  exit 2
fi

BIN_DIR=${FLUXA_BIN_DIR:-"$HOME/.local/bin"}
CONFIG_ROOT=${XDG_CONFIG_HOME:-"$HOME/.config"}/fluxa
CACHE_ROOT=${XDG_CACHE_HOME:-"$HOME/.cache"}/fluxa
REGISTRY="$CONFIG_ROOT/workspaces"
targets=("$BIN_DIR/fluxa" "$CONFIG_ROOT" "$CACHE_ROOT")

# Workspaces are registered by future Fluxa releases. Never scan a home directory
# and delete every matching Fluxa.toml: that would be an unsafe surprise.
if [[ -f "$REGISTRY" ]]; then
  while IFS= read -r workspace; do
    [[ -n "$workspace" ]] && targets+=("$workspace")
  done < "$REGISTRY"
fi

printf 'This permanently removes the following Fluxa data:\n'
printf '  %s\n' "${targets[@]}"
printf '\nThis cannot be undone.\n'
if [[ "$YES" == false ]]; then
  read -r -p 'Type DELETE FLUXA to continue: ' answer
  [[ "$answer" == 'DELETE FLUXA' ]] || { printf 'Cancelled.\n'; exit 0; }
fi

for target in "${targets[@]}"; do
  if [[ -e "$target" || -L "$target" ]]; then
    rm -rf -- "$target"
    printf 'Removed %s\n' "$target"
  fi
done

printf 'Fluxa application data has been removed. Unregistered workspaces were not deleted.\n'
