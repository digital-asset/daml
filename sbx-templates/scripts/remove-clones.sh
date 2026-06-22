#!/usr/bin/env bash
# remove-clones.sh — remove one or more feature clones once their branches are merged.
#
# Usage:
#   scripts/remove-clones.sh <feature-branch> [<feature-branch> ...]
#
# What it does per branch:
#   1. Warns if the branch has unpushed commits
#   2. Deletes the clone directory
#
# Env overrides:
#   REPO_URL   git URL — used only to derive repo name
#   BASE_DIR   tool-repo root (default: scripts/..)
#   WORKSPACE  where clones live (default: $BASE_DIR/workspace)
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/canton-network/splice}"
BASE_DIR="${BASE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
REPO_NAME="$(basename "${REPO_URL%.git}")"
WORKSPACE="${WORKSPACE:-$BASE_DIR/workspace}"
CLONE_DIR="${CLONE_DIR:-$WORKSPACE/$REPO_NAME-clones}"

log()  { printf '\033[1;32m[remove]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[remove]\033[0m WARNING: %s\n' "$*"; }
err()  { printf '\033[1;31m[remove]\033[0m ERROR: %s\n' "$*" >&2; }

if [ "$#" -lt 1 ]; then
  echo "usage: $0 <feature-branch> [<feature-branch> ...]" >&2
  exit 1
fi

for FEATURE in "$@"; do
  WT_PATH="$CLONE_DIR/$FEATURE"

  if [ ! -d "$WT_PATH" ]; then
    warn "Clone '$FEATURE' not found at $WT_PATH — skipping"
    continue
  fi

  # Warn if there are unpushed commits
  if [ -d "$WT_PATH/.git" ]; then
    UNPUSHED=$(git -C "$WT_PATH" log --oneline "@{u}.." 2>/dev/null | wc -l | tr -d ' ')
    if [ "$UNPUSHED" -gt 0 ]; then
      warn "Branch '$FEATURE' has $UNPUSHED unpushed commit(s) — make sure it's pushed before removing."
    fi
  fi

  log "Removing clone '$FEATURE' at $WT_PATH"
  rm -rf "$WT_PATH"
  log "Done: $FEATURE removed"
done
