#!/usr/bin/env bash
# run-clone.sh — launch (or resume) a sandbox for a feature clone.
#
# Usage:
#   scripts/run-clone.sh <feature-branch>
#
# Env overrides:
#   TEMPLATE   sandbox template name (default: splice-ready; daml: daml-ready)
#   REPO_URL   git URL — used only to derive repo name
#   SUBDIR     dev-env subdirectory within the clone where .envrc lives (default: ""; daml: "sdk")
#   BASE_DIR   tool-repo root (default: scripts/..)
#   WORKSPACE  where clones live (default: $BASE_DIR/workspace)
set -euo pipefail

TEMPLATE="${TEMPLATE:-splice-ready}"
REPO_URL="${REPO_URL:-https://github.com/canton-network/splice}"
SUBDIR="${SUBDIR:-}"
BASE_DIR="${BASE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
REPO_NAME="$(basename "${REPO_URL%.git}")"
WORKSPACE="${WORKSPACE:-$BASE_DIR/workspace}"
CLONE_DIR="${CLONE_DIR:-$WORKSPACE/$REPO_NAME-clones}"

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <feature-branch>" >&2
  exit 1
fi

FEATURE="$1"
WT_PATH="$CLONE_DIR/$FEATURE"

if [ ! -d "$WT_PATH" ]; then
  echo "error: clone '$FEATURE' not found at $WT_PATH" >&2
  echo "  run: REPO_URL=... scripts/setup-clones.sh $FEATURE" >&2
  exit 1
fi

# Use a path relative to BASE_DIR so sbx mounts the full sbx-templates tree,
# not just the clone subdirectory. That keeps workspace/splice-clones/CLAUDE.md
# accessible as a parent-dir CLAUDE.md inside the sandbox.
cd "$BASE_DIR"
WT_REL="${WT_PATH#"$BASE_DIR"/}"
# Point the agent at the dev-env subdir (daml: <clone>/sdk) where .envrc lives; trim any trailing /.
WS_REL="${WT_REL%/}${SUBDIR:+/$SUBDIR}"

# Resume existing sandbox by name; create it from the template on first run.
# Re-attach is `sbx run --name <sandbox>` — a bare `sbx run <name>` treats <name> as an AGENT.
# Match the sandbox name exactly (awk on the SANDBOX column) so a prefix like "feat" doesn't
# accidentally match "feat-scan".
if sbx ls 2>/dev/null | awk 'NR>1 {print $1}' | grep -qxF "$FEATURE"; then
  exec sbx run --name "$FEATURE"
else
  exec sbx run -t "$TEMPLATE" --name "$FEATURE" claude "$WS_REL"
fi
