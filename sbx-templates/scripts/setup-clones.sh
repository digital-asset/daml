#!/usr/bin/env bash
# setup-clones.sh — standalone-clone multi-agent layout.
#
# Model: ONE reference clone of the repo, N standalone clones (one per feature/agent).
# Each clone is fully independent — no shared .git state, no cross-directory dependency.
# This makes each clone work correctly in a sandbox regardless of mount configuration.
#
# Usage:
#   REPO_URL=<git-url> scripts/setup-clones.sh <feature-branch> [<feature-branch> ...]
#
# Env overrides:
#   REPO_URL        git URL to clone (default: https://github.com/canton-network/splice)
#   BASE_DIR        tool-repo root (default: scripts/..). Must stay inside the host-mounted dir.
#   WORKSPACE       where clones live (default: $BASE_DIR/workspace, gitignored)
#   MAIN_DIR        reference clone used to speed up feature clones (default: $WORKSPACE/<repo-name>)
#   BASE_REF        branch new clones fork from (default: origin/HEAD)
#   SUBDIR          dev-env subdirectory within the clone where .envrc lives (default: ""; daml: "sdk")
#   GIT_USER_NAME   git user.name to set in each clone (for DCO signoff)
#   GIT_USER_EMAIL  git user.email to set in each clone (for DCO signoff)
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/canton-network/splice}"
SUBDIR="${SUBDIR:-}"
BASE_DIR="${BASE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
REPO_NAME="$(basename "${REPO_URL%.git}")"
WORKSPACE="${WORKSPACE:-$BASE_DIR/workspace}"
MAIN_DIR="${MAIN_DIR:-$WORKSPACE/$REPO_NAME}"
CLONE_DIR="${CLONE_DIR:-$WORKSPACE/$REPO_NAME-clones}"
mkdir -p "$WORKSPACE"

log() { printf '\033[1;32m[setup]\033[0m %s\n' "$*"; }

if [ "$#" -lt 1 ]; then
  echo "usage: $0 <feature-branch> [<feature-branch> ...]" >&2
  exit 1
fi

# --- 1. Ensure reference clone exists ----------------------------------------
# Used only to speed up feature clones via --reference --dissociate.
# Feature clones are fully independent after creation — deleting this is safe.
if [ ! -d "$MAIN_DIR/.git" ]; then
  log "Cloning $REPO_URL -> $MAIN_DIR (reference clone)"
  git clone "$REPO_URL" "$MAIN_DIR"
else
  log "Reference clone exists at $MAIN_DIR — fetching latest"
  git -C "$MAIN_DIR" fetch --all --prune
fi

BASE_REF="${BASE_REF:-$(git -C "$MAIN_DIR" symbolic-ref --short refs/remotes/origin/HEAD 2>/dev/null || echo origin/main)}"
BASE_BRANCH="${BASE_REF#origin/}"
log "New clones will branch from: $BASE_REF"

mkdir -p "$CLONE_DIR"

# --- 2. Ensure CLAUDE.md exists in the feature clone parent ------------------
CLAUDE_MD="$CLONE_DIR/CLAUDE.md"
if [ ! -f "$CLAUDE_MD" ]; then
  log "WARNING: $CLAUDE_MD is missing (re-clone sbx-templates to restore it)"
fi

# --- 3. Create one standalone clone per feature ------------------------------
for FEATURE in "$@"; do
  WT_PATH="$CLONE_DIR/$FEATURE"

  if [ -d "$WT_PATH/.git" ]; then
    log "Clone '$FEATURE' already exists at $WT_PATH — skipping"
    continue
  fi

  log "Creating clone '$FEATURE' at $WT_PATH"

  # Check if branch exists on remote
  if git -C "$MAIN_DIR" show-ref --verify --quiet "refs/remotes/origin/$FEATURE"; then
    # Branch exists remotely — clone it directly
    git clone --reference "$MAIN_DIR" --dissociate -b "$FEATURE" "$REPO_URL" "$WT_PATH"
    log "  -> cloned existing branch '$FEATURE'"
  else
    # New branch — clone main and create branch
    git clone --reference "$MAIN_DIR" --dissociate -b "$BASE_BRANCH" "$REPO_URL" "$WT_PATH"
    git -C "$WT_PATH" checkout -b "$FEATURE"
    log "  -> cloned '$BASE_BRANCH' and created new branch '$FEATURE'"
  fi

  # Build/test logs land here. The clone is bind-mounted, so this dir is visible
  # on the HOST at $WT_PATH/.build-logs/ — read compile/test output there directly,
  # no need to attach to the sandbox. Keep it out of git via the clone-local exclude.
  mkdir -p "$WT_PATH/.build-logs"
  EXCLUDE_FILE="$(git -C "$WT_PATH" rev-parse --git-path info/exclude)"
  grep -qxF '/.build-logs/' "$EXCLUDE_FILE" 2>/dev/null || echo '/.build-logs/' >> "$EXCLUDE_FILE"

  if command -v direnv >/dev/null 2>&1; then
    ( cd "$WT_PATH/$SUBDIR" && direnv allow . )
  fi

  # Configure git identity for DCO signoff (git commit -s)
  if [ -n "${GIT_USER_NAME:-}" ]; then
    git -C "$WT_PATH" config user.name "$GIT_USER_NAME"
    log "  -> set git user.name"
  fi
  if [ -n "${GIT_USER_EMAIL:-}" ]; then
    git -C "$WT_PATH" config user.email "$GIT_USER_EMAIL"
    log "  -> set git user.email"
  fi
  if [ -z "${GIT_USER_NAME:-}" ] || [ -z "${GIT_USER_EMAIL:-}" ]; then
    log "  -> WARNING: GIT_USER_NAME/GIT_USER_EMAIL not set — run 'git config user.name/email' in the clone before committing"
  fi

  log "  -> done: $WT_PATH"
done

log "Feature clones:"
for d in "$CLONE_DIR"/*/; do
  [ -d "$d/.git" ] && git -C "$d" log --oneline -1 | sed "s|^|  $(basename $d): |"
done
