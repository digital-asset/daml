#!/usr/bin/env bash
# sbx-clone.sh — launch (or resume) ONE isolated `sbx --clone` sandbox per feature, so you can work
# on several splice features in parallel. Each feature gets its own named sandbox with its own
# internal clone and its own warm build cache. RUN ON THE HOST.
#
# This is the sbx-clone-mode counterpart to run-clone.sh/setup-clones.sh (which are Host clone mode):
# here the clone lives INSIDE the container, not on the host. Get the agent's commits back out with
# scripts/fetch-sandbox.sh <feature>.
#
# On first run it creates the sandbox (sbx create --clone), creates/switches to the feature branch
# INSIDE the clone, warns if the template's baked nix closure has drifted from this checkout's nix/
# setup (self-heals at runtime — just a slower first build), then attaches (sbx run --name).
# Re-running just re-attaches.
#
# Usage:
#   scripts/sbx-clone.sh <feature> [<repo-dir>]
#
#   <feature>   sandbox name == feature name (e.g. feat-scan). Re-running resumes it.
#   <repo-dir>  host checkout to clone from (default: $REPO_DIR, else the current directory).
#
# Env overrides:
#   TEMPLATE        sbx template (default: splice-ready; for daml: daml-ready)
#   REPO_DIR        host checkout to clone (default: CWD)
#   BRANCH          branch to create/switch to inside the clone (default: <feature>). Set BRANCH=
#                   (empty) to stay on the cloned HEAD. Created off the cloned HEAD if it doesn't
#                   exist — the host stays on its own branch, so this scales to many features at once.
#   GIT_USER_NAME / GIT_USER_EMAIL   if set, configured inside the sandbox (for DCO signoff)
#
# Logs: read them in the sandbox with `sbx exec -it <feature> bash` then `lnav log/` (lnav is in the
# dev shell; -it gives it the TTY it needs).
#
# Lifecycle: `sbx stop <feature>` to park (keeps clone+cache), this script to resume, `sbx rm
# <feature>` after merge. Disk is the binding constraint — keep only a few hot at once.
set -euo pipefail

TEMPLATE="${TEMPLATE:-splice-ready}"

log() { printf '\033[1;32m[sbx-clone]\033[0m %s\n' "$*"; }

[ "$#" -ge 1 ] || { echo "usage: $0 <feature> [<repo-dir>]" >&2; exit 1; }
FEATURE="$1"
REPO_DIR="${2:-${REPO_DIR:-$PWD}}"
BRANCH="${BRANCH-$FEATURE}"   # ${BRANCH-…}: unset → feature name; BRANCH= (empty) → skip branching

command -v sbx >/dev/null 2>&1 || { echo "sbx-clone: 'sbx' not found — run this on the HOST." >&2; exit 1; }

# Re-attach if a sandbox with this name already exists (--clone is a no-op on re-attach).
if sbx ls 2>/dev/null | awk 'NR>1 {print $1}' | grep -qxF "$FEATURE"; then
  log "resuming existing sandbox '$FEATURE'"
  exec sbx run --name "$FEATURE"
fi

[ -d "$REPO_DIR/.git" ] || {
  echo "sbx-clone: '$REPO_DIR' is not a git checkout — pass <repo-dir> or set REPO_DIR." >&2; exit 1; }
REPO_DIR="$(cd "$REPO_DIR" && pwd)"   # absolute; sbx mirrors this exact path inside the sandbox

log "creating --clone sandbox '$FEATURE' from $REPO_DIR (template: $TEMPLATE)"
sbx create --clone -t "$TEMPLATE" --name "$FEATURE" claude "$REPO_DIR"

# Set up the clone before attaching. sbx exec runs in the (now-started) sandbox; the clone is at the
# same absolute path as on the host, so `git -C "$REPO_DIR"` targets it.
[ -n "${GIT_USER_NAME:-}" ]  && sbx exec "$FEATURE" git config --global user.name  "$GIT_USER_NAME"  || true
[ -n "${GIT_USER_EMAIL:-}" ] && sbx exec "$FEATURE" git config --global user.email "$GIT_USER_EMAIL" || true

if [ -n "$BRANCH" ]; then
  log "ensuring branch '$BRANCH' in the clone (created off the cloned HEAD if new)"
  sbx exec "$FEATURE" git -C "$REPO_DIR" checkout "$BRANCH" 2>/dev/null \
    || sbx exec "$FEATURE" git -C "$REPO_DIR" checkout -b "$BRANCH" \
    || log "WARN: couldn't create '$BRANCH' inside — run 'git checkout -b $BRANCH' yourself once attached"
fi

# Drift check (warn only): if the template's baked nix closure differs from this checkout's nix/ setup,
# the dev shell self-heals by realizing the delta from the nix caches on first load — it just makes
# that first build slower. We only flag it; we never auto-rebuild (that's a deliberate, occasional
# `build-template.sh splice`). Silently skipped if the template predates the /etc/splice-ready.ref
# marker, or the baked commit isn't in this checkout to diff against.
baked="$(sbx exec "$FEATURE" cat /etc/splice-ready.ref 2>/dev/null | tr -d '[:space:]' || true)"
if [ -n "$baked" ] && git -C "$REPO_DIR" cat-file -e "${baked}^{commit}" 2>/dev/null; then
  if ! git -C "$REPO_DIR" diff --quiet "$baked" HEAD -- nix 2>/dev/null; then
    log "NOTE: template's baked splice nix (${baked:0:9}) differs from this checkout's nix/ at HEAD —"
    log "      the dev shell will realize the delta from the nix caches on first load (slower, not broken)."
    log "      To refresh the warm cache: bump SPLICE_REF and rebuild (scripts/build-template.sh splice)."
  fi
fi

log "attaching… logs: 'sbx exec -it $FEATURE bash' then 'lnav log/'; pull commits to host: scripts/fetch-sandbox.sh $FEATURE"
exec sbx run --name "$FEATURE"
