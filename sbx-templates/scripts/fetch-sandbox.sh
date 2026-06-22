#!/usr/bin/env bash
# fetch-sandbox.sh — pull a `sbx --clone` sandbox's commits to the HOST. Each running sandbox serves
# its internal clone over a git-daemon, which sbx exposes to the host as the remote `sandbox-<name>`.
# RUN ON THE HOST, from inside the host checkout you want the commits in.
#
# Usage:
#   scripts/fetch-sandbox.sh <feature> [<branch>]
#
#   <feature>  the sandbox name (e.g. feat-scan).
#   <branch>   optional: also create/update a local branch of that name from the sandbox's branch,
#              so you can check it out / open a PR. Skipped if that branch is currently checked out.
#
# The remote only answers while the sandbox is running (the git-daemon stops with it). Unpushed
# commits are lost if the sandbox is removed — fetch (or push from inside) before `sbx rm`.
set -euo pipefail

[ "$#" -ge 1 ] || { echo "usage: $0 <feature> [<branch>]" >&2; exit 1; }
FEATURE="$1"
BRANCH="${2:-}"
REMOTE="sandbox-$FEATURE"

git rev-parse --git-dir >/dev/null 2>&1 || { echo "fetch-sandbox: run this inside the host git checkout." >&2; exit 1; }

if ! git remote | grep -qxF "$REMOTE"; then
  echo "fetch-sandbox: no remote '$REMOTE' in this checkout." >&2
  echo "  sbx adds it automatically for a --clone sandbox — check the sandbox is up and named" >&2
  echo "  '$FEATURE' (sbx ls), and that you're in the matching host checkout." >&2
  exit 1
fi

if [ -n "$BRANCH" ]; then
  cur="$(git symbolic-ref --quiet --short HEAD || true)"
  if [ "$cur" = "$BRANCH" ]; then
    echo "fetch-sandbox: '$BRANCH' is checked out — fetching without updating it (merge/pull yourself)."
    git fetch "$REMOTE" "$BRANCH"
    echo "  integrate with:  git merge $REMOTE/$BRANCH"
  else
    # refspec creates the local branch if absent, fast-forwards/updates it otherwise.
    git fetch "$REMOTE" "$BRANCH:$BRANCH"
    echo "fetched '$REMOTE' → local branch '$BRANCH'. Check it out:  git checkout $BRANCH"
  fi
else
  git fetch "$REMOTE"
  echo "fetched '$REMOTE'. Its branches:  git branch -r | grep '^[[:space:]]*$REMOTE/'"
fi
