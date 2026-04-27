#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: sync-daml-prim-json-to-digital-asset-docs.sh [options]

Clone digital-asset/docs, run the shared daml_docs_json_to_mdx.py converter
against the checked-in daml-prim JSON from this repo, then prepare a docs PR.

Defaults are controlled by environment variables and can be overridden via flags.

Core options:
  --daml-json PATH              Input JSON in daml repo.
  --docs-repo ORG/REPO          Docs repo slug (default: digital-asset/docs).
  --docs-clone-url URL          Explicit git clone URL/path for docs repo.
  --docs-ref REF                Docs ref to checkout before conversion.
  --docs-base-ref REF           Base branch for PR creation.
  --converter-script-path PATH  Converter path in docs repo.

Output / docs.json options:
  --docs-output-dir PATH        Output dir in docs repo.
  --docs-json-path PATH         docs.json path in docs repo.
  --nav-group-name NAME         docs.json group to replace.
  --nav-base PATH               Prefix for generated docs.json page entries.

PR / git options:
  --branch-name NAME            Branch name for docs update branch.
  --commit-message MSG          Commit message.
  --pr-title TITLE              PR title.
  --pr-body BODY                PR body text.
  --no-draft                    Open PR as ready-for-review instead of draft.

Execution options:
  --workdir PATH                Working directory (default: mktemp dir).
  --keep-workdir                Keep working directory after exit.
  --dry-run                     Stop after local commit (skip push/PR).
  --github-dry-run              Pass --dry-run to `gh pr create` (still pushes).
  -h, --help                    Show this help.

Environment variables (defaults):
  DAML_DOCS_JSON_INPUT
  DAML_DOCS_REPO
  DAML_DOCS_REPO_REF
  DAML_DOCS_BASE_REF
  DAML_DOCS_CONVERTER_SCRIPT_PATH
  DAML_DOCS_OUTPUT_DIR
  DAML_DOCS_JSON_PATH
  DAML_DOCS_NAV_GROUP_NAME
  DAML_DOCS_NAV_BASE
  GITHUB_TOKEN / GH_TOKEN       GitHub auth token for cloning/pushing/PR.
  GITHUB_USERNAME               Username for HTTPS basic auth (default: x-access-token).
USAGE
}

log() {
  printf '[daml-docs-sync] %s\n' "$*"
}

require_arg() {
  local flag="$1"
  local value="${2:-}"
  if [[ -z "$value" ]]; then
    echo "Missing value for $flag" >&2
    usage >&2
    exit 1
  fi
}

random_suffix() {
  python3 - <<'PY'
import random
import string

print("".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(8)))
PY
}

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
SDK_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd -- "$SDK_DIR/.." && pwd)"

DAML_JSON="${DAML_DOCS_JSON_INPUT:-$SDK_DIR/docs/sharable/sdk/reference/daml/stdlib/daml-prim.json}"
DOCS_REPO="${DAML_DOCS_REPO:-digital-asset/docs}"
DOCS_CLONE_URL=""
DOCS_REF="${DAML_DOCS_REPO_REF:-main}"
DOCS_BASE_REF="${DAML_DOCS_BASE_REF:-main}"
CONVERTER_SCRIPT_PATH="${DAML_DOCS_CONVERTER_SCRIPT_PATH:-scripts/daml_docs_json_to_mdx.py}"
DOCS_OUTPUT_DIR="${DAML_DOCS_OUTPUT_DIR:-docs-main/appdev/reference/daml-prim-api}"
DOCS_JSON_PATH="${DAML_DOCS_JSON_PATH:-docs.json}"
NAV_GROUP_NAME="${DAML_DOCS_NAV_GROUP_NAME:-Help}"
NAV_BASE="${DAML_DOCS_NAV_BASE:-}"

BRANCH_NAME=""
COMMIT_MESSAGE=""
PR_TITLE=""
PR_BODY=""
DRAFT_PR=true
DRY_RUN=false
GITHUB_DRY_RUN=false

WORKDIR=""
KEEP_WORKDIR=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --daml-json)
      DAML_JSON="$2"
      shift 2
      ;;
    --docs-repo)
      DOCS_REPO="$2"
      shift 2
      ;;
    --docs-clone-url)
      DOCS_CLONE_URL="$2"
      shift 2
      ;;
    --docs-ref)
      DOCS_REF="$2"
      shift 2
      ;;
    --docs-base-ref)
      DOCS_BASE_REF="$2"
      shift 2
      ;;
    --converter-script-path)
      CONVERTER_SCRIPT_PATH="$2"
      shift 2
      ;;
    --docs-output-dir)
      DOCS_OUTPUT_DIR="$2"
      shift 2
      ;;
    --docs-json-path)
      DOCS_JSON_PATH="$2"
      shift 2
      ;;
    --nav-group-name)
      NAV_GROUP_NAME="$2"
      shift 2
      ;;
    --nav-base)
      NAV_BASE="$2"
      shift 2
      ;;
    --branch-name)
      BRANCH_NAME="$2"
      shift 2
      ;;
    --commit-message)
      COMMIT_MESSAGE="$2"
      shift 2
      ;;
    --pr-title)
      PR_TITLE="$2"
      shift 2
      ;;
    --pr-body)
      PR_BODY="$2"
      shift 2
      ;;
    --no-draft)
      DRAFT_PR=false
      shift
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --github-dry-run)
      GITHUB_DRY_RUN=true
      shift
      ;;
    --workdir)
      WORKDIR="$2"
      shift 2
      ;;
    --keep-workdir)
      KEEP_WORKDIR=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

require_arg "--daml-json" "$DAML_JSON"
require_arg "--docs-repo" "$DOCS_REPO"
require_arg "--docs-ref" "$DOCS_REF"
require_arg "--docs-base-ref" "$DOCS_BASE_REF"
require_arg "--converter-script-path" "$CONVERTER_SCRIPT_PATH"
require_arg "--docs-output-dir" "$DOCS_OUTPUT_DIR"
require_arg "--docs-json-path" "$DOCS_JSON_PATH"
require_arg "--nav-group-name" "$NAV_GROUP_NAME"

if [[ -z "$NAV_BASE" ]]; then
  NAV_BASE="$DOCS_OUTPUT_DIR"
fi
require_arg "--nav-base" "$NAV_BASE"

if [[ "$DOCS_REPO" != */* ]]; then
  echo "Invalid --docs-repo '$DOCS_REPO' (expected ORG/REPO)." >&2
  exit 1
fi

if [[ "$DAML_JSON" != /* ]]; then
  DAML_JSON="$REPO_ROOT/$DAML_JSON"
fi
if [[ "$WORKDIR" != "" && "$WORKDIR" != /* ]]; then
  WORKDIR="$REPO_ROOT/$WORKDIR"
fi

if [[ ! -f "$DAML_JSON" ]]; then
  echo "Input JSON not found: $DAML_JSON" >&2
  exit 1
fi

if [[ -z "$WORKDIR" ]]; then
  WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/daml-docs-sync.XXXXXX")"
else
  mkdir -p "$WORKDIR"
fi

if [[ "$KEEP_WORKDIR" == true ]]; then
  log "Keeping workdir: $WORKDIR"
else
  trap 'rm -rf "$WORKDIR"' EXIT
fi

DOCS_DIR="$WORKDIR/docs"

clone_docs_repo() {
  if [[ -n "$DOCS_CLONE_URL" ]]; then
    log "Cloning docs repo from explicit URL/path into $DOCS_DIR"
    git clone "$DOCS_CLONE_URL" "$DOCS_DIR"
    return
  fi

  local remote_url="https://github.com/${DOCS_REPO}.git"
  local token="${GITHUB_TOKEN:-${GH_TOKEN:-}}"
  if [[ -n "$token" ]]; then
    local user="${GITHUB_USERNAME:-x-access-token}"
    local auth_header
    auth_header="Authorization: basic $(printf '%s:%s' "$user" "$token" | base64 | tr -d '\n')"
    log "Cloning $DOCS_REPO with token auth"
    git -c "http.https://github.com/.extraheader=${auth_header}" clone "$remote_url" "$DOCS_DIR"
    git -C "$DOCS_DIR" remote set-url origin "$remote_url"
  else
    log "Cloning $DOCS_REPO without token auth"
    git clone "$remote_url" "$DOCS_DIR"
  fi
}

clone_docs_repo

if ! git -C "$DOCS_DIR" checkout "$DOCS_REF"; then
  git -C "$DOCS_DIR" fetch origin "$DOCS_REF"
  git -C "$DOCS_DIR" checkout FETCH_HEAD
fi

CONVERTER_SCRIPT="$DOCS_DIR/$CONVERTER_SCRIPT_PATH"
DOCS_OUTPUT_ABS="$DOCS_DIR/$DOCS_OUTPUT_DIR"
DOCS_JSON_ABS="$DOCS_DIR/$DOCS_JSON_PATH"

if [[ ! -f "$CONVERTER_SCRIPT" ]]; then
  echo "Converter script not found: $CONVERTER_SCRIPT" >&2
  exit 1
fi
if [[ ! -f "$DOCS_JSON_ABS" ]]; then
  echo "docs.json not found: $DOCS_JSON_ABS" >&2
  exit 1
fi

log "Running converter from docs repo"
python3 "$CONVERTER_SCRIPT" \
  --input-json "$DAML_JSON" \
  --output-dir "$DOCS_OUTPUT_ABS" \
  --docs-json "$DOCS_JSON_ABS" \
  --nav-group-name "$NAV_GROUP_NAME" \
  --nav-base "$NAV_BASE"

TODAY_SUFFIX="$(date -u +%Y-%m-%d)"
if [[ -z "$BRANCH_NAME" ]]; then
  BRANCH_NAME="automatic-update-daml-prim-api-${TODAY_SUFFIX}-$(random_suffix)"
fi
if [[ -z "$COMMIT_MESSAGE" ]]; then
  COMMIT_MESSAGE="Automatic update (daml-prim-api): ${TODAY_SUFFIX}"
fi
if [[ -z "$PR_TITLE" ]]; then
  PR_TITLE="$COMMIT_MESSAGE"
fi

git -C "$DOCS_DIR" checkout -b "$BRANCH_NAME"
git -C "$DOCS_DIR" add "$DOCS_OUTPUT_DIR" "$DOCS_JSON_PATH"

if [[ -z "$(git -C "$DOCS_DIR" status --porcelain)" ]]; then
  log "No docs changes detected after conversion; exiting."
  exit 0
fi

git -C "$DOCS_DIR" \
  -c user.name="${GIT_AUTHOR_NAME:-Daml Automation}" \
  -c user.email="${GIT_AUTHOR_EMAIL:-support@digitalasset.com}" \
  commit -m "$COMMIT_MESSAGE"

if [[ "$DRY_RUN" == true ]]; then
  log "Dry-run enabled; skipping push and PR creation."
  git -C "$DOCS_DIR" --no-pager show --stat --oneline HEAD
  exit 0
fi

log "Pushing docs branch $BRANCH_NAME"
git -C "$DOCS_DIR" push --set-upstream origin "$BRANCH_NAME"

if ! command -v gh >/dev/null 2>&1; then
  echo "gh CLI is required to create PRs." >&2
  exit 1
fi

if [[ -z "${GH_TOKEN:-}" && -n "${GITHUB_TOKEN:-}" ]]; then
  export GH_TOKEN="$GITHUB_TOKEN"
fi

PR_CMD=(
  gh pr create
  --repo "$DOCS_REPO"
  --base "$DOCS_BASE_REF"
  --head "$BRANCH_NAME"
  --title "$PR_TITLE"
  --body "$PR_BODY"
)
if [[ "$DRAFT_PR" == true ]]; then
  PR_CMD+=(--draft)
fi
if [[ "$GITHUB_DRY_RUN" == true ]]; then
  PR_CMD+=(--dry-run)
fi

log "Creating PR in $DOCS_REPO"
"${PR_CMD[@]}"

log "Completed docs sync successfully."
