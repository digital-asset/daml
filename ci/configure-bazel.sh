#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

echo "TODO: ci/configure-bazel.sh only exists because release-trigger calls it; remove it once release-trigger configures Bazel itself" >&2

credentials="${GOOGLE_APPLICATION_CREDENTIALS_CONTENT:-}"
unset GOOGLE_APPLICATION_CREDENTIALS_CONTENT

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
bazelrc_local="$repo_root/sdk/.bazelrc.local"

on_windows() {
  case "$(uname -s)" in
    MINGW*|MSYS*) return 0 ;;
    *) return 1 ;;
  esac
}

run_powershell() {
  powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "$(cygpath -w "$1")"
}

settings=('build --disk_cache=')

if on_windows; then
  run_powershell "$repo_root/ci/dev-env-install.ps1"
  run_powershell "$repo_root/ci/clean-bazel-build-folder.ps1"
  settings+=('startup --output_base=C:/b')
fi

if [ "${IS_FORK:-}" != "True" ] && [ -n "$credentials" ]; then
  key_dir="${AGENT_TEMPDIRECTORY:-${TMPDIR:-/tmp}}"
  if on_windows; then
    key_dir="$(cygpath -m "$key_dir")"
  fi
  key="$key_dir/gcred-${BUILD_BUILDID:-local}.json"
  rm -f "$key"
  (umask 077 && set -C && printf '%s' "$credentials" > "$key")
  settings+=("build --remote_upload_local_results=true --google_credentials=$key")
  echo "credentials present; remote cache uploads enabled"
else
  echo "no credentials; remote cache is read-only"
fi

printf '%s\n' "${settings[@]}" > "$bazelrc_local"
echo "Wrote $bazelrc_local:"
cat "$bazelrc_local"
