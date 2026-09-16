#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Copy-pasted from the Bazel Bash runfiles library v2.
set -uo pipefail; f=bazel_tools/tools/bash/runfiles/runfiles.bash
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null || \
  source "$0.runfiles/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  { echo>&2 "ERROR: cannot find $f"; exit 1; }; f=; set -e
# --- end runfiles.bash initialization v2 ---

set -euo pipefail
usage() {
  cat >&2 <<'EOF'
usage: mktgz OUTPUT ARGS...

Creates a gzip compressed tarball in OUTPUT passing ARGS to tar. The created
tarball is reproducible, i.e. it does not contain any timestamps or similar
non-deterministic inputs. See https://reproducible-builds.org/docs/archives/
EOF
}
trap usage ERR

runfile_by_name() {
  local name="$1"
  if [[ -n "${RUNFILES_DIR:-}" && -d "${RUNFILES_DIR}" ]]; then
    find "${RUNFILES_DIR}" -maxdepth 2 \( -name "$name" -o -name "$name.exe" \) | head -1
  elif [[ -n "${RUNFILES_MANIFEST_FILE:-}" && -f "${RUNFILES_MANIFEST_FILE}" ]]; then
    grep -E "(^|/)$name(\.exe)? " "${RUNFILES_MANIFEST_FILE}" | head -1 | cut -f2- -d' '
  fi
}

PIGZ="$(rlocation "pigz+/pigz" || true)"
if [[ -z "${PIGZ:-}" ]]; then
  PIGZ="$(rlocation "pigz~/pigz" || true)"
fi
if [[ -z "${PIGZ:-}" ]]; then
  PIGZ="$(runfile_by_name pigz)"
fi
TAR="$(runfile_by_name tar)"

TAR_FLAGS=(
  --format=gnutar
  --owner=0
  --group=0
  --numeric-owner
  --mtime="2000-01-01 00:00Z"
  --no-acls
  --no-xattrs
)

case "$(uname -s)" in
  CYGWIN*|MINGW*|MSYS*)
    "$TAR" "${TAR_FLAGS[@]}" -cf - "${@:2}" | "$PIGZ" -n > "${1}"
    ;;
  *)
    "$TAR" "${TAR_FLAGS[@]}" --use-compress-program "$PIGZ -n" -cf "${1}" "${@:2}"
    ;;
esac
