#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

# Add diff and daml to path
export PATH="$(rlocation diffutils_nix)/bin:$(rlocation daml-sdk-$1):$PATH"
which daml
which diff

# Set daml cache
export DAML_CACHE=$(realpath local_daml_cache)
rm -r $DAML_CACHE || true
mkdir $DAML_CACHE

function exit_with_message {
  echo "$@"
  exit 1
}

daml version | tee daml-version-output
if stat $DAML_CACHE/versions.txt; then
  exit_with_message 'Cached versions.txt should not exist after running `daml version`'
fi
if [[ $(grep -E '^  [0-9]+.[0-9]+.[0-9]+' daml-version-output | wc -l) -gt 1 ]]; then
  exit_with_message '`daml version` returns more than one version when it shouldn'\''t'
fi

daml version --force-reload yes --all yes --snapshots yes | tee daml-version-reload-all-snapshots-output
if ! stat $DAML_CACHE/versions.txt; then
  exit_with_message 'Cached versions.txt should exist after running `daml version --force-reload yes`'
fi
if [[ $(grep -c 'snapshot' daml-version-reload-all-snapshots-output) -lt 100 ]]; then
  exit_with_message '`daml version --force-reload yes --all yes --snapshots yes` displays less than 100 snapshots'
fi
if [[ $(grep -c 'snapshot' $DAML_CACHE/versions.txt) -lt 100 ]]; then
  exit_with_message 'Cached versions.txt contains less than 100 snapshots identifiers'
fi

daml version --all yes --snapshots yes | tee daml-version-all-snapshots-output

if ! diff daml-version-all-snapshots-output daml-version-reload-all-snapshots-output; then
  exit_with_message '`daml version --all yes --snapshots yes` returns different version list from previous invocation of `daml version --all yes --snapshots yes --force-reload yes`'
fi

daml version --all yes | tee daml-version-all-output
if [[ $(grep -E '^  0.[0-9]+.[0-9]+' daml-version-all-output | wc -l) -gt 1 ]]; then
  exit_with_message '`daml version --all` returns more than one version of 0.x.y'
fi
if [[ $(grep -E '^  1.[0-9]+.[0-9]+' daml-version-all-output | wc -l) -gt 1 ]]; then
  exit_with_message '`daml version --all` returns more than one version of 1.x.y'
fi

minor_versions=$(grep -oE '^  2.[0-9]+' daml-version-all-output | grep -oE '[0-9]+$' | sort -g | uniq)
maximum_minor_version=$(echo "$minor_versions" | tail -n1)
if [[ "$minor_versions" != "`seq 0 $maximum_minor_version`" ]]; then
  exit_with_message '`daml version --all` does not return a full list of every minor version.' "$minor_versions" "$maximum_minor_version"
fi

daml version | tee daml-version-output
if [[ $(grep -E '^  [0-9].[0-9]+.[0-9]+' daml-version-output | wc -l) -gt 2 ]]; then
  exit_with_message '`daml version` returns more than two versions when it shouldn'\''t'
fi

# Delete cache before `daml install latest`
rm -r $DAML_CACHE
mkdir $DAML_CACHE

daml install latest || true # daml install fails due to sandboxing, but we can still check that the version list is up to date
echo '`daml install latest` cache/versions.txt:'
cat $DAML_CACHE/versions.txt
if ! stat $DAML_CACHE/versions.txt; then
  exit_with_message 'Cached versions.txt should exist after running `daml install latest`, even if it fails'
fi
if [[ $(grep -c 'snapshot' $DAML_CACHE/versions.txt) -lt 100 ]]; then
  exit_with_message 'Cached versions.txt after `daml install latest` contains less than 100 snapshots identifiers'
fi

# Delete cache before `daml install latest --snapshots yes`
rm -r $DAML_CACHE
mkdir $DAML_CACHE

daml install latest --snapshots yes || true # daml install fails due to sandboxing, but we can still check that the version list is up to date
echo '`daml install latest --snapshots yes` cache/versions.txt:'
cat $DAML_CACHE/versions.txt
if ! stat $DAML_CACHE/versions.txt; then
  exit_with_message 'Cached versions.txt should exist after running `daml install latest --snapshots yes`, even if it fails'
fi
if [[ $(grep -c 'snapshot' $DAML_CACHE/versions.txt) -lt 100 ]]; then
  exit_with_message 'Cached versions.txt after `daml install latest --snapshots yes` contains less than 100 snapshots identifiers'
fi

exit 0
