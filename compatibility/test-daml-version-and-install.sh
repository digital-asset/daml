#!/usr/bin/env bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
# Add daml to path
export PATH="$(echo external/daml-sdk-*):$PATH"
which daml

# Set daml cache
export DAML_CACHE=$(realpath local_daml_cache)
mkdir $DAML_CACHE

function exit_with_message {
  echo "$@"
  exit 1
}

daml version | tee daml-version-output
if stat $DAML_CACHE/versions.txt; then
  exit_with_message 'Cached versions.txt should not exist after running `daml version`'
fi
if [[ $(grep -E '^  [0-9].[0-9]+.[0-9]+' daml-version-output | wc -l) -gt 1 ]]; then
  exit_with_message '`daml version` returns more than one version when it shouldn'\''t'
fi

daml version --force-reload yes --all yes --snapshots yes | tee daml-version-reload-all-snapshots-output
if ! stat $DAML_CACHE/versions.txt; then
  exit_with_message 'Cached versions.txt should exist after running `daml version --force-reload yes`'
fi

daml version --all yes --snapshots yes | tee daml-version-all-snapshots-output
if [[ $(grep -c 'snapshot' daml-version-all-snapshots-output) -lt 10 ]]; then
  exit_with_message '`daml version --all yes --snapshots yes` displays less than 100 snapshots'
fi

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

exit 1

