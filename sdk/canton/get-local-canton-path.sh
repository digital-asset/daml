#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

cd "$DIR/.."

{
  use_local_canton_line_prefix='^LOCAL_CANTON_PATH *= *'
  if ! grep -q -E "$use_local_canton_line_prefix" canton/canton_version.bzl; then
    echo "Could not find a definition of LOCAL_CANTON_PATH (matching '$use_local_canton_line_prefix') inside of canton/canton_version.bzl."
    echo "Expected a line of the form 'LOCAL_CANTON_PATH = <absolute_path_to_canton_repo>'"
    echo "Instead got:"
    cat canton/canton_version.bzl
    exit 1
  else
    LOCAL_CANTON_PATH=$(grep -E "$use_local_canton_line_prefix" canton/canton_version.bzl | sed -E "s/$use_local_canton_line_prefix//")
    if echo -n "$LOCAL_CANTON_PATH" | grep -q -E '^"[^"]+"$'; then
      LOCAL_CANTON_PATH=$(echo -n "$LOCAL_CANTON_PATH" | sed -E 's/^"(.*)"$/\1/')
      if [[ "$(realpath "$LOCAL_CANTON_PATH")" != "$LOCAL_CANTON_PATH" ]]; then
        echo "LOCAL_CANTON_PATH inside of canton/canton_version.bzl is not an absolute path."
        echo "Verbatim path in file: $LOCAL_CANTON_PATH"
        echo "Outcome of calling realpath: $(realpath "$LOCAL_CANTON_PATH")"
        exit 1
      fi
    elif echo -n "$LOCAL_CANTON_PATH" | grep -q -E '^None$'; then
      echo "LOCAL_CANTON_PATH inside of canton/canton_version.bzl is set to None. You must set this to an absolute path to a canton repo."
      exit 1
    else
      echo 'Could not recognize LOCAL_CANTON_PATH as a string (must match the pattern: ^"[^"]+"$). You must set this to a path to an absolute path to a canton repo'
      exit 1
    fi
  fi
} 1>&2

echo "$LOCAL_CANTON_PATH"
