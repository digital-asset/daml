#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

cd "$DIR/.."

{
  use_local_canton_line_prefix='^USE_LOCAL_CANTON_INSTEAD *= *'
  if ! grep -q -E "$use_local_canton_line_prefix" canton/canton_version.bzl; then
    echo "Could not find a definition of USE_LOCAL_CANTON_INSTEAD (matching '$use_local_canton_line_prefix') inside of canton/canton_version.bzl."
    echo "Expected a line of the form 'USE_LOCAL_CANTON_INSTEAD = <absolute_path_to_canton_repo>'"
    echo "Instead got:"
    cat canton/canton_version.bzl
    exit 1
  else
    USE_LOCAL_CANTON_INSTEAD=$(grep -E "$use_local_canton_line_prefix" canton/canton_version.bzl | sed -E "s/$use_local_canton_line_prefix//")
    if echo -n "$USE_LOCAL_CANTON_INSTEAD" | grep -q -E '^"[^"]+"$'; then
      USE_LOCAL_CANTON_INSTEAD=$(echo -n "$USE_LOCAL_CANTON_INSTEAD" | sed -E 's/^"(.*)"$/\1/')
      if [[ "$(realpath "$USE_LOCAL_CANTON_INSTEAD")" != "$USE_LOCAL_CANTON_INSTEAD" ]]; then
        echo "USE_LOCAL_CANTON_INSTEAD inside of canton/canton_version.bzl is not an absolute path."
        echo "Verbatim path in file: $USE_LOCAL_CANTON_INSTEAD"
        echo "Outcome of calling realpath: $(realpath "$USE_LOCAL_CANTON_INSTEAD")"
        exit 1
      fi
    elif echo -n "$USE_LOCAL_CANTON_INSTEAD" | grep -q -E '^None$'; then
      echo "USE_LOCAL_CANTON_INSTEAD inside of canton/canton_version.bzl is set to None. You must set this to an absolute path to a canton repo."
      exit 1
    else
      echo 'Could not recognize USE_LOCAL_CANTON_INSTEAD as a string (must match the pattern: ^"[^"]+"$). You must set this to a path to an absolute path to a canton repo'
      exit 1
    fi
  fi
} 1>&2

echo "$USE_LOCAL_CANTON_INSTEAD"
