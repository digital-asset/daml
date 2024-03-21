#!/usr/bin/env bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

set -eou pipefail
version=$1
executable=$2
extra_args="${@:3}"
WITH_POSTGRES=$(rlocation compatibility/bazel_tools/client_server/with-postgres/with-postgres-exe)
if [ -z "$WITH_POSTGRES" ]; then
    WITH_POSTGRES=$(rlocation compatibility/bazel_tools/client_server/with-postgres/with-postgres.exe)
fi
if [ -z "$WITH_POSTGRES" ]; then
    echo "Faild to find with-postgres wrapper"
    exit 1
fi
echo $@
echo "$(rlocation daml-sdk-$version/$executable)"
$WITH_POSTGRES $(rlocation daml-sdk-$version/$executable) $extra_args
