#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


set -euo pipefail

eval "$($(dirname "$0")/dev-env/bin/dade-assist)"

execution_log_postfix=${1:-}

export LC_ALL=en_US.UTF-8

ARTIFACT_DIRS="${BUILD_ARTIFACTSTAGINGDIRECTORY:-$PWD}"

tag_filter=""
if [[ "$execution_log_postfix" == "_Darwin" ]]; then
  tag_filter="-dont-run-on-darwin,-scaladoc,-pdfdocs"
fi

# bazel test //... will also build targets that are not tests.
bazel test //... --build_tag_filters "$tag_filter" --test_tag_filters "$tag_filter" --experimental_execution_log_file "$ARTIFACT_DIRS/test_execution${execution_log_postfix}.log"
# Make sure that Bazel query works.
bazel query 'deps(//...)' > /dev/null
# Check that we can load damlc in ghci
GHCI_SCRIPT=$(mktemp)
function cleanup {
  rm -rf "$GHCI_SCRIPT"
}
trap cleanup EXIT
# Disabled on darwin since it sometimes seem to hang and this only
# tests our dev setup rather than our code so issues are not critical.
if [[ "$(uname)" != "Darwin" ]]; then
    da-ghci --data yes //compiler/damlc:damlc -e ':main --help'
fi
# Check that our IDE works on our codebase
ghcide compiler/damlc/exe/Main.hs 2>&1 | tee ide-log
grep -q "1 file worked, 0 files failed" ide-log
