#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


# Use this script to regenerate documentation source files of inventories of error codes and error categories.

set -eou pipefail

SCRIPT_DIR=$PWD/$(dirname "$0")
cd $SCRIPT_DIR
GEN_ERROR_PAGES_DIR=$(cd ..; pwd)/resources/generated-test-evidence
BAZEL_BIN="$SCRIPT_DIR/../../bazel-bin"


mkdir -p -- $GEN_ERROR_PAGES_DIR

rm -f -- $GEN_ERROR_PAGES_DIR/security-test-evidence.csv || true
bazel build //docs:generate-security-test-evidence-docs-into-csv-file
cp -L ../../bazel-bin/docs/security-test-evidence.csv $GEN_ERROR_PAGES_DIR/security-test-evidence.csv
