#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


# Use this script to regenerate documentation source files of inventories of error codes and error categories.

set -eou pipefail

SCRIPT_DIR=$PWD/$(dirname "$0")
cd $SCRIPT_DIR
GEN_ERROR_PAGES_DIR=$(cd ..; pwd)/resources/generated-error-pages
BAZEL_BIN="$SCRIPT_DIR/../../bazel-bin"


mkdir -p -- $GEN_ERROR_PAGES_DIR

rm -f -- $GEN_ERROR_PAGES_DIR/error-codes-inventory.rst.inc || true
bazel build //docs:generate-docs-error-codes-inventory-into-rst-file
cp -L ../../bazel-bin/docs/error-codes-inventory.rst $GEN_ERROR_PAGES_DIR/error-codes-inventory.rst.inc

rm -f -- $GEN_ERROR_PAGES_DIR/error-categories-inventory.rst.inc || true
bazel build //docs:generate-docs-error-categories-inventory-into-rst-file
cp -L ../../bazel-bin/docs/error-categories-inventory.rst $GEN_ERROR_PAGES_DIR/error-categories-inventory.rst.inc
