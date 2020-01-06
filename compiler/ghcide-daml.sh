#!/usr/bin/env bash
# Copyright (c) 2020 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
cd "$(dirname "$0")"/..
export RULES_HASKELL_EXEC_ROOT=$PWD/
ARGS_FILE=$(mktemp)
bazel build @ghcide//:ghcide >/dev/null 2>&1
bazel run --define hie_bios_ghci=True //compiler/damlc:damlc@ghci -- "$ARGS_FILE" >/dev/null 2>&1
export HIE_BIOS_ARGS="$ARGS_FILE"
./bazel-bin/external/ghcide/_install/bin/ghcide $@
