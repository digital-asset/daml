#!/usr/bin/env bash
# Copyright (c) 2019 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

ENV_FILE=$1
ARGS_FILE=$2
RULES_HASKELL_EXEC_ROOT=$(dirname $(readlink ${BUILD_WORKSPACE_DIRECTORY}/bazel-out))/
# Setting -pgm* flags explicitly has the unfortunate side effect
# of resetting any program flags in the GHC settings file. So we
# restore them here. See
# https://ghc.haskell.org/trac/ghc/ticket/7929.
PGM_ARGS="-pgma {CC} -pgmc {CC} -pgml {CC} -pgmP {CC} -optc-fno-stack-protector -optP-E -optP-undef -optP-traditional"
echo "{ENV}" > "$ENV_FILE"
echo $PGM_ARGS "{ARGS}" > "$ARGS_FILE"
