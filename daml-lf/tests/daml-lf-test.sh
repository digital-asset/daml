#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eu

DAML_LF_REPL=$1
DAMLC=$2
MAIN=$3
TMPDIR=$(mktemp -d)

cleanup() {
  rm -rf "$TMPDIR"
}
trap cleanup EXIT

case "${MAIN##*.}" in
  dar)
    $DAML_LF_REPL testAll "$MAIN"
    ;;
  daml)
    $DAMLC compile "$MAIN" main -o $TMPDIR/out.dar
    $DAML_LF_REPL testAll $TMPDIR/out.dar
    ;;
  *)
    echo "Unknown file extension on $MAIN" 1>&2
    exit 1
    ;;
esac
