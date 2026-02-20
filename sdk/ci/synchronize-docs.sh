#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
DEFAULT_SHARABLE_DIR="$DIR/../docs/sharable"
SHARABLE_DIR="${1:-$DEFAULT_SHARABLE_DIR}"
MANUAL_DIR="$DIR/../docs/manually-written"
PRIM_JSON_SRC="$DIR/../bazel-bin/compiler/damlc/daml-prim.json"
PRIM_JSON_DEST="$SHARABLE_DIR/sdk/reference/daml/stdlib/daml-prim.json"

cd $DIR/..

bazel build //docs:sharable-docs //compiler/damlc:daml-prim-json-docs

rm -Rf $SHARABLE_DIR
mkdir -p $SHARABLE_DIR
tar -zxf $DIR/../bazel-bin/docs/sharable-docs.tar.gz -C $SHARABLE_DIR --strip-components=2


if [ -d "$MANUAL_DIR" ] && [ "$(ls $MANUAL_DIR)" ]; then
  cp -a $MANUAL_DIR/* $SHARABLE_DIR/
fi

mkdir -p "$(dirname "$PRIM_JSON_DEST")"
cp -L "$PRIM_JSON_SRC" "$PRIM_JSON_DEST"

rm -f $SHARABLE_DIR/LICENSE
rm -f $SHARABLE_DIR/NOTICES

# Make the generated files read-only
find $SHARABLE_DIR -type f -follow -exec chmod 0444 {} +
