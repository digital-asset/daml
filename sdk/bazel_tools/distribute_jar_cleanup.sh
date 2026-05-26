#!/bin/bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


ZIP_BIN=$1
SRC=$2
OUT=$3
NOTICES=$4
LICENSE=$5
LICENSE_EE=$6
IS_EE=$7

cp "$SRC" "$OUT"
chmod +w "$OUT"
"$ZIP_BIN" -dq "$OUT" "/LICENSE*" "/META-INF/LICENSE*" "/NOTICE*" "/META-INF/NOTICE*"

# both licenses have the wrong file name/directory, so we copy to a temp dir
TMP_LICENSE_DIR=$(mktemp -d)
cp "$NOTICES" "$TMP_LICENSE_DIR/NOTICES.txt"
if [[ "$IS_EE" -eq 1 ]]; then
  cp "$LICENSE_EE" "$TMP_LICENSE_DIR/LICENSE.txt"
else
  cp "$LICENSE" "$TMP_LICENSE_DIR/LICENSE.txt"
fi
"$ZIP_BIN" -ujq "$OUT" "$TMP_LICENSE_DIR/LICENSE.txt" "$TMP_LICENSE_DIR/NOTICES.txt"
rm -rf $TMP_LICENSE_DIR

