#!/bin/bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


SRC=$1
OUT=$2
NOTICE=$3
LICENSE=$4
LICENSE_EE=$5
IS_EE=$6

cp $SRC $OUT
chmod +w $OUT
zip -dq $OUT "/LICENSE*" "/META-INF/LICENSE*" "/NOTICE*" "/META-INF/NOTICE*"
zip -uq $OUT $NOTICE

# both licenses have the wrong file name/directory, so we copy to a temp dir
TMP_LICENSE_DIR=$(mktemp -d)
if [[ "$IS_EE" -eq 1 ]]; then
  cp $LICENSE_EE $TMP_LICENSE_DIR/LICENSE.txt
else
  cp $LICENSE $TMP_LICENSE_DIR/LICENSE.txt
fi
zip -ujq $OUT $TMP_LICENSE_DIR/LICENSE.txt
rm -rf $TMP_LICENSE_DIR

