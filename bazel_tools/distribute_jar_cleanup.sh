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
zip -dq $OUT /LICENSE* /META-INF/LICENSE* /NOTICE* /META-INF/NOTICE*
zip -uq $OUT $NOTICE

if [[ "$IS_EE" -eq 1 ]]; then
  # LICENSE_EE has the wrong file name/directory, so we copy to a temp dir and 
  TMP_LICENSE_EE_DIR=$(mktemp -d)
  cp $LICENSE_EE $TMP_LICENSE_EE_DIR/LICENSE
  zip -ujq $OUT $TMP_LICENSE_EE_DIR/LICENSE
  rm -rf $TMP_LICENSE_EE_DIR
else
  zip -uq $OUT $LICENSE
fi

