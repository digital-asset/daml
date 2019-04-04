#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eux
DAML_EXTENSION_DIR=$1
CHECK_JSON=$2

cd $DAML_EXTENSION_DIR

: Checking JSON files for proper syntax
for sf in *.json syntaxes/*.json; do
  $CHECK_JSON $sf
done
