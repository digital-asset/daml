#!/usr/bin/env bash

# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e
set -u

for file in "$@"; do
  shasum -a 256 "$file" | awk '{ print $1 }' > "$file.sha256"
done
