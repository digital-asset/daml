#!/usr/bin/env bash

# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e
set -u

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

for file in "$DIR"/src/main/resources/com/daml/ledger/on/sql/migrations/**/*.sql; do
  shasum -a 256 "$file" | awk '{ print $1 }' > "$file.sha256"
done
