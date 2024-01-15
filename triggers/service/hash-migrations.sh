#!/usr/bin/env bash

# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eou pipefail
shopt -s globstar

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

"$DIR"/../../libs-scala/flyway-testing/hash-migrations.sh \
  "$DIR"/src/main/resources/com/daml/lf/engine/trigger/db/migration/**/*.sql
