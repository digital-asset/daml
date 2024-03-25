#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
DAR=$DIR/daml/.daml/dist/daml-1.0.0.dar
GEN=$DIR/ts/generated/src/daml

ghcid --command "da-ghci //:daml2js" --reload $DAR --test ":main -o $GEN $DAR"
