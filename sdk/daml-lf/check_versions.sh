#!/usr/bin/env sh

# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

GOLDEN_FILE="$1"
GENERATED_FILE="$2"

if ! diff -u "$GOLDEN_FILE" "$GENERATED_FILE"; then
  echo ""
  echo "❌ ERROR: daml-lf-versions.json is out of date!"
  echo "👉 To fix this, run: bazel run //sdk/daml-lf:update-daml-lf-versions"
  echo ""
  exit 1
fi

