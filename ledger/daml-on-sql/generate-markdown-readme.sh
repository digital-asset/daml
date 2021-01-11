#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e
set -u

cd "$(dirname "${BASH_SOURCE[0]}")"

pandoc \
  --reference-links \
  --reference-location=section \
  --to=markdown+backtick_code_blocks-fenced_code_attributes \
  --output=./README.md \
  ./README.rst

# Pandoc generates unnecessary escape characters.
sed -i 's/\\//g' ./README.md
