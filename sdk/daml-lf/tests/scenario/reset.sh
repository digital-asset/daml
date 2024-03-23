#!/usr/bin/env bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

shopt -s nullglob
set -e
for actual in $(find . -name '*.new'); do
  mv -v "$actual" "${actual%.new}"
done
