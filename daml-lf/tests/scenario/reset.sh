#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

shopt -s nullglob
set -e
for actual in */ACTUAL.ledger; do
  cp -v "$actual" "${actual/ACTUAL/EXPECTED}"
done
