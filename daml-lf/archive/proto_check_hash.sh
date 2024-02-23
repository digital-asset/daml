#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


set -e

declare -a checkSums=(
)

for checkSum in "${checkSums[@]}"; do
  echo ${checkSum} | sha256sum -c
done
