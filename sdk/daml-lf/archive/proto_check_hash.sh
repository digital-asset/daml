#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


set -e

declare -a checkSums=(
 "3c21928962d31911efde0d607016d0fa5582e640fb6cef9900397c699f0f8afe
  daml-lf/archive/src/stable/protobuf/com/daml/daml_lf_2_1/daml_lf.proto"
)

for checkSum in "${checkSums[@]}"; do
  echo ${checkSum} | sha256sum -c
done
