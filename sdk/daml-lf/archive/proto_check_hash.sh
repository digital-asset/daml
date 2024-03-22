#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


set -e

declare -a checkSums=(
 "500eefd480e9af6940adf12e7ec4c2cf4975d4cb9b25096c15edb0d57d364de8  daml-lf/archive/src/stable/protobuf/com/daml/daml_lf_1_14/daml_lf_1.proto"
 "455dfb894ce9648a86dadb408d1ee96c36d180e0f1d625706371ea9eca95c767  daml-lf/archive/src/stable/protobuf/com/daml/daml_lf_1_14/daml_lf.proto"
)

for checkSum in "${checkSums[@]}"; do
  echo ${checkSum} | sha256sum -c
done
