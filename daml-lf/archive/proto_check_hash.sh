#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


set -ex

declare -a checkSums=(
 "1c44a789914665751f62951a1cc7b7ccba9da2c66021017b48d4fac3c7024409
  daml-lf/archive/src/stable/protobuf/com/daml/daml_lf_2_1/daml_lf2.proto"
 "a03b1b1d0ab81d43bf72b5a1276d342437d1a11965affa55558aa472cdfae74a
  daml-lf/archive/src/stable/protobuf/com/daml/daml_lf_2_1/daml_lf.proto"
)

for checkSum in "${checkSums[@]}"; do
  echo ${checkSum} | sha256sum -c
done
