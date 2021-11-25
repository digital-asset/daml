#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


set -e

declare -a checkSums=(
 "500eefd480e9af6940adf12e7ec4c2cf4975d4cb9b25096c15edb0d57d364de8  daml-lf/archive/src/stable/protobuf/com/daml/daml_lf_1_14/daml_lf_1.proto"
 "455dfb894ce9648a86dadb408d1ee96c36d180e0f1d625706371ea9eca95c767  daml-lf/archive/src/stable/protobuf/com/daml/daml_lf_1_14/daml_lf.proto"
 "83207610fc117b47ef1da586e36c791706504911ff41cbee8fc5d1da12128147  daml-lf/archive/src/stable/protobuf/com/daml/daml_lf_1_12/daml_lf_1.proto"
 "6dbc0a0288c2447af690284e786c3fc1b58a296f2786e9cd5b4053069ff7c045  daml-lf/archive/src/stable/protobuf/com/daml/daml_lf_1_12/daml_lf.proto"
 "6d0869fd8b326cc82f7507ec9deb37520af23ccc4c03a78af623683eb5df2bee  daml-lf/archive/src/stable/protobuf/com/daml/daml_lf_1_13/daml_lf_1.proto"
 "2038b49e33825c4730b0119472073f3d5da9b0bd3df2f6d21d9d338c04a49c47  daml-lf/archive/src/stable/protobuf/com/daml/daml_lf_1_13/daml_lf.proto"
 "9a9c86f4072ec08ac292517d377bb07b1436c2b9133da9ba03216c3ae8d3d27c  daml-lf/archive/src/stable/protobuf/com/daml/daml_lf_1_11/daml_lf_1.proto"
 "05eb95f6bb15042624d2ca89d366e3bcd8618934471c6093efeecc09bb9d7df4  daml-lf/archive/src/stable/protobuf/com/daml/daml_lf_1_11/daml_lf.proto"
)

for checkSum in "${checkSums[@]}"; do
  echo ${checkSum} | sha256sum -c
done
