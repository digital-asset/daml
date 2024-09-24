#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

readonly arch_dir="daml-lf/archive"
readonly buf_exe="$1"
readonly version="$2"
readonly config_file="${arch_dir}/buf.yaml"
readonly stable_dir="${arch_dir}/src/stable/protobuf"
readonly main_dir="${arch_dir}/src/main/protobuf"

# We check the stable directory contains exactly 2 proto files
find "${stable_dir}/" -follow -name '*.proto' | wc -l | grep -x 2
# We check the main directory contains exactly 2 proto files
find "${main_dir}/" -follow -name '*.proto' | wc -l | grep -x 3

# This is kind of broken, it only checks daml_lf_2.proto for wire compatibility between main and
# stable. The daml_lf.proto files are ignored because main/.../daml_lf.proto and
# sable/.../daml_lf.proto declare different proto packages. This is not too bad because
# daml_lf.proto is mostly empty while daml_lf_2.proto contains most of the definitions, but it
# is not ideal.
# The following check is there to ignore the comparison until the we have 2.3 is introduced
if [[ "$(find ${stable_dir}/ -name '*proto' | grep -v daml_lf_2_1)"  ]]; then
  "${buf_exe}" breaking --config "${config_file}" --against "${stable_dir}" "${main_dir}"
fi
