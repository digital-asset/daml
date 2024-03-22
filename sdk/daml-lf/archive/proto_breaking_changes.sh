#!/usr/bin/env bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e

readonly arch_dir="daml-lf/archive"
readonly buf_exe="$1"
readonly version="$2"
readonly config_file="${arch_dir}/buf.yaml"
readonly stable_dir="${arch_dir}/src/stable/protobuf"
readonly main_dir="${arch_dir}/src/main/protobuf"

# We check that the stable directories contain exactly 2 proto files
for dir in ${stable_dir}; do
   find "${dir}/" -follow -name '*.proto' | wc -l | grep -x 2
done

# We check that the main directory contains exactly 3 proto files
find "${main_dir}/" -follow -name '*.proto' | wc -l | grep -x 3

# This is kind of broken, it only checks daml_lf_(1|2).proto for wire compatibility between main and
# stable. The daml_lf.proto files are ignored because main/.../daml_lf.proto and
# sable/.../daml_lf.proto declare different proto packages. This is not too bad because
# daml_lf.proto is mostly empty while daml_lf_(1|2).proto contains most of the definitions, but it
# is not ideal.
"${buf_exe}" breaking --config "${config_file}" --against "${stable_dir}" "${main_dir}"
