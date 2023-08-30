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

# We check that the stable directories contain exactly 3 proto files
for dir in ${stable_dir}; do
   find "${dir}/" -follow -name '*.proto' | wc -l | grep -x 2
done

# We check that main directory contain exactly 3 proto files
find "${main_dir}/" -follow -name '*.proto' | wc -l | grep -x 3

# TODO(paul): check the main directory against the stable v2 dirs
"${buf_exe}" breaking --config "${config_file}" --against "${stable_dir}" "${main_dir}"