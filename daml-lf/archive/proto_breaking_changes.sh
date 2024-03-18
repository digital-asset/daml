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

#We check the directories contains exactly 2 proto files
for dir in "${stable_dir}" "${main_dir}"; do
   find "${dir}/" -follow -name '*.proto' | wc -l | grep -x 2
done

"${buf_exe}" breaking --config "${config_file}" --against "${stable_dir}" "${main_dir}"
