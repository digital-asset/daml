#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
build_dir="$(dirname -- "$(dirname -- "$script_dir")")/daml_build"

dump_build_folder() {
  local dir
  for dir in "$build_dir/disk_cache" "$build_dir/repository_cache" "$build_dir/user_root"; do
    if [ -e "$dir" ]; then
      echo "present: $dir ($(du -sh "$dir" 2>/dev/null | cut -f1))"
    else
      echo "absent: $dir"
    fi
  done
  df -h "$(dirname -- "$build_dir")"
}

dump_build_folder
if [ -d "$build_dir" ]; then
  chmod -R u+w "$build_dir"
  rm -rf "$build_dir"
fi
dump_build_folder
