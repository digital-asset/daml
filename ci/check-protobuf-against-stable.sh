#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

readonly SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
readonly PROJECT_ROOT="${SCRIPT_DIR}/.."

cd "${PROJECT_ROOT}"
readonly CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"

latest_stable() {
  local gitTagSearchSpace
  if [[ "${CURRENT_BRANCH}" == "main" ]]; then
    gitTagSearchSpace="--no-merged"
  else
    gitTagSearchSpace="--merged"
  fi
  git tag ${gitTagSearchSpace} | grep -v "snapshot" | sort -V | tail -1
}

LATEST_STABLE="$(latest_stable)"
echo "Checking protos against '${LATEST_STABLE}' (i.e., the most recent stable tag up to '${CURRENT_BRANCH}')"

check_against_stable_protos_buf_image() {
  local buf_image_dir
  buf_image_dir="$(mktemp -d)"
  local buf_image
  buf_image="${buf_image_dir}/buf.bin"
  trap 'rm -rf ${buf_image_dir}' EXIT
  git checkout "${LATEST_STABLE}"
  buf build -o "${buf_image}"
  git checkout "${CURRENT_BRANCH}"
  buf breaking --against "${buf_image}"
}

check_against_stable_protos_buf_image
