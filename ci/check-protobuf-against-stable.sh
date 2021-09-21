#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

readonly SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
readonly SRC_DIR="$(dirname "${SCRIPT_DIR}")"

cd "${SRC_DIR}"
readonly CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"

set_latest_stable() {
  local gitTagSearchSpace
  if [[ "${CURRENT_BRANCH}" == "main" ]]; then
    gitTagSearchSpace="--no-merged"
  else
    gitTagSearchSpace="--merged"
  fi
  LATEST_STABLE="$(git tag ${gitTagSearchSpace} | grep -v "snapshot" | sort -V | tail -1)"
}

cleanup_tmp_files() {
  rm -rf "${TMP_STABLE_PROTOS_DIR}"
}

checkout_stable_protos() {
  trap cleanup_tmp_files EXIT
  TMP_STABLE_PROTOS_DIR="$(mktemp -d -t ci-stable-protos-XXXXXXXXXX)"
  local archive;
  archive="${TMP_STABLE_PROTOS_DIR}/archive.tar.gz"
  git archive --output="${archive}" "${LATEST_STABLE}" daml-lf ledger-api ledger
  tar xzf "${archive}" -C "${TMP_STABLE_PROTOS_DIR}"
}

readonly BUF_IMAGE="buf-stable-protos-image.bin"

create_stable_protos_buf_image() {
  cp "${SRC_DIR}/buf.yaml" "${SRC_DIR}/buf.lock" "${TMP_STABLE_PROTOS_DIR}"
  cd "${TMP_STABLE_PROTOS_DIR}"
  buf build -o "${BUF_IMAGE}"
}

check_against_stable_protos_buf_image() {
  cd "${SRC_DIR}"
  buf breaking --against "${TMP_STABLE_PROTOS_DIR}/${BUF_IMAGE}"
}

set_latest_stable
echo "Checking protos against '${LATEST_STABLE}' (i.e., the most recent stable tag up to '${CURRENT_BRANCH}')"

checkout_stable_protos
create_stable_protos_buf_image
check_against_stable_protos_buf_image
