#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
DEBUG="${DEBUG:-}"
DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${DIR}"/../sdk || exit 1

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <release-tag> <output-dir> <split-release>"
    exit 1
fi

RELEASE_TAG="${1}"
OUTPUT_DIR="${2}"
SPLIT_RELEASE="${3:-false}"

if [[ x"${DEBUG}" != "x" ]]; then
  copy="cp -v"
  makedir="mkdir -p -v"
else
  copy="cp -f"
  makedir="mkdir -p"
fi

for item in "github" "artifactory" "split-release" "oci/${RELEASE_TAG}/windows"; do
  ${makedir} "${OUTPUT_DIR}/${item}"
done

SDK_CE_TARBALL="daml-sdk-${RELEASE_TAG}-windows.tar.gz"
SDK_EE_TARBALL="daml-sdk-${RELEASE_TAG}-windows-ee.tar.gz"
echo "Copyring SDK tarball CE to ${OUTPUT_DIR}/github/${SDK_CE_TARBALL}"
${copy} bazel-bin/release/sdk-release-tarball-ce.tar.gz "${OUTPUT_DIR}/github/${SDK_CE_TARBALL}"
# Used for the non-split release process.
echo "Copying SDK tarball EE to ${OUTPUT_DIR}/artifactory/${SDK_EE_TARBALL}"
${copy} bazel-bin/release/sdk-release-tarball-ee.tar.gz "${OUTPUT_DIR}/artifactory/${SDK_EE_TARBALL}"
# Used for the split release process.
echo "Copying SDK tarball EE to ${OUTPUT_DIR}/split-release/${SDK_EE_TARBALL}"
${copy} bazel-bin/release/sdk-release-tarball-ee.tar.gz "${OUTPUT_DIR}/split-release/${SDK_EE_TARBALL}"

# OCI uploads
function copy_oci {
  OCI_NAME=$1
  OCI_PATH=$2
  OCI_OUT_PATH="${OUTPUT_DIR}/oci/${RELEASE_TAG}/windows/${OCI_NAME}.tar.gz"
  echo "Copying ${OCI_NAME}-oci to ${OCI_OUT_PATH}"
  ${copy} ${OCI_PATH} ${OCI_OUT_PATH}
}

# Copy all platforms for each, publishing script picks out platform independent
copy_oci damlc bazel-bin/compiler/damlc/damlc-oci.tar.gz
copy_oci daml-script bazel-bin/daml-script/runner/daml-script-oci.tar.gz
copy_oci codegen bazel-bin/language-support/codegen-main/codegen-oci.tar.gz
copy_oci daml-new bazel-bin/daml-assistant/daml-helper/daml-new-oci.tar.gz
copy_oci upgrade-check bazel-bin/daml-assistant/upgrade-check-main/upgrade-check-oci.tar.gz
