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

for item in "github" "artifactory" "split-release"; do
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

DAMLC="damlc-${RELEASE_TAG}-windows.tar.gz"
echo "Copying damlc to ${OUTPUT_DIR}/split-release/${DAMLC}"
${copy} bazel-bin/compiler/damlc/damlc-dist.tar.gz "${OUTPUT_DIR}/split-release/${DAMLC}"

DAML2JS="daml2js-${RELEASE_TAG}-windows.tar.gz"
echo "Copying daml2js to ${OUTPUT_DIR}/split-release/${DAML2JS}"
${copy} bazel-bin/language-support/ts/codegen/daml2js-dist.tar.gz "${OUTPUT_DIR}/split-release/${DAML2JS}"

