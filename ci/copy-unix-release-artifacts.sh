#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
DEBUG="${DEBUG:-}"
DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${DIR}"/../sdk || exit 1

if [[ $# != 3 ]]; then
    echo "Usage: $0 <release-tag> <name> <output-dir>"
    exit 1
fi

RELEASE_TAG="${1}"
NAME="${2}"
OUTPUT_DIR="${3}"

if [[ x"${DEBUG}" != "x" ]]; then
  copy="cp -v"
  makedir="mkdir -p -v"
else
  copy="cp -f"
  makedir="mkdir -p"
fi

for item in "github" "artifactory" "split-release"; do
  ${makedir} "${OUTPUT_DIR}/$item"
done

SDK_CE_TARBALL="daml-sdk-${RELEASE_TAG}-${NAME}.tar.gz"
echo "Copyring SDK tarball CE to ${OUTPUT_DIR}/github/${SDK_CE_TARBALL}"
${copy} bazel-bin/release/sdk-release-tarball-ce.tar.gz "${OUTPUT_DIR}/github/${SDK_CE_TARBALL}"

# Used for the non-split release process.
SDK_EE_TARBALL="daml-sdk-${RELEASE_TAG}-${NAME}-ee.tar.gz"
echo "Copying SDK tarball EE to ${OUTPUT_DIR}/artifactory/${SDK_EE_TARBALL}"
${copy} bazel-bin/release/sdk-release-tarball-ee.tar.gz "${OUTPUT_DIR}/artifactory/${SDK_EE_TARBALL}"

# Used for the split release process.
echo "Copying SDK tarball EE to ${OUTPUT_DIR}/split-release/${SDK_EE_TARBALL}"
${copy} bazel-bin/release/sdk-release-tarball-ee.tar.gz "${OUTPUT_DIR}/split-release/${SDK_EE_TARBALL}"

DAMLC="damlc-${RELEASE_TAG}-${NAME}.tar.gz"
echo "Copying damlc to ${OUTPUT_DIR}/split-release/${DAMLC}"
${copy} bazel-bin/compiler/damlc/damlc-dist.tar.gz "${OUTPUT_DIR}/split-release/${DAMLC}"

DAML2JS="daml2js-${RELEASE_TAG}-${NAME}.tar.gz"
echo "Copying daml2js to ${OUTPUT_DIR}/split-release/${DAML2JS}"
${copy} bazel-bin/language-support/ts/codegen/daml2js-dist.tar.gz "${OUTPUT_DIR}/split-release/${DAML2JS}"

# Platform independent artifacts are only built on Linux.
if [[ "${NAME}" == "linux-intel" ]]; then
    PROTOS_ZIP="protobufs-${RELEASE_TAG}.zip"
    ${copy} bazel-bin/release/protobufs.zip "${OUTPUT_DIR}/github/${PROTOS_ZIP}"

    SCRIPT="daml-script-${RELEASE_TAG}.jar"
    ${copy} bazel-bin/daml-script/runner/daml-script-binary_distribute.jar "${OUTPUT_DIR}/artifactory/${SCRIPT}"

    CODEGEN="codegen-${RELEASE_TAG}.jar"
    ${copy} bazel-bin/language-support/java/codegen/binary.jar "${OUTPUT_DIR}/artifactory/${CODEGEN}"

# Publishing of this component is not implemented yet.
#    BUNDLED_VSIX=daml-bundled-${RELEASE_TAG}.vsix
#    cp bazel-bin/compiler/daml-extension/daml-bundled.vsix ${OUTPUT_DIR}/oci/$BUNDLED_VSIX
#    cp bazel-bin/compiler/daml-extension/webview-stylesheet.css ${OUTPUT_DIR}/oci/webview-stylesheet.css

    ${makedir} "${OUTPUT_DIR}/split-release/daml-libs/daml-script"
    ${copy} bazel-bin/daml-script/daml/*.dar "${OUTPUT_DIR}/split-release/daml-libs/daml-script/"

    ${makedir} "${OUTPUT_DIR}/split-release/docs"

    ${copy} bazel-bin/docs/sphinx-source-tree.tar.gz "${OUTPUT_DIR}/split-release/docs/sphinx-source-tree-${RELEASE_TAG}.tar.gz"
    ${copy} bazel-bin/docs/sphinx-source-tree-deps.tar.gz "${OUTPUT_DIR}/split-release/docs/sphinx-source-tree-deps-${RELEASE_TAG}.tar.gz"
    ${copy} bazel-bin/docs/pdf-fonts-tar.tar.gz "${OUTPUT_DIR}/split-release/docs/pdf-fonts-${RELEASE_TAG}.tar.gz"
    ${copy} bazel-bin/docs/non-sphinx-html-docs.tar.gz "${OUTPUT_DIR}/split-release/docs/non-sphinx-html-docs-${RELEASE_TAG}.tar.gz"

    ${copy} bazel-bin/test-evidence/daml-security-test-evidence.csv "${OUTPUT_DIR}/github/daml-security-test-evidence-${RELEASE_TAG}.csv"
    ${copy} bazel-bin/test-evidence/daml-security-test-evidence.json "${OUTPUT_DIR}/github/daml-security-test-evidence-${RELEASE_TAG}.json"
fi
