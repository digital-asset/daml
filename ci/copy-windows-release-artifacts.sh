#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -eou pipefail

RELEASE_TAG=$1
OUTPUT_DIR=$2

mkdir -p $OUTPUT_DIR/github
mkdir -p $OUTPUT_DIR/artifactory
INSTALLER=daml-sdk-$RELEASE_TAG-windows.exe
EE_INSTALLER=daml-sdk-$RELEASE_TAG-windows-ee.exe
mv "bazel-bin/release/windows-installer/daml-sdk-installer-ce.exe" "$OUTPUT_DIR/github/$INSTALLER"
mv "bazel-bin/release/windows-installer/daml-sdk-installer-ee.exe" "$OUTPUT_DIR/artifactory/$EE_INSTALLER"
chmod +wx "$OUTPUT_DIR/github/$INSTALLER"
chmod +wx "$OUTPUT_DIR/artifactory/$EE_INSTALLER"
cleanup () {
    rm -f signing_key.pfx
}
trap cleanup EXIT
echo "$SIGNING_KEY" | base64 -d > signing_key.pfx
for path in "$OUTPUT_DIR/github/$INSTALLER" "$OUTPUT_DIR/artifactory/$EE_INSTALLER"; do
    MSYS_NO_PATHCONV=1 signtool.exe sign '/f' signing_key.pfx '/fd' sha256 '/tr' "http://timestamp.digicert.com" '/v' "$path"
done
rm signing_key.pfx
trap - EXIT
TARBALL=daml-sdk-$RELEASE_TAG-windows.tar.gz
EE_TARBALL=daml-sdk-$RELEASE_TAG-windows-ee.tar.gz
cp bazel-bin/release/sdk-release-tarball-ce.tar.gz "$OUTPUT_DIR/github/$TARBALL"
cp bazel-bin/release/sdk-release-tarball-ee.tar.gz "$OUTPUT_DIR/artifactory/$EE_TARBALL"
