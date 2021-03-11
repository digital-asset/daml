#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -eou pipefail

RELEASE_TAG=$1
OUTPUT_DIR=$2

mkdir -p $OUTPUT_DIR/github
INSTALLER=daml-sdk-$RELEASE_TAG-windows.exe
mv "bazel-bin/release/windows-installer/daml-sdk-installer.exe" "$OUTPUT_DIR/github/$INSTALLER"
chmod +wx "$OUTPUT_DIR/github/$INSTALLER"
cleanup () {
    rm -f signing_key.pfx
}
trap cleanup EXIT
echo "$SIGNING_KEY" | base64 -d > signing_key.pfx
MSYS_NO_PATHCONV=1 signtool.exe sign '/f' signing_key.pfx '/fd' sha256 '/tr' "http://timestamp.digicert.com" '/v' "$(Build.StagingDirectory)/$INSTALLER"
rm signing_key.pfx
trap - EXIT
TARBALL=daml-sdk-$RELEASE_TAG-windows.tar.gz
cp bazel-bin/release/sdk-release-tarball.tar.gz "$OUTPUT_DIR/github/$TARBALL"
