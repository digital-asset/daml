#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -eou pipefail

RELEASE_TAG=$1
OUTPUT_DIR=$2

mkdir -p $OUTPUT_DIR/github
mkdir -p $OUTPUT_DIR/artifactory
INSTALLER="$OUTPUT_DIR/github/daml-sdk-$RELEASE_TAG-windows.exe"
EE_INSTALLER="$OUTPUT_DIR/artifactory/daml-sdk-$RELEASE_TAG-windows-ee.exe"
mv "bazel-bin/release/windows-installer/daml-sdk-installer-ce.exe" "$INSTALLER"
mv "bazel-bin/release/windows-installer/daml-sdk-installer-ee.exe" "$EE_INSTALLER"
chmod +wx "$INSTALLER"
chmod +wx "$EE_INSTALLER"

if ! [ -f /C/Users/u/.dotnet/tools/azuresigntool.exe ]; then
    "/C/Program Files/dotnet/dotnet.exe" tool install --global AzureSignTool --version 3.0.0
fi

/C/Users/u/.dotnet/tools/azuresigntool.exe sign \
  --azure-key-vault-url "$AZURE_KEY_VAULT_URL" \
  --azure-key-vault-client-id "$AZURE_CLIENT_ID" \
  --azure-key-vault-tenant-id "$AZURE_TENANT_ID" \
  --azure-key-vault-client-secret "$AZURE_CLIENT_SECRET" \
  --azure-key-vault-certificate "$AZURE_KEY_VAULT_CERTIFICATE" \
  --description "Daml SDK installer" \
  --description-url "https://daml.com" \
  --timestamp-rfc3161 "http://timestamp.digicert.com" \
  --file-digest sha384 \
  --verbose \
  "$INSTALLER" \
  "$EE_INSTALLER"

TARBALL=daml-sdk-$RELEASE_TAG-windows.tar.gz
EE_TARBALL=daml-sdk-$RELEASE_TAG-windows-ee.tar.gz
cp bazel-bin/release/sdk-release-tarball-ce.tar.gz "$OUTPUT_DIR/github/$TARBALL"
cp bazel-bin/release/sdk-release-tarball-ee.tar.gz "$OUTPUT_DIR/artifactory/$EE_TARBALL"
