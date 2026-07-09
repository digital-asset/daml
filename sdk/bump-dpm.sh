#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Takes optional DPM version, if none provided, latest stable version will be used.
# Only allows DPM versions from the stable registry, as this is what the bazel rule uses

# Must be run from `sdk`

DPM_VERSION=$1
DPM_REGISTRY="europe-docker.pkg.dev/da-images/public"

if [ -z "$DPM_VERSION" ]; then
  # dpm prints things to stdout when downloading, which is quite annoying
  # Run it once to cache the output, then ask again for the sanitised output
  bazel run @dpm_binary//:dpm -- repo resolve-tags --registry $DPM_REGISTRY dpm:latest > /dev/null
  DPM_VERSION=$(bazel run @dpm_binary//:dpm -- repo resolve-tags --registry $DPM_REGISTRY dpm:latest 2> /dev/null)
fi

echo "Bumping DPM to $DPM_VERSION"

platforms=(linux_arm64 darwin_amd64 darwin_arm64 linux_amd64 windows_amd64)

shas=()
for platform in "${platforms[@]}"; do
  sha=$(bazel run @dpm_binary//:oras -- manifest fetch $DPM_REGISTRY/components/dpm:$DPM_VERSION.$platform  2> /dev/null | jq -r '.layers[0].digest | split(":")[1]')
  shas+=("    \"$platform\": \"$sha\",")
done

cat << EOF > "bazel_tools/dpm_version.bzl"
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# THIS IS A GENERATED FILE
# Bump using \`bump-dpm.sh\`

DPM_VERSION = "$DPM_VERSION"

DPM_SHA256 = {
$(IFS=$'\n'; echo -e "${shas[*]}")
}
EOF
