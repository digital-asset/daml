#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
TEMP_DIR="$(mktemp -d)"
ORAS_VERSION="1.2.2"
ORAS="oras"
# DPM url pinned to known working version
# Update periodically
DPM_URL="${1:-europe-docker.pkg.dev/da-images/public/components/dpm:1.0.8}"
# Get current machine OS type and ARCH
OS_TYPE="$(uname -s | tr A-Z a-z)"
if [[ $(uname -m) == 'x86_64' ]]; then
  CPU_ARCH='amd64'
else
  CPU_ARCH="$(uname -m)"
fi
# cleanup file on exit signal
function on_exit () {
  rm -rf ${TEMP_DIR}
}
function on_failure () {
  rm -rf "${HOME}/.dpm"
}
trap on_exit EXIT
trap on_failure ERR
# ORAS CLI is also a built-in tool in some VM images for GitHub-hosted runners used for GitHub Actions,
# as well as for Microsoft-hosted agents used for Azure Pipelines,
# boxes: Ubuntu 20.04, Ubuntu 22.04 (https://oras.land/docs/installation)
if [ ! -x "$(command -v oras)" ]; then
  curl -o ${TEMP_DIR}/oras_${ORAS_VERSION}_${CPU_ARCH}.tar.gz \
   -sqL "https://github.com/oras-project/oras/releases/download/v${ORAS_VERSION}/oras_${ORAS_VERSION}_${OS_TYPE}_${CPU_ARCH}.tar.gz"
  tar -zxf ${TEMP_DIR}/oras_${ORAS_VERSION}_*.tar.gz -C ${TEMP_DIR}
  ORAS="${TEMP_DIR}/oras"
fi

# Prepare dpm install destination
mkdir -p "${HOME}/.dpm/bin"

# Get dpm from artifacts registry
rm -f ${HOME}/.dpm/bin/dpm
${ORAS} pull --platform "${OS_TYPE}/${CPU_ARCH}" -o "${HOME}/.dpm/bin" "${DPM_URL}"
# Set execute permission
chmod -v +x "${HOME}/.dpm/bin/dpm"
# Find out what version it is
version_line=$(${HOME}/.dpm/bin/dpm --version | grep -o 'version: .*')
version=${version_line#version: }

# Move to correct component location
mkdir -p "${HOME}/.dpm/cache/components/dpm/${version}"
mv -f "${HOME}/.dpm/bin/dpm" "${HOME}/.dpm/cache/components/dpm/${version}/dpm"
# Create symlink for .dpm/bin, to match DPM's current behaviour
ln -sf "${HOME}/.dpm/cache/components/dpm/${version}/dpm" "${HOME}/.dpm/bin/dpm"

"${HOME}/.dpm/bin/dpm" --version
