#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
TEMP_DIR="$(mktemp -d)"
ORAS_VERSION="1.2.2"
ORAS="oras"
# Unifi latest url
UNIFI_URL="${1:-europe-docker.pkg.dev/da-images-dev/oci-private/components/assistant:latest}"
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
trap on_exit EXIT
# ORAS CLI is also a built-in tool in some VM images for GitHub-hosted runners used for GitHub Actions,
# as well as for Microsoft-hosted agents used for Azure Pipelines,
# boxes: Ubuntu 20.04, Ubuntu 22.04 (https://oras.land/docs/installation)
if [ ! -x "$(command -v oras)" ]; then
  curl -o ${TEMP_DIR}/oras_${ORAS_VERSION}_${CPU_ARCH}.tar.gz \
   -sqL "https://github.com/oras-project/oras/releases/download/v${ORAS_VERSION}/oras_${ORAS_VERSION}_linux_${CPU_ARCH}.tar.gz"
  tar -zxf ${TEMP_DIR}/oras_${ORAS_VERSION}_*.tar.gz -C ${TEMP_DIR}
  ORAS="${TEMP_DIR}/oras"
fi

# Prepare unifi install destination
[[ ! -d "${HOME}/.unifi/bin" ]] && mkdir -pv "${HOME}/.unifi/bin"
# Get unifi from artifacts registry
${ORAS} pull --platform "${OS_TYPE}/${CPU_ARCH}" -o "${HOME}/.unifi/bin" "${UNIFI_URL}"
# Set execute permission
chmod -v +x "${HOME}/.unifi/bin/unifi"
${HOME}/.unifi/bin/unifi version --assistant
