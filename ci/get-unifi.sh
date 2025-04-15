#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

OS_TYPE="$(uname -s | tr A-Z a-z)"
 if [[ $(uname -m) == 'x86_64' ]]; then
    CPU_ARCH='amd64'
  else
    CPU_ARCH="$(uname -m)"
 fi
[[ ! -d "${HOME}/bin" ]] && mkdir -pv ${HOME}/bin
export PATH="${HOME}/bin:$PATH"

gcloud auth configure-docker europe-docker.pkg.dev
 [[ ! -d "${HOME}/.unifi/bin" ]] && mkdir -pv "${HOME}/.unifi/bin"
 nix-shell -p oras --run "oras pull --platform \"${OS_TYPE}/${CPU_ARCH}\" \
              -o \"${HOME}/.unifi/bin\" \
              \"europe-docker.pkg.dev/da-images-dev/oci-playground/components/assistant:latest\""
chmod -v +x "${HOME}/.unifi/bin/unifi"
${HOME}/.unifi/bin/unifi version --assistant

