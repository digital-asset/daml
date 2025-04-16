#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -eo pipefail
script_name="$(basename "$0")"

if [ ! -f "${HOME}/.unifi/bin/unifi" ]; then
  echo "Unifi not found! Exit."
  exit 1
fi

if [[ "$#" != 3 ]]; then
  echo "Not enough parameters!"
  echo "Usage: ${script_name} <staging_dir> <release_tag> <registry>"
  exit 1
fi

STAGING_DIR=$1
RELEASE_TAG=$2
REGISTRY=$3

info() {
  (>&2 echo -n -e "\e[90m${script_name}\e[97m: [\e[0;36mINFO\e[97m]:\e[0m $1")
}
info_done() {
  (>&2 printf "\t\t\e[97m[\e[1;32mDONE\e[97m]\e[0m\n")
}
info_fail() {
  (>&2 printf "\t\t\e[97m[\e[1;31mFAIL\e[97m]\e[0m\n")
}
on_exit() {
  info "Cleanup...\t\t\t"
  rm -rf "${STAGING_DIR}"/{dist,output.log} && info_done
}

trap on_exit EXIT

gen_manifest() {
local dest=$1
info "Generate manifest...\t\t"
echo 'spec:
  commands:
  - path: "damlc"
    name: damlc
    desc: "damlc - Compiler and IDE backend for the Daml programming language"
' > "${dest}" && info_done
}

cd "${STAGING_DIR}" || exit 1
(
  info "Create directory structure...\t"  
  mkdir -p dist/{linux,darwin}/{arm64,amd64} && info_done
  artifact="$(find . -type f -name damlc-"${RELEASE_TAG}"-linux-arm.tar.gz | head -1)"
  info "Unarchive damlc for linux/arm64..." 
  tar xzf "${artifact}" -C dist/linux/arm64 damlc/damlc && info_done
  gen_manifest dist/linux/arm64/damlc/component.yaml
  artifact="$(find . -type f -name damlc-"${RELEASE_TAG}"-linux-intel.tar.gz | head -1)"
  info "Unarchive damlc for linux/amd64..." 
  tar xzf "${artifact}" -C dist/linux/amd64 damlc/damlc && info_done
  gen_manifest dist/linux/amd64/damlc/component.yaml
  artifact="$(find . -type f -name damlc-"${RELEASE_TAG}"-macos.tar.gz | head -1 )"
  info "Unarchive damlc for darwin/arm64..." 
  tar xzf "${artifact}" -C dist/darwin/arm64 damlc/damlc && info_done
  gen_manifest dist/darwin/arm64/damlc/component.yaml
  info "Unarchive damlc for darwin/amd64..." 
  tar xzf "${artifact}" -C dist/darwin/amd64 damlc/damlc && info_done
  gen_manifest dist/darwin/amd64/damlc/component.yaml

  info "Uploading to oci registry...\t"
  "${HOME}"/.unifi/bin/unifi \
    repo publish-component \
      damlc "${RELEASE_TAG}" \
      --platform linux/arm64=dist/linux/arm64/damlc \
      --platform linux/amd64=dist/linux/amd64/damlc \
      --platform darwin/amd64=dist/darwin/amd64/damlc \
      --registry "${REGISTRY}" 2>"${STAGING_DIR}/output.log" && info_done || info_fail
      cat "${STAGING_DIR}/output.log"
)
