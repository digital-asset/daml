#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -eo pipefail
script_name="$(basename "$0")"

err() {
  (>&2 echo -e "\e[90m${script_name}\e[97m: [\e[1;31mERROR\e[97m]:\e[0m $1")
}
info() {
  (>&2 echo -n -e "\e[90m${script_name}\e[97m: [\e[0;36mINFO\e[97m]:\e[0m $1")
}
info_done() {
  (>&2 printf "\t\t\e[97m[\e[1;32mDONE\e[97m]\e[0m\n")
}
info_fail() {
  (>&2 printf "\t\t\e[97m[\e[1;31mFAIL\e[97m]\e[0m\n")
}

if [ ! -f "${HOME}/.unifi/bin/unifi" ]; then
  err "Unifi not found! Exit."
  exit 1
fi

if [[ "$#" != 3 ]]; then
  err "Not enough parameters!"
  err "Usage: ${script_name} <staging_dir> <release_tag> <registry>"
  exit 1
fi

STAGING_DIR=$1
RELEASE_TAG=$2
REGISTRY=$3

on_exit() {
  info "Cleanup...\t\t\t"
  rm -rf "${STAGING_DIR}"/{dist,output.log} && info_done
}

trap on_exit EXIT

gen_manifest() {
local component=$1
if [[ "${component}" == "damlc" ]]; then
echo '
apiVersion: digitalasset.com/v1
kind: Component
spec:
  commands:
  - path: damlc
    name: damlc
    desc: "Compiler and IDE backend for the Daml programming language"
'
elif [[ "${component}" == "daml-script" ]]; then
echo '
apiVersion: digitalasset.com/v1
kind: Component
spec:
  jar-commands:
    - path: daml-script-'"${RELEASE_TAG}"'.jar
      name: daml-script
      desc: "Daml Script Binary"
'
fi
}

cd "${STAGING_DIR}" || exit 1
(
  info "Create directory structure...\t"  
  mkdir -p dist/{linux,darwin}/{arm64,amd64} && info_done

  info "Proccesing damlc for linux/arm64..."
  (
    artifact="$(find . -type f -name damlc-"${RELEASE_TAG}"-linux-arm.tar.gz | head -1)"
    tar xzf "${artifact}" -C dist/linux/arm64
    gen_manifest damlc > dist/linux/arm64/damlc/component.yaml
  ) && info_done

  info "Proccesing damlc for linux/amd64..."
  (
    artifact="$(find . -type f -name damlc-"${RELEASE_TAG}"-linux-intel.tar.gz | head -1)"
    tar xzf "${artifact}" -C dist/linux/amd64
    gen_manifest damlc > dist/linux/amd64/damlc/component.yaml
  ) && info_done

  info "Proccesing damlc for darwin/arm64..."
  (
    artifact="$(find . -type f -name damlc-"${RELEASE_TAG}"-macos.tar.gz | head -1 )"
    tar xzf "${artifact}" -C dist/darwin/arm64
    gen_manifest damlc > dist/darwin/arm64/damlc/component.yaml
  ) && info_done

  info "Proccesing damlc for darwin/amd64..."
  (
    artifact="$(find . -type f -name damlc-"${RELEASE_TAG}"-macos.tar.gz | head -1 )"
    tar xzf "${artifact}" -C dist/darwin/amd64
    gen_manifest damlc > dist/darwin/amd64/damlc/component.yaml
  ) && info_done

  info "Uploading to oci registry...\t"
  "${HOME}"/.unifi/bin/unifi \
    repo publish-component \
      damlc "${RELEASE_TAG}" \
      --platform linux/arm64=dist/linux/arm64/damlc \
      --platform linux/amd64=dist/linux/amd64/damlc \
      --platform darwin/arm64=dist/darwin/arm64/damlc \
      --platform darwin/amd64=dist/darwin/amd64/damlc \
      --registry "${REGISTRY}" && info_done || info_fail

  info "Create directory structure...\t"
  mkdir -p dist/generic/daml-script && info_done

  info "Proccesing daml-script for generic..."
  (
    artifact="$(find . -type f -name daml-script-"${RELEASE_TAG}".jar | head -1 )"
    cp "${artifact}" dist/generic/daml-script
    gen_manifest daml-script > dist/generic/daml-script/component.yaml
  ) && info_done

  info "Uploading daml-script to oci registry...\t"
  "${HOME}"/.unifi/bin/unifi \
    repo publish-component \
      daml-script "${RELEASE_TAG}" \
      --platform generic=dist/generic/daml-script \
      --registry "${REGISTRY}" && info_done || info_fail
)
