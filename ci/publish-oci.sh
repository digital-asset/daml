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
  (>&2 echo -n -e "\e[97m[\e[1;32mDONE\e[97m]\e[0m\n")
}
info_fail() {
  (>&2 echo -n -e "\e[90m${script_name}\e[97m: \e[97m[\e[1;31mFAIL\e[97m]\e[0m $1\n")
  echo "$1" >> "${logs}/failed_artifacts.log"
}

if [ ! -f "${HOME}/.dpm/bin/dpm" ]; then
  err "DPM not found! Exit."
  exit 1
fi

if [[ "$#" != 3 ]]; then
  err "Not enough parameters!"
  err "Usage: ${script_name} <staging_dir> <release_tag> <registry>"
  exit 1
fi

STAGING_DIR=$1
RELEASE_TAG=$2
# This script is called by an azure pipeline, which will always be from main.
# To change the registry (for testing), it must be modified within this script.
# Uncomment the relevant line on the published branch to control the registry used.
DPM_REGISTRY=$3
# DPM_REGISTRY="europe-docker.pkg.dev/da-images-dev/oci-playground"

# Should match the tars copied into /release/oci during copy-{OS}-release-artifacts.sh
declare -a components=(damlc daml-script daml2js codegen daml-new)

if [[ x"$DEBUG" != x ]]; then
  unarchive="tar -x -v -z -f"
  copy="cp -v"
  makedir="mkdir -p -v"
else
  unarchive="tar -x -z -f"
  copy="cp -f"
  makedir="mkdir -p"
fi

logs="${STAGING_DIR}/logs"
${makedir} "${logs}"

function on_exit() {
  if [[ -f "${logs}/failed_artifacts.log" ]]; then
    err "Some artifacts failed to publish. See the log below:"
    cat "${logs}/failed_artifacts.log" | while read -r line; do
      err "\e[1;31m${line}\e[0m"
    done
    rm -f "${logs}/failed_artifacts.log"
    printf "\n"
  fi
  info "Cleanup...\t\t\t\t"
  rm -rf "${STAGING_DIR}"/dist && info_done
}

trap on_exit SIGHUP SIGINT SIGQUIT SIGABRT EXIT

function publish_artifact {
  local artifact_name="${1}"
  declare -a artifact_platforms=( "linux-intel,linux/amd64" "linux-arm,linux/arm64" "macos,darwin/arm64" "macos,darwin/amd64" "windows,windows/amd64" )
  declare -a platform_args
  cd "${STAGING_DIR}" || exit 1
  (
    for item in "${artifact_platforms[@]}"; do
      arch="${item##*,}"
      plat="${item%%,*}"
      ${makedir} "dist/${arch}/${artifact_name}"
      artifact_path=release-artifacts/oci/${RELEASE_TAG}/${plat}/${artifact_name}.tar.gz
      ${unarchive} "${artifact_path}" --unlink-first -C "dist/${arch}/${artifact_name}"
      if [ -f "dist/${arch}/${artifact_name}/is-agnostic" ]; then
          # If agnostic, remove the marker, upload as generic platform and break out for other platforms
          # (this will upload the first platform, i.e. linux-intel)
          rm dist/${arch}/${artifact_name}/is-agnostic
          platform_args+=( "--platform generic=dist/${arch}/${artifact_name} " )
          break
      fi
      platform_args+=( "--platform ${arch}=dist/${arch}/${artifact_name} " )
    done
    info "Uploading ${artifact_name} to oci registry...\n"
    "${HOME}"/.dpm/bin/dpm \
      repo publish-component \
        "${artifact_name}" "${RELEASE_TAG}" --extra-tags latest ${platform_args[@]} \
        --registry "${DPM_REGISTRY}" 2>&1 | tee "${logs}/${artifact_name}-${RELEASE_TAG}.log"
  )
}

for component in "${components[@]}"; do
  publish_artifact $component
done
