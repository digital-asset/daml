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

if [[ "$#" != 3 ]]; then
  err "Not enough parameters!"
  err "Usage: ${script_name} <staging_dir> <release_tag> <registry>"
  exit 1
fi

STAGING_DIR=$(cd "$1" && pwd)
RELEASE_TAG=$2
DPM_REGISTRY=$3

function extract_major_minor() {
  version="$1"
  echo "$version" | grep -oE '^[0-9]+\.[0-9]+'
}

tag_main=false
if [[ "${RELEASE_TAG}" == *"-snapshot."* ]] \
   && git fetch --quiet origin main \
   && main_prefix=$(git show FETCH_HEAD:sdk/NIGHTLY_PREFIX) \
   && [[ "$(extract_major_minor "${RELEASE_TAG}")" == "$(extract_major_minor "${main_prefix}")" ]]; then
  tag_main=true
fi

declare -a components=(damlc daml-script codegen daml-new upgrade-check)

if [[ x"$DEBUG" != x ]]; then
  unarchive="tar -x -v -z -f"
  makedir="mkdir -p -v"
else
  unarchive="tar -x -z -f"
  makedir="mkdir -p"
fi

function on_exit() {
  info "Cleanup...\t\t\t\t"
  rm -rf "${STAGING_DIR}"/dist && info_done
}

trap on_exit SIGHUP SIGINT SIGQUIT SIGABRT EXIT

function publish_artifact {
  local artifact_name="${1}"
  declare -a artifact_platforms=( "linux-x86_64,linux/amd64" "linux-aarch64,linux/arm64" "macos-aarch64,darwin/arm64" "macos-x86_64,darwin/amd64" "windows-x86_64,windows/amd64" )
  declare -a platform_args
  declare -a extra_tags_args
  declare -a platform_dirs
  cd "${STAGING_DIR}" || exit 1
  (
    for item in "${artifact_platforms[@]}"; do
      arch="${item##*,}"
      plat="${item%%,*}"
      ${makedir} "dist/${arch}/${artifact_name}"
      artifact_path=release-artifacts/oci/${RELEASE_TAG}/${plat}/${artifact_name}.tar.gz
      ${unarchive} "${artifact_path}" --unlink-first -C "dist/${arch}/${artifact_name}"
      if [ -f "dist/${arch}/${artifact_name}/is-agnostic" ]; then
          rm dist/${arch}/${artifact_name}/is-agnostic
          platform_args+=( "--platform generic=${STAGING_DIR}/dist/${arch}/${artifact_name}" )
          platform_dirs+=( "generic (from ${plat}),dist/${arch}/${artifact_name}" )
          break
      fi
      platform_args+=( "--platform ${arch}=${STAGING_DIR}/dist/${arch}/${artifact_name}" )
      platform_dirs+=( "${arch} (from ${plat}),dist/${arch}/${artifact_name}" )
    done
    if [[ "${RELEASE_TAG}" != *"-adhoc"* ]]; then
      extra_tags_args+=( "--extra-tags $(extract_major_minor ${RELEASE_TAG})" )
      if [[ "${tag_main}" == true ]]; then
        extra_tags_args+=( "--extra-tags main" )
      fi
    fi
    info "Would upload ${artifact_name} to oci registry:\n"

    printf 'dpm publish component oci://%s/components/%s:%s' "${DPM_REGISTRY}" "${artifact_name}" "${RELEASE_TAG}"
    printf ' \\\n    %s' "${extra_tags_args[@]}" "${platform_args[@]}"
    printf '\n'
    for entry in "${platform_dirs[@]}"; do
      label="${entry%%,*}"
      dir="${entry#*,}"
      printf '  %-38s %6s  %s\n' "${label}" "$(du -sh "${dir}" | cut -f1)" "$(ls -A "${dir}" | tr '\n' ' ')"
    done
    printf '\n'
  )
}

for component in "${components[@]}"; do
  publish_artifact $component
done
