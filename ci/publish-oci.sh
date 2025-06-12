#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -eo pipefail
script_name="$(basename "$0")"
FAILURE=0
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
  FAILURE=1
  (>&2 echo -n -e "\e[90m${script_name}\e[97m: \e[97m[\e[1;31mFAIL\e[97m]\e[0m $1\n")
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
UNIFI_ASSISTANT_REGISTRY=$3

declare -A publish=(
  [damlc]=publish_damlc
  [daml-script]=publish_daml_script
  [daml2js]=publish_daml2js
  [codegen]=publish_codegen
)

if [[ x"$DEBUG" != x ]]; then
  unarchive="tar -x -v -z -f"
  copy="cp -v"
  move="mv -v"
  makedir="mkdir -p -v"
else
  unarchive="tar -x -z -f"
  copy="cp -f"
  move="mv -f"
  makedir="mkdir -p"
fi
logs="${STAGING_DIR}/logs"
errors="${logs}/errors.log"
${makedir} "${logs}"

function on_exit() {
  info "Cleanup...\t\t\t\t"
  rm -rf "${STAGING_DIR}"/dist && info_done
}

trap on_exit SIGHUP SIGINT SIGQUIT SIGABRT EXIT

gen_manifest() {
  local commands="commands"
  if [[ "$artifact_path" =~ .jar ]]; then
    commands="jar-commands"
  fi
  echo '
apiVersion: digitalasset.com/v1
kind: Component
spec:
  '"${commands}"':
    - path: '"${artifact_path}"'
      name: '"${artifact_name}"'
      desc: '"${artifact_desc}"'
  '
}
function publish_artifact {
  local artifact_name="${1}"
  local artifact_path="${2}"
  local artifact_desc="${3}"
  if [[ "$artifact_path" =~ .jar ]]; then
    declare -a artifact_platforms=( "generic" )
  else
    declare -a artifact_platforms=( "linux-arm,linux/arm64" "linux-intel,linux/amd64" "macos,darwin/arm64" "macos,darwin/amd64" "windows,windows/arm64" "windows,windows/amd64" )
  fi
  declare -a platform_args
  local artifact arch search_pattern

cd "${STAGING_DIR}" || exit 1
 (
  # shellcheck disable=SC2068
  for item in ${artifact_platforms[@]}; do
    if [[ "$artifact_path" =~ .jar ]]; then
        arch="${item}"
        search_pattern="${artifact_path%%/*}"
      else
        arch="${item##*,}"
        search_pattern="${artifact_name}-${RELEASE_TAG}-${item%%,*}.tar.gz"
    fi
    info "Processing ${artifact_name} for ${arch}...\n"
    artifact="$(find . -type f -name ${search_pattern} | head -1)"
    if [[ -f "${artifact}" ]]; then
      ${makedir} "dist/${arch}/${artifact_name}"
        if [[ "$artifact_path" =~ .jar ]]; then
          ${copy} ${artifact} "dist/${arch}/${artifact_name}"
        else
          ${unarchive} "${artifact}" -C "dist/${arch}/${artifact_name}"
          ${move} "dist/${arch}/${artifact_name}/${artifact_name}" "dist/${arch}/${artifact_name}/${artifact_name}-${RELEASE_TAG}"
        fi
        gen_manifest > "dist/${arch}/${artifact_name}/component.yaml"
        platform_args+=( "--platform ${arch}=dist/${arch}/${artifact_name} " )
    else
        info_fail "Artifact not found: ${search_pattern}"
        FAILURE=1
    fi
  done
  if [[ "${FAILURE}" == 0 ]]; then
    info "Uploading ${artifact_name} to oci registry...\n"
    "${HOME}"/.unifi/bin/unifi \
      repo publish-component \
        "${artifact_name}" "${RELEASE_TAG}" --extra-tags latest ${platform_args[@]} \
        --registry "${UNIFI_ASSISTANT_REGISTRY}" 2>&1 | tee "${logs}/${artifact_name}-${RELEASE_TAG}.log"
  fi
 )
}

###
# Publish component 'damlc'
# target:  //compiler/damlc:damlc-dist.tar.gz
###
function publish_damlc {
  publish_artifact "damlc" "damlc-${RELEASE_TAG}/damlc" "Compiler and IDE backend for the Daml programming language"
}

###
# Publish component 'daml-script'
# target: //daml-script/runner:daml-script-binary_distribute.jar
###
function publish_daml_script {
  publish_artifact "daml-script" "daml-script-${RELEASE_TAG}.jar" "Daml Script Binary"
}

###
# Publish component 'daml2js'
# target: "//language-support/ts/codegen:daml2js-dist"
###
function publish_daml2js {
  publish_artifact "daml2js" "daml2js-${RELEASE_TAG}/daml2js" "Daml to JavaScript compiler"
}

###
# Publish component "codegen"
# target: "//language-support/java/codegen:binary"
###
function publish_codegen {
  publish_artifact "codegen" "codegen-${RELEASE_TAG}.jar" "Daml Codegen"
}

# __main__
for component in "${!publish[@]}"; do
 "${publish[$component]}"
done
