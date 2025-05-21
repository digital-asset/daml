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

declare -A publish=(
  [damlc]=publish_damlc
  [daml-script]=publish_daml_script
  [daml2js]=publish_daml2js
  [codegen]=publish_codegen
)

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
  info "Cleanup...\t\t\t\t"
  rm -rf "${STAGING_DIR}"/dist && info_done
}

trap on_exit SIGHUP SIGINT SIGQUIT SIGABRT EXIT

gen_manifest() {
  local commands="commands"
  if [[ "$artifact_path" =~ ".jar" ]]; then
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

###
# Publish component 'daml-script'
# target: //daml-script/runner:daml-script-binary_distribute.jar 
###
function publish_daml_script {
  local artifact_name="daml-script"
  local artifact_path="${artifact_name}-${RELEASE_TAG}.jar"
  local artifact_desc="Daml Script Binary"
  local artifact

cd "${STAGING_DIR}" || exit 1
(
  info "Create directory structure...\t\t"
  ${makedir} dist/generic/daml-script && info_done

  info "Processing daml-script for generic...\t"
  (
    artifact="$(find . -type f -name daml-script-"${RELEASE_TAG}".jar | head -1 )"
    if [ -f "${artifact}" ]; then
      ${copy} "${artifact}" dist/generic/daml-script
      gen_manifest > dist/generic/daml-script/component.yaml
    else
      info "Artifact not found: ${artifact}"
    fi
  ) && info_done

  info "Uploading daml-script to oci registry..."
  "${HOME}"/.unifi/bin/unifi \
    repo publish-component \
      "${artifact_name}" "${RELEASE_TAG}" \
      --platform generic=dist/generic/daml-script \
      --registry "${REGISTRY}" 1>&2 > "${logs}/${artifact_name}-${RELEASE_TAG}.log"
) && info_done || info_fail
}

###
# Publish component "codegen"
# target: "//language-support/java/codegen:binary"
###
function publish_codegen {
  local artifact_name="codegen"
  local artifact_path="${artifact_name}-${RELEASE_TAG}.jar"
  local artifact_desc="Daml Codegen"
  local artifact

cd "${STAGING_DIR}" || exit 1
(
  info "Create directory structure...\t\t"
  ${makedir} dist/generic/codegen && info_done

  info "Processing codegen for generic...\t"
  (
    artifact="$(find . -type f -name codegen-"${RELEASE_TAG}".jar | head -1 )"
    if [ -f "${artifact}" ]; then
      ${copy} "${artifact}" dist/generic/codegen
      gen_manifest > dist/generic/codegen/component.yaml
   else
     info "Artifact not found: ${artifact}"
   fi
  ) && info_done

  info "Uploading codegen to oci registry..."
  "${HOME}"/.unifi/bin/unifi \
    repo publish-component \
      ${artifact_name} "${RELEASE_TAG}" \
      --platform generic=dist/generic/codegen \
      --registry "${REGISTRY}" 1>&2 > "${logs}/${artifact_name}-${RELEASE_TAG}.log"
) && info_done || info_fail
}

###
# Publish component 'damlc'
# target:  //compiler/damlc:damlc-dist.tar.gz
###
function publish_damlc {
  local artifact_name="damlc"
  local artifact_path="damlc"
  local artifact_desc="Compiler and IDE backend for the Daml programming language"
  local artifact

cd "${STAGING_DIR}" || exit 1
 (
  info "Create directory structure...\t"  
  ${makedir} dist/{linux,darwin}/{arm64,amd64} && info_done

  info "Processing damlc for linux/arm64..."
  (
    artifact="$(find . -type f -name damlc-"${RELEASE_TAG}"-linux-arm.tar.gz | head -1)"
    if [ -f "${artifact}" ]; then
      ${unarchive} "${artifact}" -C dist/linux/arm64
      gen_manifest > dist/linux/arm64/damlc/component.yaml
    else 
      info "Artifact not found: ${artifact}"
    fi
  ) && info_done

  info "Processing damlc for linux/amd64..."
  (
    artifact="$(find . -type f -name damlc-"${RELEASE_TAG}"-linux-intel.tar.gz | head -1)"
    if [ -f "${artifact}" ]; then
      ${unarchive} "${artifact}" -C dist/linux/amd64
      gen_manifest > dist/linux/amd64/damlc/component.yaml
    else
      info "Artifact not found: ${artifact}"
    fi
  ) && info_done

  info "Processing damlc for darwin/arm64..."
  (
    artifact="$(find . -type f -name damlc-"${RELEASE_TAG}"-macos.tar.gz | head -1 )"
    if [ -f "${artifact}" ]; then
      ${unarchive} "${artifact}" -C dist/darwin/arm64
      gen_manifest > dist/darwin/arm64/damlc/component.yaml
    else
      info "Artifact not found: ${artifact}"
    fi
  ) && info_done

  info "Processing damlc for darwin/amd64..."
  (
    artifact="$(find . -type f -name damlc-"${RELEASE_TAG}"-macos.tar.gz | head -1 )"
    if [ -f "${artifact}" ]; then
      ${unarchive} "${artifact}" -C dist/darwin/amd64
      gen_manifest > dist/darwin/amd64/damlc/component.yaml
    else
      info "Artifact not found: ${artifact}"
    fi
  ) && info_done

  info "Uploading damlc to oci registry..."
  "${HOME}"/.unifi/bin/unifi \
    repo publish-component \
      "${artifact_name}" "${RELEASE_TAG}" \
      --platform linux/arm64=dist/linux/arm64/damlc \
      --platform linux/amd64=dist/linux/amd64/damlc \
      --platform darwin/arm64=dist/darwin/arm64/damlc \
      --platform darwin/amd64=dist/darwin/amd64/damlc \
      --registry "${REGISTRY}" 1>&2 > "${logs}/${artifact_name}-${RELEASE_TAG}.log"
 ) && info_done || info_fail
}

###
# Publish component 'daml2js'
# target: "//language-support/ts/codegen:daml2js-dist"
###
function publish_daml2js {
  local artifact_name="daml2js"
  local artifact_path="daml2js"
  local artifact_desc="Daml to JavaScript compiler"
  local artifact

cd "${STAGING_DIR}" || exit 1
 (
  info "Create directory structure...\t"
  ${makedir} dist/{linux,darwin}/{arm64,amd64} && info_done

  info "Processing daml2js for linux/arm64..."
  (
    artifact="$(find . -type f -name daml2js-"${RELEASE_TAG}"-linux-arm.tar.gz | head -1)"
    if [ -f "${artifact}" ]; then
      ${unarchive} "${artifact}" -C dist/linux/arm64
      gen_manifest > dist/linux/arm64/daml2js/component.yaml
    else
      info "Artifact not found: ${artifact}"
    fi
  ) && info_done

  info "Processing daml2js for linux/amd64..."
  (
    artifact="$(find . -type f -name daml2js-"${RELEASE_TAG}"-linux-intel.tar.gz | head -1)"
    if [ -f "${artifact}" ]; then
      ${unarchive} "${artifact}" -C dist/linux/amd64
      gen_manifest > dist/linux/amd64/daml2js/component.yaml
    else
      info "Artifact not found: ${artifact}"
    fi
  ) && info_done
  
  info "Processing daml2js for darwin/arm64..."
  (
    artifact="$(find . -type f -name daml2js-"${RELEASE_TAG}"-macos.tar.gz | head -1 )"
    if [ -f "${artifact}" ]; then
      ${unarchive} "${artifact}" -C dist/darwin/arm64
      gen_manifest > dist/darwin/arm64/daml2js/component.yaml
    else
      info "Artifact not found: ${artifact}"
    fi
  ) && info_done 
  
  info "Processing daml2js for darwin/amd64..."
  (
    artifact="$(find . -type f -name daml2js-"${RELEASE_TAG}"-macos.tar.gz | head -1 )"
    if [ -f "${artifact}" ]; then
      ${unarchive} "${artifact}" -C dist/darwin/amd64
      gen_manifest > dist/darwin/amd64/daml2js/component.yaml
    else
      info "Artifact not found: ${artifact}"
    fi
  ) && info_done
  
  info "Uploading daml2js to oci registry..."
    "${HOME}"/.unifi/bin/unifi \
      repo publish-component \
        "${artifact_name}" "${RELEASE_TAG}" \
        --platform linux/arm64=dist/linux/arm64/daml2js \
        --platform linux/amd64=dist/linux/amd64/daml2js \
        --platform darwin/arm64=dist/darwin/arm64/daml2js \
        --platform darwin/amd64=dist/darwin/amd64/daml2js \
        --registry "${REGISTRY}" 1>&2 > "${logs}/${artifact_name}-${RELEASE_TAG}.log"
 ) && info_done || info_fail
}


###
# Publish component "daml-bundled.vsix"
# target: "//compiler/daml-extension:vsix"
###


# __main__
for component in "${!publish[@]}"; do
 "${publish[$component]}"
done
