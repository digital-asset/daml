#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

STAGING_DIR=$1
RELEASE_TAG=$2
dry_run="${DRY_RUN:-false}"

if [[ "${dry_run}" != true && "${RELEASE_TAG}" == 0.0.0 ]]; then
    echo "Refusing to publish version 0.0.0, it marks a non-release build. Set DRY_RUN=true to only print the uploads." >&2
    exit 1
fi

INPUTS=$STAGING_DIR/release-artifacts/artifactory

artifactory_url() {
    local repository file
    repository=$1
    file=${2#./}
    echo "https://digitalasset.jfrog.io/artifactory/${repository}/$RELEASE_TAG/${file}"
}

push() {
    local file repository md5 sha1 url
    repository=$1
    file=$2
    url=$(artifactory_url ${repository} ${file})
    md5=$(md5sum ${file} | awk '{print $1}')
    sha1=$(sha1sum ${file} | awk '{print $1}')
    echo "PUT ${url}  md5=${md5} sha1=${sha1}"
    if [[ "${dry_run}" == true ]]; then
        return
    fi
    curl -f \
         -u "$AUTH" \
         -H "X-Checksum-MD5:${md5}" \
         -H "X-Checksum-SHA1:${sha1}" \
         -X PUT \
         -T ${file} \
         "${url}"
}

SCRIPT_RUNNER=daml-script-$RELEASE_TAG.jar

cd $INPUTS
push daml-script-runner $SCRIPT_RUNNER
if [[ "${dry_run}" == true && ! -f $SCRIPT_RUNNER.asc ]]; then
    echo "PUT $(artifactory_url daml-script-runner $SCRIPT_RUNNER.asc)  (not signed in this run)"
else
    push daml-script-runner $SCRIPT_RUNNER.asc
fi

cd $STAGING_DIR/split-release
for d in split-release; do
    (
    cd $d
    for file in $(find . -type f | sort); do
        push assembly/daml $file
    done
    )
done
