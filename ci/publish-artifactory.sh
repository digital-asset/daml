#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

STAGING_DIR=$1
RELEASE_TAG=$2

INPUTS=$STAGING_DIR/release-artifacts/artifactory

artifactory_url() {
    local repository file
    repository=$1
    file=${2#./}
    echo "https://digitalasset.jfrog.io/artifactory/${repository}/$RELEASE_TAG/${file}"
}

push() {
    local file repository md5 sha1
    repository=$1
    file=$2
    md5=$(md5sum ${file} | awk '{print $1}')
    sha1=$(sha1sum ${file} | awk '{print $1}')
    echo "PUT $(artifactory_url ${repository} ${file})  md5=${md5} sha1=${sha1}"
}

SCRIPT_RUNNER=daml-script-$RELEASE_TAG.jar

cd $INPUTS
push daml-script-runner $SCRIPT_RUNNER
if [[ -f $SCRIPT_RUNNER.asc ]]; then
    push daml-script-runner $SCRIPT_RUNNER.asc
else
    echo "PUT $(artifactory_url daml-script-runner $SCRIPT_RUNNER.asc)  (not signed in this run)"
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
