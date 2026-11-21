#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

STAGING_DIR=$1
RELEASE_TAG=$2

# We’ve already published to GH so focus on artifactory

INPUTS=$STAGING_DIR/release-artifacts/artifactory

push() {
    local file repository md5 sha1
    repository=$1
    file=$2
    md5=$(md5sum ${file} | awk '{print $1}')
    sha1=$(sha1sum ${file} | awk '{print $1}')
    curl -f \
         -u "$AUTH" \
         -H "X-Checksum-MD5:${md5}" \
         -H "X-Checksum-SHA1:${sha1}" \
         -X PUT \
         -T ${file} \
         https://digitalasset.jfrog.io/artifactory/${repository}/$RELEASE_TAG/${file}
}

SCRIPT_RUNNER=daml-script-$RELEASE_TAG.jar

cd $INPUTS
push daml-script-runner $SCRIPT_RUNNER
push daml-script-runner $SCRIPT_RUNNER.asc

# For the split release process these are not published to artifactory.
if [[ "$#" -lt 3 || $3 != "split" ]]; then
   for platform in linux macos windows; do
       EE_TARBALL=daml-sdk-$RELEASE_TAG-$platform-ee.tar.gz
       push sdk-ee $EE_TARBALL
       push sdk-ee $EE_TARBALL.asc
   done
   EE_INSTALLER=daml-sdk-$RELEASE_TAG-windows-ee.exe
   push sdk-ee $EE_INSTALLER
   push sdk-ee $EE_INSTALLER.asc
else
    # For the split release process, we publish intermediate artifacts to the
    # assembly repo, under the daml folder.
    cd $STAGING_DIR/split-release
    for d in split-release github; do
        (
        cd $d
        for file in $(find . -type f); do
            push assembly/daml $file
        done
        )
    done
fi
