#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

STAGING_DIR=$1
RELEASE_TAG=$2

# We’ve already published to GH so focus on artifactory

INPUTS=$STAGING_DIR/release-artifacts/other

push() {
    local file repository local_path md5 sha1
    repository=$1
    file=$2
    local_path=$$INPUTS/${file}
    md5=$(md5sum ${local_path} | awk '{print $1}')
    sha1=$(sha1sum ${local_path} | awk '{print $1}')
    curl -f \
         -u "$AUTH" \
         -H "X-Checksum-MD5:${md5}" \
         -H "X-Checksum-SHA1:${sha1}" \
         -X PUT \
         -T ${local_path} \
         https://digitalasset.jfrog.io/artifactory/${repository}/$(release_tag)/${file}
}

TRIGGER_RUNNER=daml-trigger-runner-$RELEASE_TAG.jar
SCRIPT_RUNNER=daml-script-$RELEASE_TAG.jar

push daml-trigger-runner $TRIGGER_RUNNER
push daml-trigger-runner $TRIGGER_RUNNER.asc
push daml-script-runner $SCRIPT_RUNNER
push daml-script-runner $SCRIPT_RUNNER.asc
