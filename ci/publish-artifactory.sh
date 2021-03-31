#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

STAGING_DIR=$1
RELEASE_TAG=$2

# We’ve already published to GH so focus on artifactory

INPUTS=$STAGING_DIR/release-artifacts/artifactory

push() {
    local file repository local_path md5 sha1
    repository=$1
    file=$2
    local_path=$INPUTS/${file}
    md5=$(md5sum ${local_path} | awk '{print $1}')
    sha1=$(sha1sum ${local_path} | awk '{print $1}')
    curl -f \
         -u "$AUTH" \
         -H "X-Checksum-MD5:${md5}" \
         -H "X-Checksum-SHA1:${sha1}" \
         -X PUT \
         -T ${local_path} \
         https://digitalasset.jfrog.io/artifactory/${repository}/$RELEASE_TAG/${file}
}

TRIGGER_RUNNER=daml-trigger-runner-$RELEASE_TAG.jar
SCRIPT_RUNNER=daml-script-$RELEASE_TAG.jar
NON_REPUDIATION=non-repudiation-$RELEASE_TAG.jar

push daml-trigger-runner $TRIGGER_RUNNER
push daml-trigger-runner $TRIGGER_RUNNER.asc
push daml-script-runner $SCRIPT_RUNNER
push daml-script-runner $SCRIPT_RUNNER.asc
push non-repudiation $NON_REPUDIATION
push non-repudiation $NON_REPUDIATION.asc

# FIXME
# Uncomment the following lines to attempt to publish these libraries.
# The previous attempts failed with a 409 Conflict error code when
# uploading the libraries POM.

#NON_REPUDIATION_CORE_JAR=non-repudiation-core-$RELEASE_TAG.jar
#NON_REPUDIATION_CORE_POM=non-repudiation-core-$RELEASE_TAG.pom
#NON_REPUDIATION_CORE_SRC=non-repudiation-core-$RELEASE_TAG-sources.jar
#NON_REPUDIATION_CORE_DOC=non-repudiation-core-$RELEASE_TAG-javadoc.jar

#push connect-ee-mvn/com/daml $NON_REPUDIATION_CORE_JAR
#push connect-ee-mvn/com/daml $NON_REPUDIATION_CORE_JAR.asc
#push connect-ee-mvn/com/daml $NON_REPUDIATION_CORE_POM
#push connect-ee-mvn/com/daml $NON_REPUDIATION_CORE_POM.asc
#push connect-ee-mvn/com/daml $NON_REPUDIATION_CORE_SRC
#push connect-ee-mvn/com/daml $NON_REPUDIATION_CORE_SRC.asc
#push connect-ee-mvn/com/daml $NON_REPUDIATION_CORE_DOC
#push connect-ee-mvn/com/daml $NON_REPUDIATION_CORE_DOC.asc

#NON_REPUDIATION_CLIENT_JAR=non-repudiation-client-$RELEASE_TAG.jar
#NON_REPUDIATION_CLIENT_POM=non-repudiation-client-$RELEASE_TAG.pom
#NON_REPUDIATION_CLIENT_SRC=non-repudiation-client-$RELEASE_TAG-sources.jar
#NON_REPUDIATION_CLIENT_DOC=non-repudiation-client-$RELEASE_TAG-javadoc.jar

#push connect-ee-mvn/com/daml $NON_REPUDIATION_CLIENT_JAR
#push connect-ee-mvn/com/daml $NON_REPUDIATION_CLIENT_JAR.asc
#push connect-ee-mvn/com/daml $NON_REPUDIATION_CLIENT_POM
#push connect-ee-mvn/com/daml $NON_REPUDIATION_CLIENT_POM.asc
#push connect-ee-mvn/com/daml $NON_REPUDIATION_CLIENT_SRC
#push connect-ee-mvn/com/daml $NON_REPUDIATION_CLIENT_SRC.asc
#push connect-ee-mvn/com/daml $NON_REPUDIATION_CLIENT_DOC
#push connect-ee-mvn/com/daml $NON_REPUDIATION_CLIENT_DOC.asc

for platform in linux macos windows; do
    EE_TARBALL=daml-sdk-$RELEASE_TAG-$platform-ee.tar.gz
    push sdk-ee $EE_TARBALL
    push sdk-ee $EE_TARBALL.asc
done
EE_INSTALLER=daml-sdk-$RELEASE_TAG-windows-ee.exe
push sdk-ee $EE_INSTALLER
push sdk-ee $EE_INSTALLER.asc
