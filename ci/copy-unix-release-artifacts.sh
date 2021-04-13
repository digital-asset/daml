#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

RELEASE_TAG=$1
NAME=$2
OUTPUT_DIR=$3

mkdir -p $OUTPUT_DIR/github
mkdir -p $OUTPUT_DIR/artifactory


TARBALL=daml-sdk-$RELEASE_TAG-$NAME.tar.gz
EE_TARBALL=daml-sdk-$RELEASE_TAG-$NAME-ee.tar.gz
cp bazel-bin/release/sdk-release-tarball-ce.tar.gz $OUTPUT_DIR/github/$TARBALL
cp bazel-bin/release/sdk-release-tarball-ee.tar.gz $OUTPUT_DIR/artifactory/$EE_TARBALL

# Platform independent artifacts are only built on Linux.
if [[ "$NAME" == "linux" ]]; then
    PROTOS_ZIP=protobufs-$RELEASE_TAG.zip
    cp bazel-bin/release/protobufs.zip $OUTPUT_DIR/github/$PROTOS_ZIP

    DAML_ON_SQL=daml-on-sql-$RELEASE_TAG.jar
    bazel build //ledger/daml-on-sql:daml-on-sql-binary_deploy.jar
    cp bazel-bin/ledger/daml-on-sql/daml-on-sql-binary_deploy.jar $OUTPUT_DIR/github/$DAML_ON_SQL

    JSON_API=http-json-$RELEASE_TAG.jar
    JSON_API_EE=http-json-$RELEASE_TAG-ee.jar
    bazel build //ledger-service/http-json:http-json-binary_deploy.jar
    cp bazel-bin/ledger-service/http-json/http-json-binary_deploy.jar $OUTPUT_DIR/github/$JSON_API
    bazel build //ledger-service/http-json:http-json-binary-ee_deploy.jar
    cp bazel-bin/ledger-service/http-json/http-json-binary-ee_deploy.jar $OUTPUT_DIR/artifactory/$JSON_API_EE

    TRIGGER_SERVICE=trigger-service-$RELEASE_TAG.jar
    TRIGGER_SERVICE_EE=trigger-service-$RELEASE_TAG-ee.jar
    bazel build //triggers/service:trigger-service-binary-ce_deploy.jar
    cp bazel-bin/triggers/service/trigger-service-binary-ce_deploy.jar $OUTPUT_DIR/github/$TRIGGER_SERVICE
    bazel build //triggers/service:trigger-service-binary-ee_deploy.jar
    cp bazel-bin/triggers/service/trigger-service-binary-ee_deploy.jar $OUTPUT_DIR/artifactory/$TRIGGER_SERVICE_EE

    OAUTH2_MIDDLEWARE=oauth2-middleware-$RELEASE_TAG.jar
    bazel build //triggers/service/auth:oauth2-middleware-binary_deploy.jar
    cp bazel-bin/triggers/service/auth/oauth2-middleware-binary_deploy.jar $OUTPUT_DIR/github/$OAUTH2_MIDDLEWARE


    TRIGGER=daml-trigger-runner-$RELEASE_TAG.jar
    bazel build //triggers/runner:trigger-runner_deploy.jar
    cp bazel-bin/triggers/runner/trigger-runner_deploy.jar $OUTPUT_DIR/artifactory/$TRIGGER

    SCRIPT=daml-script-$RELEASE_TAG.jar
    bazel build //daml-script/runner:script-runner_deploy.jar
    cp bazel-bin/daml-script/runner/script-runner_deploy.jar $OUTPUT_DIR/artifactory/$SCRIPT

    NON_REPUDIATION=non-repudiation-$RELEASE_TAG-ee.jar
    bazel build //runtime-components/non-repudiation-app:non-repudiation-app_deploy.jar
    cp bazel-bin/runtime-components/non-repudiation-app/non-repudiation-app_deploy.jar $OUTPUT_DIR/artifactory/$NON_REPUDIATION

    NON_REPUDIATION_CORE_JAR=non-repudiation-core-$RELEASE_TAG.jar
    NON_REPUDIATION_CORE_POM=non-repudiation-core-$RELEASE_TAG.pom
    NON_REPUDIATION_CORE_SRC=non-repudiation-core-$RELEASE_TAG-sources.jar
    NON_REPUDIATION_CORE_DOC=non-repudiation-core-$RELEASE_TAG-javadoc.jar
    bazel build \
          //runtime-components/non-repudiation-core/... \
          //runtime-components/non-repudiation-core:non-repudiation-core_javadoc \
          //runtime-components/non-repudiation-core:libnon-repudiation-core-src.jar
    cp bazel-bin/runtime-components/non-repudiation-core/libnon-repudiation-core.jar $OUTPUT_DIR/artifactory/$NON_REPUDIATION_CORE_JAR
    cp bazel-bin/runtime-components/non-repudiation-core/non-repudiation-core_pom.xml $OUTPUT_DIR/artifactory/$NON_REPUDIATION_CORE_POM
    cp bazel-bin/runtime-components/non-repudiation-core/libnon-repudiation-core-src.jar $OUTPUT_DIR/artifactory/$NON_REPUDIATION_CORE_SRC
    cp bazel-bin/runtime-components/non-repudiation-core/non-repudiation-core_javadoc.jar $OUTPUT_DIR/artifactory/$NON_REPUDIATION_CORE_DOC


    NON_REPUDIATION_CLIENT_JAR=non-repudiation-client-$RELEASE_TAG.jar
    NON_REPUDIATION_CLIENT_POM=non-repudiation-client-$RELEASE_TAG.pom
    NON_REPUDIATION_CLIENT_SRC=non-repudiation-client-$RELEASE_TAG-sources.jar
    NON_REPUDIATION_CLIENT_DOC=non-repudiation-client-$RELEASE_TAG-javadoc.jar
    bazel build \
          //runtime-components/non-repudiation-client/... \
          //runtime-components/non-repudiation-client:non-repudiation-client_javadoc \
          //runtime-components/non-repudiation-client:libnon-repudiation-client-src.jar
    cp bazel-bin/runtime-components/non-repudiation-client/libnon-repudiation-client.jar $OUTPUT_DIR/artifactory/$NON_REPUDIATION_CLIENT_JAR
    cp bazel-bin/runtime-components/non-repudiation-client/non-repudiation-client_pom.xml $OUTPUT_DIR/artifactory/$NON_REPUDIATION_CLIENT_POM
    cp bazel-bin/runtime-components/non-repudiation-client/libnon-repudiation-client-src.jar $OUTPUT_DIR/artifactory/$NON_REPUDIATION_CLIENT_SRC
    cp bazel-bin/runtime-components/non-repudiation-client/non-repudiation-client_javadoc.jar $OUTPUT_DIR/artifactory/$NON_REPUDIATION_CLIENT_DOC

fi

