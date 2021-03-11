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
    bazel build //ledger-service/http-json:http-json-binary_deploy.jar
    cp bazel-bin/ledger-service/http-json/http-json-binary_deploy.jar $OUTPUT_DIR/github/$JSON_API

    TRIGGER_SERVICE=trigger-service-$RELEASE_TAG.jar
    bazel build //triggers/service:trigger-service-binary_deploy.jar
    cp bazel-bin/triggers/service/trigger-service-binary_deploy.jar $OUTPUT_DIR/github/$TRIGGER_SERVICE

    OAUTH2_MIDDLEWARE=oauth2-middleware-$RELEASE_TAG.jar
    bazel build //triggers/service/auth:oauth2-middleware-binary_deploy.jar
    cp bazel-bin/triggers/service/auth/oauth2-middleware-binary_deploy.jar $OUTPUT_DIR/github/$OAUTH2_MIDDLEWARE


    TRIGGER=daml-trigger-runner-$RELEASE_TAG.jar
    bazel build //triggers/runner:trigger-runner_deploy.jar
    cp bazel-bin/triggers/runner/trigger-runner_deploy.jar $OUTPUT_DIR/artifactory/$TRIGGER

    SCRIPT=daml-script-$RELEASE_TAG.jar
    bazel build //daml-script/runner:script-runner_deploy.jar
    cp bazel-bin/daml-script/runner/script-runner_deploy.jar $OUTPUT_DIR/artifactory/$SCRIPT
fi
