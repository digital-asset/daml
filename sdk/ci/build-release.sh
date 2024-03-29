#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

cd $DIR/..

execution_log_postfix=${1:-}${2:-}

export LC_ALL=en_US.UTF-8

ARTIFACT_DIRS="${BUILD_ARTIFACTSTAGINGDIRECTORY:-$PWD}"
mkdir -p "${ARTIFACT_DIRS}/logs"

if [ "${1:-}" = "_m1" ]; then
    bazel="arch -arm64 bazel"
else
    bazel=bazel
fi

if [ "$(uname)-$(uname -m)" = "Linux-x86_64" ]; then
  # These are platform-independent so we only need to build them once
  extra_build_targets="//release:protobufs \
                       //daml-script/runner:daml-script-binary_deploy.jar \
                       //daml-script/daml:daml-script-dars \
                       //ledger-service/http-json:http-json-binary_deploy.jar \
                       //ledger-service/http-json:http-json-binary-ee_deploy.jar \
                       //triggers/service:trigger-service-binary-ce_deploy.jar \
                       //triggers/service:trigger-service-binary-ee_deploy.jar \
                       //triggers/service/auth:oauth2-middleware-binary_deploy.jar \
                       //triggers/runner:trigger-runner_deploy.jar \
                       //runtime-components/non-repudiation-app:non-repudiation-app_deploy.jar \
                       //runtime-components/non-repudiation-core/... \
                       //runtime-components/non-repudiation-core:non-repudiation-core_javadoc \
                       //runtime-components/non-repudiation-core:libnon-repudiation-core-src.jar \
                       //runtime-components/non-repudiation-client/... \
                       //runtime-components/non-repudiation-client:non-repudiation-client_javadoc \
                       //runtime-components/non-repudiation-client:libnon-repudiation-client-src.jar \
                       //docs:sphinx-source-tree \
                       //docs:pdf-fonts-tar \
                       //docs:non-sphinx-html-docs \
                       //docs:sphinx-source-tree-deps \
                       //test-evidence:generate-security-test-evidence-files"
else
  extra_build_targets=
fi

$bazel build \
  //compiler/damlc/tests:platform-independence.dar \
  //release:sdk-release-tarball-ce \
  //release:sdk-release-tarball-ee \
  //compiler/damlc:damlc-dist \
  $extra_build_targets \
  --profile build-profile.json \
  --experimental_profile_include_target_label \
  --build_event_json_file build-events.json \
  --build_event_publish_all_actions \
  --execution_log_json_file "$ARTIFACT_DIRS/logs/build_execution${execution_log_postfix}.json.gz"
