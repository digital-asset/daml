#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euxo pipefail

DAMLTOOLS_VERSION=88.1.1
NAVIGATOR_VERSION=1.1.1
NAVIGATOR_VERSION=0.3.0
SANDBOX_VERSION=6.0.0
LEDGERPROTOS_VERSION=1.4.0
STDLIB_VERSION=62.2.0
JAVABINDING_VERSION=2.6.0
JSBINDING_VERSION=0.4.0
JSDOCS_VERSION=0.4.0
PAASGUIDE_VERSION=0.0.4
APPARCH_VERSION=0.0.3
EXAMPLEUPGRADE_VERSION=1.0.0
EXAMPLEREPO_VERSION=1.0.0
EXAMPLEBOND_VERSION=1.0.0
EXAMPLECOLLATERAL_VERSION=1.1.0
EXAMPLEJAVA_VERSION=1.0.1

function fail {
  echo $1 >&2
  exit 1
}

function retry {
  local n=1
  local max=5
  local delay=5
  while true; do
    "$@" && break || {
      if [[ $n -lt $max ]]; then
        ((n++))
        echo "Command failed. Attempt $n/$max:"
        sleep $delay;
      else
        fail "The command has failed after $n attempts."
      fi
    }
  done
}


function addPkg {
    echo -n "Setting up ${1}/${2}..."
    ret=$(curl --request POST \
            --header "Content-Type: text/plain;charset=utf-8" \
            -s -w "%{http_code}\n" -o /dev/null \
            -d "${4}" \
            --url http://localhost:8882/setupPackage/${1}/${2}/${3})
    if [ "${ret}" = "200" ]; then
        echo "OK"
    else
        echo "ERROR (exit code: ${ret})"
        exit 1
    fi
}

exp=10
if [ "$#" -ne $exp ]; then
    echo "[ERROR] Illegal number of arguments. (Expected $exp, got $#)"
    echo "[ERROR] PACKAGE SETUP FAILED!!!"
    exit 1
fi

sdk_tar="${10}"

# Making a few sdk's available to test switching between versions:
retry addPkg "sdk" "0.10.2" "com/digitalasset/sdk/0.10.2/sdk-0.10.2.tar.gz" ${sdk_tar}
retry addPkg "sdk" "0.10.3" "com/digitalasset/sdk/0.10.3/sdk-0.10.3.tar.gz" ${sdk_tar}
retry addPkg "sdk" "0.2.10" "com/digitalasset/sdk/0.2.10/sdk-0.2.10.tar.gz" ${sdk_tar}

# Mock packages, their content is not used in tests (we bind SDK tarball to each of these instead of their real tar archive):
retry addPkg "example-bond-trading" "${EXAMPLEBOND_VERSION}" "com/digitalasset/docs/example-bond-trading/${EXAMPLEBOND_VERSION}/example-bond-trading-${EXAMPLEBOND_VERSION}.tar.gz" ${sdk_tar}
retry addPkg "app-arch-guide" "${APPARCH_VERSION}" "com/digitalasset/app-arch-guide/${APPARCH_VERSION}/app-arch-guide-${APPARCH_VERSION}.tar.gz" ${sdk_tar}
retry addPkg "quickstart-java" "${EXAMPLEJAVA_VERSION}" "com/digitalasset/docs/quickstart-java/${EXAMPLEJAVA_VERSION}/quickstart-java-${EXAMPLEJAVA_VERSION}.tar.gz" ${sdk_tar}
retry addPkg "example-collateral" "${EXAMPLECOLLATERAL_VERSION}" "com/digitalasset/docs/example-collateral/${EXAMPLECOLLATERAL_VERSION}/example-collateral-${EXAMPLECOLLATERAL_VERSION}.tar.gz" ${sdk_tar}
retry addPkg "example-repo-market" "${EXAMPLEREPO_VERSION}" "com/digitalasset/docs/example-repo-market/${EXAMPLEREPO_VERSION}/example-repo-market-${EXAMPLEREPO_VERSION}.tar.gz" ${sdk_tar}
retry addPkg "example-upgrade" "${EXAMPLEUPGRADE_VERSION}" "com/digitalasset/docs/example-upgrade/${EXAMPLEUPGRADE_VERSION}/example-upgrade-${EXAMPLEUPGRADE_VERSION}.tar.gz" ${sdk_tar}
retry addPkg "example-ping-pong-grpc-java" "${JAVABINDING_VERSION}" "com/digitalasset/example-ping-pong-grpc-java/${JAVABINDING_VERSION}/example-ping-pong-grpc-java-${JAVABINDING_VERSION}.tar.gz" ${sdk_tar}
retry addPkg "example-ping-pong-reactive-components-java" "${JAVABINDING_VERSION}" "com/digitalasset/example-ping-pong-reactive-components-java/${JAVABINDING_VERSION}/example-ping-pong-reactive-components-java-${JAVABINDING_VERSION}.tar.gz" ${sdk_tar}
retry addPkg "example-ping-pong-reactive-java" "${JAVABINDING_VERSION}" "com/digitalasset/example-ping-pong-reactive-java/${JAVABINDING_VERSION}/example-ping-pong-reactive-java-${JAVABINDING_VERSION}.tar.gz" ${sdk_tar}
retry addPkg "bindings-java-tutorial" "${JAVABINDING_VERSION}" "com/digitalasset/bindings-java-tutorial/${JAVABINDING_VERSION}/bindings-java-tutorial-${JAVABINDING_VERSION}.tar.gz" ${sdk_tar}
retry addPkg "tutorial-nodejs" "${JSBINDING_VERSION}" "com/digitalasset/tutorial-nodejs/${JSBINDING_VERSION}/tutorial-nodejs-${JSBINDING_VERSION}.tar.gz" ${sdk_tar}
retry addPkg "paas-user-guide" "${PAASGUIDE_VERSION}" "com/digitalasset/paas-user-guide/${PAASGUIDE_VERSION}/paas-user-guide-${PAASGUIDE_VERSION}.tar.gz" ${sdk_tar}
retry addPkg "bindings-js-docs" "${JSDOCS_VERSION}" "com/digitalasset/bindings-js-docs/${JSDOCS_VERSION}/bindings-js-docs-${JSDOCS_VERSION}.tar.gz" ${sdk_tar}

retry addPkg "daml-extension" "${DAMLTOOLS_VERSION}" "com/digitalasset/daml-extension/${DAMLTOOLS_VERSION}/daml-extension-${DAMLTOOLS_VERSION}.tar.gz" ${sdk_tar}
retry addPkg "damlc" "${DAMLTOOLS_VERSION}" "com/digitalasset/damlc/${DAMLTOOLS_VERSION}/damlc-${DAMLTOOLS_VERSION}-linux.tar.gz" ${sdk_tar}
retry addPkg "damlc" "${DAMLTOOLS_VERSION}" "com/digitalasset/damlc/${DAMLTOOLS_VERSION}/damlc-${DAMLTOOLS_VERSION}-osx.tar.gz" ${sdk_tar}

retry addPkg "navigator" "${NAVIGATOR_VERSION}" "com/digitalasset/navigator/navigator/${NAVIGATOR_VERSION}/navigator-${NAVIGATOR_VERSION}.jar" ${sdk_tar}
retry addPkg "extractor" "${EXTRACTOR_VERSION}" "com/digitalasset/extractor/${EXTRACTOR_VERSION}/extractor-${EXTRACTOR_VERSION}.tar.gz" ${sdk_tar}
retry addPkg "ledger-api-protos" "${LEDGERPROTOS_VERSION}" "com/digitalasset/ledger-api-protos/${LEDGERPROTOS_VERSION}/ledger-api-protos-${LEDGERPROTOS_VERSION}.tar.gz" ${sdk_tar}
retry addPkg "sandbox" "${SANDBOX_VERSION}" "com/digitalasset/sandbox/${SANDBOX_VERSION}/sandbox-${SANDBOX_VERSION}.tar.gz" ${sdk_tar}
