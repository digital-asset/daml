#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
cd "$(dirname "$0")/.."

## Functions

step() {
  echo "step: $*" >&2
}

to_lower() {
  echo "$1" | tr '[:upper:]' '[:lower:]'
}

## Main

kernel=$(to_lower "$(uname)")
cd "$(dirname "$0")"/..

step "loading dev-env"

eval "$(dev-env/bin/dade assist)"

step "configuring bazel"

# sets up write access to the shared remote cache if the branch is not a fork
if [[ "${IS_FORK}" = False ]]; then
  step "configuring write access to the remote cache"
  GOOGLE_APPLICATION_CREDENTIALS=$(mktemp)
  echo "$GOOGLE_APPLICATION_CREDENTIALS_CONTENT" > "$GOOGLE_APPLICATION_CREDENTIALS"
  unset GOOGLE_APPLICATION_CREDENTIALS_CONTENT
  export GOOGLE_APPLICATION_CREDENTIALS
  echo "build --remote_http_cache=https://storage.googleapis.com/daml-bazel-cache --remote_upload_local_results=true --google_credentials=${GOOGLE_APPLICATION_CREDENTIALS}" >> .bazelrc.local
fi

# build
step "./build.sh"
./build.sh "_$kernel"
