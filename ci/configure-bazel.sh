#!/usr/bin/env bash
# Copyright (c) 2020 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
cd "$(dirname "$0")/.."

## Functions

step() {
  echo "step: $*" >&2
}

is_windows() {
  [[ $os = windows ]]
}

## Main

# always run in the project root
cd "$(dirname "$0")/.."

# detect the OS
case $(uname) in
Linux)
  os=linux
  ;;
Darwin)
  os=darwin
  ;;
MINGW*)
  os=windows
  ;;
*)
  echo "unknown kernel: $(uname)"
  exit 1
  ;;
esac

cd "$(dirname "$0")"/..

step "configuring bazel"

if is_windows; then
  echo "build --config windows" > .bazelrc.local
  echo "build --config windows-ci" >> .bazelrc.local
fi

# sets up write access to the shared remote cache if the branch is not a fork
if [[ "${IS_FORK}" = False ]]; then
  step "configuring write access to the remote cache"
  GOOGLE_APPLICATION_CREDENTIALS=$(mktemp .tmp.XXXXXXXXXX)
  echo "$GOOGLE_APPLICATION_CREDENTIALS_CONTENT" > "$GOOGLE_APPLICATION_CREDENTIALS"
  unset GOOGLE_APPLICATION_CREDENTIALS_CONTENT
  export GOOGLE_APPLICATION_CREDENTIALS
  echo "build --remote_http_cache=https://storage.googleapis.com/daml-bazel-cache --remote_upload_local_results=true --google_credentials=${GOOGLE_APPLICATION_CREDENTIALS}" >> .bazelrc.local
fi
