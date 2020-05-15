#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

if [ ! -z "${BAZEL_CONFIG_DIR:-}" ]; then
    cd "$BAZEL_CONFIG_DIR"
fi

if is_windows; then
  echo "build --config windows" > .bazelrc.local
  echo "build --config windows-ci" >> .bazelrc.local

  # Modify the output path (x64_windows-opt-co) to avoid shared action keys
  # between external dependencies of the daml and compatibility workspaces.
  # These are causing issues on Windows, namely sporadic failures due to
  # "undeclared inclusion(s)" with the mingw toolchain. This doesn't modify all
  # action keys, e.g. metadata actions like `Mnemonic: Middleman` are
  # unaffected. However, all actions that produce artifacts will be affected.
  # To avoid exceeding the maximum path limit on Windows we limit the suffix to
  # three characters.
  CONFIG=${BAZEL_CONFIG_DIR-default}
  echo $BUILD_SOURCES_DIRECTORY
  echo "build --platform_suffix=-${CONFIG:0:2}" >> .bazelrc.local
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
