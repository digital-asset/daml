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

  # Modify the output path to avoid shared action keys.
  # The issue appears to be that GCC produces absolute paths
  # to system includes in .d files. These files are cached
  # so the absolute paths leak into different builds. Most of the time
  # this works since Azure reuses the working directory. However,
  # between the daily compatibility job seems to get a different
  # working directory because it is in a different pipeline.
  # To make matters worse, the working directory depends on which
  # job runs first afaict. There is a counter that simply gets incremented.
  # Sharing between the compatibility workspace and the main workspace
  # runs into the same issue.
  # To sidestep this we take a md5 hash of PWD
  # (this is what bazel does to determine the execroot name).
  # To avoid exceeding the maximum path limit on Windows we limit the suffix to
  # three characters.
  echo "Working directory: $PWD"
  SUFFIX="$(echo $PWD | md5sum)"
  SUFFIX="${SUFFIX:0:2}"
  echo "Platform suffix: $SUFFIX"
  echo "build --platform_suffix=-$SUFFIX" >> .bazelrc.local
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
