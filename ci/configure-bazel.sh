#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
cd "$(dirname "$0")/../sdk"

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

# We include the rules_haskell revision in the suffix since
# it sometimes breaks Windows due to the lack of sandboxing.
RULES_HASKELL_REV="$(sed -n 's/rules_haskell_version = "\(.*\)"$/\1/p' deps.bzl)"

if [ ! -z "${BAZEL_CONFIG_DIR:-}" ]; then
    cd "$BAZEL_CONFIG_DIR"
fi

CACHE_URL="https://storage.googleapis.com/daml-bazel-cache"

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
  SUFFIX="$(echo $PWD $RULES_HASKELL_REV | openssl dgst -md5 -r)"
  SUFFIX="${SUFFIX:0:6}"
  echo "Platform suffix: $SUFFIX"
  # We include an extra version at the end that we can bump manually.
  CACHE_SUFFIX="$SUFFIX-202309"
  CACHE_URL="$CACHE_URL/$CACHE_SUFFIX"
  echo "build:windows-ci --remote_http_cache=https://bazel-cache.da-ext.net/$CACHE_SUFFIX" >> .bazelrc.local
fi

# sets up write access to the shared remote cache if the branch is not a fork
if [[ "${IS_FORK}" = False ]]; then
  step "configuring write access to the remote cache"
  GOOGLE_APPLICATION_CREDENTIALS=$(mktemp .tmp.XXXXXXXXXX)
  echo "$GOOGLE_APPLICATION_CREDENTIALS_CONTENT" > "$GOOGLE_APPLICATION_CREDENTIALS"
  unset GOOGLE_APPLICATION_CREDENTIALS_CONTENT
  export GOOGLE_APPLICATION_CREDENTIALS
  echo "build --remote_http_cache=$CACHE_URL --remote_upload_local_results=true --google_credentials=${GOOGLE_APPLICATION_CREDENTIALS}" >> .bazelrc.local
fi
