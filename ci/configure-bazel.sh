#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

cd $DIR/../sdk
if [ -n "${BAZEL_CONFIG_DIR:-}" ]; then
  cd "$BAZEL_CONFIG_DIR"
fi

# Note: date segment here has to match .bazelrc for dev machines to benefit
# from CI cache
CACHE_URL=http://10.0.2.10/cache/202405

case $(uname) in
Linux)
  echo "build:linux --remote_cache=$CACHE_URL/ubuntu" > .bazelrc.local
  echo "build --remote_upload_local_results=true" >> .bazelrc.local
  echo "CACHE_URL=$CACHE_URL/ubuntu"
  ;;
Darwin)
  if [ "${IS_FORK:-}" = "False" ]; then
    GOOGLE_APPLICATION_CREDENTIALS=$(mktemp .tmp.XXXXXXXXXX)
    echo "$GOOGLE_APPLICATION_CREDENTIALS_CONTENT" > "$GOOGLE_APPLICATION_CREDENTIALS"
    echo "build --remote_upload_local_results=true --google_credentials=$GOOGLE_APPLICATION_CREDENTIALS" > .bazelrc.local
  fi
  echo "CACHE_URL=$(cat .bazelrc | grep -o "https://.*/macos")"
  ;;
MINGW*)
  RULES_HASKELL_REV="$(sed -n 's/rules_haskell_version = "\(.*\)"$/\1/p' $DIR/../sdk/deps.bzl)"
  echo "Working directory: $PWD"
  echo "build --config windows" > .bazelrc.local
  SUFFIX="$(echo $PWD $RULES_HASKELL_REV | openssl dgst -md5 -r)"
  CACHE_URL=$CACHE_URL/windows/${SUFFIX:0:4}
  echo "build:windows --remote_cache=$CACHE_URL" >> .bazelrc.local
  echo "build --remote_upload_local_results=true" >> .bazelrc.local
  echo "CACHE_URL=$CACHE_URL"
  ;;
*)
  echo "unknown kernel: $(uname)"
  exit 1
  ;;
esac
