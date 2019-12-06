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
