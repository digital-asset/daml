#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Run formatters and linter, anything platform-independent and quick
#
# Usage: ./fmt.sh [--test]
set -euo pipefail

cd "$(dirname "$0")"
# load the dev-env
eval "$(dev-env/bin/dade-assist)"

## Config ##
is_test=
scalafmt_args=()
dade_copyright_arg=update
buildifier_target=//:buildifier-fix

## Functions ##

log() {
  echo "fmt.sh: $*" >&2
}

run() {
  echo "$ ${*%Q}"
  "$@"
  ret=$?
  if [[ $is_test = 1 && $ret -gt 0 ]]; then
    log "command failed with return $ret"
    log
    log "run ./fmt.sh to fix the issue"
    exit 1
  fi
  return 0
}

## Main ##

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h | --help)
      cat <<USAGE
Usage: ./fmt.sh [options]

Options:
  -h, --help: shows this help
  --test:     only test for formatting changes, used by CI
USAGE
      exit
      ;;
    --test)
      shift
      is_test=1
      scalafmt_args+=(--test)
      dade_copyright_arg=check
      buildifier_target=//:buildifier
      ;;
    *)
      echo "fmt.sh: unknown argument $1" >&2
      exit 1
      ;;
  esac
done

# Check for correct copyrights
run dade-copyright-headers "$dade_copyright_arg" .

# We have a Bazel test that is meant to run HLint, but we're a little sceptical of it
# If we get this far, but hlint fails, that's a problem we should fix
function bad_hlint() {
  echo "UNEXPECTED HLINT FAILURE: The Bazel rules should have spotted this, please raise a GitHub issue"
}
trap bad_hlint EXIT
for dir in daml-foundations da-assistant daml-assistant libs-haskell compiler; do
  run pushd "$dir"
  run hlint --git -j4
  run popd
done
trap - EXIT

# check for scala code style
run ./scalafmt.sh "${scalafmt_args[@]:-}"

# check for Bazel build files code formatting
run bazel run "$buildifier_target"
