#!/usr/bin/env bash
# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Run formatters and linter, anything platform-independent and quick
#
# Usage: ./fmt.sh [--test]
set -euo pipefail

cd "$(dirname "$0")"
# load the dev-env
eval "$(dev-env/bin/dade-assist)"

# Location of reference image used for `buf`'s protobuf breaking checks, including the Ledger API's.
#
# This `buf` image will be re-built and committed automatically as part of the SDK release process;
# this means that all protobuf changes in the next SDK development cycle will be checked by `buf`
# against the new SDK's protobufs, effectively considering them stable.
#
# The `buf` image should be re-built manually only when making breaking protobuf changes that have
# been agreed to and it should be part of the same commit also containing such changes.
#
# To re-build it manually, run './fmt.sh --rebuild-buf-image'.

buf_image="buf-stable-protos-image.bin"

## Config ##
is_test=
scalafmt_args=()
javafmt_args=(--set-exit-if-changed --replace)
diff_mode=false
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
    log "run ./fmt.sh to fix formatting issues or ./fmt.sh --rebuild-buf-image if you intend to make a breaking change"
    exit 1
  fi
  return 0
}

#  We do not run on deleted files, or files that have been added since we last rebased onto main.
check_diff() {
  # $1 merge_base
  # $2 regex
  # "${@:3}" command
  changed_files=$(git diff --name-only --diff-filter=ACMRT "$1" | grep $2 || [[ $? == 1 ]])
  if [[ -n "$changed_files" ]]; then
    run "${@:3}" ${changed_files[@]:-}
  else
    echo "No changed file to check matching '$2', skipping."
  fi
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
  --rebuild-buf-image:  rebuilds reference image used for buf breaking checks
USAGE
      exit
      ;;
    --test)
      shift
      is_test=1
      scalafmt_args+=(--test)
      javafmt_args=(--set-exit-if-changed --dry-run)
      dade_copyright_arg=check
      buildifier_target=//:buildifier
      ;;
    --diff)
      shift
      merge_base="$(git merge-base origin/main HEAD)"
      scalafmt_args+=('--mode=diff' "--diff-branch=${merge_base}")
      diff_mode=true
      ;;
    --rebuild-buf-image)
      shift
      run buf build -o "${buf_image}"
      exit
      ;;
    *)
      echo "fmt.sh: unknown argument $1" >&2
      exit 1
      ;;
  esac
done

echo "\
─────────────────────────────▄██▄
──FORMAT ALL THE THINGS!!!───▀███
────────────────────────────────█
───────────────▄▄▄▄▄────────────█
──────────────▀▄────▀▄──────────█
──────────▄▀▀▀▄─█▄▄▄▄█▄▄─▄▀▀▀▄──█
─────────█──▄──█────────█───▄─█─█
─────────▀▄───▄▀────────▀▄───▄▀─█
──────────█▀▀▀────────────▀▀▀─█─█
──────────█───────────────────█─█
▄▀▄▄▀▄────█──▄█▀█▀█▀█▀█▀█▄────█─█
█▒▒▒▒█────█──█████████████▄───█─█
█▒▒▒▒█────█──██████████████▄──█─█
█▒▒▒▒█────█───██████████████▄─█─█
█▒▒▒▒█────█────██████████████─█─█
█▒▒▒▒█────█───██████████████▀─█─█
█▒▒▒▒█───██───██████████████──█─█
▀████▀──██▀█──█████████████▀──█▄█
──██───██──▀█──█▄█▄█▄█▄█▄█▀──▄█▀
──██──██────▀█─────────────▄▀▓█
──██─██──────▀█▀▄▄▄▄▄▄▄▄▄▀▀▓▓▓█
──████────────█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█
──███─────────█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█
──██──────────█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█
──██──────────█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█
──██─────────▐█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█
──██────────▐█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█
──██───────▐█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▌
──██──────▐█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▌
──██─────▐█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▌
──██────▐█▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█▌
"

# Check for correct copyrights
run dade-copyright-headers "$dade_copyright_arg" .

# We do test hlint via Bazel rules but we run it separately
# to get linting failures early.
if [ "$diff_mode" = "true" ]; then
  check_diff $merge_base '\.hs$' hlint -j4
  check_diff $merge_base '\.java$' javafmt "${javafmt_args[@]:-}"
else
  run hlint -j4 --git
  java_files=$(find . -name "*.java")
  if [[ -z "$java_files" ]]; then
    echo "Unexpected: no Java file in the repository"
    exit 1
  fi
  run javafmt "${javafmt_args[@]:-}" ${java_files[@]:-}
fi

# check for scala code style
run scalafmt "${scalafmt_args[@]:-}"

# check for Bazel build files code formatting
run bazel run "$buildifier_target"

# Check for breaking proto changes.
run buf breaking --against "${buf_image}"

# Note that we cannot use a symlink here because Windows.
if ! diff .bazelrc compatibility/.bazelrc >/dev/null; then
    echo ".bazelrc and  compatibility/.bazelrc are out of sync:"
    diff -u .bazelrc compatibility/.bazelrc
fi
