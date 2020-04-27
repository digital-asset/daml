#!/usr/bin/env bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
hlint_diff=false
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
    --diff)
      shift
      scalafmt_args+=(--mode=diff --diff-branch=origin/master)
      hlint_diff=true
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
if [ "$hlint_diff" = "true" ]; then
    changed_haskell_files="$(git diff --name-only origin/master | grep '\.hs$' || [[ $? == 1 ]])"
    if [ "" != "$changed_haskell_files" ]; then
        hlint -j4 $changed_haskell_files
    fi
else
    hlint --git -j4
fi

# check for scala code style
run scalafmt "${scalafmt_args[@]:-}"

# check for Bazel build files code formatting
run bazel run "$buildifier_target"
