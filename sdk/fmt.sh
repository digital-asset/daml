#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Run formatters and linter, anything platform-independent and quick
#
# Usage: ./fmt.sh [--test] [--diff]
set -euo pipefail

cd "$(dirname "$0")"
# load the dev-env
eval "$(dev-env/bin/dade-assist)"

## Config ##
is_test=
scalafmt_args=()
google_java_format_arg=update
diff_mode=false
dade_copyright_arg=update
buildifier_target=//bazel/checks/buildifier:update
security_evidence_arg=update
pnpm_lockfile_arg=update
prettier_args="--write"

## Functions ##

run_pprettier() {
    ./dev-tooling/node_modules/.bin/pprettier "$@"
}

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

#  We do not run on deleted files, or files that have been added since we last rebased onto main.
check_diff() {
  # $1 merge_base
  # $2 regex
  # "${@:3}" command
  readarray -t changed_files < <(git diff --name-only --diff-filter=ACMRT "$1" | grep $2 | grep -E -v '^sdk/canton(-3x)?/' || [[ $? == 1 ]])
  if [[ -z "${changed_files[@]}" ]]; then
    echo "No changed file to check matching '$2', skipping."
  else
    run "${@:3}" ${changed_files[@]##sdk/}
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
  --diff:     only consider files that changed since main
USAGE
      exit
      ;;
    --test)
      shift
      is_test=1
      scalafmt_args+=(--test)
      google_java_format_arg=check
      dade_copyright_arg=check
      buildifier_target=//bazel/checks/buildifier:check
      security_evidence_arg=check
      pnpm_lockfile_arg=check
      prettier_args="--check"
      ;;
    --diff)
      shift
      merge_base="$(git merge-base origin/main HEAD)"
      scalafmt_args+=('--mode=diff' "--diff-branch=${merge_base}")
      diff_mode=true
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

run bazelisk run //bazel/checks/pnpm -- "$pnpm_lockfile_arg"

# Install the dev-tooling workspace so we can call pprettier directly via its
# local bin, which is substantially faster than invoking it through Bazel.
run bazelisk run @pnpm//:pnpm -- --dir "$PWD/dev-tooling" install --frozen-lockfile

# update security evidence
run bazelisk run //bazel/checks/security-evidence -- "$security_evidence_arg"

# Check for correct copyrights
run bazelisk run //bazel/checks/copyright -- "$dade_copyright_arg" .

# We do test hlint via Bazel rules but we run it separately
# to get linting failures early.
if [ "$diff_mode" = "true" ]; then
  check_diff $merge_base '\.hs$' bazelisk run //bazel/checks/hlint -- check
  check_diff $merge_base '\.java$' bazelisk run //bazel/checks/google-java-format -- "$google_java_format_arg"
  check_diff $merge_base '\.\(ts\|tsx\)$' run_pprettier ${prettier_args[@]:-}
else
  run bazelisk run //bazel/checks/hlint -- check
  run bazelisk run //bazel/checks/google-java-format -- "$google_java_format_arg"
  run run_pprettier ${prettier_args[@]:-} './**/*.{ts,tsx}'
fi

# check for scala code style
run scalafmt "${scalafmt_args[@]:-}"

# check for Bazel build files code formatting
run bazelisk run "$buildifier_target"

run bazelisk run //bazel/checks/akka -- check

if [[ $is_test = 1 ]]; then
  echo "Checking .rst with vale, is_test=TRUE so --minAlertLevel=warning"
  vale ./docs/sharable --config ./.vale-ci.ini --minAlertLevel=warning
else
  echo "Checking .rst with vale, is_test=FALSE so --minAlertLevel=suggestion"
  if [ "$diff_mode" = "true" ]; then
    echo "--diff, so only linting diff"
    check_diff $merge_base '\.rst$' vale --config ./.vale-ci.ini --minAlertLevel=suggestion
  else
    vale ./docs/sharable --config ./.vale-ci.ini --minAlertLevel=suggestion
  fi
fi
