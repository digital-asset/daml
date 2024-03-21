#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

## START Bazel-mandated setup

# Bazel is unable to provide a sensible runtime environment for test scripts
# on Windows, so it requires the script to jump through these hoops. Bazel
# provides a function, rlocation, that the bash script can use to locate its
# dependencies using the same path syntax as in Bazel rules, except in a
# restricted (but not really documented) way. For some reason that function
# needs to be imported using the massive block of code below, rather than just
# have an env var that points to a single file that sets it up, as in `source
# $RUNFILES_LIB`.

# Assigns "unset" to RUNFILES_DIR only if it does not already have a value, so
# we do not crash if it is not set.
: ${RUNFILES_DIR:=unset}

# Use the Bazel-provided "source $RUNFILES_LIB" block only if we are running
# under Bazel, so the script can also be invoked manually by just running
# `./ci-tests.sh`.
if [[ "$RUNFILES_DIR" != "unset" ]]; then
    # --- begin runfiles.bash initialization v2 ---
    # Copy-pasted from the Bazel Bash runfiles library v2.
    set -uo pipefail; f=bazel_tools/tools/bash/runfiles/runfiles.bash
    source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
      source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null || \
      source "$0.runfiles/$f" 2>/dev/null || \
      source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
      source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
      { echo>&2 "ERROR: cannot find $f"; exit 1; }; f=; set -e
    # --- end runfiles.bash initialization v2 ---
    # Because path handling is too different between platforms, when running
    # under Bazel, we assume we get all the paths as arguments, rather than
    # trying to do globbing in this script. The first argument is expected to
    # be the path to the jq binary, and all the others are JSON files we need
    # to check.
    JQ_ARG="$1"
    # shift removes one argument from the argument list, so after this line all
    # arguments in `$@`, and in particular `$1`, are expected to be paths to
    # JSON files.
    shift
    # As far as I can tell, this is not documented anywhere, but sh_test
    # provides a `TEST_WORKSPACE` variable. The documentation for
    # runfiles.bash, such as it is, says the argument to rlocation is
    # `my_workspace/path/to/my/data.txt`, so you'd think calling e.g.
    # `$(rlocation jq_dev_env/bin/jq` would work. And it does on Linux and
    # macOS, but Windows seems to require the use of `TEST_WORKSPACE`.
    # Unhelpfully, on Windows, this failure is completely silent and the call
    # to `rlocation` just returns an empty string.
    JQ="$(rlocation "$TEST_WORKSPACE/$JQ_ARG")"
    TARGETS=""
    # `$#` is the number of elements in the arguments, i.e. if you think of
    # `$@` as ARGV, think of `$#` as ARGC. Except they are both variables and
    # get updated by `shift` accordingly. Bash integer logic `(( n )))` will
    # consider `0` as false and everything else as true (as opposed to normal
    # shell boolean test which considers 0 as true and the rest as false, as it
    # handles error codes).
    # This is essentially `let TARGETS = map rlocation $ tail ARGV`
    while (( "$#" )); do
        TARGETS="$TARGETS $(rlocation "$TEST_WORKSPACE/$1")"
        shift
    done
else
    # Note: this may not work on Windows.
    cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
    JQ=jq
    TARGETS=$(echo *.json syntaxes/*.json)
fi

## END Bazel-mandated setup

# Checking JSON files for proper syntax
for sf in $TARGETS; do
    cat $sf | $JQ '.' >/dev/null
done
