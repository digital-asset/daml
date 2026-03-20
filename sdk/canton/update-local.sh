#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

cd "$DIR/.."

LOG=$(mktemp)
trap "cat $LOG" EXIT

function print_help () {
  echo "Usage: $0 [-h|-v]"
  echo "  -v : verbose, don't suppress output of sbt and repin commands"
  echo "  -h : print this help"
}

VERBOSE=false
UNRECOGNIZED=false
HELP=false
for i in "$@"; do
  if [[ "$i" == "-v" || "$i" == "--verbose" ]]; then
    VERBOSE=true
  elif [[ "$i" == "-h" || "$i" == "--help" ]]; then
    HELP=true
  else
    echo "Unrecognized argument '$i'"
    UNRECOGNIZED=true
  fi
done

if $UNRECOGNIZED; then
  print_help
  exit 1
fi

if $HELP; then
  print_help
  exit 0
fi

USE_LOCAL_CANTON_INSTEAD=$(./canton/get-local-canton-path.sh)

(
  echo "Changing directory to local canton path '$USE_LOCAL_CANTON_INSTEAD'"
  cd "$USE_LOCAL_CANTON_INSTEAD"

  echo "Publish JARs to local Maven cache (saving logs to $LOG, run with -v to stream them)"
  if $VERBOSE; then
    sbt publishM2 | tee $LOG
  else
    sbt publishM2 >$LOG 2>&1
  fi

  echo "Bundle canton to a JAR (saving logs to $LOG, run with -v to stream them)"
  if $VERBOSE; then
    sbt bundle 2>&1 | tee $LOG
  else
    sbt bundle >$LOG 2>&1
  fi
)

echo "Repin Maven deps from local cache (saving logs to $LOG, run with -v to stream them)"
pwd
if $VERBOSE; then
  RJE_UNSAFE_CACHE=1 REPIN=1 bazel run @maven//:pin | tee $LOG
else
  RJE_UNSAFE_CACHE=1 REPIN=1 bazel run @maven//:pin >$LOG 2>&1
fi
