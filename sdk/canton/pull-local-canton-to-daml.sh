#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

cd "$DIR/.."

LOG=$(mktemp)
trap "cat $LOG" EXIT

function print_help () {
  echo "Usage: $0 [-h|-v] [--all|--libraries|--app|--repin]"
  echo "  -v          : verbose, don't suppress output of sbt and repin commands"
  echo "  -h          : print this help"
  echo "  --all       : run all of --libraries, --app, and --repin"
  echo "  --libraries : publish the remote canton's libraries to the local cache and pull them in by repinning Daml's local maven"
  echo "  --app       : build the canton app to a JAR, consume it"
  echo "  --repin     : repin Daml's local maven"
}

VERBOSE=false
VERBOSE_TRAILER=" (saving logs to $LOG, run with -v to stream them)"
UNRECOGNIZED=false
HELP=false
DO_ALL=false
DO_LIBRARIES=false
DO_APP=false
DO_REPIN=false
for i in "$@"; do
  if [[ "$i" == "-v" || "$i" == "--verbose" ]]; then
    VERBOSE=true
    VERBOSE_TRAILER=" (saving logs to $LOG, also streaming them because -v is set)"
  elif [[ "$i" == "-h" || "$i" == "--help" ]]; then
    HELP=true
  elif [[ "$i" == "--all" ]]; then
    DO_ALL=true
  elif [[ "$i" == "--libraries" ]]; then
    DO_LIBRARIES=true
  elif [[ "$i" == "--app" ]]; then
    DO_APP=true
  elif [[ "$i" == "--repin" ]]; then
    DO_REPIN=true
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

if ! { $DO_ALL || $DO_LIBRARIES || $DO_APP || $DO_REPIN; }; then
  echo "Must specify at least one of --all, --libraries, --app, or --repin"
  print_help
  exit 1
fi

if $DO_ALL && { $DO_LIBRARIES || $DO_APP || $DO_REPIN; }; then
  echo "Cannot specify --all and another build flag. Must specify exclusively --all or at least one of --libraries, --app, or --repin"
  print_help
  exit 1
fi

USE_LOCAL_CANTON_INSTEAD=$(./canton/get-local-canton-path.sh)

(
  echo "Changing directory to local canton path '$USE_LOCAL_CANTON_INSTEAD'"
  cd "$USE_LOCAL_CANTON_INSTEAD"

  if $DO_ALL || $DO_LIBRARIES; then
    echo "Publish JARs to local Maven cache$VERBOSE_TRAILER"
    if $VERBOSE; then
      sbt publishM2 | tee $LOG
    else
      sbt publishM2 >$LOG 2>&1
    fi
  else
    echo "Skipping publishing JARS to local Maven cache"
  fi

  if $DO_ALL || $DO_APP; then
    echo "Bundle canton app to a JAR$VERBOSE_TRAILER"
    if $VERBOSE; then
      sbt bundle 2>&1 | tee $LOG
    else
      sbt bundle >$LOG 2>&1
    fi
  else
    echo "Skipping bundling canton app to a JAR"
  fi
)

echo "Repin Daml's local Maven deps from local cache$VERBOSE_TRAILER"
if $DO_ALL || $DO_LIBRARIES || $DO_REPIN; then
  if $VERBOSE; then
    RJE_UNSAFE_CACHE=1 REPIN=1 bazel run @maven//:pin | tee $LOG
  else
    RJE_UNSAFE_CACHE=1 REPIN=1 bazel run @maven//:pin >$LOG 2>&1
  fi
else
  echo "Skipping repinning Daml's local Maven deps from local cache" 
fi
