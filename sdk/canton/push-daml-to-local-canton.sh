#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

cd "$DIR/.."

SBT_OUT=$(mktemp)
function cleanup () {
  rm $SBT_OUT
}

trap cleanup EXIT

function print_help () {
  echo "Builds and locally publishes artifacts from this Daml repository in such a way that the local canton repository in canton/canton_version.bzl:LOCAL_CANTON_OVERRIDE can use them."
  echo ""
  echo "For more information on usage, consult canton/README.md"
  echo ""
  echo "Usage: $0 [-h]"
  echo "  -h : print this help"
}

echo """**WARNING**: Publishing artifacts in this direction is dangerous - it overwrites
the Daml instance globally for the version used by your remote Canton repo. If
you run this script, all uses of that compiler version on your system will
instead use your 0.0.0 artifacts. Once you're done using this to develop your
changes, clean out your DPM with --nuke.
""" >&2

UNRECOGNIZED=false
HELP=false
for i in "$@"; do
  if [[ "$i" == "-h" || "$i" == "--help" ]]; then
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

LOCAL_CANTON_OVERRIDE=$(./canton/get-local-canton-path.sh)

echo "Changing directory to local canton path '$LOCAL_CANTON_OVERRIDE'"
cd "$LOCAL_CANTON_OVERRIDE"

echo "Getting local Canton's DamlVersion settings..."
sbt --error 'print community-base / buildInfoKeys; print ThisBuild / damlUseCustomVersion' > $SBT_OUT

DAML_PLUGIN_SDK_VERSION=$(grep damlLibrariesVersion $SBT_OUT | cut -f2 -d, | cut -f1 -d')')
echo "Daml SDK used by Canton repo : $DAML_PLUGIN_SDK_VERSION"

DAML_USE_CUSTOM_VERSION=$(grep -A 1 'ThisBuild / damlUseCustomVersion' $SBT_OUT | tail -n1 | grep -oE '[a-z]+')
echo "Is the Canton repo using a custom version of Daml? : $DAML_USE_CUSTOM_VERSION"
if $DAML_USE_CUSTOM_VERSION; then
  echo "Warning: Make sure that you set the DAML_HEAD_VERSION environment variable when working in this local canton repository, or else it won't pick up the updated Daml Script."
fi

cd -
echo "Installing damlc, codegen, and daml-script via DPM to SDK $DAML_PLUGIN_SDK_VERSION"
dpm-sdk-head --verbose --version="$DAML_PLUGIN_SDK_VERSION" --damlc --codegen --daml-script
