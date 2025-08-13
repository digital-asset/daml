#!/usr/bin/env bash
# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Copy-pasted from the Bazel Bash runfiles library v2.
set -uo pipefail; f=bazel_tools/tools/bash/runfiles/runfiles.bash
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null || \
  source "$0.runfiles/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  { echo>&2 "ERROR: cannot find $f"; exit 1; }; f=; set -e
# --- end runfiles.bash initialization v2 ---

# Make sure that runfiles and tools are still found after we change directory.
case "$(uname -s)" in
  Darwin)
    abspath() { python -c 'import os.path, sys; sys.stdout.write(os.path.abspath(sys.argv[1]))' "$@"; }
    canonicalpath() { python -c 'import os.path, sys; sys.stdout.write(os.path.realpath(sys.argv[1]))' "$@"; }
    ;;
  *)
    abspath() { realpath -s "$@"; }
    canonicalpath() { readlink -f "$@"; }
    ;;
esac

if [[ -n ${RUNFILES_DIR:-} ]]; then
  export RUNFILES_DIR=$(abspath $RUNFILES_DIR)
fi
if [[ -n ${RUNFILES_MANIFEST_FILE:-} ]]; then
  export RUNFILES_DIR=$(abspath $RUNFILES_MANIFEST_FILE)
fi

case "$(uname -s)" in
  Darwin|Linux)
    tar=$(abspath $(rlocation tar_dev_env/tar))
    mktgz=$(abspath $(rlocation com_github_digital_asset_daml/bazel_tools/sh/mktgz))
    ;;
  CYGWIN*|MINGW*|MSYS*)
    tar=$(abspath $(rlocation tar_dev_env/usr/bin/tar.exe))
    mktgz=$(abspath $(rlocation com_github_digital_asset_daml/bazel_tools/sh/mktgz.exe))
    ;;
esac

set -eou pipefail

WORKDIR="$(mktemp -d)"
trap "rm -rf $WORKDIR" EXIT

OUT=$(abspath $1)
MANIFEST=$(abspath $2)
PLATFORM_AGNOSTIC=$3
shift 3

componentpath="$WORKDIR/component.yaml"
cp $MANIFEST $WORKDIR
case "$(uname -s)" in
  Darwin|Linux)
    sed -i -e 's/${EXE}//g' $componentpath
    ;;
  CYGWIN*|MINGW*|MSYS*)
    sed -i -e 's/${EXE}/.exe/g' $componentpath
    ;;
esac

if [ $PLATFORM_AGNOSTIC -ne 0 ]
then
  touch $WORKDIR/is-agnostic
fi

for res in "$@"; do
  case $res in
    *.tar.gz)
      BASENAME=$(basename $res)
      RAWNAME=${BASENAME%%.*}
      # unzip to a directory, as these often have internal relative symlinks to top level, which oras (used by DPM to download artifacts) can't handle right now
      mkdir -p "$WORKDIR/$RAWNAME"
      $tar xf "$res" --strip-components=1 -C "$WORKDIR/$RAWNAME"
      ;;
    *)
      cp $res $WORKDIR
      ;;
  esac
done

# Windows likes adding ".exe" on the end of directory names
for dir in $(find $WORKDIR -type d -name '*.exe'); do
  mv $dir ${dir%.*}
done

cd $WORKDIR && $mktgz $OUT *
