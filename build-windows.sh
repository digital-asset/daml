#!/usr/bin/env bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

powershell $DIR/dev-env/windows/bin/dadew.ps1 install
powershell $DIR/dev-env/windows/bin/dadew.ps1 sync
powershell $DIR/dev-env/windows/bin/dadew.ps1 enable

export PATH="$DIR/dev-env/windows/bin:$HOME/dadew/scoop/shims:$PATH"
echo $PATH

find $HOME/dadew/scoop/shims
echo --
find $HOME/dadew/scoop/apps

if ! [ -f $DIR/.bazelrc.local ]; then
    echo "build --config windows" > $DIR/.bazelrc.local
fi
cat $DIR/.bazelrc.local

ARTIFACTS_DIRS=${BUILD_ARTIFACTSTAGINGDIRECTORY:-$DIR}

if ! [ -e $ARTIFACTS_DIRS/log ]; then
    mkdir -p $ARTIFACTS_DIRS/log
elif [ -f $ARTIFACTS_DIRS/log ]; then
    echo "Cannot create '$ARTIFACTS_DIRS/log': conflicting file."
    exit 1
fi

# If a previous build was forcefully terminated, then stack's lock file might
# not have been cleaned up properly leading to errors of the form
#
#   user error (hTryLock: lock already exists: C:\Users\u\AppData\Roaming\stack\pantry\hackage\hackage-security-lock)
#
# The package cache might be corrupted and just removing the lock might lead to
# errors as below, so we just nuke the entire stack cache.
#
#   Failed populating package index cache
#   IncompletePayload 56726464 844
#
if [ -e $APPDATA/stack/pantry/hackage/hackage-security-lock ]; then
    echo "Nuking stack directory"
    rm -rf $APPDATA/stack
fi

if [ -n "${ARTIFACTORY_USERNAME:-}" ] && [ -n "${ARTIFACTORY_PASSWORD:-}" ]; then
    export ARTIFACTORY_AUTH=$(echo -n "$ARTIFACTORY_USERNAME:$ARTIFACTORY_PASSWORD" | base64 -w0)
fi

bazel() (
    set -euo pipefail
    echo ">> bazel $@"
    set -x
    if bazel.exe "$@" 2>&1; then
        echo "<< bazel $1 (ok)"
    else
        exit_code=$?
        echo "<< bazel $1 (failed, exit code: $exit_code)"
        exit $exit_code
    fi
)

# ScalaCInvoker, a Bazel worker, created by rules_scala opens some of the bazel execroot's files,
# which later causes issues on Bazel init (source forest creation) on Windows. A shutdown closes workers,
# which is a workaround for this problem.
bazel shutdown

# Prefetch nodejs_dev_env to avoid permission denied errors on external/nodejs_dev_env/nodejs_dev_env/node.exe
# It isn’t clear where exactly those errors are coming from.
bazel fetch @nodejs_dev_env//...

bazel build ///... \
  --profile build-profile.json \
  --experimental_profile_include_target_label \
  --build_event_json_file build-events.json \
  --build_event_publish_all_actions \
  --experimental_execution_log_file ${ARTIFACTS_DIRS}/logs/build_execution_windows.log

bazel shutdown

if [ "${SKIP_TESTS:-}" = "False" ]; then
    # Generate mapping from shortened scala-test names on Windows to long names on Linux and MacOS.
    powershell $DIR/ci/remap-scala-test-short-names.ps1 > $DIR/scala-test-suite-name-map.json

    tag_filter="-dev-canton-test"

    bazel test //... \
      --build_tag_filters "$tag_filter" \
      --test_tag_filters "$tag_filter" \
      --profile test-profile.json \
      --experimental_profile_include_target_label \
      --build_event_json_file test-events.json \
      --build_event_publish_all_actions \
      --experimental_execution_log_file ${ARTIFACTS_DIRS}/logs/test_execution_windows.log
fi
