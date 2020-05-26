# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

set -eou pipefail

SDK_VERSION=$1
DAMLC=$(rlocation $TEST_WORKSPACE/$2)
DAML_TRIGGERS_DAR=$(rlocation $TEST_WORKSPACE/$3)
DAML_SOURCE=$(rlocation $TEST_WORKSPACE/$4)

TMP_DIR=$(mktemp -d)
mkdir -p $TMP_DIR/daml
cat <<EOF > $TMP_DIR/daml.yaml
sdk-version: $SDK_VERSION
name: trigger-scenarios
source: daml
version: 0.0.1
dependencies:
  - daml-stdlib
  - daml-prim
  - daml-trigger.dar
EOF
cp -L $DAML_TRIGGERS_DAR $TMP_DIR/
cp -L $DAML_SOURCE $TMP_DIR/daml/

# We need to run build to create the package database.
# See https://github.com/digital-asset/daml/issues/3436
$DAMLC build --project-root=$TMP_DIR
$DAMLC test --project-root=$TMP_DIR
