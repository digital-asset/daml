# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

# --- begin runfiles.bash initialization ---
# Copy-pasted from Bazel's Bash runfiles library (tools/bash/runfiles/runfiles.bash).
if [[ ! -d "${RUNFILES_DIR:-/dev/null}" && ! -f "${RUNFILES_MANIFEST_FILE:-/dev/null}" ]]; then
  if [[ -f "$0.runfiles_manifest" ]]; then
    export RUNFILES_MANIFEST_FILE="$0.runfiles_manifest"
  elif [[ -f "$0.runfiles/MANIFEST" ]]; then
    export RUNFILES_MANIFEST_FILE="$0.runfiles/MANIFEST"
  elif [[ -f "$0.runfiles/bazel_tools/tools/bash/runfiles/runfiles.bash" ]]; then
    export RUNFILES_DIR="$0.runfiles"
  fi
fi
if [[ -f "${RUNFILES_DIR:-/dev/null}/bazel_tools/tools/bash/runfiles/runfiles.bash" ]]; then
  source "${RUNFILES_DIR}/bazel_tools/tools/bash/runfiles/runfiles.bash"
elif [[ -f "${RUNFILES_MANIFEST_FILE:-/dev/null}" ]]; then
  source "$(grep -m1 "^bazel_tools/tools/bash/runfiles/runfiles.bash " \
            "$RUNFILES_MANIFEST_FILE" | cut -d ' ' -f 2-)"
else
  echo >&2 "ERROR: cannot find @bazel_tools//tools/bash/runfiles:runfiles.bash"
  exit 1
fi
# --- end runfiles.bash initialization ---

TESTS_DIR=$(dirname $(rlocation "$TEST_WORKSPACE/compiler/damlc/tests/daml-test-files/Examples.daml"))
damlc=$(rlocation "$TEST_WORKSPACE/$1")
protoc=$(rlocation "$TEST_WORKSPACE/$2")
diff="$3"
scriptdar=$(rlocation "$TEST_WORKSPACE/$4")
SDK_VERSION=$5
GHC_FRIENDLY_SDK_VERSION=$6

# Check that Daml compilation is deterministic.
TMP_SRC1=$(mktemp -d)
TMP_SRC2=$(mktemp -d)
TMP_OUT=$(mktemp -d)
PROJDIR=$(mktemp -d)

damlyaml=$(cat <<EOF
sdk-version: $SDK_VERSION
name: proj
version: 0.0.1
source: .
dependencies: [daml-prim, daml-stdlib, "$scriptdar"]
EOF
)
importargs="--package-db=./.daml/package-database --package=daml-script-$GHC_FRIENDLY_SDK_VERSION"

cleanup () {
    rm -rf "$TMP_SRC1" "$TMP_SRC2" "$TMP_OUT" "$PROJDIR"
}
trap cleanup EXIT

# We use daml build without sourcecode to bring in daml script to .daml/dependencies and .daml/package-database
# Note that we only do this once (copying from SRC1 to SRC2, rather than rebuilding in SRC2),
# as this test is for compile determinism, not dependency db building determinism
# The build determinism check is at the bottom of this file
echo -e "$damlyaml" > "$TMP_SRC1/daml.yaml"
(cd "$TMP_SRC1" && $damlc build)

cp -a $TESTS_DIR/. "$TMP_SRC1"
cp -a $TMP_SRC1/. "$TMP_SRC2"

(cd "$TMP_SRC1" && DAML_PROJECT="$TMP_SRC1" $damlc compile "Examples.daml" -o "$TMP_OUT/out_1" $importargs)
(cd "$TMP_SRC2" && DAML_PROJECT="$TMP_SRC2" $damlc compile "Examples.daml" -o "$TMP_OUT/out_2" $importargs)

# When invoked with a project root (as set by the Daml assistant)
# we should produce the same output regardless of the path with which we are invoked.
(cd "/" && DAML_PROJECT="$TMP_SRC1" $damlc compile "$TMP_SRC1/Examples.daml" -o "$TMP_OUT/out_proj_1" $importargs)
(cd "$TMP_SRC1" && DAML_PROJECT="$TMP_SRC1" $damlc compile "Examples.daml" -o "$TMP_OUT/out_proj_2" $importargs)

$protoc --decode_raw < "$TMP_OUT/out_1" > "$TMP_OUT/decoded_out_1"
$protoc --decode_raw < "$TMP_OUT/out_2" > "$TMP_OUT/decoded_out_2"
# We first diff the decoded files to get useful debugging output and
# then the non-decoded files to ensure that we actually get bitwise
# identical outputs.
$diff -u "$TMP_OUT/decoded_out_1" "$TMP_OUT/decoded_out_2"
$diff -u "$TMP_OUT/out_1" "$TMP_OUT/out_2"
$protoc --decode_raw < "$TMP_OUT/out_proj_1" > "$TMP_OUT/decoded_out_proj_1"
$protoc --decode_raw < "$TMP_OUT/out_proj_2" > "$TMP_OUT/decoded_out_proj_2"
$diff -u "$TMP_OUT/decoded_out_proj_1" "$TMP_OUT/decoded_out_proj_2"
$diff -u "$TMP_OUT/out_proj_1" "$TMP_OUT/out_proj_2"

# Check that daml build is deterministic.
# This includes things like the ZIP timestamps
# in a DAR instead of just the package id.

echo -e "$damlyaml" > "$PROJDIR/daml.yaml"

cat <<EOF > "$PROJDIR/A.daml"
module A where
EOF

$damlc build --project-root "$PROJDIR" -o "$PROJDIR/out.dar"
FIRST_SHA=$(sha256sum $PROJDIR/out.dar)

$damlc build --project-root "$PROJDIR" -o "$PROJDIR/out.dar"
SECOND_SHA=$(sha256sum $PROJDIR/out.dar)

if [[ $FIRST_SHA != $SECOND_SHA ]]; then
    echo "daml build was non-deterministic: "
    echo "$FIRST_SHA"
    echo "$SECOND_SHA"
    exit 1
fi
