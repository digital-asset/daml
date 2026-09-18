#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

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

set -euo pipefail

DAMLC=$(rlocation "$TEST_WORKSPACE/$1")
JQ=$(rlocation "$TEST_WORKSPACE/$2")
DAR=$(rlocation "$TEST_WORKSPACE/$3")
DALF=$(rlocation "$TEST_WORKSPACE/$4")

cd "$TEST_TMPDIR"

"$DAMLC" inspect --json "$DAR" > dar.json
"$JQ" -e '
    (.minor | type == "string") and .patch == 0 and
    (.Sum.daml_lf_2 | type == "object") and
    (.Sum.daml_lf_2.modules | length == 1) and
    (.Sum.daml_lf_2.interned_strings | index("InspectProbe") != null)
' dar.json || {
    echo "Expected decoded LF2 package contents and preserved version metadata" >&2
    exit 1
}

# Resolve value definitions through LF2's shared name tables using jq variables.
# shellcheck disable=SC2016
"$JQ" -e '
    .Sum.daml_lf_2 as $package |
    any($package.modules[].values[];
        .name_with_type.name_interned_dname as $name |
        $package.interned_dotted_names[$name].segments_interned_str |
        map($package.interned_strings[.]) == ["answer"]
    )
' dar.json || {
    echo "Expected an answer value definition in the decoded LF2 module" >&2
    exit 1
}

# Bazel extracts this DALF from the same DAR used above.
"$DAMLC" inspect --json "$DALF" -o dalf.json
"$JQ" -e -s '.[0] == .[1]' dar.json dalf.json

# Valid outer protobuf messages wrap an invalid LF2 Package: a truncated varint.
# Decoding the package must fail without emitting JSON.
printf '\x1a\x06\x1a\x013\x22\x01\x80' > malformed.dalf
if "$DAMLC" inspect --json malformed.dalf > malformed.json 2> malformed.err; then
    echo "Expected malformed LF2 protobuf to fail" >&2
    exit 1
fi
test ! -s malformed.json
grep -q 'Cannot decode LF2 package' malformed.err

# With no package field, inspection preserves the existing JSON representation.
printf '\x1a\x03\x1a\x013' > absent.dalf
"$DAMLC" inspect --json absent.dalf > absent.json
"$JQ" -e '.minor == "3" and .patch == 0 and has("Sum") and .Sum == null' absent.json
