#!/usr/bin/env bash
# Copyright (c) 2020 The DAML Authors. All rights reserved.
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

set -euo pipefail

JQ="$(rlocation "$TEST_WORKSPACE/$1")"
JQ_LF_LIB="$(rlocation "$TEST_WORKSPACE/$2")"
PKG_DB="$(rlocation "$TEST_WORKSPACE/$3")"
DAMLC="$(rlocation "$TEST_WORKSPACE/$4")"
DIFF=$5

get_serializable_types() {

    $DAMLC inspect $1 --json | $JQ -L $(dirname $JQ_LF_LIB) 'import "query-lf-interned" as lf; .Sum.daml_lf_1 as $pkg | $pkg.modules | .[] | (.name | lf::get_dotted_name($pkg) | join(".")) as $modname | .data_types | .[] | select(.serializable) | .name | lf::get_dotted_name($pkg) | $modname + ":" + join(".")'
}

for LF_VERSION in $PKG_DB/*; do
    # Skip 1.6 since we don’t really care about it and it removes the need to handle LF versions without
    # interning.
    if [ $(basename $LF_VERSION) != "1.6" ]; then
        stdlib=$LF_VERSION/daml-stdlib-*.dalf
        prim=$LF_VERSION/daml-prim.dalf
        # MetaEquiv is a typeclass without methods and is translated to a type synonym for Unit
        # in newer LF versions.
        if [ $(basename $LF_VERSION) == "1.dev" ] || [ $(basename $LF_VERSION) == "1.8" ]; then
            $DIFF -b -u <(get_serializable_types $stdlib) <(cat <<EOF
"DA.Random:Minstd"
"DA.Next.Set:Set"
"DA.Next.Map:Map"
"DA.Generics:DecidedStrictness"
"DA.Generics:SourceStrictness"
"DA.Generics:SourceUnpackedness"
"DA.Generics:Associativity"
"DA.Generics:Infix0"
"DA.Generics:Fixity"
"DA.Generics:K1"
"DA.Generics:Par1"
"DA.Generics:U1"
EOF
)
        else
            $DIFF -b -u <(get_serializable_types $stdlib) <(cat <<EOF
"DA.Random:Minstd"
"DA.Next.Set:Set"
"DA.Next.Map:Map"
"DA.Generics:DecidedStrictness"
"DA.Generics:SourceStrictness"
"DA.Generics:SourceUnpackedness"
"DA.Generics:Associativity"
"DA.Generics:Infix0"
"DA.Generics:Fixity"
"DA.Generics:K1"
"DA.Generics:Par1"
"DA.Generics:U1"
EOF
)
        fi
      $DIFF -b -u <(get_serializable_types $prim) <(cat <<EOF
EOF
)
    fi
done
