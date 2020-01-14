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

DAMLC="$(rlocation "$TEST_WORKSPACE/$1")"

# If the path starts with a dot, just appending TEST_WORKSPACE isn’t going to work.
# Therefore we separately check if we have that path.
if [ "$2" == "./VERSION" ]; then
    SDK_VERSION="$(rlocation "$TEST_WORKSPACE/VERSION")"
else
    SDK_VERSION="$(rlocation "$TEST_WORKSPACE/$2")"
fi

SDK_VERSION=$(cat $SDK_VERSION)
DIFF=$3

DIR=$(mktemp -d)
trap "rm -rf $DIR" EXIT

cat > $DIR/daml.yaml <<EOF
sdk-version: $SDK_VERSION
name: foobar
version: 0.1.0
source: src
dependencies: [daml-prim, daml-stdlib]
EOF
mkdir -p $DIR/src

$DAMLC build --project-root $DIR -o $DIR/out.dar
# The last line is the main dalf which we don’t need. We don’t need to worry about excluding daml-prim
# and daml-stdlib since they are only pulled in when necessary and they are clearly not required fr
# an empty package
$DIFF -u -b <($DAMLC inspect-dar $DIR/out.dar | sed '1,/following packages/d' | head -n -1) <(cat <<EOF

daml-prim-DA-Types-40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7 "40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7"
daml-prim-GHC-Prim-e491352788e56ca4603acc411ffe1a49fefd76ed8b163af86cf5ee5f4c38645b "e491352788e56ca4603acc411ffe1a49fefd76ed8b163af86cf5ee5f4c38645b"
daml-prim-GHC-Tuple-6839a6d3d430c569b2425e9391717b44ca324b88ba621d597778811b2d05031d "6839a6d3d430c569b2425e9391717b44ca324b88ba621d597778811b2d05031d"
daml-prim-GHC-Types-518032f41fd0175461b35ae0c9691e08b4aea55e62915f8360af2cc7a1f2ba6c "518032f41fd0175461b35ae0c9691e08b4aea55e62915f8360af2cc7a1f2ba6c"
daml-stdlib-DA-Date-Types-bfcd37bd6b84768e86e432f5f6c33e25d9e7724a9d42e33875ff74f6348e733f "bfcd37bd6b84768e86e432f5f6c33e25d9e7724a9d42e33875ff74f6348e733f"
daml-stdlib-DA-Internal-Any-cc348d369011362a5190fe96dd1f0dfbc697fdfd10e382b9e9666f0da05961b7 "cc348d369011362a5190fe96dd1f0dfbc697fdfd10e382b9e9666f0da05961b7"
daml-stdlib-DA-Internal-Down-057eed1fd48c238491b8ea06b9b5bf85a5d4c9275dd3f6183e0e6b01730cc2ba "057eed1fd48c238491b8ea06b9b5bf85a5d4c9275dd3f6183e0e6b01730cc2ba"
daml-stdlib-DA-Internal-Template-d14e08374fc7197d6a0de468c968ae8ba3aadbf9315476fd39071831f5923662 "d14e08374fc7197d6a0de468c968ae8ba3aadbf9315476fd39071831f5923662"
daml-stdlib-DA-Logic-Types-c1f1f00558799eec139fb4f4c76f95fb52fa1837a5dd29600baa1c8ed1bdccfd "c1f1f00558799eec139fb4f4c76f95fb52fa1837a5dd29600baa1c8ed1bdccfd"
daml-stdlib-DA-Monoid-Types-6c2c0667393c5f92f1885163068cd31800d2264eb088eb6fc740e11241b2bf06 "6c2c0667393c5f92f1885163068cd31800d2264eb088eb6fc740e11241b2bf06"
daml-stdlib-DA-NonEmpty-Types-e22bce619ae24ca3b8e6519281cb5a33b64b3190cc763248b4c3f9ad5087a92c "e22bce619ae24ca3b8e6519281cb5a33b64b3190cc763248b4c3f9ad5087a92c"
daml-stdlib-DA-Semigroup-Types-8a7806365bbd98d88b4c13832ebfa305f6abaeaf32cfa2b7dd25c4fa489b79fb "8a7806365bbd98d88b4c13832ebfa305f6abaeaf32cfa2b7dd25c4fa489b79fb"
daml-stdlib-DA-Time-Types-577ae5400f6c64bb03c8927283a96b8b9bcffd0a1a78eb3bd29d99cef227a256 "577ae5400f6c64bb03c8927283a96b8b9bcffd0a1a78eb3bd29d99cef227a256"
daml-stdlib-DA-Validation-Types-99a2705ed38c1c26cbb8fe7acf36bbf626668e167a33335de932599219e0a235 "99a2705ed38c1c26cbb8fe7acf36bbf626668e167a33335de932599219e0a235"
EOF
)
