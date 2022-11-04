#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

SDK_VERSION=$2

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

$DAMLC build --target=1.dev --project-root $DIR -o $DIR/out.dar
# The last line is the main dalf which we don’t need. We don’t need to worry about excluding daml-prim
# and daml-stdlib since they are only pulled in when necessary and they are clearly not required for
# an empty package
$DIFF -u -b <($DAMLC inspect-dar $DIR/out.dar | sed '1,/following packages/d' | head -n -1) <(cat <<EOF

daml-prim-DA-Exception-ArithmeticError-cb0552debf219cc909f51cbb5c3b41e9981d39f8f645b1f35e2ef5be2e0b858a "cb0552debf219cc909f51cbb5c3b41e9981d39f8f645b1f35e2ef5be2e0b858a"
daml-prim-DA-Exception-AssertionFailed-3f4deaf145a15cdcfa762c058005e2edb9baa75bb7f95a4f8f6f937378e86415 "3f4deaf145a15cdcfa762c058005e2edb9baa75bb7f95a4f8f6f937378e86415"
daml-prim-DA-Exception-GeneralError-86828b9843465f419db1ef8a8ee741d1eef645df02375ebf509cdc8c3ddd16cb "86828b9843465f419db1ef8a8ee741d1eef645df02375ebf509cdc8c3ddd16cb"
daml-prim-DA-Exception-PreconditionFailed-f20de1e4e37b92280264c08bf15eca0be0bc5babd7a7b5e574997f154c00cb78 "f20de1e4e37b92280264c08bf15eca0be0bc5babd7a7b5e574997f154c00cb78"
daml-prim-DA-Internal-Erased-76bf0fd12bd945762a01f8fc5bbcdfa4d0ff20f8762af490f8f41d6237c6524f "76bf0fd12bd945762a01f8fc5bbcdfa4d0ff20f8762af490f8f41d6237c6524f"
daml-prim-DA-Internal-NatSyn-38e6274601b21d7202bb995bc5ec147decda5a01b68d57dda422425038772af7 "38e6274601b21d7202bb995bc5ec147decda5a01b68d57dda422425038772af7"
daml-prim-DA-Internal-PromotedText-d58cf9939847921b2aab78eaa7b427dc4c649d25e6bee3c749ace4c3f52f5c97 "d58cf9939847921b2aab78eaa7b427dc4c649d25e6bee3c749ace4c3f52f5c97"
daml-prim-DA-Types-40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7 "40f452260bef3f29dede136108fc08a88d5a5250310281067087da6f0baddff7"
daml-prim-GHC-Prim-e491352788e56ca4603acc411ffe1a49fefd76ed8b163af86cf5ee5f4c38645b "e491352788e56ca4603acc411ffe1a49fefd76ed8b163af86cf5ee5f4c38645b"
daml-prim-GHC-Tuple-6839a6d3d430c569b2425e9391717b44ca324b88ba621d597778811b2d05031d "6839a6d3d430c569b2425e9391717b44ca324b88ba621d597778811b2d05031d"
-daml-prim-GHC-Tuple-Check-1b04d27b93f5c72a4cd7716feacae7b7aef27235869daaabd5fe07edb14d6002 "1b04d27b93f5c72a4cd7716feacae7b7aef27235869daaabd5fe07edb14d6002"
daml-prim-GHC-Types-518032f41fd0175461b35ae0c9691e08b4aea55e62915f8360af2cc7a1f2ba6c "518032f41fd0175461b35ae0c9691e08b4aea55e62915f8360af2cc7a1f2ba6c"
daml-stdlib-DA-Date-Types-bfcd37bd6b84768e86e432f5f6c33e25d9e7724a9d42e33875ff74f6348e733f "bfcd37bd6b84768e86e432f5f6c33e25d9e7724a9d42e33875ff74f6348e733f"
daml-stdlib-DA-Internal-Any-cc348d369011362a5190fe96dd1f0dfbc697fdfd10e382b9e9666f0da05961b7 "cc348d369011362a5190fe96dd1f0dfbc697fdfd10e382b9e9666f0da05961b7"
daml-stdlib-DA-Internal-Down-057eed1fd48c238491b8ea06b9b5bf85a5d4c9275dd3f6183e0e6b01730cc2ba "057eed1fd48c238491b8ea06b9b5bf85a5d4c9275dd3f6183e0e6b01730cc2ba"
daml-stdlib-DA-Internal-Interface-AnyView-Types-6df2d1fd8ea994ed048a79587b2722e3a887ac7592abf31ecf46fe09ac02d689 "6df2d1fd8ea994ed048a79587b2722e3a887ac7592abf31ecf46fe09ac02d689"
daml-stdlib-DA-Internal-Template-d14e08374fc7197d6a0de468c968ae8ba3aadbf9315476fd39071831f5923662 "d14e08374fc7197d6a0de468c968ae8ba3aadbf9315476fd39071831f5923662"
daml-stdlib-DA-Logic-Types-c1f1f00558799eec139fb4f4c76f95fb52fa1837a5dd29600baa1c8ed1bdccfd "c1f1f00558799eec139fb4f4c76f95fb52fa1837a5dd29600baa1c8ed1bdccfd"
daml-stdlib-DA-Monoid-Types-6c2c0667393c5f92f1885163068cd31800d2264eb088eb6fc740e11241b2bf06 "6c2c0667393c5f92f1885163068cd31800d2264eb088eb6fc740e11241b2bf06"
daml-stdlib-DA-NonEmpty-Types-e22bce619ae24ca3b8e6519281cb5a33b64b3190cc763248b4c3f9ad5087a92c "e22bce619ae24ca3b8e6519281cb5a33b64b3190cc763248b4c3f9ad5087a92c"
daml-stdlib-DA-Semigroup-Types-8a7806365bbd98d88b4c13832ebfa305f6abaeaf32cfa2b7dd25c4fa489b79fb "8a7806365bbd98d88b4c13832ebfa305f6abaeaf32cfa2b7dd25c4fa489b79fb"
daml-stdlib-DA-Set-Types-97b883cd8a2b7f49f90d5d39c981cf6e110cf1f1c64427a28a6d58ec88c43657 "97b883cd8a2b7f49f90d5d39c981cf6e110cf1f1c64427a28a6d58ec88c43657"
daml-stdlib-DA-Time-Types-733e38d36a2759688a4b2c4cec69d48e7b55ecc8dedc8067b815926c917a182a "733e38d36a2759688a4b2c4cec69d48e7b55ecc8dedc8067b815926c917a182a"
daml-stdlib-DA-Validation-Types-99a2705ed38c1c26cbb8fe7acf36bbf626668e167a33335de932599219e0a235 "99a2705ed38c1c26cbb8fe7acf36bbf626668e167a33335de932599219e0a235"
EOF
)
