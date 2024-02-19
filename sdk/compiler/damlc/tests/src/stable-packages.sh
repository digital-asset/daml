#!/usr/bin/env bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

$DAMLC build --target=2.dev --project-root $DIR -o $DIR/out.dar
# The last line is the main dalf which we don’t need. We don’t need to worry about excluding daml-prim
# and daml-stdlib since they are only pulled in when necessary and they are clearly not required for
# an empty package
$DIFF -u -b <($DAMLC inspect-dar $DIR/out.dar | sed '1,/following packages/d' | head -n -1) <(cat <<EOF

daml-prim-DA-Exception-ArithmeticError-ee33fb70918e7aaa3d3fc44d64a399fb2bf5bcefc54201b1690ecd448551ba88 "ee33fb70918e7aaa3d3fc44d64a399fb2bf5bcefc54201b1690ecd448551ba88"
daml-prim-DA-Exception-AssertionFailed-6da1f43a10a179524e840e7288b47bda213339b0552d92e87ae811e52f59fc0e "6da1f43a10a179524e840e7288b47bda213339b0552d92e87ae811e52f59fc0e"
daml-prim-DA-Exception-GeneralError-f181cd661f7af3a60bdaae4b0285a2a67beb55d6910fc8431dbae21a5825ec0f "f181cd661f7af3a60bdaae4b0285a2a67beb55d6910fc8431dbae21a5825ec0f"
daml-prim-DA-Exception-PreconditionFailed-91e167fa7a256f21f990c526a0a0df840e99aeef0e67dc1f5415b0309486de74 "91e167fa7a256f21f990c526a0a0df840e99aeef0e67dc1f5415b0309486de74"
daml-prim-DA-Internal-Erased-0e4a572ab1fb94744abb02243a6bbed6c78fc6e3c8d3f60c655f057692a62816 "0e4a572ab1fb94744abb02243a6bbed6c78fc6e3c8d3f60c655f057692a62816"
daml-prim-DA-Internal-NatSyn-e5411f3d75f072b944bd88e652112a14a3d409c491fd9a51f5f6eede6d3a3348 "e5411f3d75f072b944bd88e652112a14a3d409c491fd9a51f5f6eede6d3a3348"
daml-prim-DA-Internal-PromotedText-ab068e2f920d0e06347975c2a342b71f8b8e3b4be0f02ead9442caac51aa8877 "ab068e2f920d0e06347975c2a342b71f8b8e3b4be0f02ead9442caac51aa8877"
daml-prim-DA-Types-5aee9b21b8e9a4c4975b5f4c4198e6e6e8469df49e2010820e792f393db870f4 "5aee9b21b8e9a4c4975b5f4c4198e6e6e8469df49e2010820e792f393db870f4"
daml-prim-GHC-Prim-fcee8dfc1b81c449b421410edd5041c16ab59c45bbea85bcb094d1b17c3e9df7 "fcee8dfc1b81c449b421410edd5041c16ab59c45bbea85bcb094d1b17c3e9df7"
daml-prim-GHC-Tuple-19f0df5fdaf5a96e137b6ea885fdb378f37bd3166bd9a47ee11518e33fa09a20 "19f0df5fdaf5a96e137b6ea885fdb378f37bd3166bd9a47ee11518e33fa09a20"
daml-prim-GHC-Types-e7e0adfa881e7dbbb07da065ae54444da7c4bccebcb8872ab0cb5dcf9f3761ce "e7e0adfa881e7dbbb07da065ae54444da7c4bccebcb8872ab0cb5dcf9f3761ce"
daml-stdlib-DA-Action-State-Type-a1fa18133ae48cbb616c4c148e78e661666778c3087d099067c7fe1868cbb3a1 "a1fa18133ae48cbb616c4c148e78e661666778c3087d099067c7fe1868cbb3a1"
daml-stdlib-DA-Date-Types-fa79192fe1cce03d7d8db36471dde4cf6c96e6d0f07e1c391dd49e355af9b38c "fa79192fe1cce03d7d8db36471dde4cf6c96e6d0f07e1c391dd49e355af9b38c"
daml-stdlib-DA-Internal-Any-6f8e6085f5769861ae7a40dccd618d6f747297d59b37cab89b93e2fa80b0c024 "6f8e6085f5769861ae7a40dccd618d6f747297d59b37cab89b93e2fa80b0c024"
daml-stdlib-DA-Internal-Down-86d888f34152dae8729900966b44abcb466b9c111699678de58032de601d2b04 "86d888f34152dae8729900966b44abcb466b9c111699678de58032de601d2b04"
daml-stdlib-DA-Internal-Interface-AnyView-Types-c280cc3ef501d237efa7b1120ca3ad2d196e089ad596b666bed59a85f3c9a074 "c280cc3ef501d237efa7b1120ca3ad2d196e089ad596b666bed59a85f3c9a074"
daml-stdlib-DA-Internal-Template-9e70a8b3510d617f8a136213f33d6a903a10ca0eeec76bb06ba55d1ed9680f69 "9e70a8b3510d617f8a136213f33d6a903a10ca0eeec76bb06ba55d1ed9680f69"
daml-stdlib-DA-Logic-Types-cae345b5500ef6f84645c816f88b9f7a85a9f3c71697984abdf6849f81e80324 "cae345b5500ef6f84645c816f88b9f7a85a9f3c71697984abdf6849f81e80324"
daml-stdlib-DA-Monoid-Types-52854220dc199884704958df38befd5492d78384a032fd7558c38f00e3d778a2 "52854220dc199884704958df38befd5492d78384a032fd7558c38f00e3d778a2"
daml-stdlib-DA-NonEmpty-Types-bde4bd30749e99603e5afa354706608601029e225d4983324d617825b634253a "bde4bd30749e99603e5afa354706608601029e225d4983324d617825b634253a"
daml-stdlib-DA-Random-Types-bfda48f9aa2c89c895cde538ec4b4946c7085959e031ad61bde616b9849155d7 "bfda48f9aa2c89c895cde538ec4b4946c7085959e031ad61bde616b9849155d7"
daml-stdlib-DA-Semigroup-Types-d095a2ccf6dd36b2415adc4fa676f9191ba63cd39828dc5207b36892ec350cbc "d095a2ccf6dd36b2415adc4fa676f9191ba63cd39828dc5207b36892ec350cbc"
daml-stdlib-DA-Set-Types-c3bb0c5d04799b3f11bad7c3c102963e115cf53da3e4afcbcfd9f06ebd82b4ff "c3bb0c5d04799b3f11bad7c3c102963e115cf53da3e4afcbcfd9f06ebd82b4ff"
daml-stdlib-DA-Stack-Types-60c61c542207080e97e378ab447cc355ecc47534b3a3ebbff307c4fb8339bc4d "60c61c542207080e97e378ab447cc355ecc47534b3a3ebbff307c4fb8339bc4d"
daml-stdlib-DA-Time-Types-b70db8369e1c461d5c70f1c86f526a29e9776c655e6ffc2560f95b05ccb8b946 "b70db8369e1c461d5c70f1c86f526a29e9776c655e6ffc2560f95b05ccb8b946"
daml-stdlib-DA-Validation-Types-3cde94fe9be5c700fc1d9a8ad2277e2c1214609f8c52a5b4db77e466875b8cb7 "3cde94fe9be5c700fc1d9a8ad2277e2c1214609f8c52a5b4db77e466875b8cb7"
EOF
)
