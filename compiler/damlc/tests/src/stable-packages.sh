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

daml-prim-DA-Exception-ArithmeticError-ded2974feb90808a03199cad3355a505bf930a717456a85cd7ac6b03ace303c9 "ded2974feb90808a03199cad3355a505bf930a717456a85cd7ac6b03ace303c9"
daml-prim-DA-Exception-AssertionFailed-5548421c4a31fac59b22505f2216177920df47059071a34da3f8d8c07dfeb7f6 "5548421c4a31fac59b22505f2216177920df47059071a34da3f8d8c07dfeb7f6"
daml-prim-DA-Exception-GeneralError-449a5a5c62a70ef892325acbd396b77eab3fd5e1e8cb780df40c856bb22a23ea "449a5a5c62a70ef892325acbd396b77eab3fd5e1e8cb780df40c856bb22a23ea"
daml-prim-DA-Exception-PreconditionFailed-9c64df81897c6b98c86063b3a2a4503d756bb7994f06c290ea3d6ad719b76c72 "9c64df81897c6b98c86063b3a2a4503d756bb7994f06c290ea3d6ad719b76c72"
daml-prim-DA-Internal-Erased-a486f9d83acf91ddcb27a9a8743c042f310beab20be676cfc37220961df03900 "a486f9d83acf91ddcb27a9a8743c042f310beab20be676cfc37220961df03900"
daml-prim-DA-Internal-NatSyn-ce33df2997d69e8ac89f00951c322753e60abccdfdd92d47d804518a2029748f "ce33df2997d69e8ac89f00951c322753e60abccdfdd92d47d804518a2029748f"
daml-prim-DA-Internal-PromotedText-ad8708bc34bce0096a8f43500940f0d62fbf947aed8484efa92dc6ae2f9126ac "ad8708bc34bce0096a8f43500940f0d62fbf947aed8484efa92dc6ae2f9126ac"
daml-prim-DA-Types-26b14ad5a8a2ed45d75e3c774aeb1c41a918ef2f4a7d2bd40f9716f26c46bfdf "26b14ad5a8a2ed45d75e3c774aeb1c41a918ef2f4a7d2bd40f9716f26c46bfdf"
daml-prim-GHC-Prim-574f715baa8298bf09261ba87a77589f5aeef88e12b7b672cb80c4d2604035fa "574f715baa8298bf09261ba87a77589f5aeef88e12b7b672cb80c4d2604035fa"
daml-prim-GHC-Tuple-9c1f8a2f36dfdbf1f30087c75e654fa39cb5fc614503979485b263f70a2e5422 "9c1f8a2f36dfdbf1f30087c75e654fa39cb5fc614503979485b263f70a2e5422"
daml-prim-GHC-Types-48b29a202dfd2b7c892f113aff1e70ff124059df9f756af4bcf1faf75fc41b19 "48b29a202dfd2b7c892f113aff1e70ff124059df9f756af4bcf1faf75fc41b19"
daml-stdlib-DA-Action-State-Type-1bf85ad08ef3be26f2d8a864b4bf907f38f65051ddaa18bf1ec5872756010276 "1bf85ad08ef3be26f2d8a864b4bf907f38f65051ddaa18bf1ec5872756010276"
daml-stdlib-DA-Date-Types-001109f95f991bea2ce8d641c2188d9f8c9d2909786549ba6d652024b3680e63 "001109f95f991bea2ce8d641c2188d9f8c9d2909786549ba6d652024b3680e63"
daml-stdlib-DA-Internal-Any-053b10c09112715e460733385963e120a75768abf5a5539428a6437017792e65 "053b10c09112715e460733385963e120a75768abf5a5539428a6437017792e65"
daml-stdlib-DA-Internal-Down-54abeb11f0eed3da544d37cbad04d8f866d83acba977bb014b3e346f2eb9e551 "54abeb11f0eed3da544d37cbad04d8f866d83acba977bb014b3e346f2eb9e551"
daml-stdlib-DA-Internal-Interface-AnyView-Types-2513dbd49a110892bfbfdad4bd0b5aef82e34979d59529c1f7e74b425e561977 "2513dbd49a110892bfbfdad4bd0b5aef82e34979d59529c1f7e74b425e561977"
daml-stdlib-DA-Internal-Template-ace2eb6a9cd13bca35ce7f068b942ab5c47987eed34efea52470b3aa0458a2f5 "ace2eb6a9cd13bca35ce7f068b942ab5c47987eed34efea52470b3aa0458a2f5"
daml-stdlib-DA-Logic-Types-edb5aeef08a062be44018bcd548d8141951fcadc6705e483fa7e62d908d84dea "edb5aeef08a062be44018bcd548d8141951fcadc6705e483fa7e62d908d84dea"
daml-stdlib-DA-Monoid-Types-c6ac07a6623e57d226f3289e934c22cd251dda95eb1d82108374023a7e032254 "c6ac07a6623e57d226f3289e934c22cd251dda95eb1d82108374023a7e032254"
daml-stdlib-DA-NonEmpty-Types-d6ae362400b05ec4ed649cc313f5e5bb06a1fed92cce72589ec8ee45573962dc "d6ae362400b05ec4ed649cc313f5e5bb06a1fed92cce72589ec8ee45573962dc"
daml-stdlib-DA-Random-Types-c8463c6500cba09d1f52d6851f94882ebfe8b0d9c782291e98f483f8c21e7ae2 "c8463c6500cba09d1f52d6851f94882ebfe8b0d9c782291e98f483f8c21e7ae2"
daml-stdlib-DA-Semigroup-Types-8bf075ed0f9b502294940d256cadace47e71b7adfa7cce854c1829c2bddf241f "8bf075ed0f9b502294940d256cadace47e71b7adfa7cce854c1829c2bddf241f"
daml-stdlib-DA-Set-Types-9511092860971d9c6ba81c73fed994a3670e5279d5bf193e4bbb02063281dab7 "9511092860971d9c6ba81c73fed994a3670e5279d5bf193e4bbb02063281dab7"
daml-stdlib-DA-Stack-Types-5ba9b13b8f42b1d5d0cdbea93247c8816bfabd2101a9c5972b6852a3151f7100 "5ba9b13b8f42b1d5d0cdbea93247c8816bfabd2101a9c5972b6852a3151f7100"
daml-stdlib-DA-Time-Types-13f71afbf5d73853a854c2ad9269e47acf5a94c2f533141b5522542f66e86526 "13f71afbf5d73853a854c2ad9269e47acf5a94c2f533141b5522542f66e86526"
daml-stdlib-DA-Validation-Types-7851ba55b61ff1efd2dc04e55093ba273843501d3cb792c5be6e983e94530dd2 "7851ba55b61ff1efd2dc04e55093ba273843501d3cb792c5be6e983e94530dd2"
EOF
)
