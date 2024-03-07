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

daml-prim-DA-Exception-ArithmeticError-191e9eeb0373ef8ff509580203df748baf1aece04d33322031ec022f191009f0 "191e9eeb0373ef8ff509580203df748baf1aece04d33322031ec022f191009f0"
daml-prim-DA-Exception-AssertionFailed-dd10c0f359ffe8f0bc7cbad8025083d9d434e36b3e925fac34f23032252609e1 "dd10c0f359ffe8f0bc7cbad8025083d9d434e36b3e925fac34f23032252609e1"
daml-prim-DA-Exception-GeneralError-cc9a532c9fda12935410c6c969165db2f9358456031e02021fce8866209baa1d "cc9a532c9fda12935410c6c969165db2f9358456031e02021fce8866209baa1d"
daml-prim-DA-Exception-PreconditionFailed-531344b5311200a73feed927e6e4b1a350bffd72fdfb9ee469aa342755134ade "531344b5311200a73feed927e6e4b1a350bffd72fdfb9ee469aa342755134ade"
daml-prim-DA-Internal-Erased-0e4a572ab1fb94744abb02243a6bbed6c78fc6e3c8d3f60c655f057692a62816 "0e4a572ab1fb94744abb02243a6bbed6c78fc6e3c8d3f60c655f057692a62816"
daml-prim-DA-Internal-NatSyn-e5411f3d75f072b944bd88e652112a14a3d409c491fd9a51f5f6eede6d3a3348 "e5411f3d75f072b944bd88e652112a14a3d409c491fd9a51f5f6eede6d3a3348"
daml-prim-DA-Internal-PromotedText-ab068e2f920d0e06347975c2a342b71f8b8e3b4be0f02ead9442caac51aa8877 "ab068e2f920d0e06347975c2a342b71f8b8e3b4be0f02ead9442caac51aa8877"
daml-prim-DA-Types-7ee79b4990f1fa9ff0a882c94ca5d9abb674f71060fb239f5f87a811ae9d2cd5 "7ee79b4990f1fa9ff0a882c94ca5d9abb674f71060fb239f5f87a811ae9d2cd5"
daml-prim-GHC-Prim-b40adc6e0110430a66f0e40dfa6e0ab08f15b812eedcac68bdc346d374353dff "b40adc6e0110430a66f0e40dfa6e0ab08f15b812eedcac68bdc346d374353dff"
daml-prim-GHC-Tuple-bc1ffdb649f65c0b877cdd7e883776a7cb4c40aef10769e41f612ee69bf27116 "bc1ffdb649f65c0b877cdd7e883776a7cb4c40aef10769e41f612ee69bf27116"
daml-prim-GHC-Types-e7e0adfa881e7dbbb07da065ae54444da7c4bccebcb8872ab0cb5dcf9f3761ce "e7e0adfa881e7dbbb07da065ae54444da7c4bccebcb8872ab0cb5dcf9f3761ce"
daml-stdlib-DA-Action-State-Type-c841b7c33fd5d6cf5f53663bc8d65bf1e63a5be354c92902694465f5186528ba "c841b7c33fd5d6cf5f53663bc8d65bf1e63a5be354c92902694465f5186528ba"
daml-stdlib-DA-Date-Types-fa79192fe1cce03d7d8db36471dde4cf6c96e6d0f07e1c391dd49e355af9b38c "fa79192fe1cce03d7d8db36471dde4cf6c96e6d0f07e1c391dd49e355af9b38c"
daml-stdlib-DA-Internal-Any-b1838bb519cfaaaaf1f69b9ae6b9f4dc12a00d339a569c1a150cd087cf2e722e "b1838bb519cfaaaaf1f69b9ae6b9f4dc12a00d339a569c1a150cd087cf2e722e"
daml-stdlib-DA-Internal-Down-a33e01b652ba07d12feb6b8b79c2143ca782f84897753b352a47f08c62315f65 "a33e01b652ba07d12feb6b8b79c2143ca782f84897753b352a47f08c62315f65"
daml-stdlib-DA-Internal-Interface-AnyView-Types-750c0ef0d94d0257677cdca3c6e14f5a7423852baad984c066ea2002741508b1 "750c0ef0d94d0257677cdca3c6e14f5a7423852baad984c066ea2002741508b1"
daml-stdlib-DA-Internal-Template-9e70a8b3510d617f8a136213f33d6a903a10ca0eeec76bb06ba55d1ed9680f69 "9e70a8b3510d617f8a136213f33d6a903a10ca0eeec76bb06ba55d1ed9680f69"
daml-stdlib-DA-Logic-Types-94ffd86652eb70134fea581ea5b7b388757de273b715187a7fdf071e49dfefb5 "94ffd86652eb70134fea581ea5b7b388757de273b715187a7fdf071e49dfefb5"
daml-stdlib-DA-Monoid-Types-287d92b136e74f9c9fefd0e58174b2a3c339dec43440ff010808a270e5b75a23 "287d92b136e74f9c9fefd0e58174b2a3c339dec43440ff010808a270e5b75a23"
daml-stdlib-DA-NonEmpty-Types-29cb5e4da423956deabc8856d35efd874e117a24807600b4675e6bdd49f6b4d4 "29cb5e4da423956deabc8856d35efd874e117a24807600b4675e6bdd49f6b4d4"
daml-stdlib-DA-Random-Types-511fc57543d8653a785765725b2b428a56083368499af4e1a140582157f37756 "511fc57543d8653a785765725b2b428a56083368499af4e1a140582157f37756"
daml-stdlib-DA-Semigroup-Types-58fe32944daab7bcaa5594da72f3250eb654e14fc54c3f47c7d4c55718b2b89b "58fe32944daab7bcaa5594da72f3250eb654e14fc54c3f47c7d4c55718b2b89b"
daml-stdlib-DA-Set-Types-ee2c19710fd30f986d160cb00f369f531f2c8500022412e59e0723f3056add74 "ee2c19710fd30f986d160cb00f369f531f2c8500022412e59e0723f3056add74"
daml-stdlib-DA-Stack-Types-2500e4ee15914defd4dc79f1b92fe61e04e8ec2fd9e36519a05475be5dfe4b36 "2500e4ee15914defd4dc79f1b92fe61e04e8ec2fd9e36519a05475be5dfe4b36"
daml-stdlib-DA-Time-Types-7b058b12a91f69c75afb471f39a822b5072f6a0f0ed38ea79dbebacdddbc3fea "7b058b12a91f69c75afb471f39a822b5072f6a0f0ed38ea79dbebacdddbc3fea"
daml-stdlib-DA-Validation-Types-7feb38b086104e02a42a744ab1ee3dc50292a1a64d204f6f84f234b4cff8acd6 "7feb38b086104e02a42a744ab1ee3dc50292a1a64d204f6f84f234b4cff8acd6"
EOF
)
