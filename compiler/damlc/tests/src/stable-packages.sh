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

daml-prim-DA-Exception-ArithmeticError-0e35772044c88dda5159f70a9170eae0f07e2f1942af4ab401e7cd79166e8c94 "0e35772044c88dda5159f70a9170eae0f07e2f1942af4ab401e7cd79166e8c94"
daml-prim-DA-Exception-AssertionFailed-ffc462638e7338aaf5d45b3eae8aba5d8e9259a2e44d1ec9db70ed4ee83601e0 "ffc462638e7338aaf5d45b3eae8aba5d8e9259a2e44d1ec9db70ed4ee83601e0"
daml-prim-DA-Exception-GeneralError-48426ca53c6510a1d641cdc05dd5b3cea288bd4bcd54311ffaa284b5097d4b9d "48426ca53c6510a1d641cdc05dd5b3cea288bd4bcd54311ffaa284b5097d4b9d"
daml-prim-DA-Exception-PreconditionFailed-4d035c16dee0b8d75814624a05de9fcb062e942ff3b3b60d913335b468a84789 "4d035c16dee0b8d75814624a05de9fcb062e942ff3b3b60d913335b468a84789"
daml-prim-DA-Internal-Erased-71ca307ec24fc584d601fd6d5f49ec76d100730f56eef290e41248f32ccadfb1 "71ca307ec24fc584d601fd6d5f49ec76d100730f56eef290e41248f32ccadfb1"
daml-prim-DA-Internal-NatSyn-eb6926e50bb83fbc8f3e154c7c88b1219b31a3c0b812f26b276d46e212c2dd71 "eb6926e50bb83fbc8f3e154c7c88b1219b31a3c0b812f26b276d46e212c2dd71"
daml-prim-DA-Internal-PromotedText-c2bac57e7a921c98523ae40766bfefab9f5b7bf4bf34e1c09d9a9d1483417b6c "c2bac57e7a921c98523ae40766bfefab9f5b7bf4bf34e1c09d9a9d1483417b6c"
daml-prim-DA-Types-87530dd1038863bad7bdf02c59ae851bc00f469edb2d7dbc8be3172daafa638c "87530dd1038863bad7bdf02c59ae851bc00f469edb2d7dbc8be3172daafa638c"
daml-prim-GHC-Prim-37375ebfb7ebef8a38aa0d037db55833bcac40eb04074b9024ed81c249ea2387 "37375ebfb7ebef8a38aa0d037db55833bcac40eb04074b9024ed81c249ea2387"
daml-prim-GHC-Tuple-afbf83d4d9ef0fb1a4ee4d1d6bef98b5cea044a6d395c1c27834abd7e8eb57ae "afbf83d4d9ef0fb1a4ee4d1d6bef98b5cea044a6d395c1c27834abd7e8eb57ae"
daml-prim-GHC-Types-2f07eb3e4731beccfd88fcd19177268f120f9a2b06a52534ee808a4ba1e89720 "2f07eb3e4731beccfd88fcd19177268f120f9a2b06a52534ee808a4ba1e89720"
daml-stdlib-DA-Action-State-Type-2d7314e12cc21ce1482d7a59ab8b42f8deafbe3cdb62eae9fcd4f583ba0ad8d0 "2d7314e12cc21ce1482d7a59ab8b42f8deafbe3cdb62eae9fcd4f583ba0ad8d0"
daml-stdlib-DA-Date-Types-a4c44a89461229bb4a4fddbdeabe3f9dfaf6db35896c87d76f638cd45e1f0678 "a4c44a89461229bb4a4fddbdeabe3f9dfaf6db35896c87d76f638cd45e1f0678"
daml-stdlib-DA-Internal-Any-7de198711a7a5b3c897eff937b85811438d22f48452a118222590b0ec080bf54 "7de198711a7a5b3c897eff937b85811438d22f48452a118222590b0ec080bf54"
daml-stdlib-DA-Internal-Down-35df06619756af233f13051ba77718e172589b979d22dd3ec40c8787563bb435 "35df06619756af233f13051ba77718e172589b979d22dd3ec40c8787563bb435"
daml-stdlib-DA-Internal-Interface-AnyView-Types-db7b27684bd900f7ca47c854d526eeaffd9e84d761859c66cc6cf5957ad89ed4 "db7b27684bd900f7ca47c854d526eeaffd9e84d761859c66cc6cf5957ad89ed4"
daml-stdlib-DA-Internal-Template-c2eed01333d3c95b12ca5ef0f196db5cd481c58902e01c8ac6b1e49a62875aa5 "c2eed01333d3c95b12ca5ef0f196db5cd481c58902e01c8ac6b1e49a62875aa5"
daml-stdlib-DA-Logic-Types-19cfdcbac283f2a26c05bfcea437177cfb7adb0b2eb8aa82944e91b13b671912 "19cfdcbac283f2a26c05bfcea437177cfb7adb0b2eb8aa82944e91b13b671912"
daml-stdlib-DA-Monoid-Types-bb581ddc78c4c0e682727d9a2302dc1eba5941809c528aca149a5fdaf25c6cbd "bb581ddc78c4c0e682727d9a2302dc1eba5941809c528aca149a5fdaf25c6cbd"
daml-stdlib-DA-NonEmpty-Types-d3a94c9e99da7fbb5e35d52d05eec84db27233d4c1aed75548dba1057c84ad81 "d3a94c9e99da7fbb5e35d52d05eec84db27233d4c1aed75548dba1057c84ad81"
daml-stdlib-DA-Random-Types-58f4cb7b68a305d056a067c03083f80550ed7d98d6fe100a5ddfa282851ba49a "58f4cb7b68a305d056a067c03083f80550ed7d98d6fe100a5ddfa282851ba49a"
daml-stdlib-DA-Semigroup-Types-ceb729ab26af3934f35fb803534d633c4dd37b466888afcced34a32155e9c2cd "ceb729ab26af3934f35fb803534d633c4dd37b466888afcced34a32155e9c2cd"
daml-stdlib-DA-Set-Types-9d88bb9904dab8f44a47e4f27c8d8ee4fc57fece9c2e3d385ef7ed19fcc24049 "9d88bb9904dab8f44a47e4f27c8d8ee4fc57fece9c2e3d385ef7ed19fcc24049"
daml-stdlib-DA-Stack-Types-747f749a860db32a01ae0c5c741e6648497b93ffcfef3948854c31cc8167eacf "747f749a860db32a01ae0c5c741e6648497b93ffcfef3948854c31cc8167eacf"
daml-stdlib-DA-Time-Types-b47113ba94c31372c553e3869fffed9a45ef1c0f5ac1be3287857cd9450c0bae "b47113ba94c31372c553e3869fffed9a45ef1c0f5ac1be3287857cd9450c0bae"
daml-stdlib-DA-Validation-Types-4687117abb53238857bccdb0d00be7fc005eb334e1f232de3d78152b90b3f202 "4687117abb53238857bccdb0d00be7fc005eb334e1f232de3d78152b90b3f202"
EOF
)
