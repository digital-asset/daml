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

daml-prim-DA-Exception-ArithmeticError-e1713757215a2462915fed49bbe47c3e459dfabb3fb9521a832cc7bb32499bae "e1713757215a2462915fed49bbe47c3e459dfabb3fb9521a832cc7bb32499bae"
daml-prim-DA-Exception-AssertionFailed-f3ae1c80664957609d28f76e8f0bf8a5aaec25d93c786501b785ab5a49b2a8f7 "f3ae1c80664957609d28f76e8f0bf8a5aaec25d93c786501b785ab5a49b2a8f7"
daml-prim-DA-Exception-GeneralError-7e1ea9e0fbef673f4ee6c0059c7ede41dfd893a6736a23e263ad9b4395d89ff1 "7e1ea9e0fbef673f4ee6c0059c7ede41dfd893a6736a23e263ad9b4395d89ff1"
daml-prim-DA-Exception-PreconditionFailed-4c290f7b9fe25f59a2d0ad07ee506d5e0a3ecfdb413e5c32e60dafa1052fdd6a "4c290f7b9fe25f59a2d0ad07ee506d5e0a3ecfdb413e5c32e60dafa1052fdd6a"
daml-prim-DA-Internal-Erased-a486f9d83acf91ddcb27a9a8743c042f310beab20be676cfc37220961df03900 "a486f9d83acf91ddcb27a9a8743c042f310beab20be676cfc37220961df03900"
daml-prim-DA-Internal-NatSyn-ce33df2997d69e8ac89f00951c322753e60abccdfdd92d47d804518a2029748f "ce33df2997d69e8ac89f00951c322753e60abccdfdd92d47d804518a2029748f"
daml-prim-DA-Internal-PromotedText-ad8708bc34bce0096a8f43500940f0d62fbf947aed8484efa92dc6ae2f9126ac "ad8708bc34bce0096a8f43500940f0d62fbf947aed8484efa92dc6ae2f9126ac"
daml-prim-DA-Types-202599a30d109125440918fdd6cd5f35c9e76175ef43fa5c9d6d9fd1eb7b66ff "202599a30d109125440918fdd6cd5f35c9e76175ef43fa5c9d6d9fd1eb7b66ff"
daml-prim-GHC-Prim-574f715baa8298bf09261ba87a77589f5aeef88e12b7b672cb80c4d2604035fa "574f715baa8298bf09261ba87a77589f5aeef88e12b7b672cb80c4d2604035fa"
daml-prim-GHC-Tuple-91c8a48444c590867fe71f4da1c3001516b45ecaf2fdad381ad60d179224cd9b "91c8a48444c590867fe71f4da1c3001516b45ecaf2fdad381ad60d179224cd9b"
daml-prim-GHC-Types-2f07eb3e4731beccfd88fcd19177268f120f9a2b06a52534ee808a4ba1e89720 "2f07eb3e4731beccfd88fcd19177268f120f9a2b06a52534ee808a4ba1e89720"
daml-stdlib-DA-Action-State-Type-d6da817f1d703324619eaae87be5a80c16eb0a1da0cdae48d1dd5111aee5be30 "d6da817f1d703324619eaae87be5a80c16eb0a1da0cdae48d1dd5111aee5be30"
daml-stdlib-DA-Date-Types-001109f95f991bea2ce8d641c2188d9f8c9d2909786549ba6d652024b3680e63 "001109f95f991bea2ce8d641c2188d9f8c9d2909786549ba6d652024b3680e63"
daml-stdlib-DA-Internal-Any-34b23589825aee625d6e4fb70d56404cfd999083a2ec4abf612757d48f7c14df "34b23589825aee625d6e4fb70d56404cfd999083a2ec4abf612757d48f7c14df"
daml-stdlib-DA-Internal-Down-dcbdacc42764d3887610f44ffe5134f09a500ffcfea5b0696a0693e82b104cda "dcbdacc42764d3887610f44ffe5134f09a500ffcfea5b0696a0693e82b104cda"
daml-stdlib-DA-Internal-Interface-AnyView-Types-5e93adc04d625458acbfc898bc92665d1d1947c10ff0a42661cd44b9f8b84d57 "5e93adc04d625458acbfc898bc92665d1d1947c10ff0a42661cd44b9f8b84d57"
daml-stdlib-DA-Internal-Template-ace2eb6a9cd13bca35ce7f068b942ab5c47987eed34efea52470b3aa0458a2f5 "ace2eb6a9cd13bca35ce7f068b942ab5c47987eed34efea52470b3aa0458a2f5"
daml-stdlib-DA-Logic-Types-3f28b0de0e1a9f71e5d8e1e6c2dc0625aefd32672fb29c9fdd8145a07bedd5f1 "3f28b0de0e1a9f71e5d8e1e6c2dc0625aefd32672fb29c9fdd8145a07bedd5f1"
daml-stdlib-DA-Monoid-Types-db242b5ad988328f01188f72988552a89bfbe630b8157e1e11f60758499a2ce5 "db242b5ad988328f01188f72988552a89bfbe630b8157e1e11f60758499a2ce5"
daml-stdlib-DA-NonEmpty-Types-a997b91f62098e8ed20659cc42265ece0c2edc3bde2fbc082bfb0a5bb6ead7e5 "a997b91f62098e8ed20659cc42265ece0c2edc3bde2fbc082bfb0a5bb6ead7e5"
daml-stdlib-DA-Random-Types-840302f70e3557d06c98e73282c84542b705a81d5fa4ba669b5750acae42d15d "840302f70e3557d06c98e73282c84542b705a81d5fa4ba669b5750acae42d15d"
daml-stdlib-DA-Semigroup-Types-1b912db5849106a731884f56cfdf8414a573a535ecc9422d95410e6b52aae93c "1b912db5849106a731884f56cfdf8414a573a535ecc9422d95410e6b52aae93c"
daml-stdlib-DA-Set-Types-eb6c6231bdfb99f78b09c2241af1774a96ee2d557f8269deba92c3ce6340e90e "eb6c6231bdfb99f78b09c2241af1774a96ee2d557f8269deba92c3ce6340e90e"
daml-stdlib-DA-Stack-Types-baf3f887f97419ebd2b87e25b6b7ab8b66145c9813ef15851249a9eff75efb79 "baf3f887f97419ebd2b87e25b6b7ab8b66145c9813ef15851249a9eff75efb79"
daml-stdlib-DA-Time-Types-f141230257fa9c6467b03e6ae3cc73a42ff1fdaf14ff172d91ec78cfeb181633 "f141230257fa9c6467b03e6ae3cc73a42ff1fdaf14ff172d91ec78cfeb181633"
daml-stdlib-DA-Validation-Types-4ad518a6e60589d5bd7f0e20196f89cb017b5e193160841ab36e39e9a35b3118 "4ad518a6e60589d5bd7f0e20196f89cb017b5e193160841ab36e39e9a35b3118"
EOF
)
