#!/usr/bin/env bash
# Copyright (c) 2019 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eou pipefail

if [ "$#" -ne 1 ]; then
    echo "Expected exactly one argument."
    echo "Usage: ./build_packages.sh TARGET_DIR"
    exit 1
fi

cd "$(dirname ${BASH_SOURCE[0]})/../../.."
TARGET_DIR=$1

BAZEL_BIN=$(bazel info bazel-bin)

pushd daml-assistant
cabal new-sdist
cp dist-newstyle/sdist/daml-project-config-0.1.0.tar.gz "$TARGET_DIR"
popd

pushd libs-haskell/da-hs-base
cabal new-sdist
cp dist-newstyle/sdist/da-hs-base-0.1.0.tar.gz "$TARGET_DIR"
popd


pushd nix/third-party/gRPC-haskell/core
cabal new-sdist
cp dist-newstyle/sdist/grpc-haskell-core-0.0.0.0.tar.gz "$TARGET_DIR"
popd

pushd nix/third-party/gRPC-haskell
cabal new-sdist
cp dist-newstyle/sdist/grpc-haskell-0.0.0.0.tar.gz "$TARGET_DIR"
popd

pushd compiler/daml-lf-ast
cabal new-sdist
cp dist-newstyle/sdist/daml-lf-ast-0.1.0.tar.gz "$TARGET_DIR"
popd

pushd compiler/daml-lf-proto
cabal new-sdist
cp dist-newstyle/sdist/daml-lf-proto-0.1.0.tar.gz "$TARGET_DIR"
popd

bazel build //daml-lf/archive:daml_lf_haskell_proto
DIR=$(mktemp -d)
mkdir -p "$DIR/src"
cp -RL "$BAZEL_BIN/daml-lf/archive/Da" "$DIR/src/Da"
cat <<EOF > "$DIR/daml-lf-proto-types.cabal"
cabal-version: 2.4
name: daml-lf-proto-types
build-type: Simple
version: 0.1.0

library
  default-language: Haskell2010
  hs-source-dirs: src
  build-depends:
    base,
    bytestring,
    containers,
    deepseq,
    proto3-suite,
    proto3-wire,
    text,
    vector,
  exposed-modules:
    Da.DamlLf
    Da.DamlLf0
    Da.DamlLf1
EOF
pushd "$DIR"
cabal new-sdist
cp dist-newstyle/sdist/daml-lf-proto-types-0.1.0.tar.gz "$TARGET_DIR"
popd
rm -rf "$DIR"

bazel build //ledger-api/grpc-definitions:ledger-api-haskellpb
DIR=$(mktemp -d)
mkdir -p "$DIR/src"
cp -RL "$BAZEL_BIN/ledger-api/grpc-definitions/Google" "$DIR/src/Google"
cp -RL "$BAZEL_BIN/ledger-api/grpc-definitions/Com" "$DIR/src/Com"
cat <<EOF > "$DIR/ledger-api-haskellpb.cabal"
cabal-version: 2.4
name: ledger-api-haskellpb
build-type: Simple
version: 0.1.0

library
  default-language: Haskell2010
  hs-source-dirs: src
  build-depends:
    base,
    bytestring,
    containers,
    deepseq,
    grpc-haskell,
    proto3-suite,
    proto3-wire,
    text,
    vector,
  exposed-modules:
    Com.Digitalasset.Ledger.Api.V1.ActiveContractsService
    Com.Digitalasset.Ledger.Api.V1.Admin.PackageManagementService
    Com.Digitalasset.Ledger.Api.V1.Admin.PartyManagementService
    Com.Digitalasset.Ledger.Api.V1.CommandCompletionService
    Com.Digitalasset.Ledger.Api.V1.CommandService
    Com.Digitalasset.Ledger.Api.V1.CommandSubmissionService
    Com.Digitalasset.Ledger.Api.V1.Commands
    Com.Digitalasset.Ledger.Api.V1.Completion
    Com.Digitalasset.Ledger.Api.V1.Event
    Com.Digitalasset.Ledger.Api.V1.LedgerConfigurationService
    Com.Digitalasset.Ledger.Api.V1.LedgerIdentityService
    Com.Digitalasset.Ledger.Api.V1.LedgerOffset
    Com.Digitalasset.Ledger.Api.V1.PackageService
    Com.Digitalasset.Ledger.Api.V1.Testing.ResetService
    Com.Digitalasset.Ledger.Api.V1.Testing.TimeService
    Com.Digitalasset.Ledger.Api.V1.TraceContext
    Com.Digitalasset.Ledger.Api.V1.Transaction
    Com.Digitalasset.Ledger.Api.V1.TransactionFilter
    Com.Digitalasset.Ledger.Api.V1.TransactionService
    Com.Digitalasset.Ledger.Api.V1.Value
    Google.Protobuf.Any
    Google.Protobuf.Duration
    Google.Protobuf.Empty
    Google.Protobuf.Timestamp
    Google.Protobuf.Wrappers
    Google.Rpc.Status
EOF
pushd "$DIR"
cabal new-sdist
cp dist-newstyle/sdist/ledger-api-haskellpb-0.1.0.tar.gz "$TARGET_DIR"
popd
rm -rf "$DIR"

pushd language-support/hs/bindings
cabal new-sdist
cp dist-newstyle/sdist/daml-ledger-0.1.0.tar.gz "$TARGET_DIR"
popd

if [ ! -f "$TARGET_DIR/cabal.project" ]; then
    cat <<EOF > "$TARGET_DIR/cabal.project"
packages:
  ./.
  ./daml-project-config-0.1.0.tar.gz
  ./daml-ledger-0.1.0.tar.gz
  ./da-hs-base-0.1.0.tar.gz
  ./grpc-haskell-0.0.0.0.tar.gz
  ./grpc-haskell-core-0.0.0.0.tar.gz
  ./ledger-api-haskellpb-0.1.0.tar.gz
  ./daml-lf-ast-0.1.0.tar.gz
  ./daml-lf-proto-0.1.0.tar.gz
  ./daml-lf-proto-types-0.1.0.tar.gz

-- package grpc-haskell-core
--   extra-lib-dirs: /usr/local/grpc/lib
--   extra-include-dirs: /usr/local/grpc/include
EOF
    echo "Wrote $TARGET_DIR/cabal.project"
fi

