#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eou pipefail

if [ "$#" -ne 1 ]; then
    echo "Expected exactly one argument."
    echo "Usage: ./extract-codegen-cabal.sh TARBALL"
    exit 1
fi

TARBALL=$1
cd "$(dirname ${BASH_SOURCE[0]})"

WORK_DIR=$(mktemp -d)
trap "rm -rf $WORK_DIR" EXIT
mkdir -p $WORK_DIR/daml2ts-1.0.0
TARGET_DIR=$WORK_DIR/daml2ts-1.0.0

cat <<EOF > "$TARGET_DIR/daml2ts.cabal"
cabal-version: 2.4
name: daml2ts
version: 1.0.0

build-type: Simple

executable daml2ts
  default-language: Haskell2010
  build-depends:
    aeson,
    aeson-pretty,
    ansi-terminal,
    base,
    binary,
    blaze-html,
    bytestring,
    containers,
    cryptonite,
    Decimal,
    deepseq,
    directory,
    either,
    extra,
    filepath,
    ghc,
    hashable,
    lens,
    megaparsec,
    memory,
    mtl,
    optparse-applicative,
    pretty,
    proto3-suite,
    proto3-wire,
    recursion-schemes,
    safe-exceptions,
    semver,
    template-haskell,
    text,
    time,
    unordered-containers,
    utf8-string,
    vector,
    yaml,
    zip-archive
  main-is: TsCodeGenMain.hs
  ghc-options: -main-is TsCodeGenMain.main
  hs-source-dirs: src
  default-extensions:
    BangPatterns
    DeriveDataTypeable
    DeriveFoldable
    DeriveGeneric
    DeriveTraversable
    FlexibleContexts
    GeneralizedNewtypeDeriving
    LambdaCase
    NamedFieldPuns
    NondecreasingIndentation
    OverloadedStrings
    PackageImports
    RankNTypes
    RecordWildCards
    ScopedTypeVariables
    StandaloneDeriving
    TemplateHaskell
    TupleSections
    TypeApplications
    ViewPatterns
  other-modules:
    Com.Daml.DamlLfDev.DamlLf
    Com.Daml.DamlLfDev.DamlLf1
    Control.Lens.Ast
    Control.Lens.MonoTraversal
    DA.Daml.LF.Ast
    DA.Daml.LF.Ast.Base
    DA.Daml.LF.Ast.Numeric
    DA.Daml.LF.Ast.Optics
    DA.Daml.LF.Ast.Pretty
    DA.Daml.LF.Ast.Recursive
    DA.Daml.LF.Ast.TypeLevelNat
    DA.Daml.LF.Ast.Util
    DA.Daml.LF.Ast.Version
    DA.Daml.LF.Ast.World
    DA.Daml.LF.Mangling
    DA.Daml.LF.Proto3.Archive
    DA.Daml.LF.Proto3.Archive.Decode
    DA.Daml.LF.Proto3.Archive.Encode
    DA.Daml.LF.Proto3.Decode
    DA.Daml.LF.Proto3.DecodeV1
    DA.Daml.LF.Proto3.Encode
    DA.Daml.LF.Proto3.EncodeV1
    DA.Daml.LF.Proto3.Error
    DA.Daml.LF.Proto3.Util
    DA.Daml.LF.Reader
    DA.Daml.LF.TemplateOrInterface
    DA.Daml.Project.Consts
    DA.Daml.Project.Types
    DA.Daml.StablePackagesList
    DA.Pretty
    Data.NameMap
    Data.Text.Extended
    Data.Vector.Extended
    Orphans.Lib_pretty
    Text.PrettyPrint.Annotated.Extended
EOF

mkdir -p $TARGET_DIR/src
cp src/TsCodeGenMain.hs $TARGET_DIR/src/
DAML_REPO=../../..
cp -r $DAML_REPO/compiler/daml-lf-proto/src/* $TARGET_DIR/src/
cp -r $DAML_REPO/compiler/daml-lf-ast/src/* $TARGET_DIR/src/
cp -r $DAML_REPO/compiler/daml-lf-reader/src/* $TARGET_DIR/src/
cp -r $DAML_REPO/libs-haskell/da-hs-base/src/* $TARGET_DIR/src/
cp -r $DAML_REPO/daml-assistant/daml-project-config/DA $TARGET_DIR/src/
bazel build //language-support/ts/codegen/...
cp -r $DAML_REPO/bazel-bin/daml-lf/archive/Com/ $TARGET_DIR/src/
cp -r $DAML_REPO/bazel-bin/compiler/damlc/stable-packages/DA/ $TARGET_DIR/src/
chmod -R +w $TARGET_DIR/src/
sed -i 's/import Module/import GHC.Unit.Types/g' $TARGET_DIR/src/DA/Daml/LF/Ast/Util.hs

tar cfz $TARBALL -C $WORK_DIR daml2ts-1.0.0
