#!/usr/bin/env bash
# Copyright (c) 2019 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eou pipefail

if [ "$#" -ne 1 ]; then
    echo "Expected exactly one argument."
    echo "Usage: ./build_packages.sh TARGET_DIR"
    exit 1
fi

cd "$(dirname ${BASH_SOURCE[0]})"
TARGET_DIR=$1

BAZEL_BIN=$(bazel info bazel-bin)

pushd daml-assistant
cabal new-sdist
cp dist-newstyle/sdist/daml-project-config-0.1.0.tar.gz "$TARGET_DIR"
popd

pushd libs-haskell/da-hs-base
# removed the GCP logger from the exposed modules to avoid SdkVersion.hs
cabal new-sdist
cp dist-newstyle/sdist/da-hs-base-0.1.0.tar.gz "$TARGET_DIR"
popd

pushd compiler/daml-lf-ast
cabal new-sdist
cp dist-newstyle/sdist/daml-lf-ast-0.1.0.tar.gz "$TARGET_DIR"
popd

pushd compiler/daml-lf-proto
cabal new-sdist
cp dist-newstyle/sdist/daml-lf-proto-0.1.0.tar.gz "$TARGET_DIR"
popd

DIR=$(mktemp -d)
mkdir -p "$DIR/protobuf/com/daml"
cp -RL "daml-lf/archive/src/main/protobuf/com/daml/daml_lf_dev" "$DIR/protobuf/com/daml/"
# generate code from protobuf using the matching tool, as a configure script
cat <<EOF >"$DIR/configure"
mkdir src
for file in \$(ls protobuf/com/daml/daml_lf_dev/ | grep 'proto\$'); do
     echo \$file
     compile-proto-file --includeDir protobuf \
                        --out src \
                        --proto com/daml/daml_lf_dev/\$file
done
EOF
chmod +x "$DIR/configure"
cat <<EOF > "$DIR/daml-lf-proto-types.cabal"
cabal-version: 2.4
name: daml-lf-proto-types
build-type: Configure
version: 0.1.0

extra-source-files:
  configure
  protobuf/com/daml/daml_lf_dev/*.proto

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
    Com.Daml.DamlLfDev.DamlLf
    Com.Daml.DamlLfDev.DamlLf0
    Com.Daml.DamlLfDev.DamlLf1
  autogen-modules:
    Com.Daml.DamlLfDev.DamlLf
    Com.Daml.DamlLfDev.DamlLf0
    Com.Daml.DamlLfDev.DamlLf1
EOF
pushd "$DIR"
cabal new-sdist
cp dist-newstyle/sdist/daml-lf-proto-types-0.1.0.tar.gz "$TARGET_DIR"
popd
rm -rf "$DIR"

if [ ! -f "$TARGET_DIR/cabal.project" ]; then
    cat <<EOF > "$TARGET_DIR/cabal.project"
packages:
--   ./. -- add this if the top project is cabalised
  ./daml-project-config-0.1.0.tar.gz
  ./da-hs-base-0.1.0.tar.gz
  ./daml-lf-ast-0.1.0.tar.gz
  ./daml-lf-proto-0.1.0.tar.gz
  ./daml-lf-proto-types-0.1.0.tar.gz
EOF
    echo "Wrote $TARGET_DIR/cabal.project"
fi
