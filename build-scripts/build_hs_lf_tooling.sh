#!/usr/bin/env bash
# Copyright (c) 2019 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eou pipefail

if [ "$#" -ne 1 ]; then
    echo "Expected exactly one argument."
    echo "Usage: ./build_packages.sh TARGET_DIR"
    exit 1
fi

TARGET_DIR=$PWD/$1
cd "$(dirname ${BASH_SOURCE[0]})/.."

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
cat <<EOF >"$DIR/Setup.hs"
-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
--
-- Parts of this code were adapted from
--     https://hackage.haskell.org/package/proto-lens-setup-0.4.0.4
-- which is (c) 2016 Google Inc. with a BSD-style license that can be found at
-- https://developers.google.com/open-source/licenses/bsd

{-# Language RecordWildCards #-}
module Main where

import Distribution.Simple
import Distribution.Simple.BuildPaths (autogenPackageModulesDir)
import Distribution.Simple.LocalBuildInfo
import Distribution.Simple.Utils
    ( createDirectoryIfMissingVerbose
    , installOrdinaryFile
    )
import Distribution.PackageDescription

import System.FilePath -- filepath package

import System.Directory
import System.Process

main :: IO ()
main = defaultMainWithHooks $ generateHooks
  where
    generateHooks :: UserHooks
    generateHooks =
      simple { buildHook   = \p l h f -> generate l >> buildHook simple p l h f
             , haddockHook = \p l h f -> generate l >> haddockHook simple p l h f
             , replHook    = \p l h f args -> generate l >> replHook simple p l h f args
             }

    simple = simpleUserHooks

    generate :: LocalBuildInfo -> IO ()
    generate locInfo =
      generateFromProtos locInfo "protobuf" (autogenPackageModulesDir locInfo)

-- generates Haskell from all proto files from extra-source-files which are
-- under a given srcDir path (used as include directory)
generateFromProtos :: LocalBuildInfo -> FilePath -> FilePath -> IO ()
generateFromProtos locInfo srcDir outDir = do
  let protos = collectProtos srcDir
  mapM_ (generateProto srcDir outDir) protos
    where
      collectProtos :: FilePath -> [FilePath]
      collectProtos path =
        map (makeRelative path)
          . filter ((== ".proto") . takeExtension)
          . filter (isInside path) $
          (extraSrcFiles $ localPkgDescr locInfo)

      isInside :: FilePath -> FilePath -> Bool
      isInside path file = isRelative file &&
                           equalFilePath file (path </> makeRelative path file)

-- | runs @compile-proto-file@ for the given path @proto@, assumed relative to
-- @imports@, output in @output@ path.
generateProto :: FilePath -> FilePath -> FilePath -> IO ()
generateProto imports output proto = do
  putStrLn $ "generate from proto file " ++ proto
  createDirectoryIfMissing True output
  callProcess "compile-proto-file"
    ["--includeDir", imports, "--out", output, "--proto", proto]
EOF
cat <<EOF > "$DIR/daml-lf-proto-types.cabal"
cabal-version: 2.4
name: daml-lf-proto-types
version: 0.1.0

extra-source-files:
  protobuf/com/daml/daml_lf_dev/daml_lf.proto
  protobuf/com/daml/daml_lf_dev/daml_lf_1.proto
build-type: Custom
custom-setup
  setup-depends:
    base,
    Cabal,
    directory,
    filepath,
    process

library
  default-language: Haskell2010
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
    Com.Daml.DamlLfDev.DamlLf1
  autogen-modules:
    Com.Daml.DamlLfDev.DamlLf
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
