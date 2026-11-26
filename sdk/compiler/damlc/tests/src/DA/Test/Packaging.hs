-- Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DA.Test.Packaging (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import qualified "zip-archive" Codec.Archive.Zip as Zip
import Control.Monad.Extra
import Control.Exception.Safe
import DA.Bazel.Runfiles
import DA.Cli.Damlc.Packaging (BuildLfPackageGraphArgs' (..), BuildLfPackageGraphMetaArgs (..), buildLfPackageGraph')
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Reader (readDalfManifest, readDalfs, packageName, Dalfs(..), DalfManifest(DalfManifest), mainDalfPath, dalfPaths)
import qualified DA.Daml.LF.Proto3.Archive as LFArchive
import DA.Test.Process
import DA.Test.Util
import Data.Conduit.Tar.Extra (dropDirectory1)
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSL.Char8
import Data.Foldable (asum)
import qualified Data.Graph as Graph (path)
import Data.List.Extra
import Data.Maybe
import qualified Data.Set as Set
import qualified Data.Text as Text
import System.Directory.Extra
import System.Environment.Blank
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit
import Test.Tasty.QuickCheck

import "ghc-lib-parser" Module (stringToUnitId)

import ComponentVersion (ComponentVersioned, componentVersionString, withComponentVersions)

main :: IO ()
main = withComponentVersions $ do
    setEnv "TASTY_NUM_THREADS" "1" True
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    defaultMain $ tests Tools{..}

data Tools = Tools -- and places
  { damlc :: FilePath
  }

tests :: ComponentVersioned => Tools -> TestTree
tests Tools{damlc} = testGroup "Packaging" $
    [ testCaseSteps "Five layer-deep expression-only dependency tree" $ \step -> withTempDir $ \tmpDir -> do
        -- This test is for https://github.com/digital-asset/daml/issues/20939
        let package :: Integer -> String
            package i = tmpDir </> ("layer" <> show i)
        let mkAndAssertLayer :: Integer -> [String] -> IO ()
            mkAndAssertLayer i lines = do
              step $ "Generating source for layer" <> show i
              createDirectoryIfMissing True (package i)
              writeFile (package i </> "daml.yaml") $ unlines
                  [ "sdk-version: " <> componentVersionString
                  , "name: layer" <> show i
                  , "version: 1.0.0"
                  , "source: Layer" <> show i <> ".daml"
                  , "dependencies:"
                  , "  - daml-prim"
                  , "  - daml-stdlib"
                  , "data-dependencies:"
                  , if i <= 1
                      then ""
                      else "  - ../layer" <> show (i - 1) <> "/.daml/dist/layer" <> show (i - 1) <> "-1.0.0.dar"
                  ]

              writeFile (package i </> ("Layer" <> show i <> ".daml")) $ unlines $ ("module Layer" <> show i <> " where") : lines

              step $ "Building DAR for layer" <> show i
              buildPackage (package i)

              output <- inspectDar (package i) (".daml/dist/layer" <> show i <> "-1.0.0.dar")
              forM_ [1..i-1] $ \depI -> do
                step $ "Checking that DAR for layer" <> show i <> " depends on DALF for layer" <> show depI
                assertInfixOf ("\nlayer" <> show depI) output

        mkAndAssertLayer 1
            [ "layer1 : Text -> Text"
            , "layer1 t = \"layer1 \" <> t"
            ]

        mkAndAssertLayer 2
            [ "import Layer1"
            , "layer2 : Text -> Text"
            , "layer2 t = \"layer2 \" <> layer1 t"
            ]

        mkAndAssertLayer 3
            [ "import Layer2"
            , "layer3 : Text -> Text"
            , "layer3 t = \"layer3 \" <> layer2 t"
            ]

        mkAndAssertLayer 4
            [ "import Layer3"
            , "layer4 : Text -> Text"
            , "layer4 t = \"layer4 \" <> layer3 t"
            ]

        mkAndAssertLayer 5
            [ "import Layer4"
            , "layer5 : Text -> Text"
            , "layer5 t = \"layer5 \" <> layer4 t"
            ]
    , testCaseSteps "Build package with dependency" $ \step -> withTempDir $ \tmpDir -> do
        let packageA = tmpDir </> "a"
        let packageB = tmpDir </> "b"
        let aDar = packageA </> ".daml" </> "dist" </> "a-1.0.dar"
        let bDar = packageB </> ".daml" </> "dist" </> "b-1.0.dar"
        step "Creating package a..."
        createDirectoryIfMissing True (packageA </> "daml" </> "Foo" </> "Bar")
        writeFileUTF8 (packageA </> "daml" </> "A.daml") $ unlines
            [ "module A (a) where"
            , "a : ()"
            , "a = ()"
            ]
        writeFileUTF8 (packageA </> "daml" </> "Foo" </> "Bar" </> "Baz.daml") $ unlines
            [ "module Foo.Bar.Baz (c) where"
            , "import A (a)"
            , "c : ()"
            , "c = a"
            ]
        writeFileUTF8 (packageA </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: a"
            , "version: \"1.0\""
            , "source: daml"
            , "exposed-modules: [A, Foo.Bar.Baz]"
            , "dependencies:"
            , "  - daml-prim"
            , "  - daml-stdlib"
            ]
        buildPackage packageA
        assertFileExists aDar
        step "Creating package b..."
        createDirectoryIfMissing True (packageB </> "daml")
        writeFileUTF8 (packageB </> "daml" </> "B.daml") $ unlines
            [ "module B where"
            , "import C"
            , "import Foo.Bar.Baz"
            , "b : ()"
            , "b = a"
            , "d : ()"
            , "d = c"
            ]
        writeFileUTF8 (packageB </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "version: \"1.0\""
            , "name: b"
            , "source: daml"
            , "exposed-modules: [B]"
            , "dependencies:"
            , "  - daml-prim"
            , "  - daml-stdlib"
            , "  - " <> aDar
            , "build-options: ['--package', 'a-1.0 with (A as C)']"
            ]
            -- the last option checks that module aliases work and modules imported without aliases
            -- are still exposed.
        buildPackage packageB
        assertFileExists bDar
    , testCaseSteps "Dependency on a package with source: A.daml" $ \step -> withTempDir $ \tmpDir -> do
        let packageA = tmpDir </> "a"
        let packageB = tmpDir </> "b"
        let aDar = packageA </> ".daml" </> "dist" </> "a-1.0.dar"
        let bDar = packageB </> ".daml" </> "dist" </> "b-1.0.dar"
        step "Creating package a..."
        createDirectoryIfMissing True packageA
        writeFileUTF8 (packageA </> "A.daml") $ unlines
            [ "module A () where"
            ]
        writeFileUTF8 (packageA </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: a"
            , "version: \"1.0\""
            , "source: A.daml"
            , "dependencies:"
            , "  - daml-prim"
            , "  - daml-stdlib"
            ]
        buildPackage packageA
        assertFileExists aDar
        step "Creating package b..."
        createDirectoryIfMissing True packageB
        writeFileUTF8 (packageB </> "B.daml") $ unlines
            [ "module B where"
            , "import A ()"
            ]
        writeFileUTF8 (packageB </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "version: \"1.0\""
            , "name: b"
            , "source: ."
            , "dependencies:"
            , "  - daml-prim"
            , "  - daml-stdlib"
            , "  - " <> aDar
            ]
        buildPackage packageB
        assertFileExists bDar
        darFiles <- Zip.filesInArchive . Zip.toArchive <$> BSL.readFile bDar
        assertBool "b.dar contains source file from package database" $
            not $ any ("A.daml" `isSuffixOf`) darFiles
    , testCase "Top-level source files" $ withTempDir $ \tmpDir -> do
        -- Test that a source file in the package root will be included in the
        -- DAR file. Regression test for #1048.
        let projDir = tmpDir </> "proj"
        createDirectoryIfMissing True projDir
        writeFileUTF8 (projDir </> "A.daml") $ unlines
          [ "module A (a) where"
          , "a : ()"
          , "a = ()"
          ]
        writeFileUTF8 (projDir </> "daml.yaml") $ unlines
          [ "sdk-version: " <> componentVersionString
          , "name: proj"
          , "version: \"1.0\""
          , "source: ."
          , "exposed-modules: [A]"
          , "dependencies:"
          , "  - daml-prim"
          , "  - daml-stdlib"
          ]
        buildPackage projDir
        let dar = projDir </> ".daml" </> "dist" </> "proj-1.0.dar"
        assertFileExists dar
        darFiles <- Zip.filesInArchive . Zip.toArchive <$> BSL.readFile dar
        assertBool "A.daml is missing" (any (\f -> takeFileName f == "A.daml") darFiles)

    , testCase "Check that DAR manifest prefers DAML_SDK_VERSION over daml.yaml sdk-version." $ withTempDir $ \tmpDir -> do
        -- Regression test for bug fixed in #3587.
        let projDir = tmpDir </> "proj"
        createDirectoryIfMissing True projDir
        writeFileUTF8 (projDir </> "A.daml") $ unlines
          [ "module A (a) where"
          , "a : ()"
          , "a = ()"
          ]
        writeFileUTF8 (projDir </> "daml.yaml") $ unlines
          [ "sdk-version: 0.0.0-a-bad-sdk-version"
          , "name: proj"
          , "version: \"1.0\""
          , "source: ."
          , "exposed-modules: [A]"
          , "dependencies:"
          , "  - daml-prim"
          , "  - daml-stdlib"
          ]

        bracket
            (setEnv "DAML_SDK_VERSION" componentVersionString True)
            (\ _ -> unsetEnv "DAML_SDK_VERSION")
            (\ _ -> buildPackage projDir)

        let dar = projDir </> ".daml" </> "dist" </> "proj-1.0.dar"
        assertFileExists dar
        archive <- Zip.toArchive <$> BSL.readFile dar
        Just entry <- pure $ Zip.findEntryByPath "META-INF/MANIFEST.MF" archive
        let lines = BSL.Char8.lines (Zip.fromEntry entry)
            expectedLine = "Sdk-Version: " <> BSL.Char8.pack componentVersionString
        assertBool "META-INF/MANIFEST.MF picked up the wrong sdk version" (expectedLine `elem` lines)

    , testCase "Non-root sources files" $ withTempDir $ \projDir -> do
        -- Test that all daml source files get included in the dar if "source" points to a file
        -- rather than a directory
        writeFileUTF8 (projDir </> "A.daml") $ unlines
          [ "module A where"
          , "import B ()"
          ]
        writeFileUTF8 (projDir </> "B.daml") $ unlines
          [ "module B where"
          ]
        writeFileUTF8 (projDir </> "daml.yaml") $ unlines
          [ "sdk-version: " <> componentVersionString
          , "name: proj"
          , "version: 0.1.0"
          , "source: A.daml"
          , "dependencies: [daml-prim, daml-stdlib]"
          ]
        buildPackage projDir
        let dar = projDir </> ".daml/dist/proj-0.1.0.dar"
        assertFileExists dar
        darFiles <- Zip.filesInArchive . Zip.toArchive <$> BSL.readFile dar
        forM_ ["A.daml", "A.hi", "A.hie", "B.daml", "B.hi", "B.hie"] $ checkDarFile darFiles "."
    , testCase "Root source file in subdir" $ withTempDir $ \projDir -> do
        -- Test that the daml source files get included properly if "source" points to a file
        -- in a subdirectory.
        createDirectoryIfMissing True (projDir </> "A")
        createDirectoryIfMissing True (projDir </> "B")
        writeFileUTF8 (projDir </> "A/B.daml") $ unlines
          [ "module A.B where"
          , "import B.C ()"
          ]
        writeFileUTF8 (projDir </> "B/C.daml") $ unlines
          [ "module B.C where"
          ]
        writeFileUTF8 (projDir </> "daml.yaml") $ unlines
          [ "sdk-version: " <> componentVersionString
          , "name: proj"
          , "version: 0.1.0"
          , "source: A/B.daml"
          , "dependencies: [daml-prim, daml-stdlib]"
          ]
        buildPackage projDir
        let dar = projDir </> ".daml/dist/proj-0.1.0.dar"
        assertFileExists dar
        darFiles <- Zip.filesInArchive . Zip.toArchive <$> BSL.readFile dar
        checkDarFile darFiles "A" "B.daml"
        checkDarFile darFiles "A" "B.hi"
        checkDarFile darFiles "A" "B.hie"
        checkDarFile darFiles "B" "C.daml"
        checkDarFile darFiles "B" "C.hi"
        checkDarFile darFiles "B" "C.hie"

    , testCase "Dalf dependencies get package id suffices" $ withTempDir $ \projDir -> do
        createDirectoryIfMissing True (projDir </> "daml")
        writeFileUTF8 (projDir </> "daml/A.daml") $ unlines
          [ "module A where"
          , "data A = A ()"
          ]
        writeFileUTF8 (projDir </> "daml.yaml") $ unlines
          [ "sdk-version: " <> componentVersionString
          , "name: proj"
          , "version: 0.1.0"
          , "source: daml"
          , "dependencies: [daml-prim, daml-stdlib]"
          ]
        buildPackage projDir
        let dar = projDir </> ".daml/dist/proj-0.1.0.dar"
        assertFileExists dar
        darFiles <- Zip.filesInArchive . Zip.toArchive <$> BSL.readFile dar
        let allDalfFilesHavePkgId = and $ do
              fp <- darFiles
              guard $ "dalf" `isExtensionOf` fp
              let (_s, pId) = fromMaybe ("", "not a package id") $ stripInfixEnd "-" $ takeBaseName fp
              pure $ all (`elem` ['a' .. 'f'] ++ ['0' .. '9']) pId
        assertBool "Dalf files without package ids" allDalfFilesHavePkgId

    , testCase "Imports from different directories" $ withTempDir $ \projDir -> do
        -- Regression test for #2929
        createDirectory (projDir </> "A")
        writeFileUTF8 (projDir </> "A.daml") $ unlines
          [ "module A where"
          , "import A.B ()"
          , "import A.C ()"
          ]
        writeFileUTF8 (projDir </> "A/B.daml") $ unlines
          [ "module A.B where"
          , "import A.C ()"
          ]
        writeFileUTF8 (projDir </> "A/C.daml") $ unlines
          [ "module A.C where"
          ]
        writeFileUTF8 (projDir </> "daml.yaml") $ unlines
          [ "sdk-version: " <> componentVersionString
          , "name: proj"
          , "version: 0.1.0"
          , "source: ."
          , "dependencies: [daml-prim, daml-stdlib]"
          ]
        buildPackage projDir

    , testCase "Package without exposed modules" $ withTempDir $ \projDir -> do
        writeFileUTF8 (projDir </> "A.daml") $ unlines
            [ "module A (a) where"
            , "a : ()"
            , "a = ()"
            ]
        writeFileUTF8 (projDir </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: proj"
            , "version: \"1.0\""
            , "source: A.daml"
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
        buildPackage projDir

    , testCase "Empty package" $ withTempDir $ \projDir -> do
        writeFileUTF8 (projDir </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: proj"
            , "version: 0.0.1"
            , "source: src"
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
        createDirectoryIfMissing True (projDir </> "src")
        buildPackage projDir

    , testCase "Package-wide name collision" $ withTempDir $ \projDir -> do
        createDirectoryIfMissing True (projDir </> "src")
        createDirectoryIfMissing True (projDir </> "src" </> "A")
        writeFileUTF8 (projDir </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: proj"
            , "version: 0.0.1"
            , "source: src"
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
        writeFileUTF8 (projDir </> "src" </> "A.daml") $ unlines
            [ "module A where"
            , "data B = B Int"
            ]
        writeFileUTF8 (projDir </> "src" </> "A" </> "B.daml") $ unlines
            [ "module A.B where"
            , "data C = C Int"
            ]
        buildPackageError projDir "" "name collision"

    , testCase "Virtual module name collision" $ withTempDir $ \projDir -> do
        createDirectoryIfMissing True (projDir </> "src" </> "A" </> "B")
        writeFileUTF8 (projDir </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: proj"
            , "version: 0.0.1"
            , "source: src"
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
        writeFileUTF8 (projDir </> "src" </> "A.daml") $ unlines
            [ "module A where"
            , "data B = B Int"
            ]
        writeFileUTF8 (projDir </> "src" </> "A" </> "B" </> "C.daml") $ unlines
            [ "module A.B.C where"
            , "data C = C Int"
            ]
        (exitCode, out, err) <- readProcessWithExitCode damlc ["build", "--package-root", projDir] ""
        out @?= ""
        assertInfixOf "Running single package build of proj as no multi-package.yaml was found.\n" err
        assertInfixOf "collision between module prefix A.B (from A.B.C) and variant A:B" err
        exitCode @?= ExitFailure 1

    , testCase "Manifest name" $ withTempDir $ \projDir -> do
          createDirectoryIfMissing True (projDir </> "src")
          writeFileUTF8 (projDir </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: foobar"
            , "version: 0.0.1"
            , "source: src"
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
          withCurrentDirectory projDir $ callProcessSilent damlc ["build", "-o", "baz.dar"]
          Right manifest <- readDalfManifest . Zip.toArchive  <$> BSL.readFile (projDir </> "baz.dar")
          -- Verify that the name in the manifest is independent of the DAR name.
          packageName manifest @?= Just "foobar-0.0.1"

    , testCase "Package metadata - no files" $ withTempDir $ \projDir -> do
          -- The no files case is somewhat special since it relies on the default metadata
          -- set in mergePkgs.
          createDirectoryIfMissing True projDir
          writeFileUTF8 (projDir </> "daml.yaml") $ unlines
              [ "sdk-version: " <> componentVersionString
              , "name: foobar"
              , "version: 1.2.3"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib]"
              ]
          withCurrentDirectory projDir $ callProcessSilent damlc ["build", "-o", "foobar.dar", "--target=2.dev"]
          Right Dalfs{..} <- readDalfs . Zip.toArchive <$> BSL.readFile (projDir </> "foobar.dar")
          (_pkgId, pkg) <- either (fail . show) pure (LFArchive.decodeArchive LFArchive.DecodeAsMain (BSL.toStrict mainDalf))
          LF.packageMetadata pkg @?= LF.PackageMetadata (LF.PackageName "foobar") (LF.PackageVersion "1.2.3") Nothing

    , testCase "Package metadata - single file" $ withTempDir $ \projDir -> do
          createDirectoryIfMissing True projDir
          writeFileUTF8 (projDir </> "daml.yaml") $ unlines
              [ "sdk-version: " <> componentVersionString
              , "name: foobar"
              , "version: 1.2.3"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib]"
              ]
          writeFileUTF8 (projDir </> "A.daml") $ unlines
              [ "module A where"
              ]
          withCurrentDirectory projDir $ callProcessSilent damlc ["build", "-o", "foobar.dar", "--target=2.dev"]
          Right Dalfs{..} <- readDalfs . Zip.toArchive <$> BSL.readFile (projDir </> "foobar.dar")
          (_pkgId, pkg) <- either (fail . show) pure (LFArchive.decodeArchive LFArchive.DecodeAsMain (BSL.toStrict mainDalf))
          LF.packageMetadata pkg @?= LF.PackageMetadata (LF.PackageName "foobar") (LF.PackageVersion "1.2.3") Nothing

    , testCase "Transitive package deps" $ withTempDir $ \projDir -> do
          -- Check that the depends field in the package config files does not depend on the name of the DAR.
          let projA = projDir </> "a"
          let projB = projDir </> "b"
          let projC = projDir </> "c"

          createDirectoryIfMissing True (projA </> "src")
          writeFileUTF8 (projA </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: a"
            , "version: 0.0.1"
            , "source: src"
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
          writeFileUTF8 (projA </> "src" </> "A.daml") $ unlines
            [ "module A where"
            ]
          withCurrentDirectory projA $ callProcessSilent damlc ["build", "-o", "foo.dar"]

          createDirectoryIfMissing True (projB </> "src")
          writeFileUTF8 (projB </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: b"
            , "version: 0.0.1"
            , "source: src"
            , "dependencies:"
            , " - daml-prim"
            , " - daml-stdlib"
            , " - " <> projA </> "foo.dar"
            ]
          writeFileUTF8 (projB </> "src" </> "B.daml") $ unlines
            [ "module B where"
            , "import A"
            ]
          withCurrentDirectory projB $ callProcessSilent damlc ["build", "-o", "bar.dar"]

          createDirectoryIfMissing True (projC </> "src")
          writeFileUTF8 (projC </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: c"
            , "version: 0.0.1"
            , "source: src"
            , "dependencies:"
            , " - daml-prim"
            , " - daml-stdlib"
            , " - " <> projA </> "foo.dar"
            , " - " <> projB </> "bar.dar"
            ]
          writeFileUTF8 (projC </> "src" </> "C.daml") $ unlines
            [ "module C where"
            , "import A"
            , "import B"
            ]
          withCurrentDirectory projC $ callProcessSilent damlc ["build", "-o", "baz.dar"]
    , testCase "Detects unitId collisions in dependencies" $ withTempDir $ \projDir -> do
          -- Check that two pacages with the same unit id is flagged as an error.
          let projA = projDir </> "a"
          let projB = projDir </> "b"
          let projC = projDir </> "c"

          createDirectoryIfMissing True (projA </> "src")
          createDirectoryIfMissing True (projB </> "src")
          createDirectoryIfMissing True (projC </> "src")

          writeFileUTF8 (projA </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: a"
            , "version: 0.0.1"
            , "source: src"
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
          writeFileUTF8 (projA </> "src" </> "A.daml") $ unlines
            [ "module A where"
            , "foo : Int"
            , "foo = 10"
            ]
          withCurrentDirectory projA $ callProcessSilent damlc ["build", "-o", "a.dar"]
          packageIdA1 <- head <$> darPackageIds (projA </> "a.dar")

          writeFileUTF8 (projB </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: b"
            , "version: 0.0.1"
            , "source: src"
            , "dependencies:"
            , " - daml-prim"
            , " - daml-stdlib"
            , " - " <> projA </> "a.dar"
            ]
          writeFileUTF8 (projB </> "src" </> "B.daml") $ unlines
            [ "module B where"
            , "import A ()"
            ]
          withCurrentDirectory projB $ callProcessSilent damlc ["build", "-o", "b.dar"]

          writeFileUTF8 (projA </> "src" </> "A.daml") $ unlines
            [ "module A where"
            , "foo : Int"
            , "foo = 20"
            ]
          withCurrentDirectory projA $ callProcessSilent damlc ["build", "-o", "a.dar"]
          packageIdA2 <- head <$> darPackageIds (projA </> "a.dar")
          assertBool "Expected two different package IDs" (packageIdA1 /= packageIdA2)

          writeFileUTF8 (projC </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: c"
            , "version: 0.0.1"
            , "source: src"
            , "dependencies:"
            , " - daml-prim"
            , " - daml-stdlib"
            , " - " <> projA </> "a.dar"
            , " - " <> projB </> "b.dar"
            ]
          writeFileUTF8 (projC </> "src" </> "C.daml") $ unlines
            [ "module C where"
            , "import A ()"
            , "import B ()"
            ]
          buildPackageError projC "" "dependencies with same unit id but conflicting package ids: a-0.0.1"

    , testCase "Detects unitId collisions in data-dependencies" $ withTempDir $ \projDir -> do
          -- Check that two pacages with the same unit id is flagged as an error.
          let projA = projDir </> "a"
          let projB = projDir </> "b"
          let projC = projDir </> "c"

          createDirectoryIfMissing True (projA </> "src")
          createDirectoryIfMissing True (projB </> "src")
          createDirectoryIfMissing True (projC </> "src")

          writeFileUTF8 (projA </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: a"
            , "version: 0.0.1"
            , "source: src"
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
          writeFileUTF8 (projA </> "src" </> "A.daml") $ unlines
            [ "module A where"
            , "foo : Int"
            , "foo = 10"
            ]
          withCurrentDirectory projA $ callProcessSilent damlc ["build", "-o", "a.dar"]
          packageIdA1 <- head <$> darPackageIds (projA </> "a.dar")

          writeFileUTF8 (projB </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: b"
            , "version: 0.0.1"
            , "source: src"
            , "dependencies:"
            , " - daml-prim"
            , " - daml-stdlib"
            , "data-dependencies:"
            , " - " <> projA </> "a.dar"
            ]
          writeFileUTF8 (projB </> "src" </> "B.daml") $ unlines
            [ "module B where"
            , "import A ()"
            ]
          withCurrentDirectory projB $ callProcessSilent damlc ["build", "-o", "b.dar"]

          writeFileUTF8 (projA </> "src" </> "A.daml") $ unlines
            [ "module A where"
            , "foo : Int"
            , "foo = 20"
            ]
          withCurrentDirectory projA $ callProcessSilent damlc ["build", "-o", "a.dar"]
          packageIdA2 <- head <$> darPackageIds (projA </> "a.dar")
          assertBool "Expected two different package IDs" (packageIdA1 /= packageIdA2)

          writeFileUTF8 (projC </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: c"
            , "version: 0.0.1"
            , "source: src"
            , "dependencies:"
            , " - daml-prim"
            , " - daml-stdlib"
            , "data-dependencies:"
            , " - " <> projA </> "a.dar"
            , " - " <> projB </> "b.dar"
            ]
          writeFileUTF8 (projC </> "src" </> "C.daml") $ unlines
            [ "module C where"
            , "import A ()"
            , "import B ()"
            ]
          buildPackageError projC "" "dependencies with same unit id but conflicting package ids: a-0.0.1"

    , testCaseSteps "Error on newer LF data-dependency" $ \step -> withTempDir $ \tmpDir -> do
          step "Building 'a"
          createDirectoryIfMissing True (tmpDir </> "a")
          writeFileUTF8 (tmpDir </> "a" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> componentVersionString
              , "version: 0.0.1"
              , "name: a"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib]"
              , "build-options: [--target=2.dev]"
              ]
          writeFileUTF8 (tmpDir </> "a" </> "A.daml") $ unlines
              [ "module A where"
              ]
          withCurrentDirectory (tmpDir </> "a") $ callProcessSilent damlc ["build", "-o", tmpDir </> "a" </> "a.dar"]

          step "Building b"
          createDirectoryIfMissing True (tmpDir </> "b")
          writeFileUTF8 (tmpDir </> "b" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> componentVersionString
              , "version: 0.0.1"
              , "name: b"
              , "source: ."
              , "dependencies:"
              , "  - daml-prim"
              , "  - daml-stdlib"
              , "data-dependencies:"
              , "  - " <> show (tmpDir </> "a" </> "a.dar")
              , "build-options: [--target=2.1]"
              ]
          writeFileUTF8 (tmpDir </> "b" </> "B.daml") $ unlines
              [ "module B where"
              , "import A ()"
              ]
          buildPackageError (tmpDir </> "b") "" "Targeted LF version 2.1 but dependencies have incompatible LF versions"

    , testCaseSteps "Error on newer LF dependency" $ \step -> withTempDir $ \tmpDir -> do
          step "Building 'a"
          createDirectoryIfMissing True (tmpDir </> "a")
          writeFileUTF8 (tmpDir </> "a" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> componentVersionString
              , "version: 0.0.1"
              , "name: a"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib]"
              , "build-options: [--target=2.dev]"
              ]
          writeFileUTF8 (tmpDir </> "a" </> "A.daml") $ unlines
              [ "module A where"
              ]
          withCurrentDirectory (tmpDir </> "a") $ callProcessSilent damlc ["build", "-o", tmpDir </> "a" </> "a.dar"]

          step "Building b"
          createDirectoryIfMissing True (tmpDir </> "b")
          writeFileUTF8 (tmpDir </> "b" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> componentVersionString
              , "version: 0.0.1"
              , "name: b"
              , "source: ."
              , "dependencies:"
              , "  - daml-prim"
              , "  - daml-stdlib"
              , "  - " <> show (tmpDir </> "a" </> "a.dar")
              , "build-options: [--target=2.1]"
              ]
          writeFileUTF8 (tmpDir </> "b" </> "B.daml") $ unlines
              [ "module B where"
              , "import A ()"
              ]
          buildPackageError (tmpDir </> "b") "" "Targeted LF version 2.1 but dependencies have different LF versions"

    , testCaseSteps "Error on inconsistent LF dependency" $ \step -> withTempDir $ \tmpDir -> do
          step "Building 'a"
          createDirectoryIfMissing True (tmpDir </> "a")
          writeFileUTF8 (tmpDir </> "a" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> componentVersionString
              , "version: 0.0.1"
              , "name: a"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib]"
              , "build-options: [--target=2.1]"
              ]
          writeFileUTF8 (tmpDir </> "a" </> "A.daml") $ unlines
              [ "module A where"
              ]
          withCurrentDirectory (tmpDir </> "a") $ callProcessSilent damlc ["build", "-o", tmpDir </> "a" </> "a.dar"]

          step "Building b"
          createDirectoryIfMissing True (tmpDir </> "b")
          writeFileUTF8 (tmpDir </> "b" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> componentVersionString
              , "version: 0.0.1"
              , "name: b"
              , "source: ."
              , "dependencies:"
              , "  - daml-prim"
              , "  - daml-stdlib"
              , "  - " <> show (tmpDir </> "a" </> "a.dar")
              , "build-options: [--target=2.dev]"
              ]
          writeFileUTF8 (tmpDir </> "b" </> "B.daml") $ unlines
              [ "module B where"
              , "import A ()"
              ]
          buildPackageError (tmpDir </> "b") "" "Targeted LF version 2.dev but dependencies have different LF versions"

    , testCase "build-options + package-root" $ withTempDir $ \projDir -> do
          createDirectoryIfMissing True (projDir </> "src")
          writeFileUTF8 (projDir </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: a"
            , "version: 0.0.1"
            , "source: src"
            , "dependencies: [daml-prim, daml-stdlib]"
            , "build-options: [\"--ghc-option=-Werror\"]"
            ]
          writeFileUTF8 (projDir </> "src" </> "A.daml") $ unlines
            [ "module A where"
            , "f : Optional a -> a"
            , "f (Some a) = a"
            ]
          (exitCode, _, stderr) <- readProcessWithExitCode damlc ["build", "--package-root", projDir] ""
          exitCode @?= ExitFailure 1
          assertBool ("Expected \"non-exhaustive\" error in stderr but got: " <> show stderr) ("non-exhaustive" `isInfixOf` stderr)

    , testCaseSteps "data-dependencies + exposed-modules" $ \step -> withTempDir $ \projDir -> do
        -- Since the order in which dependencies are processed depends on their PackageIds,
        -- which in turn depends on their contents, we also test buildLfPackageGraph' directly,
        -- in the property-based test below.
          step "Building dependency"
          createDirectoryIfMissing True (projDir </> "dependency")
          writeFileUTF8 (projDir </> "dependency" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: dependency"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib]"
            , "exposed-modules: [B]"
            ]
          writeFileUTF8 (projDir </> "dependency" </> "A.daml") $ unlines
            [ "module A where"
            ]
          writeFileUTF8 (projDir </> "dependency" </> "B.daml") $ unlines
            [ "module B where"
            , "class C a where f : a"
            ]
          withCurrentDirectory (projDir </> "dependency") $ callProcessSilent damlc ["build", "-o", "dependency.dar"]

          step "Building data-dependency"
          createDirectoryIfMissing True (projDir </> "data-dependency")
          writeFileUTF8 (projDir </> "data-dependency" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: data-dependency"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
          writeFileUTF8 (projDir </> "data-dependency" </> "B.daml") $ unlines
            [ "module B where"
            , "class C a where f : a"
            ]
          writeFileUTF8 (projDir </> "data-dependency" </> "C.daml") $ unlines
            [ "module C where"
            , "import B"
            , "data Foo = Foo"
            , "instance C Foo where f = Foo"
            ]
          withCurrentDirectory (projDir </> "data-dependency") $ callProcessSilent damlc ["build", "-o", "data-dependency.dar"]

          step "Building main"
          createDirectoryIfMissing True (projDir </> "main")
          writeFileUTF8 (projDir </> "main" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: main"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib, " <> show (projDir </> "dependency" </> "dependency.dar") <> "]"
            , "data-dependencies: [" <> show (projDir </> "data-dependency" </> "data-dependency.dar") <>  "]"
            ]
          writeFileUTF8 (projDir </> "main" </> "Main.daml") $ unlines
            [ "module Main where"
            , "import \"dependency\" B"
            , "import C"
            , "foo : Foo"
            , "foo = f"
            ]
          withCurrentDirectory (projDir </> "main") $ callProcessSilent damlc ["build", "-o", "main.dar"]

    , testProperty "a dependency without data-dependencies should be reachable from any data-dependency in the dependency graph" $ \(n1 :: Int) (n2 :: Int) ->
        let
          intPkgId = LF.PackageId . Text.pack . show
          p1 = intPkgId n1
          p2 = intPkgId n2
          (graph, _vertexToNode, keyToVertex) =
            buildLfPackageGraph' @LF.PackageId @LF.PackageId
              BuildLfPackageGraphMetaArgs
                { getDecodedDalfPath = show
                , getDecodedDalfUnitId = stringToUnitId . show
                , getDecodedDalfPkg = id
                , getDalfPkgId = id
                , getDalfPkgRefs = const []
                }
              BuildLfPackageGraphArgs
                { builtinDeps = Set.empty
                , stablePkgs = Set.empty
                , dataDeps = [p1]
                , deps = [p2]
                }
        in
          fromMaybe False $
            Graph.path graph
              <$> keyToVertex p1
              <*> keyToVertex p2

    , testCaseSteps "dependency with data-dependency" $ \step -> withTempDir $ \projDir -> do
          -- This tests that a Daml package ('main') can depend on a package ('dependency') which in turn
          -- has a data-dependency on a third package ('data-dependency'). Note that, as usual, all the
          -- transitive dependencies of a dependency need to be stated in the `daml.yaml` file -
          -- in this case, 'data-dependency' is also a data-dependency of 'main'.
          step "Building data-dependency"
          createDirectoryIfMissing True (projDir </> "data-dependency")
          writeFileUTF8 (projDir </> "data-dependency" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: data-dependency"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
          writeFileUTF8 (projDir </> "data-dependency" </> "A.daml") $ unlines
            [ "module A where"
            , "class C a where f : a"
            ]
          writeFileUTF8 (projDir </> "data-dependency" </> "B.daml") $ unlines
            [ "module B where"
            , "import A"
            , "data Foo = Foo deriving (Eq)"
            , "instance C Foo where f = Foo"
            ]
          withCurrentDirectory (projDir </> "data-dependency") $ callProcessSilent damlc ["build", "-o", "data-dependency.dar"]

          step "Building dependency"
          createDirectoryIfMissing True (projDir </> "dependency")
          writeFileUTF8 (projDir </> "dependency" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: dependency"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib]"
            , "data-dependencies: [" <> show (projDir </> "data-dependency" </> "data-dependency.dar") <>  "]"
            ]
          writeFileUTF8 (projDir </> "dependency" </> "C.daml") $ unlines
            [ "module C where"
            , "import A"
            , "import B"
            , "bar : (Foo, Foo)"
            , "bar = (Foo, f)"
            ]
          withCurrentDirectory (projDir </> "dependency") $ callProcessSilent damlc ["build", "-o", "dependency.dar"]

          step "Building main"
          createDirectoryIfMissing True (projDir </> "main")
          writeFileUTF8 (projDir </> "main" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: main"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib, " <> show (projDir </> "dependency" </> "dependency.dar") <> "]"
            , "data-dependencies: [" <> show (projDir </> "data-dependency" </> "data-dependency.dar") <>  "]"
            ]
          writeFileUTF8 (projDir </> "main" </> "Main.daml") $ unlines
            [ "module Main where"
            , "import B"
            , "import C"
            , "eq : Bool"
            , "eq = x == y && x == Foo"
            , "  where (x, y) = bar"
            ]
          withCurrentDirectory (projDir </> "main") $ callProcessSilent damlc ["build", "-o", "main.dar"]
    , testCaseSteps "dependency with data-dependency 2" $ \step -> withTempDir $ \projDir -> do
          -- This is identical to the test case above except for the fact that
          -- 'data-dependency' is a _dependency_ (NOT a data-dependency) of 'main',
          -- which should still work.
          step "Building data-dependency"
          createDirectoryIfMissing True (projDir </> "data-dependency")
          writeFileUTF8 (projDir </> "data-dependency" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: data-dependency"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
          writeFileUTF8 (projDir </> "data-dependency" </> "A.daml") $ unlines
            [ "module A where"
            , "class C a where f : a"
            ]
          writeFileUTF8 (projDir </> "data-dependency" </> "B.daml") $ unlines
            [ "module B where"
            , "import A"
            , "data Foo = Foo deriving (Eq)"
            , "instance C Foo where f = Foo"
            ]
          withCurrentDirectory (projDir </> "data-dependency") $ callProcessSilent damlc ["build", "-o", "data-dependency.dar"]

          step "Building dependency"
          createDirectoryIfMissing True (projDir </> "dependency")
          writeFileUTF8 (projDir </> "dependency" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: dependency"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib]"
            , "data-dependencies: [" <> show (projDir </> "data-dependency" </> "data-dependency.dar") <>  "]"
            ]
          writeFileUTF8 (projDir </> "dependency" </> "C.daml") $ unlines
            [ "module C where"
            , "import A"
            , "import B"
            , "bar : (Foo, Foo)"
            , "bar = (Foo, f)"
            ]
          withCurrentDirectory (projDir </> "dependency") $ callProcessSilent damlc ["build", "-o", "dependency.dar"]

          step "Building main"
          createDirectoryIfMissing True (projDir </> "main")
          writeFileUTF8 (projDir </> "main" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: main"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib, " <> show (projDir </> "dependency" </> "dependency.dar") <> ","
                <> show (projDir </> "data-dependency" </> "data-dependency.dar") <>  "]"
            ]
          writeFileUTF8 (projDir </> "main" </> "Main.daml") $ unlines
            [ "module Main where"
            , "import B"
            , "import C"
            , "eq : Bool"
            , "eq = x == y && x == Foo"
            , "  where (x, y) = bar"
            ]
          withCurrentDirectory (projDir </> "main") $ callProcessSilent damlc ["build", "-o", "main.dar"]

    , testCaseSteps "dependency with data-dependency fail" $ \step -> withTempDir $ \projDir -> do
          -- This is identical to the two test cases above except for the fact that
          -- 'data-dependency' is not a dependency nor a data-dependency of 'main',
          -- so building 'main' fails.
          step "Building data-dependency"
          createDirectoryIfMissing True (projDir </> "data-dependency")
          writeFileUTF8 (projDir </> "data-dependency" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: data-dependency"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
          writeFileUTF8 (projDir </> "data-dependency" </> "A.daml") $ unlines
            [ "module A where"
            , "class C a where f : a"
            ]
          writeFileUTF8 (projDir </> "data-dependency" </> "B.daml") $ unlines
            [ "module B where"
            , "import A"
            , "data Foo = Foo deriving (Eq)"
            , "instance C Foo where f = Foo"
            ]
          withCurrentDirectory (projDir </> "data-dependency") $ callProcessSilent damlc ["build", "-o", "data-dependency.dar"]

          step "Building dependency"
          createDirectoryIfMissing True (projDir </> "dependency")
          writeFileUTF8 (projDir </> "dependency" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: dependency"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib]"
            , "data-dependencies: [" <> show (projDir </> "data-dependency" </> "data-dependency.dar") <>  "]"
            ]
          writeFileUTF8 (projDir </> "dependency" </> "C.daml") $ unlines
            [ "module C where"
            , "import A"
            , "import B"
            , "bar : (Foo, Foo)"
            , "bar = (Foo, f)"
            ]
          withCurrentDirectory (projDir </> "dependency") $ callProcessSilent damlc ["build", "-o", "dependency.dar"]

          step "Building main"
          createDirectoryIfMissing True (projDir </> "main")
          writeFileUTF8 (projDir </> "main" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: main"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib, " <> show (projDir </> "dependency" </> "dependency.dar") <> "]"
            ]
          writeFileUTF8 (projDir </> "main" </> "Main.daml") $ unlines
            [ "module Main where"
            , "import B"
            , "import C"
            , "eq : Bool"
            , "eq = x == y && x == Foo"
            , "  where (x, y) = bar"
            ]
          buildPackageError (projDir </> "main") "" "cannot satisfy --package dependency-0.0.1"

    , testCaseSteps "module-prefixes" $ \step -> withTempDir $ \dir -> do
          step "Create dep1"
          createDirectoryIfMissing True (dir </> "dep1")
          writeFileUTF8 (dir </> "dep1" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: dep"
            , "version: 1.0.0"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
          writeFileUTF8 (dir </> "dep1" </> "A.daml") $ unlines
            [ "module A where"
            , "dep1 = 0"
            ]
          callProcessSilent
            damlc
            ["build", "--package-root", dir </> "dep1", "-o", dir </> "dep1" </> "dep1.dar"]
          createDirectoryIfMissing True (dir </> "dep2")
          writeFileUTF8 (dir </> "dep2" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: dep"
            , "version: 2.0.0"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
          writeFileUTF8 (dir </> "dep2" </> "A.daml") $ unlines
            [ "module A where"
            , "dep2 = 0"
            ]
          callProcessSilent
            damlc
            ["build", "--package-root", dir </> "dep2", "-o", dir </> "dep2" </> "dep2.dar"]
          step "Building main"
          createDirectoryIfMissing True (dir </> "main")
          writeFileUTF8 (dir </> "main" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: main"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib]"
            , "data-dependencies:"
            , "  - " <> show (dir </> "dep1" </> "dep1.dar")
            , "  - " <> show (dir </> "dep2" </> "dep2.dar")
            , "module-prefixes:"
            , "  dep-1.0.0: Dep1"
            , "  dep-2.0.0: Dep2"
            ]
          writeFileUTF8 (dir </> "main" </> "A.daml") $ unlines
            [ "module A where"
            , "import Dep1.A"
            , "import Dep2.A"
            , "main = dep1 + dep2"
            ]
          callProcessSilent damlc ["build", "--package-root", dir </> "main", "-o", "main.dar"]
          step "Changing module prefixes"
          writeFileUTF8 (dir </> "main" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: main"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib]"
            , "data-dependencies:"
            , "  - " <> show (dir </> "dep1" </> "dep1.dar")
            , "  - " <> show (dir </> "dep2" </> "dep2.dar")
            , "module-prefixes:"
            , "  dep-1.0.0: Dep3"
            , "  dep-2.0.0: Dep4"
            ]
          buildPackageError (dir </> "main") "" "Could not find module"
    , testCaseSteps "the DAR carries a daml.yaml describing the build" $ \step -> withTempDir $ \projDir -> do
          step "Building a package"
          writeFileUTF8 (projDir </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: embedded"
            , "version: 1.0.0"
            , "source: daml"
            , "dependencies: [daml-prim, daml-stdlib]"
            , "build-options:"
            , "  - --target=2.1"
            ]
          createDirectoryIfMissing True (projDir </> "daml")
          writeFileUTF8 (projDir </> "daml" </> "Main.daml") $ unlines
            [ "module Main where"
            , "value : Int"
            , "value = 1"
            ]
          withCurrentDirectory projDir $ callProcessSilent damlc ["build", "-o", "embedded.dar"]

          step "Checking the embedded daml.yaml"
          let dar = projDir </> "embedded.dar"
          assertFileExists dar
          archive <- Zip.toArchive <$> BSL.readFile dar
          Just entry <- pure $ Zip.findEntryByPath "daml.yaml" archive
          let embedded = BSL.Char8.unpack (Zip.fromEntry entry)
          assertInfixOf ("damlc:" <> componentVersionString) embedded
          assertInfixOf "name: embedded" embedded
          assertInfixOf "version: 1.0.0" embedded
          assertInfixOf "source: daml" embedded
          assertInfixOf "--target=2.1" embedded
          assertInfixOf "environment-variable-interpolation: false" embedded

    , testGroup "rebuilding a DAR from the daml.yaml it carries"
      [ testCase "a package with no dependencies" $ withTempDir $ \dir -> do
            let proj = dir </> "plain"
            createDirectoryIfMissing True (proj </> "daml")
            writeFileUTF8 (proj </> "daml.yaml") $ unlines
                [ "sdk-version: " <> componentVersionString
                , "name: plain"
                , "version: 1.0.0"
                , "source: daml"
                , "dependencies: [daml-prim, daml-stdlib]"
                ]
            writeFileUTF8 (proj </> "daml" </> "Main.daml") $ unlines
                [ "module Main where"
                , "template Holder"
                , "  with"
                , "    owner : Party"
                , "  where"
                , "    signatory owner"
                ]
            expectSameId =<< roundTripThroughDar damlc proj (dir </> "rebuild")

      , testCase "a dependency the package pointed at" $ withTempDir $ \dir -> do
            dep <- buildDependencyDar damlc dir "dep"
            let proj = dir </> "pointed"
            createDirectoryIfMissing True (proj </> "daml")
            writeFileUTF8 (proj </> "daml.yaml") $ unlines
                [ "sdk-version: " <> componentVersionString
                , "name: pointed"
                , "version: 1.0.0"
                , "source: daml"
                , "dependencies: [daml-prim, daml-stdlib]"
                , "data-dependencies: [" <> show dep <> "]"
                ]
            writeFileUTF8 (proj </> "daml" </> "Main.daml") $ unlines
                [ "module Main where"
                , "import Dep"
                , "template Holder"
                , "  with"
                , "    owner : Party"
                , "  where"
                , "    signatory owner"
                , "    nonconsuming choice Make : ContractId Asset"
                , "      controller owner"
                , "      do create Asset with owner = owner"
                ]
            expectSameId =<< roundTripThroughDar damlc proj (dir </> "rebuild")

      , -- Fails today: it passes once every dependency can be read as a data-dependency
        testCase "a dependency the package named" $ withTempDir $ \dir -> do
            dep <- buildDependencyDar damlc dir "dep"
            let proj = dir </> "named"
            createDirectoryIfMissing True (proj </> "daml")
            writeFileUTF8 (proj </> "daml.yaml") $ unlines
                [ "sdk-version: " <> componentVersionString
                , "name: named"
                , "version: 1.0.0"
                , "source: daml"
                , "dependencies: [daml-prim, daml-stdlib, " <> show dep <> "]"
                ]
            writeFileUTF8 (proj </> "daml" </> "Main.daml") $ unlines
                [ "module Main where"
                , "import Dep"
                , "template Holder"
                , "  with"
                , "    owner : Party"
                , "  where"
                , "    signatory owner"
                , "    nonconsuming choice Make : ContractId Asset"
                , "      controller owner"
                , "      do create Asset with owner = owner"
                ]
            expectSameId =<< roundTripThroughDar damlc proj (dir </> "rebuild")

      , testCase "a package whose modules sit in subdirectories" $ withTempDir $ \dir -> do
            let proj = dir </> "nested"
            createDirectoryIfMissing True (proj </> "src" </> "A" </> "B")
            writeFileUTF8 (proj </> "daml.yaml") $ unlines
                [ "sdk-version: " <> componentVersionString
                , "name: nested"
                , "version: 1.0.0"
                , "source: src"
                , "dependencies: [daml-prim, daml-stdlib]"
                ]
            writeFileUTF8 (proj </> "src" </> "A" </> "B" </> "C.daml") $ unlines
                [ "module A.B.C where"
                , "value : Int"
                , "value = 1"
                ]
            writeFileUTF8 (proj </> "src" </> "Main.daml") $ unlines
                [ "module Main where"
                , "import A.B.C"
                , "other : Int"
                , "other = value"
                ]
            expectSameId =<< roundTripThroughDar damlc proj (dir </> "rebuild")

      , testCase "a package with module prefixes" $ withTempDir $ \dir -> do
            dep <- buildDependencyDar damlc dir "dep"
            let proj = dir </> "prefixed"
            createDirectoryIfMissing True (proj </> "daml")
            writeFileUTF8 (proj </> "daml.yaml") $ unlines
                [ "sdk-version: " <> componentVersionString
                , "name: prefixed"
                , "version: 1.0.0"
                , "source: daml"
                , "dependencies: [daml-prim, daml-stdlib]"
                , "data-dependencies: [" <> show dep <> "]"
                , "module-prefixes:"
                , "  dep-1.0.0: V1"
                ]
            writeFileUTF8 (proj </> "daml" </> "Main.daml") $ unlines
                [ "module Main where"
                , "import V1.Dep"
                , "template Holder"
                , "  with"
                , "    owner : Party"
                , "  where"
                , "    signatory owner"
                , "    nonconsuming choice Make : ContractId Asset"
                , "      controller owner"
                , "      do create Asset with owner = owner"
                ]
            expectSameId =<< roundTripThroughDar damlc proj (dir </> "rebuild")

      , testCaseSteps "source changed after the fact does not reproduce the package" $ \_step -> withTempDir $ \dir -> do
            let proj = dir </> "tampered"
            createDirectoryIfMissing True (proj </> "daml")
            writeFileUTF8 (proj </> "daml.yaml") $ unlines
                [ "sdk-version: " <> componentVersionString
                , "name: tampered"
                , "version: 1.0.0"
                , "source: daml"
                , "dependencies: [daml-prim, daml-stdlib]"
                ]
            writeFileUTF8 (proj </> "daml" </> "Main.daml") $ unlines
                [ "module Main where"
                , "value : Int"
                , "value = 1"
                ]
            let tamper rebuildDir = writeFileUTF8 (rebuildDir </> "daml" </> "Main.daml") $ unlines
                    [ "module Main where"
                    , "value : Int"
                    , "value = 2"
                    ]
            (built, rebuilt) <-
                roundTripThroughDarWith tamper damlc proj (dir </> "rebuild")
            assertBool "tampered source reproduced the original package id" (built /= rebuilt)

      , testCase "a package whose daml.yaml used an environment variable" $ withTempDir $ \dir -> do
            let proj = dir </> "interpolated"
            createDirectoryIfMissing True (proj </> "daml")
            writeFileUTF8 (proj </> "daml.yaml") $ unlines
                [ "sdk-version: " <> componentVersionString
                , "name: interpolated"
                , "version: 1.0.0"
                , "source: ${PACKAGING_TEST_SOURCE}"
                , "dependencies: [daml-prim, daml-stdlib]"
                ]
            writeFileUTF8 (proj </> "daml" </> "Main.daml") $ unlines
                [ "module Main where"
                , "template Holder"
                , "  with"
                , "    owner : Party"
                , "  where"
                , "    signatory owner"
                ]
            built <- bracket
                (setEnv "PACKAGING_TEST_SOURCE" "daml" True)
                (\_ -> unsetEnv "PACKAGING_TEST_SOURCE")
                (\_ -> withCurrentDirectory proj $ do
                    callProcessSilent damlc ["build", "-o", "built.dar"]
                    mainPackageIdOf (proj </> "built.dar"))
            -- Rebuilt with the variable gone from the environment.
            rebuilt <- rebuildFromDar damlc (proj </> "built.dar") (dir </> "rebuild")
            expectSameId (built, rebuilt)

      , testCase "a package that set its own output path" $ withTempDir $ \dir -> do
            let proj = dir </> "output"
            createDirectoryIfMissing True (proj </> "daml")
            writeFileUTF8 (proj </> "daml.yaml") $ unlines
                [ "sdk-version: " <> componentVersionString
                , "name: output"
                , "version: 1.0.0"
                , "source: daml"
                , "dependencies: [daml-prim, daml-stdlib]"
                , "build-options:"
                , "  - --output=" <> (dir </> "elsewhere" </> "custom.dar")
                ]
            writeFileUTF8 (proj </> "daml" </> "Main.daml") $ unlines
                [ "module Main where"
                , "template Holder"
                , "  with"
                , "    owner : Party"
                , "  where"
                , "    signatory owner"
                ]
            expectSameId =<< roundTripThroughDar damlc proj (dir </> "rebuild")

      , testCase "a package that upgrades another" $ withTempDir $ \dir -> do
            let v1 = dir </> "v1"
            createDirectoryIfMissing True (v1 </> "daml")
            writeFileUTF8 (v1 </> "daml.yaml") $ unlines
                [ "sdk-version: " <> componentVersionString
                , "name: upgraded"
                , "version: 1.0.0"
                , "source: daml"
                , "dependencies: [daml-prim, daml-stdlib]"
                ]
            writeFileUTF8 (v1 </> "daml" </> "Main.daml") $ unlines
                [ "module Main where"
                , "template Holder"
                , "  with"
                , "    owner : Party"
                , "  where"
                , "    signatory owner"
                ]
            withCurrentDirectory v1 $ callProcessSilent damlc ["build", "-o", "v1.dar"]
            predecessor <- mainPackageIdOf (v1 </> "v1.dar")

            let v2 = dir </> "v2"
            createDirectoryIfMissing True (v2 </> "daml")
            writeFileUTF8 (v2 </> "daml.yaml") $ unlines
                [ "sdk-version: " <> componentVersionString
                , "name: upgraded"
                , "version: 2.0.0"
                , "source: daml"
                , "dependencies: [daml-prim, daml-stdlib]"
                , "upgrades: " <> (v1 </> "v1.dar")
                ]
            writeFileUTF8 (v2 </> "daml" </> "Main.daml") $ unlines
                [ "module Main where"
                , "template Holder"
                , "  with"
                , "    owner : Party"
                , "  where"
                , "    signatory owner"
                ]
            withCurrentDirectory v2 $ callProcessSilent damlc ["build", "-o", "v2.dar"]
            embedded <- embeddedDamlYamlOf (v2 </> "v2.dar")
            assertInfixOf ("upgrades: " <> Text.unpack (LF.unPackageId predecessor)) embedded
            assertInfixOf "--typecheck-upgrades=no" embedded
            rebuilt <- rebuildFromDar damlc (v2 </> "v2.dar") (dir </> "rebuild")
            built <- mainPackageIdOf (v2 </> "v2.dar")
            expectSameId (built, rebuilt)
      ]

    , testCase "a module included from outside source warns" $ withTempDir $ \projDir -> do
          createDirectoryIfMissing True (projDir </> "daml")
          createDirectoryIfMissing True (projDir </> "extra")
          writeFileUTF8 (projDir </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: included"
            , "version: 1.0.0"
            , "source: daml"
            , "dependencies: [daml-prim, daml-stdlib]"
            , "build-options: [--include=extra]"
            ]
          writeFileUTF8 (projDir </> "extra" </> "Extra.daml") $ unlines
            [ "module Extra where"
            , "extra : Int"
            , "extra = 1"
            ]
          writeFileUTF8 (projDir </> "daml" </> "Main.daml") $ unlines
            [ "module Main where"
            , "import Extra"
            , "value : Int"
            , "value = extra"
            ]
          (exitCode, _, err) <- readProcessWithExitCode damlc ["build", "--package-root", projDir] ""
          exitCode @?= ExitSuccess
          assertInfixOf "This DAR will not carry sources" err
          assertInfixOf "Extra.daml" err

    , testCaseSteps "relative output filepath" $ \step -> withTempDir $ \dir -> do
          step "Create package"
          writeFileUTF8 (dir </> "daml.yaml") $ unlines
            [ "sdk-version: " <> componentVersionString
            , "name: dep"
            , "version: 1.0.0"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
          writeFileUTF8 (dir </> "A.daml") $ unlines
            [ "module A where"
            , "dep1 = 0"
            ]
          let outDir = dir </> "out"
          createDirectoryIfMissing True outDir
          withCurrentDirectory outDir $
            callProcessSilent damlc ["build", "--package-root", dir, "-o", "A.dar"]
          assertFileExists $ outDir </> "A.dar"
    ] <>
    [ lfVersionTests damlc
    ]
  where
      buildPackage' :: FilePath -> FilePath -> IO ()
      buildPackage' damlc dir = withCurrentDirectory dir $ callProcessSilent damlc ["build"]
      buildPackage = buildPackage' damlc

      inspectDar dir file = withCurrentDirectory dir $ callProcessForStdout damlc ["inspect-dar", file]

      buildPackageError :: FilePath -> String -> String -> IO ()
      buildPackageError dir expectedOut expectedErr = withCurrentDirectory dir $ do
          (exitCode, out, err) <- readProcessWithExitCode damlc ["build"] ""
          if exitCode /= ExitSuccess then do
              unless (expectedOut `isInfixOf` out && expectedErr `isInfixOf` err) $ do
                  hPutStrLn stderr $ unlines
                      [ "TEST FAILED:"
                      , "    Command \"damlc build\" failed as expected, but did not produce expected output."
                      , "    stdout = " <> show out
                      , "    stderr = " <> show err
                      ]
                  exitFailure
          else do
              hPutStrLn stderr $ unlines
                  [ "TEST FAILED:"
                  , "    Command \"damlc build\" was expected to fail, but it succeeded."
                  , "    stdout = " <> show out
                  , "    stderr = " <> show err
                  ]
              exitFailure

-- | Test that a package build with --target=targetVersion never has a dependency on a package with version > targetVersion
lfVersionTests :: ComponentVersioned => FilePath -> TestTree
lfVersionTests damlc = testGroup "LF version dependencies"
    [ testCase ("Package in " <> LF.renderVersion version) $ withTempDir $ \projDir -> do
          writeFileUTF8 (projDir </> "daml.yaml") $ unlines
              [ "sdk-version: " <> componentVersionString
              , "name: proj"
              , "version: 0.1.0"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib]"
              ]
          writeFileUTF8 (projDir </> "A.daml") $ unlines
              [ "module A where"]
          withCurrentDirectory projDir $ callProcessSilent damlc ["build", "-o", projDir </> "proj.dar", "--target", LF.renderVersion version]
          archive <- Zip.toArchive <$> BSL.readFile (projDir </> "proj.dar")
          DalfManifest {mainDalfPath, dalfPaths} <- either fail pure $ readDalfManifest archive
          Dalfs main other <- either fail pure $ readDalfs archive
          forM_ (zip (mainDalfPath : dalfPaths) (main : other)) $ \(path, bytes) -> do
              Right (_, pkg) <- pure $ LFArchive.decodeArchive LFArchive.DecodeAsMain $ BSL.toStrict bytes
              assertBool ("Expected LF version <=" <> show version <> " but got " <> show (LF.packageLfVersion pkg) <> " in " <> path) $
                  version `LF.canDependOn` LF.packageLfVersion pkg
    | version <- LF.compilerOutputLfVersions
    ]

-- | Build a package, then rebuild it from the daml.yaml its own DAR carries,
-- and hand back the main package id of each. Tests supply where to
-- write the output.
roundTripThroughDar :: FilePath -> FilePath -> FilePath -> IO (LF.PackageId, LF.PackageId)
roundTripThroughDar = roundTripThroughDarWith (const (pure ()))

roundTripThroughDarWith
    :: (FilePath -> IO ())
    -> FilePath -> FilePath -> FilePath -> IO (LF.PackageId, LF.PackageId)
roundTripThroughDarWith beforeRebuild damlc projDir rebuildDir = do
    withCurrentDirectory projDir $ callProcessSilent damlc ["build", "-o", "built.dar"]
    built <- mainPackageIdOf (projDir </> "built.dar")

    createDirectoryIfMissing True rebuildDir
    archive <- Zip.toArchive <$> BSL.readFile (projDir </> "built.dar")
    Zip.extractFilesFromArchive [Zip.OptDestination rebuildDir] archive

    source <- sourceDirectoryOf (rebuildDir </> "daml.yaml")
    shipped <- shippedDamlFiles rebuildDir
    forM_ shipped $ \(packageDir, relative) -> do
        let destination = rebuildDir </> source </> relative
        createDirectoryIfMissing True (takeDirectory destination)
        renamePath (rebuildDir </> packageDir </> relative) destination

    beforeRebuild rebuildDir
    rebuildOrFail damlc rebuildDir
    rebuilt <- mainPackageIdOf (rebuildDir </> "rebuilt.dar")
    pure (built, rebuilt)

-- | Rebuild the unpacked tree
rebuildOrFail :: FilePath -> FilePath -> IO ()
rebuildOrFail damlc rebuildDir = do
    (exit, out, err) <- readCreateProcessWithExitCode
        (proc damlc ["build", "-o", "rebuilt.dar"]) { cwd = Just rebuildDir } ""
    unless (exit == ExitSuccess) $
        assertFailure $ "the unpacked DAR did not rebuild:\n"
            <> unlines (takeEnd 12 (lines (out <> err)))

-- | A one-module package to depend on, built and handed back as a DAR path.
buildDependencyDar :: ComponentVersioned => FilePath -> FilePath -> String -> IO FilePath
buildDependencyDar damlc dir name = do
    let proj = dir </> name
    createDirectoryIfMissing True (proj </> "daml")
    writeFileUTF8 (proj </> "daml.yaml") $ unlines
        [ "sdk-version: " <> componentVersionString
        , "name: " <> name
        , "version: 1.0.0"
        , "source: daml"
        , "dependencies: [daml-prim, daml-stdlib]"
        ]
    writeFileUTF8 (proj </> "daml" </> "Dep.daml") $ unlines
        [ "module Dep where"
        , "template Asset"
        , "  with"
        , "    owner : Party"
        , "  where"
        , "    signatory owner"
        ]
    withCurrentDirectory proj $ callProcessSilent damlc ["build", "-o", name <> ".dar"]
    pure (proj </> (name <> ".dar"))

-- | The second half of 'roundTripThroughDar', for cases that have to build
-- the DAR themselves.
rebuildFromDar :: FilePath -> FilePath -> FilePath -> IO LF.PackageId
rebuildFromDar damlc dar rebuildDir = do
    createDirectoryIfMissing True rebuildDir
    archive <- Zip.toArchive <$> BSL.readFile dar
    Zip.extractFilesFromArchive [Zip.OptDestination rebuildDir] archive
    source <- sourceDirectoryOf (rebuildDir </> "daml.yaml")
    shipped <- shippedDamlFiles rebuildDir
    forM_ shipped $ \(packageDir, relative) -> do
        let destination = rebuildDir </> source </> relative
        createDirectoryIfMissing True (takeDirectory destination)
        renamePath (rebuildDir </> packageDir </> relative) destination
    rebuildOrFail damlc rebuildDir
    mainPackageIdOf (rebuildDir </> "rebuilt.dar")

mainPackageIdOf :: FilePath -> IO LF.PackageId
mainPackageIdOf dar = do
    Right Dalfs{..} <- readDalfs . Zip.toArchive <$> BSL.readFile dar
    (pkgId, _pkg) <- either (fail . show) pure $
        LFArchive.decodeArchive LFArchive.DecodeAsMain (BSL.toStrict mainDalf)
    pure pkgId

embeddedDamlYamlOf :: FilePath -> IO String
embeddedDamlYamlOf dar = do
    archive <- Zip.toArchive <$> BSL.readFile dar
    case Zip.findEntryByPath "daml.yaml" archive of
        Nothing -> fail (dar <> " carries no daml.yaml")
        Just entry -> pure (BSL.Char8.unpack (Zip.fromEntry entry))

sourceDirectoryOf :: FilePath -> IO FilePath
sourceDirectoryOf damlYaml = do
    contents <- readFileUTF8 damlYaml
    case [unquote (dropWhile (== ' ') rest) | line <- lines contents, Just rest <- [stripPrefix "source:" line]] of
        source : _ -> pure source
        [] -> fail $ damlYaml <> " names no source directory"
  where
    unquote value = fromMaybe value $ asum
        [ stripPrefix "\"" value >>= stripSuffix "\""
        , stripPrefix "'" value >>= stripSuffix "'"
        ]

-- | The .daml files a DAR ships, as the package directory they sit in and
-- their path relative to it.
shippedDamlFiles :: FilePath -> IO [(FilePath, FilePath)]
shippedDamlFiles unpacked = do
    entries <- listDirectory unpacked
    packageDirs <- filterM (doesDirectoryExist . (unpacked </>)) entries
    fmap concat $ forM packageDirs $ \packageDir -> do
        files <- listFilesRecursive (unpacked </> packageDir)
        pure [ (packageDir, makeRelative (unpacked </> packageDir) file)
             | file <- files, takeExtension file == ".daml" ]

-- | The claim every case in the round-trip group makes.
expectSameId :: (LF.PackageId, LF.PackageId) -> IO ()
expectSameId (built, rebuilt) =
    assertEqual "the DAR did not rebuild to the same package" built rebuilt

darPackageIds :: FilePath -> IO [LF.PackageId]
darPackageIds fp = do
    archive <- Zip.toArchive <$> BSL.readFile fp
    Dalfs mainDalf dalfDeps <- either fail pure $ readDalfs archive
    Right dalfPkgIds  <- pure $ mapM (LFArchive.decodeArchivePackageId . BSL.toStrict) $ mainDalf : dalfDeps
    pure dalfPkgIds

-- | Check that the given file exists in the dar in the given directory.
--
-- This function automatically strips away the root directory e.g.
-- foobar-0.0.1-b2d63d90f3cb73434ae005ee1c9762166bb84563ac9d108a606c8384803f09f2
-- so to check that foobar-0.0.1-b2d63d90f3cb73434ae005ee1c9762166bb84563ac9d108a606c8384803f09f2/A/B.daml
-- exists use checkDarFile darFiles "A" "B.daml"
checkDarFile :: [FilePath] -> FilePath -> FilePath -> IO ()
checkDarFile darFiles dir file =
    assertBool (dir </> file <> " not in " <> show darFiles) $
    any (\f -> normalise (dropDirectory1 f) == normalise (dir </> file)) darFiles
