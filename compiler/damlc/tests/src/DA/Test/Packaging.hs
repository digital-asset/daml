-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

import SdkVersion

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    defaultMain $ tests Tools{..}

data Tools = Tools -- and places
  { damlc :: FilePath
  }

tests :: Tools -> TestTree
tests Tools{damlc} = testGroup "Packaging" $
    [ testCaseSteps "Build package with dependency" $ \step -> withTempDir $ \tmpDir -> do
        let projectA = tmpDir </> "a"
        let projectB = tmpDir </> "b"
        let aDar = projectA </> ".daml" </> "dist" </> "a-1.0.dar"
        let bDar = projectB </> ".daml" </> "dist" </> "b-1.0.dar"
        step "Creating project a..."
        createDirectoryIfMissing True (projectA </> "daml" </> "Foo" </> "Bar")
        writeFileUTF8 (projectA </> "daml" </> "A.daml") $ unlines
            [ "module A (a) where"
            , "a : ()"
            , "a = ()"
            ]
        writeFileUTF8 (projectA </> "daml" </> "Foo" </> "Bar" </> "Baz.daml") $ unlines
            [ "module Foo.Bar.Baz (c) where"
            , "import A (a)"
            , "c : ()"
            , "c = a"
            ]
        writeFileUTF8 (projectA </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: a"
            , "version: \"1.0\""
            , "source: daml"
            , "exposed-modules: [A, Foo.Bar.Baz]"
            , "dependencies:"
            , "  - daml-prim"
            , "  - daml-stdlib"
            ]
        buildProject projectA
        assertFileExists aDar
        step "Creating project b..."
        createDirectoryIfMissing True (projectB </> "daml")
        writeFileUTF8 (projectB </> "daml" </> "B.daml") $ unlines
            [ "module B where"
            , "import C"
            , "import Foo.Bar.Baz"
            , "b : ()"
            , "b = a"
            , "d : ()"
            , "d = c"
            ]
        writeFileUTF8 (projectB </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
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
        buildProject projectB
        assertFileExists bDar
    , testCaseSteps "Dependency on a package with source: A.daml" $ \step -> withTempDir $ \tmpDir -> do
        let projectA = tmpDir </> "a"
        let projectB = tmpDir </> "b"
        let aDar = projectA </> ".daml" </> "dist" </> "a-1.0.dar"
        let bDar = projectB </> ".daml" </> "dist" </> "b-1.0.dar"
        step "Creating project a..."
        createDirectoryIfMissing True projectA
        writeFileUTF8 (projectA </> "A.daml") $ unlines
            [ "module A () where"
            ]
        writeFileUTF8 (projectA </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: a"
            , "version: \"1.0\""
            , "source: A.daml"
            , "dependencies:"
            , "  - daml-prim"
            , "  - daml-stdlib"
            ]
        buildProject projectA
        assertFileExists aDar
        step "Creating project b..."
        createDirectoryIfMissing True projectB
        writeFileUTF8 (projectB </> "B.daml") $ unlines
            [ "module B where"
            , "import A ()"
            ]
        writeFileUTF8 (projectB </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "version: \"1.0\""
            , "name: b"
            , "source: ."
            , "dependencies:"
            , "  - daml-prim"
            , "  - daml-stdlib"
            , "  - " <> aDar
            ]
        buildProject projectB
        assertFileExists bDar
        darFiles <- Zip.filesInArchive . Zip.toArchive <$> BSL.readFile bDar
        assertBool "b.dar contains source file from package database" $
            not $ any ("A.daml" `isSuffixOf`) darFiles
    , testCase "Top-level source files" $ withTempDir $ \tmpDir -> do
        -- Test that a source file in the project root will be included in the
        -- DAR file. Regression test for #1048.
        let projDir = tmpDir </> "proj"
        createDirectoryIfMissing True projDir
        writeFileUTF8 (projDir </> "A.daml") $ unlines
          [ "module A (a) where"
          , "a : ()"
          , "a = ()"
          ]
        writeFileUTF8 (projDir </> "daml.yaml") $ unlines
          [ "sdk-version: " <> sdkVersion
          , "name: proj"
          , "version: \"1.0\""
          , "source: ."
          , "exposed-modules: [A]"
          , "dependencies:"
          , "  - daml-prim"
          , "  - daml-stdlib"
          ]
        buildProject projDir
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
          [ "sdk-version: a-bad-sdk-version"
          , "name: proj"
          , "version: \"1.0\""
          , "source: ."
          , "exposed-modules: [A]"
          , "dependencies:"
          , "  - daml-prim"
          , "  - daml-stdlib"
          ]

        bracket
            (setEnv "DAML_SDK_VERSION" sdkVersion True)
            (\ _ -> unsetEnv "DAML_SDK_VERSION")
            (\ _ -> buildProject projDir)

        let dar = projDir </> ".daml" </> "dist" </> "proj-1.0.dar"
        assertFileExists dar
        archive <- Zip.toArchive <$> BSL.readFile dar
        Just entry <- pure $ Zip.findEntryByPath "META-INF/MANIFEST.MF" archive
        let lines = BSL.Char8.lines (Zip.fromEntry entry)
            expectedLine = "Sdk-Version: " <> BSL.Char8.pack sdkVersion
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
          [ "sdk-version: " <> sdkVersion
          , "name: proj"
          , "version: 0.1.0"
          , "source: A.daml"
          , "dependencies: [daml-prim, daml-stdlib]"
          ]
        buildProject projDir
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
          [ "sdk-version: " <> sdkVersion
          , "name: proj"
          , "version: 0.1.0"
          , "source: A/B.daml"
          , "dependencies: [daml-prim, daml-stdlib]"
          ]
        buildProject projDir
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
          [ "sdk-version: " <> sdkVersion
          , "name: proj"
          , "version: 0.1.0"
          , "source: daml"
          , "dependencies: [daml-prim, daml-stdlib]"
          ]
        buildProject projDir
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
          [ "sdk-version: " <> sdkVersion
          , "name: proj"
          , "version: 0.1.0"
          , "source: ."
          , "dependencies: [daml-prim, daml-stdlib]"
          ]
        buildProject projDir

    , testCase "Project without exposed modules" $ withTempDir $ \projDir -> do
        writeFileUTF8 (projDir </> "A.daml") $ unlines
            [ "module A (a) where"
            , "a : ()"
            , "a = ()"
            ]
        writeFileUTF8 (projDir </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: proj"
            , "version: \"1.0\""
            , "source: A.daml"
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
        buildProject projDir

    , testCase "Empty package" $ withTempDir $ \projDir -> do
        writeFileUTF8 (projDir </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: proj"
            , "version: 0.0.1"
            , "source: src"
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
        createDirectoryIfMissing True (projDir </> "src")
        buildProject projDir

    , testCase "Package-wide name collision" $ withTempDir $ \projDir -> do
        createDirectoryIfMissing True (projDir </> "src")
        createDirectoryIfMissing True (projDir </> "src" </> "A")
        writeFileUTF8 (projDir </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
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
        buildProjectError projDir "" "name collision"

    , testCase "Virtual module name collision" $ withTempDir $ \projDir -> do
        createDirectoryIfMissing True (projDir </> "src" </> "A" </> "B")
        writeFileUTF8 (projDir </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
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
        (exitCode, out, err) <- readProcessWithExitCode damlc ["build", "--project-root", projDir] ""
        out @?= ""
        assertInfixOf "collision between module prefix A.B (from A.B.C) and variant A:B" err
        exitCode @?= ExitFailure 1

    , testCase "Manifest name" $ withTempDir $ \projDir -> do
          createDirectoryIfMissing True (projDir </> "src")
          writeFileUTF8 (projDir </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
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
              [ "sdk-version: " <> sdkVersion
              , "name: foobar"
              , "version: 1.2.3"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib]"
              ]
          withCurrentDirectory projDir $ callProcessSilent damlc ["build", "-o", "foobar.dar", "--target=1.dev"]
          Right Dalfs{..} <- readDalfs . Zip.toArchive <$> BSL.readFile (projDir </> "foobar.dar")
          (_pkgId, pkg) <- either (fail . show) pure (LFArchive.decodeArchive LFArchive.DecodeAsMain (BSL.toStrict mainDalf))
          LF.packageMetadata pkg @?= Just (LF.PackageMetadata (LF.PackageName "foobar") (LF.PackageVersion "1.2.3"))

    , testCase "Package metadata - single file" $ withTempDir $ \projDir -> do
          createDirectoryIfMissing True projDir
          writeFileUTF8 (projDir </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: foobar"
              , "version: 1.2.3"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib]"
              ]
          writeFileUTF8 (projDir </> "A.daml") $ unlines
              [ "module A where"
              ]
          withCurrentDirectory projDir $ callProcessSilent damlc ["build", "-o", "foobar.dar", "--target=1.dev"]
          Right Dalfs{..} <- readDalfs . Zip.toArchive <$> BSL.readFile (projDir </> "foobar.dar")
          (_pkgId, pkg) <- either (fail . show) pure (LFArchive.decodeArchive LFArchive.DecodeAsMain (BSL.toStrict mainDalf))
          LF.packageMetadata pkg @?= Just (LF.PackageMetadata (LF.PackageName "foobar") (LF.PackageVersion "1.2.3"))

    , testCase "Transitive package deps" $ withTempDir $ \projDir -> do
          -- Check that the depends field in the package config files does not depend on the name of the DAR.
          let projA = projDir </> "a"
          let projB = projDir </> "b"
          let projC = projDir </> "c"

          createDirectoryIfMissing True (projA </> "src")
          writeFileUTF8 (projA </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
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
            [ "sdk-version: " <> sdkVersion
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
            [ "sdk-version: " <> sdkVersion
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
            [ "sdk-version: " <> sdkVersion
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
            [ "sdk-version: " <> sdkVersion
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
            [ "sdk-version: " <> sdkVersion
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
          buildProjectError projC "" "dependencies with same unit id but conflicting package ids: a-0.0.1"

    , testCase "Detects unitId collisions in data-dependencies" $ withTempDir $ \projDir -> do
          -- Check that two pacages with the same unit id is flagged as an error.
          let projA = projDir </> "a"
          let projB = projDir </> "b"
          let projC = projDir </> "c"

          createDirectoryIfMissing True (projA </> "src")
          createDirectoryIfMissing True (projB </> "src")
          createDirectoryIfMissing True (projC </> "src")

          writeFileUTF8 (projA </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
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
            [ "sdk-version: " <> sdkVersion
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
            [ "sdk-version: " <> sdkVersion
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
          buildProjectError projC "" "dependencies with same unit id but conflicting package ids: a-0.0.1"

    , testCaseSteps "Error on newer LF data-dependency" $ \step -> withTempDir $ \tmpDir -> do
          step "Building 'a"
          createDirectoryIfMissing True (tmpDir </> "a")
          writeFileUTF8 (tmpDir </> "a" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "version: 0.0.1"
              , "name: a"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib]"
              , "build-options: [--target=1.dev]"
              ]
          writeFileUTF8 (tmpDir </> "a" </> "A.daml") $ unlines
              [ "module A where"
              ]
          withCurrentDirectory (tmpDir </> "a") $ callProcessSilent damlc ["build", "-o", tmpDir </> "a" </> "a.dar"]

          step "Building b"
          createDirectoryIfMissing True (tmpDir </> "b")
          writeFileUTF8 (tmpDir </> "b" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "version: 0.0.1"
              , "name: b"
              , "source: ."
              , "dependencies:"
              , "  - daml-prim"
              , "  - daml-stdlib"
              , "data-dependencies:"
              , "  - " <> show (tmpDir </> "a" </> "a.dar")
              , "build-options: [--target=1.14]"
              ]
          writeFileUTF8 (tmpDir </> "b" </> "B.daml") $ unlines
              [ "module B where"
              , "import A ()"
              ]
          buildProjectError (tmpDir </> "b") "" "Targeted LF version 1.14 but dependencies have newer LF versions"

    , testCaseSteps "Error on newer LF dependency" $ \step -> withTempDir $ \tmpDir -> do
          step "Building 'a"
          createDirectoryIfMissing True (tmpDir </> "a")
          writeFileUTF8 (tmpDir </> "a" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "version: 0.0.1"
              , "name: a"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib]"
              , "build-options: [--target=1.dev]"
              ]
          writeFileUTF8 (tmpDir </> "a" </> "A.daml") $ unlines
              [ "module A where"
              ]
          withCurrentDirectory (tmpDir </> "a") $ callProcessSilent damlc ["build", "-o", tmpDir </> "a" </> "a.dar"]

          step "Building b"
          createDirectoryIfMissing True (tmpDir </> "b")
          writeFileUTF8 (tmpDir </> "b" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "version: 0.0.1"
              , "name: b"
              , "source: ."
              , "dependencies:"
              , "  - daml-prim"
              , "  - daml-stdlib"
              , "  - " <> show (tmpDir </> "a" </> "a.dar")
              , "build-options: [--target=1.14]"
              ]
          writeFileUTF8 (tmpDir </> "b" </> "B.daml") $ unlines
              [ "module B where"
              , "import A ()"
              ]
          buildProjectError (tmpDir </> "b") "" "Targeted LF version 1.14 but dependencies have different LF versions"

    , testCaseSteps "Error on inconsistent LF dependency" $ \step -> withTempDir $ \tmpDir -> do
          step "Building 'a"
          createDirectoryIfMissing True (tmpDir </> "a")
          writeFileUTF8 (tmpDir </> "a" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "version: 0.0.1"
              , "name: a"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib]"
              , "build-options: [--target=1.14]"
              ]
          writeFileUTF8 (tmpDir </> "a" </> "A.daml") $ unlines
              [ "module A where"
              ]
          withCurrentDirectory (tmpDir </> "a") $ callProcessSilent damlc ["build", "-o", tmpDir </> "a" </> "a.dar"]

          step "Building b"
          createDirectoryIfMissing True (tmpDir </> "b")
          writeFileUTF8 (tmpDir </> "b" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "version: 0.0.1"
              , "name: b"
              , "source: ."
              , "dependencies:"
              , "  - daml-prim"
              , "  - daml-stdlib"
              , "  - " <> show (tmpDir </> "a" </> "a.dar")
              , "build-options: [--target=1.dev]"
              ]
          writeFileUTF8 (tmpDir </> "b" </> "B.daml") $ unlines
              [ "module B where"
              , "import A ()"
              ]
          buildProjectError (tmpDir </> "b") "" "Targeted LF version 1.dev but dependencies have different LF versions"

    , testCase "build-options + project-root" $ withTempDir $ \projDir -> do
          createDirectoryIfMissing True (projDir </> "src")
          writeFileUTF8 (projDir </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
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
          (exitCode, _, stderr) <- readProcessWithExitCode damlc ["build", "--project-root", projDir] ""
          exitCode @?= ExitFailure 1
          assertBool ("Expected \"non-exhaustive\" error in stderr but got: " <> show stderr) ("non-exhaustive" `isInfixOf` stderr)

    , testCaseSteps "data-dependencies + exposed-modules" $ \step -> withTempDir $ \projDir -> do
        -- Since the order in which dependencies are processed depends on their PackageIds,
        -- which in turn depends on their contents, we also test buildLfPackageGraph' directly,
        -- in the property-based test below.
          step "Building dependency"
          createDirectoryIfMissing True (projDir </> "dependency")
          writeFileUTF8 (projDir </> "dependency" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
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
            [ "sdk-version: " <> sdkVersion
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
            [ "sdk-version: " <> sdkVersion
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
          -- This tests that a Daml project ('main') can depend on a package ('dependency') which in turn
          -- has a data-dependency on a third package ('data-dependency'). Note that, as usual, all the
          -- transitive dependencies of a dependency need to be stated in the `daml.yaml` file -
          -- in this case, 'data-dependency' is also a data-dependency of 'main'.
          step "Building data-dependency"
          createDirectoryIfMissing True (projDir </> "data-dependency")
          writeFileUTF8 (projDir </> "data-dependency" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
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
            [ "sdk-version: " <> sdkVersion
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
            [ "sdk-version: " <> sdkVersion
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
            [ "sdk-version: " <> sdkVersion
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
            [ "sdk-version: " <> sdkVersion
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
            [ "sdk-version: " <> sdkVersion
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
            [ "sdk-version: " <> sdkVersion
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
            [ "sdk-version: " <> sdkVersion
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
            [ "sdk-version: " <> sdkVersion
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
          buildProjectError (projDir </> "main") "" "cannot satisfy --package dependency-0.0.1"

    , testCaseSteps "module-prefixes" $ \step -> withTempDir $ \dir -> do
          step "Create dep1"
          createDirectoryIfMissing True (dir </> "dep1")
          writeFileUTF8 (dir </> "dep1" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
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
            ["build", "--project-root", dir </> "dep1", "-o", dir </> "dep1" </> "dep1.dar"]
          createDirectoryIfMissing True (dir </> "dep2")
          writeFileUTF8 (dir </> "dep2" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
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
            ["build", "--project-root", dir </> "dep2", "-o", dir </> "dep2" </> "dep2.dar"]
          step "Building main"
          createDirectoryIfMissing True (dir </> "main")
          writeFileUTF8 (dir </> "main" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
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
          callProcessSilent damlc ["build", "--project-root", dir </> "main", "-o", "main.dar"]
    , testCaseSteps "relative output filepath" $ \step -> withTempDir $ \dir -> do
          step "Create project"
          writeFileUTF8 (dir </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
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
            callProcessSilent damlc ["build", "--project-root", dir, "-o", "A.dar"]
          assertFileExists $ outDir </> "A.dar"
    ] <>
    [ lfVersionTests damlc
    ]
  where
      buildProject' :: FilePath -> FilePath -> IO ()
      buildProject' damlc dir = withCurrentDirectory dir $ callProcessSilent damlc ["build"]
      buildProject = buildProject' damlc

      buildProjectError :: FilePath -> String -> String -> IO ()
      buildProjectError dir expectedOut expectedErr = withCurrentDirectory dir $ do
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
lfVersionTests :: FilePath -> TestTree
lfVersionTests damlc = testGroup "LF version dependencies"
    [ testCase ("Package in " <> LF.renderVersion version) $ withTempDir $ \projDir -> do
          writeFileUTF8 (projDir </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
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
                  LF.packageLfVersion pkg <= version
    | version <- LF.supportedOutputVersions
    ]

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
