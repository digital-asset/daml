-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.Packaging (main) where

import qualified "zip-archive" Codec.Archive.Zip as Zip
import Control.Monad.Extra
import Control.Exception.Safe
import DA.Bazel.Runfiles
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.LF.Reader (readDalfManifest, readDalfs, packageName, Dalfs(..), DalfManifest(DalfManifest), mainDalfPath, dalfPaths)
import qualified DA.Daml.LF.Proto3.Archive as LFArchive
import Data.Conduit.Tar.Extra (dropDirectory1)
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Char8 as BSL.Char8
import Data.List.Extra
import Data.Maybe
import System.Directory.Extra
import System.Environment.Blank
import System.Exit
import System.FilePath
import System.Info.Extra
import System.IO.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit

import SdkVersion

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    repl <- locateRunfiles (mainWorkspace </> "daml-lf" </> "repl" </> exe "repl")
    davlDar <- locateRunfiles ("davl" </> "released" </> "davl-v3.dar")
    defaultMain $ tests damlc repl davlDar

tests :: FilePath -> FilePath -> FilePath -> TestTree
tests damlc repl davlDar = testGroup "Packaging" $
    [ testCaseSteps "Build package with dependency" $ \step -> withTempDir $ \tmpDir -> do
        let projectA = tmpDir </> "a"
        let projectB = tmpDir </> "b"
        let aDar = projectA </> ".daml" </> "dist" </> "a-1.0.dar"
        let bDar = projectB </> ".daml" </> "dist" </> "b-1.0.dar"
        step "Creating project a..."
        createDirectoryIfMissing True (projectA </> "daml" </> "Foo" </> "Bar")
        writeFileUTF8 (projectA </> "daml" </> "A.daml") $ unlines
            [ "daml 1.2"
            , "module A (a) where"
            , "a : ()"
            , "a = ()"
            ]
        writeFileUTF8 (projectA </> "daml" </> "Foo" </> "Bar" </> "Baz.daml") $ unlines
            [ "daml 1.2"
            , "module Foo.Bar.Baz (c) where"
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
        assertBool "a-1.0.dar was not created." =<< doesFileExist aDar
        step "Creating project b..."
        createDirectoryIfMissing True (projectB </> "daml")
        writeFileUTF8 (projectB </> "daml" </> "B.daml") $ unlines
            [ "daml 1.2"
            , "module B where"
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
        assertBool "b.dar was not created." =<< doesFileExist bDar
    , testCaseSteps "Dependency on a package with source: A.daml" $ \step -> withTempDir $ \tmpDir -> do
        let projectA = tmpDir </> "a"
        let projectB = tmpDir </> "b"
        let aDar = projectA </> ".daml" </> "dist" </> "a-1.0.dar"
        let bDar = projectB </> ".daml" </> "dist" </> "b-1.0.dar"
        step "Creating project a..."
        createDirectoryIfMissing True projectA
        writeFileUTF8 (projectA </> "A.daml") $ unlines
            [ "daml 1.2"
            , "module A () where"
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
        assertBool "a-1.0.dar was not created." =<< doesFileExist aDar
        step "Creating project b..."
        createDirectoryIfMissing True projectB
        writeFileUTF8 (projectB </> "B.daml") $ unlines
            [ "daml 1.2"
            , "module B where"
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
        assertBool "b.dar was not created." =<< doesFileExist bDar
        darFiles <- Zip.filesInArchive . Zip.toArchive <$> BSL.readFile bDar
        assertBool "b.dar contains source file from package database" $
            not $ any ("A.daml" `isSuffixOf`) darFiles
    , testCase "Top-level source files" $ withTempDir $ \tmpDir -> do
        -- Test that a source file in the project root will be included in the
        -- DAR file. Regression test for #1048.
        let projDir = tmpDir </> "proj"
        createDirectoryIfMissing True projDir
        writeFileUTF8 (projDir </> "A.daml") $ unlines
          [ "daml 1.2"
          , "module A (a) where"
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
        assertBool "proj.dar was not created." =<< doesFileExist dar
        darFiles <- Zip.filesInArchive . Zip.toArchive <$> BSL.readFile dar
        assertBool "A.daml is missing" (any (\f -> takeFileName f == "A.daml") darFiles)

    , testCase "Check that DAR manifest prefers DAML_SDK_VERSION over daml.yaml sdk-version." $ withTempDir $ \tmpDir -> do
        -- Regression test for bug fixed in #3587.
        let projDir = tmpDir </> "proj"
        createDirectoryIfMissing True projDir
        writeFileUTF8 (projDir </> "A.daml") $ unlines
          [ "daml 1.2"
          , "module A (a) where"
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
        assertBool "proj.dar was not created." =<< doesFileExist dar
        archive <- Zip.toArchive <$> BSL.readFile dar
        Just entry <- pure $ Zip.findEntryByPath "META-INF/MANIFEST.MF" archive
        let lines = BSL.Char8.lines (Zip.fromEntry entry)
            expectedLine = "Sdk-Version: " <> BSL.Char8.pack sdkVersion
        assertBool "META-INF/MANIFEST.MF picked up the wrong sdk version" (expectedLine `elem` lines)

    , testCase "Non-root sources files" $ withTempDir $ \projDir -> do
        -- Test that all daml source files get included in the dar if "source" points to a file
        -- rather than a directory
        writeFileUTF8 (projDir </> "A.daml") $ unlines
          [ "daml 1.2"
          , "module A where"
          , "import B ()"
          ]
        writeFileUTF8 (projDir </> "B.daml") $ unlines
          [ "daml 1.2"
          , "module B where"
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
        assertBool "proj-0.1.0.dar was not created." =<< doesFileExist dar
        darFiles <- Zip.filesInArchive . Zip.toArchive <$> BSL.readFile dar
        forM_ ["A.daml", "A.hi", "A.hie", "B.daml", "B.hi", "B.hie"] $ checkDarFile darFiles "."
    , testCase "Root source file in subdir" $ withTempDir $ \projDir -> do
        -- Test that the daml source files get included properly if "source" points to a file
        -- in a subdirectory.
        createDirectoryIfMissing True (projDir </> "A")
        createDirectoryIfMissing True (projDir </> "B")
        writeFileUTF8 (projDir </> "A/B.daml") $ unlines
          [ "daml 1.2"
          , "module A.B where"
          , "import B.C ()"
          ]
        writeFileUTF8 (projDir </> "B/C.daml") $ unlines
          [ "daml 1.2"
          , "module B.C where"
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
        assertBool "proj-0.1.0.dar was not created." =<< doesFileExist dar
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
          [ "daml 1.2"
          , "module A where"
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
        assertBool "proj-0.1.0.dar was not created." =<< doesFileExist dar
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
          [ "daml 1.2"
          , "module A where"
          , "import A.B ()"
          , "import A.C ()"
          ]
        writeFileUTF8 (projDir </> "A/B.daml") $ unlines
          [ "daml 1.2"
          , "module A.B where"
          , "import A.C ()"
          ]
        writeFileUTF8 (projDir </> "A/C.daml") $ unlines
          [ "daml 1.2"
          , "module A.C where"
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
            [ "daml 1.2"
            , "module A (a) where"
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
            [ "daml 1.2"
            , "module A where"
            , "data B = B Int"
            ]
        writeFileUTF8 (projDir </> "src" </> "A" </> "B.daml") $ unlines
            [ "daml 1.2"
            , "module A.B where"
            , "data C = C Int"
            ]
        buildProjectError projDir "" "name collision"

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
            [ "daml 1.2"
            , "module A where"
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
            [ "daml 1.2"
            , "module B where"
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
            [ "daml 1.2"
            , "module C where"
            , "import A"
            , "import B"
            ]
          withCurrentDirectory projC $ callProcessSilent damlc ["build", "-o", "baz.dar"]

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
            [ "daml 1.2"
            , "module A where"
            , "f : Optional a -> a"
            , "f (Some a) = a"
            ]
          (exitCode, _, stderr) <- readProcessWithExitCode damlc ["build", "--project-root", projDir] ""
          exitCode @?= ExitFailure 1
          assertBool ("non-exhaustive error in " <> stderr) ("non-exhaustive" `isInfixOf` stderr)
    ] <>
    [ damlcTestTests damlc
    , lfVersionTests damlc
    , dataDependencyTests damlc repl davlDar
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
              [ "daml 1.2 module A where"]
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

damlcTestTests :: FilePath -> TestTree
damlcTestTests damlc = testGroup "damlc test" $
    [ testCase "damlc test --files outside of project" $ withTempDir $ \projDir -> do
          writeFileUTF8 (projDir </> "Main.daml") $ unlines
            [ "daml 1.2"
            , "module Main where"
            , "test = scenario do"
            , "  assert True"
            ]
          (exitCode, stdout, stderr) <- readProcessWithExitCode damlc ["test", "--files", projDir </> "Main.daml"] ""
          exitCode @?= ExitSuccess
          assertBool ("Succeeding scenario in " <> stdout) ("Main.daml:test: ok" `isInfixOf` stdout)
          stderr @?= ""
    ] <>
    [ testCase ("damlc test " <> unwords (args "") <> " in project") $ withTempDir $ \projDir -> do
          createDirectoryIfMissing True (projDir </> "a")
          writeFileUTF8 (projDir </> "a" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: a"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
          writeFileUTF8 (projDir </> "a" </> "A.daml") $ unlines
            [ "daml 1.2 module A where"
            , "a = 1"
            ]
          callProcessSilent damlc ["build", "--project-root", projDir </> "a"]
          createDirectoryIfMissing True (projDir </> "b")
          writeFileUTF8 (projDir </> "b" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: b"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib, " <> show (projDir </> "a/.daml/dist/a-0.0.1.dar") <> "]"
            ]
          writeFileUTF8 (projDir </> "b" </> "B.daml") $ unlines
            [ "daml 1.2 module B where"
            , "import A"
            , "b = a"
            , "test = scenario do"
            , "  assert True"
            ]
          (exitCode, stdout, stderr) <- readProcessWithExitCode damlc ("test" : "--project-root" : (projDir </> "b") : args projDir) ""
          stderr @?= ""
          assertBool ("Succeeding scenario in " <> stdout) ("B.daml:test: ok" `isInfixOf` stdout)
          exitCode @?= ExitSuccess
    | args <- [\projDir -> ["--files", projDir </> "b" </> "B.daml"], const []]
    ]

darPackageIds :: FilePath -> IO [LF.PackageId]
darPackageIds fp = do
    archive <- Zip.toArchive <$> BSL.readFile fp
    Dalfs mainDalf dalfDeps <- either fail pure $ readDalfs archive
    Right parsedDalfs <- pure $ mapM (LFArchive.decodeArchive LFArchive.DecodeAsMain . BSL.toStrict) $ mainDalf : dalfDeps
    pure $ map fst parsedDalfs


numStablePackages :: LF.Version -> Int
numStablePackages ver
  | ver == LF.version1_6 = 13
  | ver == LF.version1_7 = 14
  | ver == LF.versionDev = 14
  | otherwise = error $ "Unsupported LF version: " <> show ver

dataDependencyTests :: FilePath -> FilePath -> FilePath -> TestTree
dataDependencyTests damlc repl davlDar = testGroup "Data Dependencies" $
    [ testCaseSteps ("Cross DAML-LF version: " <> LF.renderVersion depLfVer <> " -> " <> LF.renderVersion targetLfVer)  $ \step -> withTempDir $ \tmpDir -> do
          let proja = tmpDir </> "proja"
          let projb = tmpDir </> "projb"

          step "Build proja"
          createDirectoryIfMissing True (proja </> "src")
          writeFileUTF8 (proja </> "src" </> "A.daml") $ unlines
              [" daml 1.2"
              , "module A where"
              , "import DA.Text"
              , "data A = A Int deriving Show"
              -- This ensures that we have a reference to daml-stdlib and therefore daml-prim.
              , "x : [Text]"
              , "x = lines \"abc\\ndef\""
              , "data X = X" -- This should generate a DAML-LF enum
              ]
          writeFileUTF8 (proja </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: proja"
              , "version: 0.0.1"
              , "source: src"
              , "dependencies: [daml-prim, daml-stdlib]"
              ]
          withCurrentDirectory proja $ callProcessSilent damlc ["build", "--target=" <> LF.renderVersion depLfVer, "-o", proja </> "proja.dar"]
          projaPkgIds <- darPackageIds (proja </> "proja.dar")
          -- daml-stdlib, daml-prim and proja
          length projaPkgIds @?= numStablePackages depLfVer + 2 + 1

          step "Build projb"
          createDirectoryIfMissing True (projb </> "src")
          writeFileUTF8 (projb </> "src" </> "B.daml") $ unlines
              [ "daml 1.2"
              , "module B where"
              , "import A"
              , "data B = B A"
              , "f : X"
              , "f = X"
              ]
          writeFileUTF8 (projb </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: projb"
              , "version: 0.0.1"
              , "source: src"
              , "dependencies: [daml-prim, daml-stdlib]"
              , "data-dependencies: [" <> show (proja </> "proja.dar") <> "]"
              ]
          -- TODO Users should not have to pass --hide-all-packages, see https://github.com/digital-asset/daml/issues/4094
          withCurrentDirectory projb $ callProcessSilent damlc
            [ "build", "--target=" <> LF.renderVersion targetLfVer, "-o", projb </> "projb.dar"
            , "--hide-all-packages"
            , "--package", "daml-prim"
            , "--package", damlStdlib
            , "--package", "proja-0.0.1"
            ]
          callProcessSilent repl ["validate", projb </> "projb.dar"]
          projbPkgIds <- darPackageIds (projb </> "projb.dar")
          -- daml-prim, daml-stdlib for targetLfVer, daml-prim, daml-stdlib for depLfVer if targetLfVer /= depLfVer, proja and projb
          length projbPkgIds @?= numStablePackages
            targetLfVer + 2 + (if targetLfVer /= depLfVer then 2 else 0) + 1 + 1
          length (filter (`notElem` projaPkgIds) projbPkgIds) @?=
              (numStablePackages targetLfVer - numStablePackages depLfVer) + -- new stable packages
              1 + -- projb
              (if targetLfVer /= depLfVer then 2 else 0) -- different daml-stdlib/daml-prim
    | depLfVer <- LF.supportedOutputVersions
    , targetLfVer <- LF.supportedOutputVersions
    , targetLfVer >= depLfVer
    ] <>
    [ testCaseSteps "Cross-SDK dependency on DAVL" $ \step -> withTempDir $ \tmpDir -> do
          step "Building DAR"
          writeFileUTF8 (tmpDir </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "version: 0.0.1"
              , "name: foobar"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib]"
              , "data-dependencies: [" <> show davlDar <> "]"
              ]
          writeFileUTF8 (tmpDir </> "Main.daml") $ unlines
              [ "daml 1.2"
              , "module Main where"

              , "import DAVL"
              , "import DA.Assert"
              , "import qualified OldStdlib.DA.Internal.Template as OldStdlib"

              -- We exploit internals of the template desugaring here
              -- until we can reconstruct typeclasses or at least functions.
              , "instance HasCreate EmployeeProposal where"
              , "  create = GHC.Types.primitive @\"UCreate\""
              , "instance HasFetch EmployeeProposal where"
              , "  fetch = GHC.Types.primitive @\"UFetch\""
              , "instance HasExercise EmployeeProposal OldStdlib.Archive () where"
              , "  exercise = GHC.Types.primitive @\"UExercise\""

              , "test = scenario do"
              , "  alice <- getParty \"Alice\""
              , "  bob <- getParty \"Bob\""
              , "  eve <- getParty \"eve\""
              , "  let role = EmployeeRole bob alice eve"
              , "  cid <- submit alice $ create (EmployeeProposal role 42)"
              , "  EmployeeProposal{employeeRole} <- submit bob $ fetch cid"
              , "  employee employeeRole === bob"
              , "  company employeeRole === alice"
              , "  () <- submit alice $ exercise cid OldStdlib.Archive"
              , "  pure ()"
              ]
          withCurrentDirectory tmpDir $ callProcessSilent damlc
            [ "build", "-o", tmpDir </> "foobar.dar"
            , "--hide-all-packages"
            , "--package", "daml-prim"
            , "--package", damlStdlib
            -- We need to use the old stdlib for the Archive type
            , "--package", "daml-stdlib-cc6d52aa624250119006cd19d51c60006762bd93ca5a6d288320a703024b33da (DA.Internal.Template as OldStdlib.DA.Internal.Template)"
            , "--package", "davl-0.0.3"
            ]
          step "Validating DAR"
          callProcessSilent repl ["validate", tmpDir </> "foobar.dar"]
          step "Testing scenario"
          callProcessSilent repl ["test", "Main:test", tmpDir </> "foobar.dar"]
    , testCaseSteps "Mixed dependencies and data-dependencies" $ \step -> withTempDir $ \tmpDir -> do
          step "Building 'lib'"
          createDirectoryIfMissing True (tmpDir </> "lib")
          writeFileUTF8 (tmpDir </> "lib" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "version: 0.0.1"
              , "name: lib"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib]"
              ]
          writeFileUTF8 (tmpDir </> "lib" </> "Lib.daml") $ unlines
              [ "daml 1.2 module Lib where"
              , "inc : Int -> Int"
              , "inc = (+ 1)"
              ]
          withCurrentDirectory (tmpDir </> "lib") $ callProcessSilent damlc ["build", "-o", tmpDir </> "lib" </> "lib.dar"]
          libPackageIds <- darPackageIds (tmpDir </> "lib" </> "lib.dar")

          step "Building 'a'"
          createDirectoryIfMissing True (tmpDir </> "a")
          writeFileUTF8 (tmpDir </> "a" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "version: 0.0.1"
              , "name: a"
              , "source: ."
              , "dependencies:"
              , "  - daml-prim"
              , "  - daml-stdlib"
              , "  - " <> show (tmpDir </> "lib" </> "lib.dar")
              ]
          writeFileUTF8 (tmpDir </> "a" </> "A.daml") $ unlines
              [ "daml 1.2 module A where"
              , "import Lib"
              , "two : Int"
              , "two = inc 1"
              ]
          withCurrentDirectory (tmpDir </> "a") $ callProcessSilent damlc ["build", "-o", tmpDir </> "a" </> "a.dar"]
          aPackageIds <- darPackageIds (tmpDir </> "a" </> "a.dar")
          length aPackageIds @?= length libPackageIds + 1

          step "Building 'b'"
          createDirectoryIfMissing True (tmpDir </> "b")
          writeFileUTF8 (tmpDir </> "b" </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "version: 0.0.1"
              , "name: b"
              , "source: ."
              , "dependencies:"
              , "  - daml-prim"
              , "  - daml-stdlib"
              , "  - " <> show (tmpDir </> "lib" </> "lib.dar")
              , "data-dependencies: [" <> show (tmpDir </> "a" </> "a.dar") <> "]"
              ]
          writeFileUTF8 (tmpDir </> "b" </> "B.daml") $ unlines
              [ "daml 1.2 module B where"
              , "import Lib"
              , "import A"
              , "three : Int"
              , "three = inc two"
              ]
          withCurrentDirectory (tmpDir </> "b") $ callProcessSilent damlc ["build", "-o", tmpDir </> "b" </> "b.dar"]
          projbPackageIds <- darPackageIds (tmpDir </> "b" </> "b.dar")
          length projbPackageIds @?= length libPackageIds + 2

          step "Validating DAR"
          callProcessSilent repl ["validate", tmpDir </> "b" </> "b.dar"]
    ] <>
    [ testCaseSteps "Source generation edge cases" $ \step -> withTempDir $ \tmpDir -> do
      writeFileUTF8 (tmpDir </> "Foo.daml") $ unlines
        [ "daml 1.2"
        , "module Foo where"
        , "template Bar"
        , "   with"
        , "     p : Party"
        , "     t : (Text, Int)" -- check for correct tuple type generation
        , "   where"
        , "     signatory p"
        ]
      withCurrentDirectory tmpDir $ do
        step "Compile source to dalf ..."
        callProcessSilent damlc ["compile", "Foo.daml", "-o", "Foo.dalf"]
        step "Regenerate source ..."
        callProcessSilent damlc ["generate-src", "Foo.dalf", "--srcdir=gen"]
        step "Compile generated source ..."
        callProcessSilent
            damlc
            [ "compile"
            , "--generated-src"
            , "gen/Foo.daml"
            , "-o"
            , "FooGen.dalf"
            , "--package"
            , damlStdlib <> " (DA.Internal.LF as CurrentSdk.DA.Internal.LF, DA.Internal.Prelude as CurrentSdk.DA.Internal.Prelude, DA.Internal.Template as CurrentSdk.DA.Internal.Template)"
            , "--package"
            , "daml-prim (DA.Types as CurrentSdk.DA.Types, GHC.Types as CurrentSdk.GHC.Types)"
            ]
        assertBool "FooGen.dalf was not created" =<< doesFileExist "FooGen.dalf"
    ] <>
    [ testCase ("Dalf imports (withArchiveChoice=" <> show withArchiveChoice <> ")") $ withTempDir $ \projDir -> do
        let genSimpleDalfExe
              | isWindows = "generate-simple-dalf.exe"
              | otherwise = "generate-simple-dalf"
        genSimpleDalf <-
            locateRunfiles
            (mainWorkspace </> "compiler" </> "damlc" </> "tests" </> genSimpleDalfExe)
        writeFileUTF8 (projDir </> "daml.yaml") $ unlines
          [ "sdk-version: " <> sdkVersion
          , "name: proj"
          , "version: 0.1.0"
          , "source: ."
          , "dependencies: [daml-prim, daml-stdlib]"
          , "data-dependencies: [simple-dalf-0.0.0.dalf]"
          , "build-options:"
          , "- '--package=" <> damlStdlib <> "'"
          ]
        writeFileUTF8 (projDir </> "A.daml") $ unlines
            [ "daml 1.2"
            , "module A where"
            , "import DA.Assert"
            , "import qualified \"simple-dalf\" Module"
            , "swapParties : Module.Template -> Module.Template"
            , "swapParties (Module.Template a b) = Module.Template b a"
            , "getThis : Module.Template -> Party"
            , "getThis (Module.Template this _) = this"
            , "getArg : Module.Template -> Party"
            , "getArg (Module.Template _ arg) = arg"
            , "test_methods = scenario do"
            , "  alice <- getParty \"Alice\""
            , "  bob <- getParty \"Bob\""
            , "  let t = Module.Template alice bob"
            , "  getThis (Module.Template alice bob) === alice"
            , "  getArg (Module.Template alice bob) === bob"
            , "  getThis (swapParties (Module.Template alice bob)) === bob"
            , "  getArg (swapParties (Module.Template alice bob)) === alice"
            -- Disabled until we support reusing old type classes
            -- , "  let t = newTemplate alice bob"
            -- , "  assert $ signatory t == [alice, bob]"
            -- , "  assert $ observer t == []"
            -- , "  assert $ ensure t"
            -- , "  assert $ agreement t == \"\""
            -- , "  coid <- submit alice $ createTemplate alice alice"
            -- , "  " <> (if withArchiveChoice then "submit" else "submitMustFail") <> " alice $ archive coid"
            -- , "  coid1 <- submit bob $ createTemplate bob bob"
            -- , "  t1 <- submit bob $ fetch coid1"
            -- , "  assert $ signatory t1 == [bob, bob]"
            -- , "  let anyTemplate = toAnyTemplate t1"
            -- , "  let (Some t2 : Optional Module.Template) = fromAnyTemplate anyTemplate"
            -- , "  submit bob $ exercise coid1 Module.Choice2 with choiceArg = ()"
            -- , "  pure ()"
            ]
        withCurrentDirectory projDir $
            callProcessSilent genSimpleDalf $
            ["--with-archive-choice" | withArchiveChoice ] <> ["simple-dalf-0.0.0.dalf"]
        withCurrentDirectory projDir $ callProcess damlc ["build", "--target=1.dev", "--generated-src"]
        let dar = projDir </> ".daml/dist/proj-0.1.0.dar"
        assertBool "proj-0.1.0.dar was not created." =<< doesFileExist dar
        callProcessSilent damlc ["test", "--target=1.dev", "--project-root", projDir, "--generated-src"]
    | withArchiveChoice <- [False, True]
    ] <>
    [ testCaseSteps ("Importing toplevel monomorphic template functions from DAML-LF " <> LF.renderVersion depLfVer <> " to " <> LF.renderVersion targetLfVer) $ \step -> withTempDir $ \tmpDir -> do
          let proja = tmpDir </> "proja"
          let projb = tmpDir </> "projb"

          step "Build proja"
          createDirectoryIfMissing True (proja </> "src")
          writeFileUTF8 (proja </> "src" </> "A.daml") $ unlines
              [ "daml 1.2"
              , "module A where"
              , ""
              , "template T"
              , "  with"
              , "    p : Party"
              , "  where"
              , "    signatory p"
              , ""
              , "createT = create @T"
              , "signatoryT = signatory @T"
              , "archiveT = archive @T"
              ]
          writeFileUTF8 (proja </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: proja"
              , "version: 0.0.1"
              , "source: src"
              , "dependencies: [daml-prim, daml-stdlib]"
              ]
          withCurrentDirectory proja $ callProcessSilent damlc ["build", "--target=" <> LF.renderVersion depLfVer, "-o", proja </> "proja.dar"]

          step "Build projb"
          createDirectoryIfMissing True (projb </> "src")
          writeFileUTF8 (projb </> "src" </> "B.daml") $ unlines
              [ "daml 1.2"
              , "module B where"
              , "import A"
              , "import DA.Assert"
              , ""
              , "test = scenario do"
              , "  alice <- getParty \"Alice\""
              , "  let t = T alice"
              , "  signatoryT t === [alice]"
              , "  cid <- submit alice $ createT t"
              , "  submit alice $ archiveT cid"
              ]
          writeFileUTF8 (projb </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: projb"
              , "version: 0.0.1"
              , "source: src"
              , "dependencies: [daml-prim, daml-stdlib]"
              , "data-dependencies: [" <> show (proja </> "proja.dar") <> "]"
              ]
          withCurrentDirectory projb $ callProcessSilent damlc
            [ "build", "--target=" <> LF.renderVersion targetLfVer, "-o", projb </> "projb.dar"
            , "--hide-all-packages"
            , "--package", "daml-prim"
            , "--package", damlStdlib
            , "--package", "proja-0.0.1"
            ]
          callProcessSilent repl ["validate", projb </> "projb.dar"]

    | depLfVer <- LF.supportedOutputVersions
    , targetLfVer <- LF.supportedOutputVersions
    , targetLfVer >= depLfVer
    ] <>
    [ testCaseSteps ("Typeclass imports from DAML-LF " <> LF.renderVersion depLfVer <> " to " <> LF.renderVersion targetLfVer) $ \step -> withTempDir $ \tmpDir -> do
          let proja = tmpDir </> "proja"
          let projb = tmpDir </> "projb"

          step "Build proja"
          createDirectoryIfMissing True (proja </> "src")
          writeFileUTF8 (proja </> "src" </> "A.daml") $ unlines
              [ "daml 1.2"
              , "module A where"
              , ""
              , "class Foo t where"
              , "  foo : Int -> t"
              , ""
              , "class Foo t => Bar t where"
              , "  bar : Int -> t"
              , ""
              , "usingFoo : Foo t => t"
              , "usingFoo = foo 0"
              ]
          writeFileUTF8 (proja </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: proja"
              , "version: 0.0.1"
              , "source: src"
              , "dependencies: [daml-prim, daml-stdlib]"
              ]
          withCurrentDirectory proja $ callProcessSilent damlc ["build", "--target=" <> LF.renderVersion depLfVer, "-o", proja </> "proja.dar"]

          step "Build projb"
          createDirectoryIfMissing True (projb </> "src")
          writeFileUTF8 (projb </> "src" </> "B.daml") $ unlines
              [ "daml 1.2"
              , "module B where"
              , "import A ( Foo (foo), Bar (..), usingFoo )"
              , ""
              , "data T = T Int"
              , "instance Foo T where"
              , "    foo = T"
              , ""
              , "instance Bar T where"
              , "    bar = T"
              , ""
              , "usingFooIndirectly : T"
              , "usingFooIndirectly = usingFoo"
              ]
          writeFileUTF8 (projb </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: projb"
              , "version: 0.0.1"
              , "source: src"
              , "dependencies: [daml-prim, daml-stdlib]"
              , "data-dependencies: [" <> show (proja </> "proja.dar") <> "]"
              ]
          withCurrentDirectory projb $ callProcessSilent damlc
            [ "build", "--target=" <> LF.renderVersion targetLfVer, "-o", projb </> "projb.dar"
            , "--hide-all-packages"
            , "--package", "daml-prim"
            , "--package", damlStdlib
            , "--package", "proja-0.0.1"
            ]
          callProcessSilent repl ["validate", projb </> "projb.dar"]

    | depLfVer <- LF.supportedOutputVersions
    , targetLfVer <- LF.supportedOutputVersions
    , targetLfVer >= depLfVer
    , LF.supports depLfVer LF.featureTypeSynonyms -- only test for new-style typeclasses
    ]


-- | Only displays stdout and stderr on errors
callProcessSilent :: FilePath -> [String] -> IO ()
callProcessSilent cmd args = do
    (exitCode, out, err) <- readProcessWithExitCode cmd args ""
    unless (exitCode == ExitSuccess) $ do
      hPutStrLn stderr $ "Failure: Command \"" <> cmd <> " " <> unwords args <> "\" exited with " <> show exitCode
      hPutStrLn stderr $ unlines ["stdout:", out]
      hPutStrLn stderr $ unlines ["stderr: ", err]
      exitFailure

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
