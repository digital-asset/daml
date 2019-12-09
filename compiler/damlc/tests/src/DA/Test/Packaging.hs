-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.Packaging (main) where

import qualified "zip-archive" Codec.Archive.Zip as Zip
import Control.Monad.Extra
import Control.Exception.Safe
import DA.Bazel.Runfiles
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
    defaultMain $ tests damlc

tests :: FilePath -> TestTree
tests damlc = testGroup "Packaging"
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
            , "build-options:"
            , "- '--package=(\"a-1.0\", True, [(\"A\", \"C\")])'"
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
            , "import A()" -- TODO [#3252]: Remove this import, so we can catch the name collision even when there isn't a strict dependency.
            , "data C = C Int"
            ]
        buildProjectError projDir "" "name collision between module A.B and variant A:B"

    , dataDependencyTests damlc
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

dataDependencyTests :: FilePath -> TestTree
dataDependencyTests damlc = testGroup "Data Dependencies" $
    (do
      withArchiveChoice <- [False,True] -- run two variations of the test
      return $ testCase ("Dalf imports (withArchiveChoice=" <> show withArchiveChoice <> ")") $ withTempDir $ \projDir -> do
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
          , "- '--package=(\"daml-stdlib-" <> sdkVersion <> "\", True, [])'"
          ]
        writeFileUTF8 (projDir </> "A.daml") $ unlines
            [ "daml 1.2"
            , "module A where"
            , "import qualified \"instances-simple-dalf\" Module"
            , "import DA.Internal.Template (toAnyTemplate, fromAnyTemplate)"
            , "newTemplate : Party -> Party -> Module.Template"
            , "newTemplate p1 p2 = Module.Template with Module.this = p1, Module.arg = p2"
            , "newChoice : Module.Choice"
            , "newChoice = Module.Choice ()"
            , "createTemplate : Party -> Party -> Update (ContractId Module.Template)"
            , "createTemplate p1 p2 = create $ newTemplate p1 p2"
            , "fetchTemplate : ContractId Module.Template -> Update Module.Template"
            , "fetchTemplate = fetch"
            , "archiveTemplate : ContractId Module.Template -> Update ()"
            , "archiveTemplate = archive"
            , "signatoriesTemplate : Module.Template -> [Party]"
            , "signatoriesTemplate = signatory"
            , "observersTemplate : Module.Template -> [Party]"
            , "observersTemplate = observer"
            , "ensureTemplate : Module.Template -> Bool"
            , "ensureTemplate = ensure"
            , "agreementTemplate : Module.Template -> Text"
            , "agreementTemplate = agreement"
            , "toAnyTemplateTemplate : Module.Template -> AnyTemplate"
            , "toAnyTemplateTemplate = toAnyTemplate"
            , "fromAnyTemplateTemplate : AnyTemplate -> Optional Module.Template"
            , "fromAnyTemplateTemplate = fromAnyTemplate"
            , "test_methods = scenario do"
            , "  alice <- getParty \"Alice\""
            , "  bob <- getParty \"Bob\""
            , "  let t = newTemplate alice bob"
            , "  assert $ signatory t == [alice, bob]"
            , "  assert $ observer t == []"
            , "  assert $ ensure t"
            , "  assert $ agreement t == \"\""
            , "  coid <- submit alice $ createTemplate alice alice"
            , "  " <> (if withArchiveChoice then "submit" else "submitMustFail") <> " alice $ archive coid"
            , "  coid1 <- submit bob $ createTemplate bob bob"
            , "  t1 <- submit bob $ fetch coid1"
            , "  assert $ signatory t1 == [bob, bob]"
            , "  let anyTemplate = toAnyTemplate t1"
            , "  let (Some t2 : Optional Module.Template) = fromAnyTemplate anyTemplate"
            , "  submit bob $ exercise coid1 Module.Choice2 with choiceArg = ()"
            , "  pure ()"
            ]
        withCurrentDirectory projDir $
            callProcessSilent genSimpleDalf $
            ["--with-archive-choice" | withArchiveChoice ] <> ["simple-dalf-0.0.0.dalf"]
        withCurrentDirectory projDir $ callProcessSilent damlc ["build", "--target=1.dev", "--generated-src"]
        let dar = projDir </> ".daml/dist/proj-0.1.0.dar"
        assertBool "proj-0.1.0.dar was not created." =<< doesFileExist dar
        callProcessSilent damlc ["test", "--target=1.dev", "--project-root", projDir, "--generated-src"]
    ) <>
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
            , "--package=(" <> show damlStdlib <>
              ", False, [(\"DA.Internal.LF\", \"Sdk.DA.Internal.LF\"), (\"DA.Internal.Prelude\", \"Sdk.DA.Internal.Prelude\")])"
            ]
        assertBool "FooGen.dalf was not created" =<< doesFileExist "FooGen.dalf"
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
