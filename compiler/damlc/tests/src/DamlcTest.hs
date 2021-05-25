-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DamlcTest
   ( main
   ) where

import Data.List.Extra (isInfixOf, isSuffixOf)
import System.Directory
import System.Environment.Blank
import System.Exit
import System.FilePath
import System.IO.Extra
import System.Process
import Test.Tasty
import Test.Tasty.HUnit
import qualified "zip-archive" Codec.Archive.Zip as ZA
import qualified Data.ByteString.Lazy as BSL (readFile,writeFile)
import qualified Data.ByteString.Lazy.Char8 as BSL (pack)
import qualified Data.Text.Extended as T

import DA.Bazel.Runfiles
import DA.Test.Process
import DA.Test.Util
import SdkVersion

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    defaultMain (tests damlc)

tests :: FilePath -> TestTree
tests damlc = testGroup "damlc" $ map (\f -> f damlc)
  [ testsForDamlcValidate
  , testsForDamlcTest
  ]

testsForDamlcValidate :: FilePath -> TestTree
testsForDamlcValidate damlc = testGroup "damlc validate-dar"
  [ testCase "Non-existent file" $ do
      (exitCode, stdout, stderr) <- readProcessWithExitCode damlc ["validate-dar", "does-not-exist.dar"] ""
      exitCode @?= ExitFailure 1
      assertInfixOf "does not exist" stderr
      stdout @?= ""

  , testCaseSteps "Good (simple)" $ \step -> withTempDir $ \projDir -> do
      step "prepare"
      writeFileUTF8 (projDir </> "daml.yaml") $ unlines
        [ "sdk-version: " <> sdkVersion
        , "name: good"
        , "version: 0.0.1"
        , "source: ."
        , "dependencies: [daml-prim, daml-stdlib]"
        ]
      writeFileUTF8 (projDir </> "Good.daml") $ unlines
        [ "module Good where"
        , "good = 1 + 2"
        ]
      step "build"
      callProcessSilent damlc ["build", "--project-root", projDir]
      let dar = projDir </> ".daml/dist/good-0.0.1.dar"
      step "validate"
      (exitCode, stdout, stderr) <- readProcessWithExitCode damlc ["validate-dar", dar] ""
      exitCode @?= ExitSuccess
      assertInfixOf "DAR is valid" stdout
      stderr @?= ""

  , testCaseSteps "Good (template)" $ \step -> withTempDir $ \projDir -> do
      step "prepare"
      writeFileUTF8 (projDir </> "daml.yaml") $ unlines
        [ "sdk-version: " <> sdkVersion
        , "name: good"
        , "version: 0.0.1"
        , "source: ."
        , "dependencies: [daml-prim, daml-stdlib]"
        ]
      writeFileUTF8 (projDir </> "Good.daml") $ unlines
        [ "module Good where"
        , "template MyT"
        , "  with"
        , "    myParty : Party"
        , "  where"
        , "    signatory [myParty]"
        ]
      step "build"
      callProcessSilent damlc ["build", "--project-root", projDir]
      let dar = projDir </> ".daml/dist/good-0.0.1.dar"
      step "validate"
      (exitCode, stdout, stderr) <- readProcessWithExitCode damlc ["validate-dar", dar] ""
      exitCode @?= ExitSuccess
      assertInfixOf "DAR is valid" stdout
      stderr @?= ""

  , testCaseSteps "Bad, DAR contains a bad DALF" $ \step -> withTempDir $ \projDir -> do
      step "prepare"
      writeFileUTF8 (projDir </> "daml.yaml") $ unlines
        [ "sdk-version: " <> sdkVersion
        , "name: good"
        , "version: 0.0.1"
        , "source: ."
        , "dependencies: [daml-prim, daml-stdlib]"
        ]
      writeFileUTF8 (projDir </> "Good.daml") $ unlines
        [ "module Good where"
        , "good = 1"
        ]
      step "build"
      callProcessSilent damlc ["build", "--project-root", projDir]
      let origDar = projDir </> ".daml/dist/good-0.0.1.dar"
      let hackedDar = projDir </> "hacked.dar"
      step "unzip/trash-selected.dalf/re-zip"
      modArchiveWith origDar hackedDar $ \archive -> do
        let trash = BSL.pack $ unlines [ "I am not a DALF file." ]
        let nameToTrash = "daml-stdlib-DA-Date-Types"
        foldr ZA.addEntryToArchive archive -- replaces existing entries
          [ ZA.toEntry fp 0 trash | e <- ZA.zEntries archive, let fp = ZA.eRelativePath e, nameToTrash `isInfixOf` fp ]
      step "validate"
      (exitCode, stdout, stderr) <- readProcessWithExitCode damlc ["validate-dar", hackedDar] ""
      exitCode @?= ExitFailure 1
      assertInfixOf "Invalid DAR" stderr
      assertInfixOf "DALF entry cannot be decoded" stderr
      stdout @?= ""

  , testCaseSteps "Bad, unclosed" $ \step -> withTempDir $ \projDir -> do
      step "prepare"
      writeFileUTF8 (projDir </> "daml.yaml") $ unlines
        [ "sdk-version: " <> sdkVersion
        , "name: good"
        , "version: 0.0.1"
        , "source: ."
        , "dependencies: [daml-prim, daml-stdlib]"
        ]
      writeFileUTF8 (projDir </> "Good.daml") $ unlines
        [ "module Good where"
        , "good = 1"
        ]
      step "build"
      callProcessSilent damlc ["build", "--project-root", projDir]
      let origDar = projDir </> ".daml/dist/good-0.0.1.dar"
      let hackedDar = projDir </> "hacked.dar"
      step "unzip/rm-file/re-zip"
      let nameToDrop = "daml-stdlib-DA-Date-Types"
      modArchiveWith origDar hackedDar $ \archive -> do
        foldr ZA.deleteEntryFromArchive archive
          [ fp | e <- ZA.zEntries archive, let fp = ZA.eRelativePath e, nameToDrop `isInfixOf` fp ]
      step "validate"
      (exitCode, stdout, stderr) <- readProcessWithExitCode damlc ["validate-dar", hackedDar] ""
      exitCode @?= ExitFailure 1
      assertInfixOf "Package does not contain" stderr
      assertInfixOf nameToDrop stderr
      stdout @?= ""

  ]

testsForDamlcTest :: FilePath -> TestTree
testsForDamlcTest damlc = testGroup "damlc test" $
    [ testCase "Non-existent file" $ do
          (exitCode, stdout, stderr) <- readProcessWithExitCode damlc ["test", "--files", "foobar"] ""
          stdout @?= ""
          assertInfixOf "does not exist" stderr
          exitCode @?= ExitFailure 1
    , testCase "File with compile error" $ do
        withTempDir $ \dir -> do
            let file = dir </> "Foo.daml"
            T.writeFileUtf8 file $ T.unlines
              [ "module Foo where"
              , "abc"
              ]
            (exitCode, stdout, stderr) <- readProcessWithExitCode damlc ["test", "--files", file] ""
            stdout @?= ""
            assertInfixOf "Parse error" stderr
            exitCode @?= ExitFailure 1
    , testCase "Test coverage report" $ do
        withTempDir $ \dir -> do
            let file = dir </> "Foo.daml"
            T.writeFileUtf8 file $ T.unlines
              [ "module Foo where"
              , "template S with p : Party where"
              , "  signatory p"
              , "template T with p : Party where"
              , "  signatory p"
              , "  choice X : () with controller p"
              , "    do pure ()"
              , "x = scenario do"
              , "      alice <- getParty \"Alice\""
              , "      c <- submit alice $ create T with p = alice"
              , "      submit alice $ exercise c X with"
              ]
            (exitCode, stdout, _stderr) <- readProcessWithExitCode damlc ["test", "--files", file] ""
            exitCode @?= ExitSuccess
            assertBool ("test coverage is reported correctly: " <> stdout)
                       ("test coverage: templates 50%, choices 33%\n" `isSuffixOf` stdout)
    , testCase "Full test coverage report" $ do
        withTempDir $ \dir -> do
            let file = dir </> "Foo.daml"
            T.writeFileUtf8 file $ T.unlines
              [ "module Foo where"
              , "template S with p : Party where"
              , "  signatory p"
              , "template T with p : Party where"
              , "  signatory p"
              , "  choice X : () with controller p"
              , "    do pure ()"
              , "x = scenario do"
              , "      alice <- getParty \"Alice\""
              , "      c <- submit alice $ create T with p = alice"
              , "      submit alice $ exercise c X with"
              ]
            (exitCode, stdout, _stderr) <-
              readProcessWithExitCode damlc ["test", "--show-coverage", "--files", file] ""
            exitCode @?= ExitSuccess
            assertBool
                ("test coverage is reported correctly: " <> stdout)
                (unlines
                     [ "templates never created:"
                     , "Foo:S"
                     , "choices never executed:"
                     , "Foo:S:Archive"
                     , "Foo:T:Archive\n"
                     ] `isSuffixOf`
                 stdout)
    , testCase "Full test coverage report with --all set" $ withTempDir $ \projDir -> do
          createDirectoryIfMissing True (projDir </> "a")
          writeFileUTF8 (projDir </> "a" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: a"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
          writeFileUTF8 (projDir </> "a" </> "A.daml") $ unlines
            [ "module A where"
            , "template U with p : Party where"
            , "  signatory p"
            , "  choice Y : () with controller p"
            , "    do pure ()"
            , "testA = scenario do"
            , "  alice <- getParty \"Alice\""
            , "  c <- submit alice $ create U with p = alice"
            , "  submit alice $ exercise c Y with"
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
          let bFilePath = projDir </> "b" </> "B.daml"
          writeFileUTF8 bFilePath $ unlines
            [ "module B where"
            , "import A"
            , "type C = ContractId U"
            , "template S with p : Party where"
            , "  signatory p"
            , "template T with p : Party where"
            , "  signatory p"
            , "  choice X : () with controller p"
            , "    do pure ()"
            , "x = scenario do"
            , "      alice <- getParty \"Alice\""
            , "      c <- submit alice $ create T with p = alice"
            , "      submit alice $ exercise c X with"
            ]
          (exitCode, stdout, stderr) <- readProcessWithExitCode damlc
            ["test" , "--show-coverage" , "--all" , "--project-root" , projDir </> "b", "--files", bFilePath]
            ""
          stderr @?= ""
          assertBool ("Test coverage is reported correctly: " <> stdout)
            (unlines
                 [ "B.daml:x: ok, 0 active contracts, 2 transactions."
                 , "a:testA: ok, 0 active contracts, 2 transactions."
                 , "test coverage: templates 67%, choices 40%"
                 , "templates never created:"
                 , "B:S"
                 , "choices never executed:"
                 , "B:S:Archive"
                 , "B:T:Archive"
                 , "a:A:U:Archive\n"
                 ] `isSuffixOf`
             stdout)
          exitCode @?= ExitSuccess
    , testCase "Full test coverage report without --all set (but imports)" $ withTempDir $ \projDir -> do
          createDirectoryIfMissing True (projDir </> "a")
          writeFileUTF8 (projDir </> "a" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: a"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
          writeFileUTF8 (projDir </> "a" </> "A.daml") $ unlines
            [ "module A where"
            , "template U with p : Party where"
            , "  signatory p"
            , "  choice Y : () with controller p"
            , "    do pure ()"
            , "testA = scenario do"
            , "  alice <- getParty \"Alice\""
            , "  c <- submit alice $ create U with p = alice"
            , "  submit alice $ exercise c Y with"
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
          let bFilePath = projDir </> "b" </> "B.daml"
          writeFileUTF8 bFilePath $ unlines
            [ "module B where"
            , "import A"
            , "type C = ContractId U"
            , "template S with p : Party where"
            , "  signatory p"
            , "template T with p : Party where"
            , "  signatory p"
            , "  choice X : () with controller p"
            , "    do pure ()"
            , "x = scenario do"
            , "      alice <- getParty \"Alice\""
            , "      c <- submit alice $ create T with p = alice"
            , "      submit alice $ exercise c X with"
            ]
          (exitCode, stdout, stderr) <- readProcessWithExitCode damlc
            ["test" , "--show-coverage" , "--project-root" , projDir </> "b", "--files", bFilePath]
            ""
          stderr @?= ""
          assertBool ("Test coverage is reported correctly: " <> stdout)
            (unlines
                 [ "B.daml:x: ok, 0 active contracts, 2 transactions."
                 , "test coverage: templates 50%, choices 33%"
                 , "templates never created:"
                 , "B:S"
                 , "choices never executed:"
                 , "B:S:Archive"
                 , "B:T:Archive\n"
                 ] `isSuffixOf`
             stdout)
          exitCode @?= ExitSuccess
    , testCase "File with failing scenario" $ do
        withTempDir $ \dir -> do
            let file = dir </> "Foo.daml"
            T.writeFileUtf8 file $ T.unlines
              [ "module Foo where"
              , "x = scenario $ assert False"
              ]
            (exitCode, stdout, stderr) <- readProcessWithExitCode damlc ["test", "--files", file] ""
            stdout @?= ""
            assertInfixOf "Scenario execution failed" stderr
            exitCode @?= ExitFailure 1
    , testCase "damlc test --files outside of project" $ withTempDir $ \projDir -> do
          writeFileUTF8 (projDir </> "Main.daml") $ unlines
            [ "module Main where"
            , "test = scenario do"
            , "  assert True"
            ]
          (exitCode, stdout, stderr) <- readProcessWithExitCode damlc ["test", "--files", projDir </> "Main.daml"] ""
          exitCode @?= ExitSuccess
          assertBool ("Succeeding scenario in " <> stdout) ("Main.daml:test: ok" `isInfixOf` stdout)
          stderr @?= ""
    , testCase "damlc test --project-root relative" $ withTempDir $ \projDir -> do
          createDirectoryIfMissing True (projDir </> "relative")
          writeFileUTF8 (projDir </> "relative" </> "Main.daml") $ unlines
            [ "module Main where"
            , "test = scenario do"
            , "  assert True"
            ]
          writeFileUTF8 (projDir </> "relative" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: relative"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
          withCurrentDirectory projDir $
              callProcessSilent damlc ["test", "--project-root=relative"]
    , testCase "damlc test --project-root proj --junit a.xml" $ withTempDir $ \tempDir -> do
          createDirectoryIfMissing True (tempDir </> "proj")
          writeFileUTF8 (tempDir </> "proj" </> "Main.daml") $ unlines
            [ "module Main where"
            , "test = scenario do"
            , "  assert True"
            ]
          writeFileUTF8 (tempDir </> "proj" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: relative"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib]"
            ]
          withCurrentDirectory tempDir $
              callProcessSilent damlc ["test", "--project-root=proj", "--junit=a.xml"]
          exists <- doesFileExist $ tempDir </> "a.xml"
          -- Check that the junit output was created relative to CWD not the project.
          assertBool "JUnit output was not created" exists
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
            [ "module A where"
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
            [ "module B where"
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


-- | Modify a zip-archive on disk (creating a new file), using a pure `Archive->Archive` function
modArchiveWith :: FilePath -> FilePath -> (ZA.Archive -> ZA.Archive) -> IO ()
modArchiveWith inFile outFile f = do
  archive <- ZA.toArchive <$> BSL.readFile inFile
  BSL.writeFile outFile $ ZA.fromArchive (f archive)
