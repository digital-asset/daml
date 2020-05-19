-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DamlcTest
   ( main
   ) where

import Data.List.Extra (isInfixOf)
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
        [ "daml 1.2 module Good where"
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
        [ "daml 1.2 module Good where"
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
        [ "daml 1.2 module Good where"
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
        [ "daml 1.2 module Good where"
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
              [ "daml 1.2"
              , "module Foo where"
              , "abc"
              ]
            (exitCode, stdout, stderr) <- readProcessWithExitCode damlc ["test", "--files", file] ""
            stdout @?= ""
            assertInfixOf "Parse error" stderr
            exitCode @?= ExitFailure 1
    , testCase "File with failing scenario" $ do
        withTempDir $ \dir -> do
            let file = dir </> "Foo.daml"
            T.writeFileUtf8 file $ T.unlines
              [ "daml 1.2"
              , "module Foo where"
              , "x = scenario $ assert False"
              ]
            (exitCode, stdout, stderr) <- readProcessWithExitCode damlc ["test", "--files", file] ""
            stdout @?= ""
            assertInfixOf "Scenario execution failed" stderr
            exitCode @?= ExitFailure 1
    , testCase "damlc test --files outside of project" $ withTempDir $ \projDir -> do
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


-- | Modify a zip-archive on disk (creating a new file), using a pure `Archive->Archive` function
modArchiveWith :: FilePath -> FilePath -> (ZA.Archive -> ZA.Archive) -> IO ()
modArchiveWith inFile outFile f = do
  archive <- ZA.toArchive <$> BSL.readFile inFile
  BSL.writeFile outFile $ ZA.fromArchive (f archive)
