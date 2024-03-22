-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DamlcTest
   ( main
   ) where

import Data.List.Extra (isInfixOf, isSuffixOf, isPrefixOf)
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
    scriptDar <- locateRunfiles (mainWorkspace </> "daml-script" </> "daml" </> "daml-script.dar")

    -- TODO https://github.com/digital-asset/daml/issues/12051
    --   Remove once Daml-LF 1.15 is the default compiler output
    script1DevDar <- locateRunfiles (mainWorkspace </> "daml-script" </> "daml" </> "daml-script-1.dev.dar")

    defaultMain (tests damlc scriptDar script1DevDar)


-- TODO https://github.com/digital-asset/daml/issues/12051
--   Remove script1DevDar arg once Daml-LF 1.15 is the default compiler output
tests :: FilePath -> FilePath -> FilePath -> TestTree
tests damlc scriptDar script1DevDar = testGroup "damlc"
  [ testsForDamlcValidate damlc
  , testsForDamlcTest damlc scriptDar script1DevDar
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

-- TODO https://github.com/digital-asset/daml/issues/12051
--   Remove script1DevDar arg once Daml-LF 1.15 is the default compiler output
testsForDamlcTest :: FilePath -> FilePath -> FilePath -> TestTree
testsForDamlcTest damlc scriptDar _ = testGroup "damlc test" $
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
    , testCase "Test coverage report and summary" $ do
        withTempDir $ \dir -> do
            writeFileUTF8 (dir </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: test-coverage-report"
              , "version: 0.0.1"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib, " <> show scriptDar <> "]"
              ]
            let file = dir </> "Foo.daml"
            T.writeFileUtf8 file $ T.unlines
              [ "module Foo where"
              , "import Daml.Script"
              , "template S with p : Party where"
              , "  signatory p"
              , "template T with p : Party where"
              , "  signatory p"
              , "  choice X : () with controller p"
              , "    do pure ()"
              , "x = script do"
              , "      alice <- allocateParty \"Alice\""
              , "      c <- submit alice $ createCmd T with p = alice"
              , "      submit alice $ exerciseCmd c X"
              ]
            (exitCode, stdout, _stderr) <-
              readProcessWithExitCode
                damlc
                [ "test"
                , "--project-root"
                , dir ]
                ""
            exitCode @?= ExitSuccess
            let out = lines stdout
            assertBool ("test coverage is reported correctly: " <> out!!4)
                       ("test coverage: templates 50%, choices 33%" == (out!!4))
            assertBool ("test summary is reported correctly: " <> out!!1)
                       ("Test Summary" `isPrefixOf` (out!!1))
            assertBool ("test summary is reported correctly: " <> out!!3)
                       ("./Foo.daml:x: ok, 0 active contracts, 2 transactions." == (out!!3))
    , testCase "Full test coverage report" $ do
        withTempDir $ \dir -> do
            writeFileUTF8 (dir </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: full-test-coverage-report"
              , "version: 0.0.1"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib, " <> show scriptDar <> "]"
              ]
            let file = dir </> "Foo.daml"
            T.writeFileUtf8 file $ T.unlines
              [ "module Foo where"
              , "import Daml.Script"
              , "template S with p : Party where"
              , "  signatory p"
              , "template T with p : Party where"
              , "  signatory p"
              , "  choice X : () with controller p"
              , "    do pure ()"
              , "x = script do"
              , "      alice <- allocateParty \"Alice\""
              , "      c <- submit alice $ createCmd T with p = alice"
              , "      submit alice $ exerciseCmd c X"
              ]
            (exitCode, stdout, _stderr) <-
              readProcessWithExitCode
                damlc
                  [ "test"
                  , "--show-coverage"
                  , "--project-root"
                  , dir ]
                  ""
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
-- TODO: https://github.com/digital-asset/daml/issues/13044
-- reactive the test
--    , testCase "Full test coverage report with interfaces" $ do
--        withTempDir $ \dir -> do
--            writeFileUTF8 (dir </> "daml.yaml") $ unlines
--              [ "sdk-version: " <> sdkVersion
--              , "name: full-test-coverage-report-with-interfaces"
--              -- TODO https://github.com/digital-asset/daml/issues/12051
--              --   Remove once Daml-LF 1.15 is the default compiler output
--              , "build-options: [ --target=1.dev ]"
--              , "version: 0.0.1"
--              , "source: ."
--              -- TODO https://github.com/digital-asset/daml/issues/12051
--              --   Replace with scriptDar once Daml-LF 1.15 is the default compiler output
--              , "dependencies: [daml-prim, daml-stdlib, " <> show script1DevDar <> "]"
--              ]
--            let file = dir </> "Foo.daml"
--            T.writeFileUtf8 file $ T.unlines
--              [ "module Foo where"
--              , "import Daml.Script"
--
--              , "interface I where"
--              , "  iGetParty: Party"
--              , "  choice IC: ()"
--              , "    controller iGetParty this"
--              , "    do pure ()"
--              , "interface J where"
--              , "  jGetParty: Party"
--              , "  choice JC: ()"
--              , "    controller jGetParty this"
--              , "    do pure ()"
--
--              , "template S with p: Party where"
--              , "  signatory p"
--              , "  implements I where"
--              , "    iGetParty = p"
--              , "  implements J where"
--              , "    jGetParty = p"
--              , "template T with p: Party where"
--              , "  signatory p"
--              , "  implements I where"
--              , "    iGetParty = p"
--              , "  implements J where"
--              , "    jGetParty = p"
--
--              , "x = script do"
--              , "      alice <- allocateParty \"Alice\""
--              , "      c <- submit alice $ createCmd T with p = alice"
--              , "      submit alice $ exerciseCmd c IC"
--              ]
--            (exitCode, stdout, stderr) <-
--              readProcessWithExitCode
--                damlc
--                  [ "test"
--                  , "--show-coverage"
--                  , "--project-root"
--                  , dir ]
--                  ""
--            stderr @?= ""
--            exitCode @?= ExitSuccess
--            assertBool
--                ("test coverage is reported correctly: " <> stdout)
--                (unlines
--                     [ "test coverage: templates 50%, choices 17%"
--                     , "templates never created:"
--                     , "Foo:S"
--                     , "choices never executed:"
--                     , "Foo:S:Archive"
--                     , "Foo:S:IC (inherited from Foo:I)"
--                     , "Foo:S:JC (inherited from Foo:J)"
--                     , "Foo:T:Archive"
--                     , "Foo:T:JC (inherited from Foo:J)\n"
--                     ] `isSuffixOf`
--                 stdout)
    , testCase "Full test coverage report with --all set" $ withTempDir $ \projDir -> do
          createDirectoryIfMissing True (projDir </> "a")
          writeFileUTF8 (projDir </> "a" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: a"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib, " <> show scriptDar <> "]"
            ]
          writeFileUTF8 (projDir </> "a" </> "A.daml") $ unlines
            [ "module A where"
            , "import Daml.Script"
            , "template U with p : Party where"
            , "  signatory p"
            , "  choice Y : () with controller p"
            , "    do pure ()"
            , "testA = script do"
            , "  alice <- allocateParty \"Alice\""
            , "  c <- submit alice $ createCmd U with p = alice"
            , "  submit alice $ exerciseCmd c Y"
            ]
          callProcessSilent
            damlc
            [ "build"
            , "--project-root"
            , projDir </> "a"
            ]
          createDirectoryIfMissing True (projDir </> "b")
          writeFileUTF8 (projDir </> "b" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: b"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib, " <> show (projDir </> "a/.daml/dist/a-0.0.1.dar") <> ", " <> show scriptDar <> "]"
            ]
          let bFilePath = projDir </> "b" </> "B.daml"
          writeFileUTF8 bFilePath $ unlines
            [ "module B where"
            , "import Daml.Script"
            , "import A"
            , "type C = ContractId U"
            , "template S with p : Party where"
            , "  signatory p"
            , "template T with p : Party where"
            , "  signatory p"
            , "  choice X : () with controller p"
            , "    do pure ()"
            , "x = script do"
            , "      alice <- allocateParty \"Alice\""
            , "      c <- submit alice $ createCmd T with p = alice"
            , "      submit alice $ exerciseCmd c X"
            ]
          (exitCode, stdout, stderr) <-
            readProcessWithExitCode
              damlc
                [ "test"
                , "--show-coverage"
                , "--all"
                , "--project-root"
                , projDir </> "b"
                , "--files"
                , bFilePath ]
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
    , testCase "Filter tests with --test-pattern" $ withTempDir $ \projDir -> do
          createDirectoryIfMissing True (projDir </> "a")
          writeFileUTF8 (projDir </> "a" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: a"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib, " <> show scriptDar <> "]"
            ]
          writeFileUTF8 (projDir </> "a" </> "A.daml") $ unlines
            [ "module A where"
            , "import Daml.Script"
            , "test_needleHaystack = script $ pure ()"
            ]
          callProcessSilent
            damlc
            [ "build"
            , "--project-root"
            , projDir </> "a"
            ]
          createDirectoryIfMissing True (projDir </> "b")
          writeFileUTF8 (projDir </> "b" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: b"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib, " <> show (projDir </> "a/.daml/dist/a-0.0.1.dar") <> ", " <> show scriptDar <> "]"
            ]
          let bFilePath = projDir </> "b" </> "B.daml"
          writeFileUTF8 bFilePath $ unlines
            [ "module B where"
            , "import Daml.Script"
            , "import qualified A"
            , "test = script $ pure ()"
            , "needleHaystack = script $ pure ()"
            , "test1 = A.test_needleHaystack"
            ]
          (exitCode, stdout, stderr) <-
            readProcessWithExitCode
              damlc
                [ "test"
                , "--test-pattern"
                , "needle"
                , "--all"
                , "--project-root"
                , projDir </> "b"
                , "--files"
                , bFilePath ]
                ""
          stderr @?= ""
          assertBool ("Test coverage is reported correctly: " <> stdout)
            (unlines
              ["B.daml:needleHaystack: ok, 0 active contracts, 0 transactions."
              , "a:test_needleHaystack: ok, 0 active contracts, 0 transactions."
              , "test coverage: templates 100%, choices 100%"
              ] `isSuffixOf` stdout)
          exitCode @?= ExitSuccess
    , testCase "Full test coverage report without --all set (but imports)" $ withTempDir $ \projDir -> do
          createDirectoryIfMissing True (projDir </> "a")
          writeFileUTF8 (projDir </> "a" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: a"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib, " <> show scriptDar <> "]"
            ]
          writeFileUTF8 (projDir </> "a" </> "A.daml") $ unlines
            [ "module A where"
            , "import Daml.Script"
            , "template U with p : Party where"
            , "  signatory p"
            , "  choice Y : () with controller p"
            , "    do pure ()"
            , "testA = script do"
            , "  alice <- allocateParty \"Alice\""
            , "  c <- submit alice $ createCmd U with p = alice"
            , "  submit alice $ exerciseCmd c Y with"
            ]
          callProcessSilent
            damlc
            [ "build"
            , "--project-root"
            , projDir </> "a" ]
          createDirectoryIfMissing True (projDir </> "b")
          writeFileUTF8 (projDir </> "b" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: b"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib, " <> show (projDir </> "a/.daml/dist/a-0.0.1.dar") <> ", " <> show scriptDar <> "]"
            ]
          let bFilePath = projDir </> "b" </> "B.daml"
          writeFileUTF8 bFilePath $ unlines
            [ "module B where"
            , "import Daml.Script"
            , "import A"
            , "type C = ContractId U"
            , "template S with p : Party where"
            , "  signatory p"
            , "template T with p : Party where"
            , "  signatory p"
            , "  choice X : () with controller p"
            , "    do pure ()"
            , "x = script do"
            , "      alice <- allocateParty \"Alice\""
            , "      c <- submit alice $ createCmd T with p = alice"
            , "      submit alice $ exerciseCmd c X"
            ]
          (exitCode, stdout, stderr) <-
            readProcessWithExitCode
              damlc
              ["test"
              , "--show-coverage"
              , "--project-root"
              , projDir </> "b"
              , "--files"
              , bFilePath]
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
    , testCase "File with failing script" $ do
        withTempDir $ \dir -> do
            writeFileUTF8 (dir </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: test-failing-script"
              , "version: 0.0.1"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib, " <> show scriptDar <> "]"
              ]
            let file = dir </> "Foo.daml"
            T.writeFileUtf8 file $ T.unlines
              [ "module Foo where"
              , "import Daml.Script"
              , "x = script $ assert False"
              , "y = script $ assert True"
              ]
            (exitCode, stdout, stderr) <-
              readProcessWithExitCode
                damlc
                ["test"
                , "--project-root"
                , dir
                , "--files"
                , file ]
                ""
            assertInfixOf "Script execution failed" stderr
            exitCode @?= ExitFailure 1

            let out = lines stdout
            assertInfixOf "Test Summary" (out!!1)
            assertInfixOf "Foo.daml:y: ok" (out!!3)
            assertInfixOf "Foo.daml:x: failed" (out!!4)
    , testCase "damlc test --files outside of project" $
        -- TODO: does this test make sense with a daml.yaml file?
        withTempDir $ \projDir -> do
          writeFileUTF8 (projDir </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: test-files-outside-project"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib, " <> show scriptDar <> "]"
            ]
          writeFileUTF8 (projDir </> "Main.daml") $ unlines
            [ "module Main where"
            , "import Daml.Script"
            , "test = script do"
            , "  assert True"
            ]
          (exitCode, stdout, stderr) <-
            readProcessWithExitCode
              damlc
                [ "test"
                , "--project-root"
                , projDir
                , "--files"
                , projDir </> "Main.daml" ]
                ""
          exitCode @?= ExitSuccess
          assertBool ("Succeeding script in " <> stdout) ("Main.daml:test: ok" `isInfixOf` stdout)
          stderr @?= ""
    , testCase "damlc test --project-root relative" $ withTempDir $ \projDir -> do
          createDirectoryIfMissing True (projDir </> "relative")
          writeFileUTF8 (projDir </> "relative" </> "Main.daml") $ unlines
            [ "module Main where"
            , "import Daml.Script"
            , "test = script do"
            , "  assert True"
            ]
          writeFileUTF8 (projDir </> "relative" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: relative"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib, " <> show scriptDar <> "]"
            ]
          withCurrentDirectory projDir $
            callProcessSilent
              damlc
              [ "test"
              , "--project-root=relative" ]
    , testCase "damlc test --project-root proj --junit a.xml" $ withTempDir $ \tempDir -> do
          createDirectoryIfMissing True (tempDir </> "proj")
          writeFileUTF8 (tempDir </> "proj" </> "Main.daml") $ unlines
            [ "module Main where"
            , "import Daml.Script"
            , "test = script do"
            , "  assert True"
            ]
          writeFileUTF8 (tempDir </> "proj" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: relative"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib, " <> show scriptDar <> "]"
            ]
          withCurrentDirectory tempDir $
            callProcessSilent
              damlc
                [ "test"
                , "--project-root=proj"
                , "--junit=a.xml"
                ]
          exists <- doesFileExist $ tempDir </> "a.xml"
          -- Check that the junit output was created relative to CWD not the project.
          assertBool "JUnit output was not created" exists
    , testCase "grpc-max-message-size applies to service results" $ withTempDir $ \tempDir -> do
        writeFileUTF8 (tempDir </> "Main.daml") $ unlines
          [ "module Main where"
          , "import Daml.Script"
          , "test = script do"
          , "  pure $ replicate 500000 \"hello world\""
          ]
        writeFileUTF8 (tempDir </> "daml.yaml") $ unlines
          [ "sdk-version: " <> sdkVersion
          , "name: foobar"
          , "version: 0.0.1"
          , "source: ."
          , "dependencies: [daml-prim, daml-stdlib, " <> show scriptDar <> "]"
          , "scenario-service:"
          , "  grpc-max-message-size: 10000000"
          ]
        callProcessSilent
          damlc
            [ "test"
            , "--project-root"
            , tempDir ]
    , testCase "Rollback archive" $ do
        withTempDir $ \dir -> do
            writeFileUTF8 (dir </> "daml.yaml") $ unlines
              [ "sdk-version: " <> sdkVersion
              , "name: test-rollback-archive"
              , "version: 0.0.1"
              , "source: ."
              , "dependencies: [daml-prim, daml-stdlib, " <> show scriptDar <> "]"
              ]
            let file = dir </> "Main.daml"
            T.writeFileUtf8 file $ T.unlines
                  [ "module Main where"
                  , "import Daml.Script"
                  , "import DA.Exception"
                  , ""
                  , "template Foo"
                  , "  with"
                  , "    owner : Party"
                  , "  where"
                  , "    signatory owner"
                  , "    nonconsuming choice Catch : ()"
                  , "      controller owner"
                  , "        do try do"
                  , "              exercise self Fail"
                  , "            catch"
                  , "              GeneralError _ -> pure ()"
                  , "    nonconsuming choice Fail : ()"
                  , "      controller owner"
                  , "        do  exercise self Archive"
                  , "            abort \"\""
                  , ""
                  , "test: Script ()"
                  , "test = script do"
                  , "  a <- allocateParty \"a\""
                  , "  c <- submit a do"
                  , "    createCmd Foo with"
                  , "      owner = a"
                  , "  submit a do"
                  , "    exerciseCmd c Catch"
                  , "  submit a do"
                  , "    exerciseCmd c Catch"
                  ]
            (exitCode, stdout, _stderr) <-
              readProcessWithExitCode
                damlc
                [ "test"
                , "--project-root"
                , dir ]
                ""
            exitCode @?= ExitSuccess
            let out = lines stdout
            out!!3 @?= "./Main.daml:test: ok, 1 active contracts, 3 transactions."
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
            , "dependencies: [daml-prim, daml-stdlib, " <> show (projDir </> "a/.daml/dist/a-0.0.1.dar") <> ", " <> show scriptDar <> "]"
            ]
          writeFileUTF8 (projDir </> "b" </> "B.daml") $ unlines
            [ "module B where"
            , "import Daml.Script"
            , "import A"
            , "b = a"
            , "test = script do"
            , "  assert True"
            ]
          (exitCode, stdout, stderr) <-
            readProcessWithExitCode
              damlc
              ( "test"
              : "--project-root"
              : (projDir </> "b")
              : args projDir )
              ""
          stderr @?= ""
          assertBool ("Succeeding script in " <> stdout) ("B.daml:test: ok" `isInfixOf` stdout)
          exitCode @?= ExitSuccess
    | args <- [\projDir -> ["--files", projDir </> "b" </> "B.daml"], const []]
    ]


-- | Modify a zip-archive on disk (creating a new file), using a pure `Archive->Archive` function
modArchiveWith :: FilePath -> FilePath -> (ZA.Archive -> ZA.Archive) -> IO ()
modArchiveWith inFile outFile f = do
  archive <- ZA.toArchive <$> BSL.readFile inFile
  BSL.writeFile outFile $ ZA.fromArchive (f archive)
