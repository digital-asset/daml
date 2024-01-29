-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
module DamlcTest
   ( main
   ) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Data.List.Extra (isInfixOf, isPrefixOf)
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
import SdkVersion (SdkVersioned, sdkVersion, withSdkVersions)

main :: IO ()
main = withSdkVersions $ do
    setEnv "TASTY_NUM_THREADS" "1" True
    damlc <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> exe "damlc")
    scriptDar <- locateRunfiles (mainWorkspace </> "daml-script" </> "daml" </> "daml-script.dar")

    -- TODO https://github.com/digital-asset/daml/issues/12051
    --   Remove once Daml-LF 1.15 is the default compiler output
    script1DevDar <- locateRunfiles (mainWorkspace </> "daml-script" </> "daml" </> "daml-script-1.dev.dar")

    defaultMain (tests damlc scriptDar script1DevDar)


-- TODO https://github.com/digital-asset/daml/issues/12051
--   Remove script1DevDar arg once Daml-LF 1.15 is the default compiler output
tests :: SdkVersioned => FilePath -> FilePath -> FilePath -> TestTree
tests damlc scriptDar script1DevDar = testGroup "damlc"
  [ testsForDamlcValidate damlc
  , testsForDamlcTest damlc scriptDar script1DevDar
  ]

testsForDamlcValidate :: SdkVersioned => FilePath -> TestTree
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

  , testCaseSteps "Good (interface)" $ \step -> withTempDir $ \projDir -> do
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
        , "data MyIView = MyIView {}"
        , "interface MyI where"
        , "  viewtype MyIView"
        , "  iMethod : ()"
        ]
      step "build"
      callProcessSilent damlc ["build", "--project-root", projDir]
      let dar = projDir </> ".daml/dist/good-0.0.1.dar"
      step "validate"
      (exitCode, stdout, stderr) <- readProcessWithExitCode damlc ["validate-dar", dar] ""
      stderr @?= ""
      exitCode @?= ExitSuccess
      assertInfixOf "DAR is valid" stdout

  , testCaseSteps "Good (interface instance)" $ \step -> withTempDir $ \projDir -> do
      step "prepare"
      writeFileUTF8 (projDir </> "daml.yaml") $ unlines
        [ "sdk-version: " <> sdkVersion
        , "name: good"
        , "version: 0.0.1"
        , "source: ."
        , "dependencies: [daml-prim, daml-stdlib]"
        ]
      writeFileUTF8 (projDir </> "Interface.daml") $ unlines
        [ "module Interface where"
        , "data MyIView = MyIView {}"
        , "interface MyI where"
        , "  viewtype MyIView"
        , "  iMethod : ()"
        ]
      writeFileUTF8 (projDir </> "Good.daml") $ unlines
        [ "module Good where"
        , "import Interface"
        , "template MyT"
        , "  with"
        , "    myParty : Party"
        , "  where"
        , "    signatory [myParty]"
        , "    interface instance MyI for MyT where"
        , "      view = MyIView"
        , "      iMethod = ()"
        ]
      step "build"
      callProcessSilent damlc ["build", "--project-root", projDir]
      let dar = projDir </> ".daml/dist/good-0.0.1.dar"
      step "validate"
      (exitCode, stdout, stderr) <- readProcessWithExitCode damlc ["validate-dar", dar] ""
      stderr @?= ""
      exitCode @?= ExitSuccess
      assertInfixOf "DAR is valid" stdout

  , testCaseSteps "Good (retroactive interface instance)" $ \step -> withTempDir $ \projDir -> do
      step "prepare"
      writeFileUTF8 (projDir </> "daml.yaml") $ unlines
        [ "sdk-version: " <> sdkVersion
        , "name: good"
        , "version: 0.0.1"
        , "source: ."
        , "dependencies: [daml-prim, daml-stdlib]"
        ]
      writeFileUTF8 (projDir </> "Template.daml") $ unlines
        [ "module Template where"
        , "template MyT"
        , "  with"
        , "    myParty : Party"
        , "  where"
        , "    signatory [myParty]"
        ]
      writeFileUTF8 (projDir </> "Good.daml") $ unlines
        [ "module Good where"
        , "import Template"
        , "data MyIView = MyIView {}"
        , "interface MyI where"
        , "  viewtype MyIView"
        , "  iMethod : ()"
        , "  interface instance MyI for MyT where"
        , "    view = MyIView"
        , "    iMethod = ()"
        ]
      step "build"
      callProcessSilent damlc ["build", "--project-root", projDir]
      let dar = projDir </> ".daml/dist/good-0.0.1.dar"
      step "validate"
      (exitCode, stdout, stderr) <- readProcessWithExitCode damlc ["validate-dar", dar] ""
      stderr @?= ""
      exitCode @?= ExitSuccess
      assertInfixOf "DAR is valid" stdout

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
testsForDamlcTest :: SdkVersioned => FilePath -> FilePath -> FilePath -> TestTree
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
            (exitCode, stdout, stderr) <-
              readProcessWithExitCode
                damlc
                [ "test"
                , "--project-root"
                , dir ]
                ""
            stderr @?= ""
            exitCode @?= ExitSuccess
            let out = lines stdout
            assertBool ("test coverage is reported correctly: " <> stdout)
                       ( unlines
                       [ "Modules internal to this package:"
                       , "- Internal templates"
                       , "  2 defined"
                       , "  1 ( 50.0%) created"
                       , "- Internal template choices"
                       , "  3 defined"
                       , "  1 ( 33.3%) exercised"
                       ]
                       `isInfixOf` stdout)
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
            (exitCode, stdout, stderr) <-
              readProcessWithExitCode
                damlc
                  [ "test"
                  , "--show-coverage"
                  , "--project-root"
                  , dir ]
                  ""
            stderr @?= ""
            exitCode @?= ExitSuccess
            assertBool
                ("template creation coverage is reported correctly: " <> stdout)
                (unlines
                     [ "  internal templates never created: 1"
                     , "    Foo:S"
                     ] `isInfixOf`
                 stdout)
            assertBool
                ("template choice coverage is reported correctly: " <> stdout)
                (unlines
                     [ "  internal template choices never exercised: 2"
                     , "    Foo:S:Archive"
                     , "    Foo:T:Archive"
                     ] `isInfixOf`
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
--              , "  interface instance I for S where"
--              , "    iGetParty = p"
--              , "  interface instance J for S where"
--              , "    jGetParty = p"
--              , "template T with p: Party where"
--              , "  signatory p"
--              , "  interface instance I for T where"
--              , "    iGetParty = p"
--              , "  interface instance J for T where"
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
                 ] `isInfixOf`
             stdout)
          assertBool ("Internal module test coverage is reported correctly: " <> stdout)
            (unlines
                 [ "Modules internal to this package:"
                 , "- Internal templates"
                 , "  2 defined"
                 , "  1 ( 50.0%) created"
                 , "  internal templates never created: 1"
                 , "    B:S"
                 , "- Internal template choices"
                 , "  3 defined"
                 , "  1 ( 33.3%) exercised"
                 , "  internal template choices never exercised: 2"
                 , "    B:S:Archive"
                 , "    B:T:Archive"
                 ] `isInfixOf`
             stdout)
          assertBool ("External module test coverage is reported correctly: " <> stdout)
            (unlines
                 [ "Modules external to this package:"
                 , "- External templates"
                 , "  1 defined"
                 , "  1 (100.0%) created in any tests"
                 , "  0 (  0.0%) created in internal tests"
                 , "  1 (100.0%) created in external tests"
                 , "  external templates never created: 0"
                 , "- External template choices"
                 , "  2 defined"
                 , "  1 ( 50.0%) exercised in any tests"
                 , "  0 (  0.0%) exercised in internal tests"
                 , "  1 ( 50.0%) exercised in external tests"
                 , "  external template choices never exercised: 1"
                 , "    a:A:U:Archive"
                 ] `isInfixOf`
             stdout)
          exitCode @?= ExitSuccess
    , testCase "Filter test coverage report using --coverage-ignore-choice" $ withTempDir $ \projDir -> do
          createDirectoryIfMissing True (projDir </> "a")
          writeFileUTF8 (projDir </> "a" </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: a"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib, " <> show scriptDar <> "]"
            ]
          writeFileUTF8 (projDir </> "a" </> "Mod1.daml") $ unlines
            [ "module Mod1 where"
            , "import Mod1Dep"
            , "import Daml.Script"
            , "template Mod1T1 with p : Party where"
            , "  signatory p"
            , "  choice Mod1T1C1 : () with controller p"
            , "    do pure ()"
            , "testMod1 = script do"
            , "  alice <- allocateParty \"Alice\""
            , "  c <- submit alice $ createCmd Mod1T1 with p = alice"
            , "  submit alice $ exerciseCmd c Mod1T1C1"
            , "  c <- submit alice $ createCmd Mod1DepT1 with p = alice"
            , "  submit alice $ exerciseCmd c Mod1DepT1C1"
            ]
          writeFileUTF8 (projDir </> "a" </> "Mod1Dep.daml") $ unlines
            [ "module Mod1Dep where"
            , "template Mod1DepT1 with p : Party where"
            , "  signatory p"
            , "  choice Mod1DepT1C1 : () with controller p"
            , "    do pure ()"
            , "  choice Mod1DepT1C2 : () with controller p"
            , "    do pure ()"
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
          writeFileUTF8 (projDir </> "b" </> "Mod2.daml") $ unlines
            [ "module Mod2 where"
            , "import Mod2Dep"
            , "import Daml.Script"
            , "template Mod2T1 with p : Party where"
            , "  signatory p"
            , "  choice Mod2T1C1 : () with controller p"
            , "    do pure ()"
            , "testMod1 = script do"
            , "  alice <- allocateParty \"Alice\""
            , "  c <- submit alice $ createCmd Mod2T1 with p = alice"
            , "  submit alice $ exerciseCmd c Mod2T1C1"
            , "  c <- submit alice $ createCmd Mod2DepT1 with p = alice"
            , "  submit alice $ exerciseCmd c Mod2DepT1C1"
            ]
          writeFileUTF8 (projDir </> "b" </> "Mod2Dep.daml") $ unlines
            [ "module Mod2Dep where"
            , "template Mod2DepT1 with p : Party where"
            , "  signatory p"
            , "  choice Mod2DepT1C1 : () with controller p"
            , "    do pure ()"
            , "  choice Mod2DepT1C2 : () with controller p"
            , "    do pure ()"
            ]

          -- test without excluding anything
          (exitCode, stdout, stderr) <-
            readProcessWithExitCode
              damlc
                [ "test"
                , "--show-coverage"
                , "--all"
                , "--project-root"
                , projDir </> "b" ]
                ""
          stderr @?= ""
          assertBool ("Test coverage is reported correctly: " <> stdout)
            (unlines
                 [ "./Mod2.daml:testMod1: ok, 0 active contracts, 4 transactions."
                 , "a:testMod1: ok, 0 active contracts, 4 transactions."
                 ] `isInfixOf`
             stdout)
          assertBool ("Internal module test coverage is reported correctly: " <> stdout)
            (unlines
                 [ "Modules internal to this package:"
                 , "- Internal templates"
                 , "  2 defined"
                 , "  2 (100.0%) created"
                 , "  internal templates never created: 0"
                 , "- Internal template choices"
                 , "  5 defined"
                 , "  2 ( 40.0%) exercised"
                 , "  internal template choices never exercised: 3"
                 , "    Mod2:Mod2T1:Archive"
                 , "    Mod2Dep:Mod2DepT1:Archive"
                 , "    Mod2Dep:Mod2DepT1:Mod2DepT1C2"
                 ] `isInfixOf`
             stdout)
          assertBool ("External module test coverage is reported correctly: " <> stdout)
            (unlines
                 [ "Modules external to this package:"
                 , "- External templates"
                 , "  2 defined"
                 , "  2 (100.0%) created in any tests"
                 , "  0 (  0.0%) created in internal tests"
                 , "  2 (100.0%) created in external tests"
                 , "  external templates never created: 0"
                 , "- External template choices"
                 , "  5 defined"
                 , "  2 ( 40.0%) exercised in any tests"
                 , "  0 (  0.0%) exercised in internal tests"
                 , "  2 ( 40.0%) exercised in external tests"
                 , "  external template choices never exercised: 3"
                 , "    a:Mod1:Mod1T1:Archive"
                 , "    a:Mod1Dep:Mod1DepT1:Archive"
                 , "    a:Mod1Dep:Mod1DepT1:Mod1DepT1C2"
                 ] `isInfixOf`
             stdout)
          exitCode @?= ExitSuccess

          -- test excluding dependency modules
          (exitCode, stdout, stderr) <-
            readProcessWithExitCode
              damlc
                [ "test"
                , "--coverage-ignore-choice"
                , "Dep"
                , "--show-coverage"
                , "--all"
                , "--project-root"
                , projDir </> "b" ]
                ""
          stderr @?= ""
          assertBool ("Exclude Deps: Test coverage is reported correctly: " <> stdout)
            (unlines
                 [ "./Mod2.daml:testMod1: ok, 0 active contracts, 4 transactions."
                 , "a:testMod1: ok, 0 active contracts, 4 transactions."
                 ] `isInfixOf`
             stdout)
          assertBool ("Exclude Deps: Internal module test coverage is reported correctly: " <> stdout)
            (unlines
                 [ "Modules internal to this package:"
                 , "- Internal templates"
                 , "  2 defined"
                 , "  2 (100.0%) created"
                 , "  internal templates never created: 0"
                 , "- Internal template choices"
                 , "  2 defined"
                 , "  1 ( 50.0%) exercised"
                 , "  internal template choices never exercised: 1"
                 , "    Mod2:Mod2T1:Archive"
                 ] `isInfixOf`
             stdout)
          assertBool ("Exclude Deps: External module test coverage is reported correctly: " <> stdout)
            (unlines
                 [ "Modules external to this package:"
                 , "- External templates"
                 , "  2 defined"
                 , "  2 (100.0%) created in any tests"
                 , "  0 (  0.0%) created in internal tests"
                 , "  2 (100.0%) created in external tests"
                 , "  external templates never created: 0"
                 , "- External template choices"
                 , "  2 defined"
                 , "  1 ( 50.0%) exercised in any tests"
                 , "  0 (  0.0%) exercised in internal tests"
                 , "  1 ( 50.0%) exercised in external tests"
                 , "  external template choices never exercised: 1"
                 , "    a:Mod1:Mod1T1:Archive"
                 ] `isInfixOf`
             stdout)
          exitCode @?= ExitSuccess

          -- test excluding Archives
          (exitCode, stdout, stderr) <-
            readProcessWithExitCode
              damlc
                [ "test"
                , "--coverage-ignore-choice"
                , ":Archive$"
                , "--show-coverage"
                , "--all"
                , "--project-root"
                , projDir </> "b" ]
                ""
          stderr @?= ""
          assertBool ("Exclude Archive: Test coverage is reported correctly: " <> stdout)
            (unlines
                 [ "./Mod2.daml:testMod1: ok, 0 active contracts, 4 transactions."
                 , "a:testMod1: ok, 0 active contracts, 4 transactions."
                 ] `isInfixOf`
             stdout)
          assertBool ("Exclude Archive: Internal module test coverage is reported correctly: " <> stdout)
            (unlines
                 [ "Modules internal to this package:"
                 , "- Internal templates"
                 , "  2 defined"
                 , "  2 (100.0%) created"
                 , "  internal templates never created: 0"
                 , "- Internal template choices"
                 , "  3 defined"
                 , "  2 ( 66.7%) exercised"
                 , "  internal template choices never exercised: 1"
                 , "    Mod2Dep:Mod2DepT1:Mod2DepT1C2"
                 ] `isInfixOf`
             stdout)
          assertBool ("Exclude Archive: External module test coverage is reported correctly: " <> stdout)
            (unlines
                 [ "Modules external to this package:"
                 , "- External templates"
                 , "  2 defined"
                 , "  2 (100.0%) created in any tests"
                 , "  0 (  0.0%) created in internal tests"
                 , "  2 (100.0%) created in external tests"
                 , "  external templates never created: 0"
                 , "- External template choices"
                 , "  3 defined"
                 , "  2 ( 66.7%) exercised in any tests"
                 , "  0 (  0.0%) exercised in internal tests"
                 , "  2 ( 66.7%) exercised in external tests"
                 , "  external template choices never exercised: 1"
                 , "    a:Mod1Dep:Mod1DepT1:Mod1DepT1C2"
                 ] `isInfixOf`
             stdout)
          exitCode @?= ExitSuccess

          -- test excluding both deps and Archives
          (exitCode, stdout, stderr) <-
            readProcessWithExitCode
              damlc
                [ "test"
                , "--coverage-ignore-choice"
                , ":Archive$"
                , "--coverage-ignore-choice"
                , "Dep"
                , "--show-coverage"
                , "--all"
                , "--project-root"
                , projDir </> "b" ]
                ""
          stderr @?= ""
          assertBool ("Exclude Archive and Dep: Test coverage is reported correctly: " <> stdout)
            (unlines
                 [ "./Mod2.daml:testMod1: ok, 0 active contracts, 4 transactions."
                 , "a:testMod1: ok, 0 active contracts, 4 transactions."
                 ] `isInfixOf`
             stdout)
          assertBool ("Exclude Archive and Dep: Internal module test coverage is reported correctly: " <> stdout)
            (unlines
                 [ "Modules internal to this package:"
                 , "- Internal templates"
                 , "  2 defined"
                 , "  2 (100.0%) created"
                 , "  internal templates never created: 0"
                 , "- Internal template choices"
                 , "  1 defined"
                 , "  1 (100.0%) exercised"
                 , "  internal template choices never exercised: 0"
                 ] `isInfixOf`
             stdout)
          assertBool ("Exclude Archive and Dep: External module test coverage is reported correctly: " <> stdout)
            (unlines
                 [ "Modules external to this package:"
                 , "- External templates"
                 , "  2 defined"
                 , "  2 (100.0%) created in any tests"
                 , "  0 (  0.0%) created in internal tests"
                 , "  2 (100.0%) created in external tests"
                 , "  external templates never created: 0"
                 , "- External template choices"
                 , "  1 defined"
                 , "  1 (100.0%) exercised in any tests"
                 , "  0 (  0.0%) exercised in internal tests"
                 , "  1 (100.0%) exercised in external tests"
                 , "  external template choices never exercised: 0"
                 ] `isInfixOf`
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
              , "Modules internal to this package:"
              , "- Internal templates"
              , "  0 defined"
              , "  0 (100.0%) created"
              , "- Internal template choices"
              , "  0 defined"
              , "  0 (100.0%) exercised"
              ] `isInfixOf` stdout)
          exitCode @?= ExitSuccess
    , testCase "Serialized results aggregate correctly" $ withTempDir $ \projDir -> do
          createDirectoryIfMissing True (projDir </> "a")
          writeFileUTF8 (projDir </> "daml.yaml") $ unlines
            [ "sdk-version: " <> sdkVersion
            , "name: a"
            , "version: 0.0.1"
            , "source: ."
            , "dependencies: [daml-prim, daml-stdlib, " <> show scriptDar <> "]"
            ]
          writeFileUTF8 (projDir </> "Main.daml") $ unlines
            [ "module Main where"
            , ""
            , "import Daml.Script"
            , ""
            , "template T1 with"
            , "    party: Party"
            , "  where"
            , "    signatory party"
            , "    nonconsuming choice T1C1 : ()"
            , "      controller party"
            , "      do pure ()"
            , "    nonconsuming choice T1C2 : ()"
            , "      controller party"
            , "      do pure ()"
            , ""
            , "template T2 with"
            , "    party: Party"
            , "  where"
            , "    signatory party"
            , "    nonconsuming choice T2C1 : ()"
            , "      controller party"
            , "      do pure ()"
            , "    nonconsuming choice T2C2 : ()"
            , "      controller party"
            , "      do pure ()"
            , ""
            , "testT1 : Script ()"
            , "testT1 = do"
            , "  alice <- allocateParty \"Alice\""
            , "  t1 <- alice `submit` createCmd (T1 alice)"
            , "  alice `submit` exerciseCmd t1 T1C1"
            , "  alice `submit` exerciseCmd t1 Archive"
            , "  pure ()"
            , ""
            , "testBoth : Script ()"
            , "testBoth = do"
            , "  alice <- allocateParty \"Alice\""
            , "  t1 <- alice `submit` createCmd (T1 alice)"
            , "  t2 <- alice `submit` createCmd (T2 alice)"
            , "  alice `submit` exerciseCmd t1 Archive"
            , "  alice `submit` exerciseCmd t2 Archive"
            , "  pure ()"
            ]
          callProcessSilent
            damlc
            [ "build"
            , "--project-root"
            , projDir
            ]
          (exitCode, stdoutTestT1Run, stderr) <-
            readProcessWithExitCode
              damlc
                [ "test"
                , "--project-root", projDir
                , "-p", "testT1"
                , "--save-coverage", projDir </> "testT1-results"
                ]
                ""
          stderr @?= ""
          exitCode @?= ExitSuccess

          (exitCode, stdoutTestBothRun, stderr) <-
            readProcessWithExitCode
              damlc
                [ "test"
                , "--project-root", projDir
                , "-p", "testBoth"
                , "--save-coverage", projDir </> "testBoth-results"
                ]
                ""
          stderr @?= ""
          exitCode @?= ExitSuccess

          (exitCode, stdoutTestAllRun, stderr) <-
            readProcessWithExitCode
              damlc
                [ "test"
                , "--project-root", projDir
                , "--save-coverage", projDir </> "test-all-results"
                ]
                ""
          stderr @?= ""
          exitCode @?= ExitSuccess

          (exitCode, stdoutEmptyAggregate, stderr) <-
            readProcessWithExitCode
              damlc
                [ "test"
                , "--project-root", projDir
                , "--load-coverage-only"
                ]
                ""
          stderr @?= ""
          exitCode @?= ExitSuccess

          assertBool ("Coverage with only aggregation and no load-coverage is empty: " <> stdoutEmptyAggregate)
            (unlines
              [ "Modules internal to this package:"
              , "- Internal templates"
              , "  0 defined"
              , "  0 (100.0%) created"
              , "- Internal template choices"
              , "  0 defined"
              , "  0 (100.0%) exercised"
              ] `isInfixOf` stdoutEmptyAggregate)

          (exitCode, stdoutAggregateTestT1, stderr) <-
            readProcessWithExitCode
              damlc
                [ "test"
                , "--project-root", projDir
                , "--load-coverage-only"
                , "--load-coverage", projDir </> "testT1-results"
                ]
                ""
          stderr @?= ""
          exitCode @?= ExitSuccess

          assertBool ("Coverage with only aggregation of testT1 results is correct: " <> stdoutAggregateTestT1)
            (unlines
              [ "Modules internal to this package:"
              , "- Internal templates"
              , "  2 defined"
              , "  1 ( 50.0%) created"
              , "- Internal template choices"
              , "  6 defined"
              , "  2 ( 33.3%) exercised"
              ] `isInfixOf` stdoutAggregateTestT1)

          assertBool ("Coverage with only aggregation of testT1 results is equivalent to original testT1 output: " <> stdoutAggregateTestT1 <> stdoutTestT1Run)
            (stdoutAggregateTestT1 `isInfixOf` stdoutTestT1Run)

          (exitCode, stdoutAggregateTestBoth, stderr) <-
            readProcessWithExitCode
              damlc
                [ "test"
                , "--project-root", projDir
                , "--load-coverage-only"
                , "--load-coverage", projDir </> "testBoth-results"
                ]
                ""
          stderr @?= ""
          exitCode @?= ExitSuccess

          assertBool ("Coverage with only aggregation of testBoth results is correct: " <> stdoutAggregateTestBoth)
            (unlines
              [ "Modules internal to this package:"
              , "- Internal templates"
              , "  2 defined"
              , "  2 (100.0%) created"
              , "- Internal template choices"
              , "  6 defined"
              , "  2 ( 33.3%) exercised"
              ] `isInfixOf` stdoutAggregateTestBoth)

          assertBool ("Coverage with only aggregation of testBoth results is equivalent to original testBoth output: " <> stdoutAggregateTestBoth <> stdoutTestBothRun)
            (stdoutAggregateTestBoth `isInfixOf` stdoutTestBothRun)

          (exitCode, stdoutAggregateTestT1AndTestBoth, stderr) <-
            readProcessWithExitCode
              damlc
                [ "test"
                , "--project-root", projDir
                , "--load-coverage-only"
                , "--load-coverage", projDir </> "testT1-results"
                , "--load-coverage", projDir </> "testBoth-results"
                ]
                ""
          stderr @?= ""
          exitCode @?= ExitSuccess

          assertBool ("Coverage with aggregation of independent results from testT1 and testBoth is correct: " <> stdoutAggregateTestT1AndTestBoth)
            (unlines
              [ "Modules internal to this package:"
              , "- Internal templates"
              , "  2 defined"
              , "  2 (100.0%) created"
              , "- Internal template choices"
              , "  6 defined"
              , "  3 ( 50.0%) exercised"
              ] `isInfixOf` stdoutAggregateTestT1AndTestBoth)

          assertBool ("Coverage with aggregation of independent results from testT1 and testBoth is equivalent to running both tests: " <> stdoutAggregateTestT1AndTestBoth <> stdoutTestAllRun)
            (stdoutAggregateTestT1AndTestBoth `isInfixOf` stdoutTestAllRun)

          (exitCode, stdoutAggregateTestT1AndTestBothReordered, stderr) <-
            readProcessWithExitCode
              damlc
                [ "test"
                , "--project-root", projDir
                , "--load-coverage-only"
                , "--load-coverage", projDir </> "testBoth-results" -- reorder the way in which we read the results, should be identical
                , "--load-coverage", projDir </> "testT1-results"
                ]
                ""
          stderr @?= ""
          exitCode @?= ExitSuccess

          assertBool ("Coverage with aggregation of independent results from testT1 and testBoth is order invariant: " <> stdoutAggregateTestT1AndTestBoth <> stdoutAggregateTestT1AndTestBothReordered)
            (stdoutAggregateTestT1AndTestBothReordered == stdoutAggregateTestT1AndTestBoth)

          (exitCode, stdoutAggregateTestT1WithRunTestBoth, stderr) <-
            readProcessWithExitCode
              damlc
                [ "test"
                , "--project-root", projDir
                , "--load-coverage", projDir </> "testT1-results"
                , "-p", "testBoth"
                ]
                ""
          stderr @?= ""
          exitCode @?= ExitSuccess

          assertBool ("Coverage with reading results from testT1 and running testBoth is equivalent to running both tests: " <> stdoutAggregateTestT1WithRunTestBoth <> stdoutAggregateTestT1AndTestBoth)
            (stdoutAggregateTestT1AndTestBoth `isInfixOf` stdoutAggregateTestT1WithRunTestBoth)

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
                 , "Modules internal to this package:"
                 , "- Internal templates"
                 , "  2 defined"
                 , "  1 ( 50.0%) created"
                 , "  internal templates never created: 1"
                 , "    B:S"
                 , "- Internal template choices"
                 , "  3 defined"
                 , "  1 ( 33.3%) exercised"
                 , "  internal template choices never exercised: 2"
                 , "    B:S:Archive"
                 , "    B:T:Archive"
                 ] `isInfixOf`
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
            (exitCode, stdout, stderr) <-
              readProcessWithExitCode
                damlc
                [ "test"
                , "--project-root"
                , dir ]
                ""
            stderr @?= ""
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
