-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.ScriptService (main) where

import Control.Exception
import Control.Monad
import DA.Bazel.Runfiles
import DA.Cli.Damlc.Packaging
import qualified DA.Daml.LF.Ast.Version as LF
import DA.Daml.LF.PrettyScenario (prettyScenarioError, prettyScenarioResult)
import qualified DA.Daml.LF.ScenarioServiceClient as SS
import DA.Daml.Options.Types
import DA.Daml.Package.Config
import DA.Daml.Project.Types
import DA.Pretty
import qualified DA.Service.Logger as Logger
import qualified DA.Service.Logger.Impl.IO as Logger
import Data.Default (def)
import qualified Data.HashSet as HashSet
import Data.List
import qualified Data.Text as T
import Development.IDE.Core.Debouncer (noopDebouncer)
import Development.IDE.Core.FileStore (makeVFSHandle, setBufferModified)
import Development.IDE.Core.IdeState.Daml (getDamlIdeState)
import Development.IDE.Core.OfInterest (setFilesOfInterest)
import Development.IDE.Core.RuleTypes.Daml (RunScripts (..), VirtualResource (..))
import Development.IDE.Core.Rules.Daml (worldForFile)
import Development.IDE.Core.Service (getDiagnostics, runActionSync, shutdown)
import Development.IDE.Core.Shake (use)
import Development.IDE.Types.Diagnostics (showDiagnostics)
import Development.IDE.Types.Location (toNormalizedFilePath')
import Development.IDE.Types.Options (IdeReportProgress (..))
import qualified Language.Haskell.LSP.Types as LSP
import SdkVersion
import System.Directory.Extra
import System.Environment.Blank
import System.FilePath
import System.IO.Extra
import Test.Tasty
import Test.Tasty.HUnit
import Text.Regex.TDFA

main :: IO ()
main =
  withTempDir $ \dir -> do
    withCurrentDirectory dir $ do
      setEnv "TASTY_NUM_THREADS" "1" True

      -- Package DB setup, we only need to do this once so we do it at the beginning.
      scriptDar <- locateRunfiles $ mainWorkspace </> "daml-script/daml/daml-script.dar"
      writeFileUTF8 "daml.yaml" $
        unlines
          [ "sdk-version: " <> sdkVersion,
            "name: script-service",
            "version: 0.0.1",
            "source: .",
            "dependencies:",
            "- daml-prim",
            "- daml-stdlib",
            "- " <> show scriptDar
          ]
      withPackageConfig (ProjectPath ".") $ \PackageConfigFields {..} ->
        createProjectPackageDb
          (toNormalizedFilePath' dir)
          options
          pSdkVersion
          pModulePrefixes
          pDependencies
          pDataDependencies

      logger <- Logger.newStderrLogger Logger.Debug "script-service"

      -- Spinning up the scenario service is expensive so we do it once at the beginning.
      SS.withScenarioService LF.versionDefault logger scenarioConfig $ \scriptService ->
        defaultMain $
          testGroup
            "Script Service"
            [ testCase "createCmd + exerciseCmd + createAndExerciseCmd" $ do
                rs <-
                  runScripts
                    scriptService
                    [ "module Test where",
                      "import Daml.Script",
                      "template T",
                      "  with",
                      "    p : Party",
                      "    v : Int",
                      "  where",
                      "    signatory p",
                      "    choice C : Int",
                      "      controller p",
                      "      do pure v",
                      "testCreate = do",
                      "  p <- allocateParty \"p\"",
                      "  submit p $ createCmd (T p 42)",
                      "testExercise = do",
                      "  p <- allocateParty \"p\"",
                      "  cid <- submit p $ createCmd (T p 42)",
                      "  submit p $ exerciseCmd cid C",
                      "testCreateAndExercise = do",
                      "  p <- allocateParty \"p\"",
                      "  submit p $ createAndExerciseCmd (T p 42) C",
                      "testMulti = do",
                      "  p <- allocateParty \"p\"",
                      "  (cid1, cid2) <- submit p $ (,) <$> createCmd (T p 23) <*> createCmd (T p 42)",
                      "  submit p $ (,) <$> exerciseCmd cid1 C <*> exerciseCmd cid2 C",
                      "testArchive = do",
                      "  p <- allocateParty \"p\"",
                      "  cid <- submit p (createCmd (T p 42))",
                      "  submit p (archiveCmd cid)"
                    ]
                expectScriptSuccess rs (vr "testCreate") $ \r ->
                  matchRegex r "Active contracts:  #0:0\n\nReturn value: #0:0\n\n$"
                expectScriptSuccess rs (vr "testExercise") $ \r ->
                  matchRegex r "Active contracts: \n\nReturn value: 42\n\n$"
                expectScriptSuccess rs (vr "testCreateAndExercise") $ \r ->
                  matchRegex r "Active contracts: \n\nReturn value: 42\n\n$"
                expectScriptSuccess rs (vr "testMulti") $ \r ->
                  matchRegex r $
                    T.unlines
                      [ "Active contracts: ",
                        "",
                        "Return value:",
                        "  DA\\.Types:Tuple2@[a-z0-9]+ with",
                        "    _1 = 23; _2 = 42",
                        ""
                      ]
                expectScriptSuccess rs (vr "testArchive") $ \r ->
                  matchRegex r "'p' exercises Archive on #0:0",
              testCase "exerciseByKeyCmd" $ do
                rs <-
                  runScripts
                    scriptService
                    [ "module Test where",
                      "import DA.Assert",
                      "import Daml.Script",
                      "template WithKey",
                      "  with",
                      "    p : Party",
                      "    v : Int",
                      "  where",
                      "    signatory p",
                      "    key p : Party",
                      "    maintainer key",
                      "    choice C : Int",
                      "      controller p",
                      "      do pure v",
                      "testExerciseByKey = do",
                      "  p <- allocateParty \"p\"",
                      "  submit p $ createCmd (WithKey p 42)",
                      "  submit p $ exerciseByKeyCmd @WithKey p C"
                    ]
                expectScriptSuccess rs (vr "testExerciseByKey") $ \r ->
                  matchRegex r "Active contracts: \n\nReturn value: 42\n\n$",
              testCase "failing transactions" $ do
                rs <-
                  runScripts
                    scriptService
                    [ "module Test where",
                      "import Daml.Script",
                      "template MultiSignatory",
                      "  with",
                      "    p1 : Party",
                      "    p2 : Party",
                      "  where",
                      "    signatory p1, p2",
                      "template TKey",
                      "  with",
                      "    p : Party",
                      "  where",
                      "    signatory p",
                      "    key p : Party",
                      "    maintainer key",
                      "template Helper",
                      "  with",
                      "    p : Party",
                      "  where",
                      "    signatory p",
                      "    choice Fetch : TKey",
                      "      with cid : ContractId TKey",
                      "      controller p",
                      "      do fetch cid",
                      "    choice Error : ()",
                      "      controller p",
                      "      do error \"errorCrash\"",
                      "    choice Abort : ()",
                      "      controller p",
                      "      do abort \"abortCrash\"",
                      "testMissingAuthorization = do",
                      "  p1 <- allocateParty \"p1\"",
                      "  p2 <- allocateParty \"p2\"",
                      "  submit p1 (createCmd (MultiSignatory p1 p2))",
                      "testDuplicateKey = do",
                      "  p <- allocateParty \"p\"",
                      "  submit p (createCmd (TKey p))",
                      "  submit p (createCmd (TKey p))",
                      "testNotVisible = do",
                      "  p1 <- allocateParty \"p1\"",
                      "  p2 <- allocateParty \"p2\"",
                      "  cid <- submit p1 (createCmd (TKey p1))",
                      "  helperCid <- submit p2 (createCmd (Helper p2))",
                      "  submit p2 (exerciseCmd helperCid (Fetch cid))",
                      "testError = do",
                      "  p <- allocateParty \"p\"",
                      "  cid <- submit p (createCmd (Helper p))",
                      "  submit p (exerciseCmd cid Error)",
                      "testAbort = do",
                      "  p <- allocateParty \"p\"",
                      "  cid <- submit p (createCmd (Helper p))",
                      "  submit p (exerciseCmd cid Abort)",
                      "testPartialSubmit = do",
                      "  p1 <- allocateParty \"p1\"",
                      "  p2 <- allocateParty \"p2\"",
                      "  submit p1 (createCmd (Helper p1))",
                      "  submit p2 (createCmd (Helper p1))",
                      "testPartialSubmitMustFail = do",
                      "  p1 <- allocateParty \"p1\"",
                      "  p2 <- allocateParty \"p2\"",
                      "  submit p1 (createCmd (Helper p1))",
                      "  submitMustFail p2 (createCmd (Helper p2))"
                    ]
                expectScriptFailure rs (vr "testMissingAuthorization") $ \r ->
                  matchRegex r "failed due to a missing authorization from 'p2'"
                expectScriptFailure rs (vr "testDuplicateKey") $ \r ->
                  matchRegex r "due to unique key violation for key"
                expectScriptFailure rs (vr "testNotVisible") $ \r ->
                  matchRegex r "Attempt to fetch or exercise a contract not visible to the reading parties"
                expectScriptFailure rs (vr "testError") $ \r ->
                  matchRegex r "Aborted:  errorCrash"
                expectScriptFailure rs (vr "testAbort") $ \r ->
                  matchRegex r "Aborted:  abortCrash"
                expectScriptFailure rs (vr "testPartialSubmit") $ \r ->
                  matchRegex r  $ T.unlines
                    [ "Scenario execution failed on commit at Test:57:3:"
                    , ".*"
                    , ".*failed due to a missing authorization.*"
                    , ".*"
                    , ".*"
                    , ".*"
                    , "Partial transaction:"
                    , "  Sub-transactions:"
                    , "     0"
                    , ".*create Test:Helper.*"
                    ]
                expectScriptFailure rs (vr "testPartialSubmitMustFail") $ \r ->
                  matchRegex r $ T.unlines
                    [ "Scenario execution failed on commit at Test:62:3:"
                    , "  Aborted:  Expected submit to fail but it succeeded"
                    , ".*"
                    , ".*"
                    , ".*"
                    , "Partial transaction:"
                    , "  Sub-transactions:"
                    , "     0"
                    , ".*create Test:Helper.*"
                    ]
                pure (),
              testCase "query" $
                do
                  rs <-
                    runScripts
                      scriptService
                      [ "module Test where",
                        "import Daml.Script",
                        "import DA.Assert",
                        "template T1",
                        "  with",
                        "    p : Party, v: Int",
                        "  where",
                        "    signatory p",
                        "template T2",
                        "  with",
                        "    p : Party, v : Int",
                        "  where",
                        "    signatory p",
                        "template TShared",
                        "  with",
                        "    p1 : Party",
                        "    p2 : Party",
                        "  where",
                        "    signatory p1",
                        "    observer p2",
                        "template Divulger",
                        "  with",
                        "    divulgee : Party",
                        "    sig : Party",
                        "  where",
                        "    signatory divulgee",
                        "    observer sig",
                        "    nonconsuming choice Divulge : T1",
                        "      with cid : ContractId T1",
                        "      controller sig",
                        "      do fetch cid",
                        "testQueryInactive = do",
                        "  p <- allocateParty \"p\"",
                        "  cid1_1 <- submit p (createCmd (T1 p 42))",
                        "  cid1_2 <- submit p (createCmd (T1 p 43))",
                        "  cid2_1 <- submit p (createCmd (T2 p 23))",
                        "  cid2_2 <- submit p (createCmd (T2 p 24))",
                        "  r1 <- query @T1 p",
                        "  r1 === [(cid1_1, T1 p 42), (cid1_2, T1 p 43)]",
                        "  r2 <- query @T2 p",
                        "  r2 === [(cid2_1, T2 p 23), (cid2_2, T2 p 24)]",
                        "  submit p (exerciseCmd cid1_2 Archive)",
                        "  submit p (exerciseCmd cid2_2 Archive)",
                        "  r1 <- query @T1 p",
                        "  r1 === [(cid1_1, T1 p 42)]",
                        "  r2 <- query @T2 p",
                        "  r2 === [(cid2_1, T2 p 23)]",
                        "testQueryVisibility = do",
                        "  p1 <- allocateParty \"p1\"",
                        "  p2 <- allocateParty \"p2\"",
                        "  divulger <- submit p2 (createCmd (Divulger p2 p1))",
                        "  cidT1p1 <- submit p1 (createCmd (T1 p1 42))",
                        "  cidT1p2 <- submit p2 (createCmd (T1 p2 23))",
                        "  cidSharedp1 <- submit p1 (createCmd (TShared p1 p2))",
                        "  cidSharedp2 <- submit p2 (createCmd (TShared p2 p1))",
                        "  t1p1 <- query @T1 p1",
                        "  t1p1 === [(cidT1p1, T1 p1 42)]",
                        "  t1p2 <- query @T1 p2",
                        "  t1p2 === [(cidT1p2, T1 p2 23)]",
                        -- Divulgence should not influence query result
                        "  submit p1 $ exerciseCmd divulger (Divulge cidT1p1)",
                        "  t1p2 <- query @T1 p2",
                        "  t1p2 === [(cidT1p2, T1 p2 23)]",
                        "  sharedp1 <- query @TShared p1",
                        "  sharedp1 === [(cidSharedp1, TShared p1 p2), (cidSharedp2, TShared p2 p1)]",
                        "  sharedp2 <- query @TShared p2",
                        "  sharedp2 === [(cidSharedp1, TShared p1 p2), (cidSharedp2, TShared p2 p1)]"
                      ]
                  expectScriptSuccess rs (vr "testQueryInactive") $ \r ->
                    matchRegex r "Active contracts:  #2:0, #0:0\n\n"
                  expectScriptSuccess rs (vr "testQueryVisibility") $ \r ->
                    matchRegex r "Active contracts:  #4:0, #3:0, #2:0, #0:0, #1:0\n\n"
                  pure (),
              testCase "submitMustFail" $ do
                  rs <-
                    runScripts
                      scriptService
                      [ "module Test where",
                        "import Daml.Script",
                        "template T",
                        "  with",
                        "    p : Party",
                        "  where",
                        "    signatory p",
                        "    choice AssertFail : ()",
                        "      controller p",
                        "      do assert False",
                        "testAssertFail = do",
                        "  p <- allocateParty \"p\"",
                        "  cid <- submit p (createCmd (T p))",
                        "  submitMustFail p (exerciseCmd cid AssertFail)",
                        -- Make sure that the script service still works afterwards.
                        "  cid <- submit p (createCmd (T p))",
                        "  pure ()"
                      ]
                  expectScriptSuccess rs (vr "testAssertFail") $ \r ->
                    matchRegex r "Active contracts:  #0:0, #1:0\n\nReturn value: {}\n\n$"
                  pure (),
              testCase "contract keys" $ do
                rs <-
                  runScripts
                    scriptService
                    [ "module Test where",
                      "import Daml.Script",
                      "import DA.Assert",
                      "template T",
                      "  with",
                      "    p : Party",
                      "  where",
                      "    signatory p",
                      "    key p : Party",
                      "    maintainer key",
                      "template Helper",
                      "  with",
                      "    p : Party",
                      "  where",
                      "    signatory p",
                      "    nonconsuming choice FetchKey : (ContractId T, T)",
                      "      with k : Party",
                      "      controller p",
                      "      do fetchByKey @T p",
                      "testFetchByKey = do",
                      "  p <- allocateParty \"p\"",
                      "  cid <- submit p (createCmd (T p))",
                      "  helper <- submit p (createCmd (Helper p))",
                      "  (fetchedCid, t) <- submit p (exerciseCmd helper (FetchKey p))",
                      "  fetchedCid === cid",
                      "  t === T p"
                    ]
                expectScriptSuccess rs (vr "testFetchByKey") $ \r ->
                  matchRegex r "Active contracts:  #0:0, #1:0\n\n",
              testCase "time" $ do
                rs <-
                  runScripts
                    scriptService
                    [ "module Test where",
                      "import Daml.Script",
                      "import DA.Date",
                      "import DA.Time",
                      "import DA.Assert",
                      "template T",
                      "  with",
                      "    p : Party",
                      "  where",
                      "    signatory p",
                      "    nonconsuming choice GetTime : Time",
                      "      controller p",
                      "      do getTime",
                      "testTime = do",
                      "  t0 <- getTime",
                      "  setTime (time (date 2000 Feb 2) 0 1 2)",
                      "  t1 <- getTime",
                      "  pure (t0, t1)",
                      "testChoiceTime = do",
                      "  p <- allocateParty \"p\"",
                      "  cid <- submit p $ createCmd T with p",
                      "  t0 <- submit p $ exerciseCmd cid GetTime",
                      "  setTime (time (date 2000 Feb 2) 0 1 2)",
                      "  t1 <- submit p $ exerciseCmd cid GetTime",
                      "  pure (t0, t1)",
                      "testPassTime = do",
                      "  p <- allocateParty \"p\"",
                      "  t0 <- getTime",
                      "  passTime (days 1)",
                      "  t1 <- getTime",
                      "  t1 === addRelTime t0 (days 1)",
                      "  cid <- submit p $ createCmd (T p)",
                      "  passTime (days (-1))",
                      "  submit p $ exerciseCmd cid Archive"
                    ]
                expectScriptSuccess rs (vr "testTime") $ \r ->
                    matchRegex r $
                      T.unlines
                        [ "Return value:",
                          "  DA\\.Types:Tuple2@[a-z0-9]+ with",
                          "    _1 = 1970-01-01T00:00:00Z; _2 = 2000-02-02T00:01:02Z",
                          ""
                        ]
                expectScriptSuccess rs (vr "testChoiceTime") $ \r ->
                    matchRegex r $
                      T.unlines
                        [ "Return value:",
                          "  DA\\.Types:Tuple2@[a-z0-9]+ with",
                          "    _1 = 1970-01-01T00:00:00Z; _2 = 2000-02-02T00:01:02Z",
                          ""
                        ]
                expectScriptFailure rs (vr "testPassTime") $ \r ->
                    matchRegex r "Attempt to fetch or exercise a contract not yet effective"
            ,
              testCase "partyManagement" $ do
                rs <-
                  runScripts
                    scriptService
                    [ "module Test where",
                      "import DA.Assert",
                      "import DA.Optional",
                      "import Daml.Script",
                      "template T",
                      "  with",
                      "    owner : Party",
                      "    observer : Party",
                      "  where",
                      "    signatory owner",
                      "    observer observer",
                      "    choice InventObserver : ContractId T with name : Text",
                      "      controller owner",
                      "        do create this { observer = fromSome $ partyFromText name }",
                      "partyManagement = do",
                      "  alice <- allocatePartyWithHint \"alice\" (PartyIdHint \"alice\")",
                      "  alice1 <- allocateParty \"alice\"",
                      "  t1 <- submit alice $ createCmd T { owner = alice, observer = alice1 }",
                      "  t2 <- submit alice $ exerciseCmd t1 (InventObserver \"bob\")",
                      "  bob1 <- allocateParty \"bob\"",
                      "  details <- listKnownParties",
                      "  assertEq (length details) 4",
                      "  let [aliceDetails, alice1Details, bobDetails, bob1Details] = details",
                      "  assertEq aliceDetails (PartyDetails alice (Some \"alice\") True)",
                      "  assertEq alice1Details (PartyDetails alice1 (Some \"alice\") True)",
                      "  assertEq bobDetails (PartyDetails (fromSome $ partyFromText \"bob\") None True)",
                      "  assertEq bob1Details (PartyDetails bob1 (Some \"bob\") True)",
                      "duplicateAllocateWithHint = do",
                      "  _ <- allocatePartyWithHint \"alice\" (PartyIdHint \"alice\")",
                      "  _ <- allocatePartyWithHint \"alice\" (PartyIdHint \"alice\")",
                      "  pure ()",
                      "duplicatePartyFromText = do",
                      "  alice <- allocateParty \"alice\"",
                      "  _ <- submit alice $ createAndExerciseCmd (T alice alice) (InventObserver \"bob\")",
                      "  _ <- allocatePartyWithHint \"bob\" (PartyIdHint \"bob\")",
                      "  pure ()"
                    ]
                expectScriptSuccess rs (vr "partyManagement") $ \r ->
                  matchRegex r "Active contracts:  #1:1\n\nReturn value: {}\n\n$"
                expectScriptFailure rs (vr "duplicateAllocateWithHint") $ \r ->
                  matchRegex r "Tried to allocate a party that already exists:  alice"
                expectScriptFailure rs (vr "duplicatePartyFromText") $ \r ->
                  matchRegex r "Tried to allocate a party that already exists:  bob"
            , testCase "queryContractId/Key" $ do
                rs <-
                  runScripts
                    scriptService
                    [ "module Test where"
                    , "import DA.Assert"
                    , "import Daml.Script"
                    , "template T"
                    , "  with"
                    , "    owner : Party"
                    , "    observer : Party"
                    , "  where"
                    , "    key (owner, observer) : (Party, Party)"
                    , "    maintainer key._1"
                    , "    signatory owner"
                    , "    observer observer"
                    , "template Divulger"
                    , "  with"
                    , "    divulgee : Party"
                    , "    sig : Party"
                    , "  where"
                    , "    signatory divulgee"
                    , "    observer sig"
                    , "    nonconsuming choice Divulge : T"
                    , "      with cid : ContractId T"
                    , "      controller sig"
                    , "      do fetch cid"
                    , "testQueryContract = do"
                    , "  p1 <- allocateParty \"p1\""
                    , "  p2 <- allocateParty \"p2\""
                    , "  onlyP1 <- submit p1 $ createCmd (T p1 p1)"
                    , "  both <- submit p1 $ createCmd (T p1 p2)"
                    , "  divulger <- submit p2 $ createCmd (Divulger p2 p1)"
                    , "  optOnlyP1 <- queryContractId p1 onlyP1"
                    , "  optOnlyP1 === Some (T p1 p1)"
                    , "  optOnlyP1 <- queryContractKey @T p1 (p1, p1)"
                    , "  optOnlyP1 === Some (onlyP1, T p1 p1)"
                    , "  optOnlyP1 <- queryContractId p2 onlyP1"
                    , "  optOnlyP1 === None"
                    , "  optBoth <- queryContractKey @T p1 (p1, p2)"
                    , "  optBoth === Some (both, T p1 p2)"
                    , "  optOnlyP1 <- queryContractKey @T p2 (p1, p1)"
                    , "  optOnlyP1 === None"
                    , "  optBoth <- queryContractKey @T p2 (p1, p2)"
                    , "  optBoth === Some (both, T p1 p2)"
                    , "  optBoth <- queryContractId p1 both"
                    , "  optBoth === Some (T p1 p2)"
                    , "  optBoth <- queryContractId p2 both"
                    , "  optBoth === Some (T p1 p2)"
                    -- Divulged contracts should not be returned in queries
                    , "  submit p1 $ exerciseCmd divulger (Divulge onlyP1)"
                    , "  optOnlyP1 <- queryContractId p2 onlyP1"
                    , "  optOnlyP1 === None"
                    , "  optOnlyP1 <- queryContractKey @T p2 (p1, p1)"
                    , "  optOnlyP1 === None"
                    , "  pure ()"
                    ]
                expectScriptSuccess rs (vr "testQueryContract") $ \r ->
                  matchRegex r "Active contracts:  #2:0, #0:0, #1:0",
              testCase "trace" $ do
                rs <-
                  runScripts
                    scriptService
                    [ "module Test where"
                    , "import Daml.Script"
                    , "template T"
                    , "  with p : Party"
                    , "  where"
                    , "    signatory p"
                    , "    choice C : ()"
                    , "      controller p"
                    , "      do debug \"logLedger\""
                    , "    choice Failing : ()"
                    , "      controller p"
                    , "      do debug \"please don't die\""
                    , "         abort \"die\""
                    , "testTrace = do"
                    , "  debug \"logClient1\""
                    , "  p <- allocateParty \"p\""
                    , "  submit p (createAndExerciseCmd (T p) C)"
                    , "  debug \"logClient2\""
                    , "  submit p (createAndExerciseCmd (T p) Failing)"
                    , "  pure ()"
                    ]
                expectScriptFailure rs (vr "testTrace") $ \r ->
                  matchRegex r $ T.concat
                    [ "Trace: \n"
                    , "  \"logClient1\"\n"
                    , "  \"logLedger\"\n"
                    , "  \"logClient2\"\n"
                    , "  \"please don't die\""
                    ],
              testCase "multi-party query" $ do
                rs <-
                  runScripts
                    scriptService
                    [ "module Test where"
                    , "import DA.Assert"
                    , "import DA.List"
                    , "import Daml.Script"
                    , "template T"
                    , "  with p : Party, v : Int"
                    , "  where"
                    , "    signatory p"
                    , "    key p : Party"
                    , "    maintainer key"
                    , "test = do"
                    , "  p0 <- allocateParty \"p0\""
                    , "  p1 <- allocateParty \"p1\""
                    , "  cid0 <- submit p0 (createCmd (T p0 42))"
                    , "  cid1 <- submit p1 (createCmd (T p1 23))"
                    , "  r <- query @T p0"
                    , "  r === [(cid0, T p0 42)]"
                    , "  r <- query @T p1"
                    , "  r === [(cid1, T p1 23)]"
                    , "  r <- query @T [p0, p1]"
                    , "  sortOn (\\(_, c) -> c.v) r === [(cid1, T p1 23), (cid0, T p0 42)]"
                    , "  Some r <- queryContractId @T [p0, p1] cid0"
                    , "  r === T p0 42"
                    , "  Some r <- queryContractId @T [p0, p1] cid1"
                    , "  r === T p1 23"
                    , "  Some (r, _) <- queryContractKey @T [p0, p1] p0"
                    , "  r === cid0"
                    , "  Some (r, _) <- queryContractKey @T [p0, p1] p1"
                    , "  r === cid1"
                    , "  pure ()"
                    ]
                expectScriptSuccess rs (vr "test") $ \r ->
                  matchRegex r "Active contracts:  #0:0, #1:0",
              testCase "multi-party submissions" $ do
                rs <-
                  runScripts
                    scriptService
                    [ "module Test where"
                    , "import DA.Assert"
                    , "import DA.List"
                    , "import Daml.Script"
                    , "template T"
                    , "  with p0 : Party, p1 : Party"
                    , "  where"
                    , "    signatory p0, p1"
                    , "    nonconsuming choice C : T"
                    , "      with cid : ContractId T"
                    , "      controller p1"
                    , "      do fetch cid"
                    , "testSucceed = do"
                    , "  p0 <- allocateParty \"p0\""
                    , "  p1 <- allocateParty \"p1\""
                    , "  submitMultiMustFail [p0] [] (createCmd (T p0 p1))"
                    , "  submitMultiMustFail [p0] [p1] (createCmd (T p0 p1))"
                    , "  cid <- submitMulti [p0, p1] ([] : [Party]) (createCmd (T p0 p1))"
                    , "  cidp0 <- submit p0 (createCmd (T p0 p0))"
                    , "  submitMultiMustFail [p1] [] (exerciseCmd cid (C cidp0))"
                    , "  submitMulti [p1] [p0] (exerciseCmd cid (C cidp0))"
                    , "testFail = do"
                    , "  p0 <- allocateParty \"p0\""
                    , "  p1 <- allocateParty \"p1\""
                    , "  submitMulti [p0] [p1] (createCmd (T p0 p1))"
                    ]
                expectScriptSuccess rs (vr "testSucceed") $ \r ->
                  matchRegex r "Active contracts:  #0:0, #1:0"
                expectScriptFailure rs (vr "testFail") $ \r ->
                  matchRegex r "missing authorization from 'p1'"
            ]
  where
    scenarioConfig = SS.defaultScenarioServiceConfig {SS.cnfJvmOptions = ["-Xmx200M"]}
    vr n = VRScenario (toNormalizedFilePath' "Test.daml") n

matchRegex :: T.Text -> T.Text -> Bool
matchRegex s regex = matchTest (makeRegex regex :: Regex) s

expectScriptSuccess ::
  -- | The list of script results.
  [(VirtualResource, Either T.Text T.Text)] ->
  -- | VR of the script
  VirtualResource ->
  -- | Predicate on the result
  (T.Text -> Bool) ->
  -- | Succeeds if there is a successful result for the given
  -- VR and the predicate holds.
  Assertion
expectScriptSuccess xs vr pred = case find ((vr ==) . fst) xs of
  Nothing -> assertFailure $ "No result for " <> show vr
  Just (_, Left err) ->
    assertFailure $
      "Expected success for " <> show vr <> " but got "
        <> show err
  Just (_, Right r) ->
    unless (pred r) $
      assertFailure $ "Predicate for " <> show vr <> " failed on " <> show r

expectScriptFailure ::
  -- | The list of script results.
  [(VirtualResource, Either T.Text T.Text)] ->
  -- | VR of the script
  VirtualResource ->
  -- | Predicate on the result
  (T.Text -> Bool) ->
  -- | Succeeds if there is a failing result for the given
  -- VR and the predicate holds.
  Assertion
expectScriptFailure xs vr pred = case find ((vr ==) . fst) xs of
  Nothing -> assertFailure $ "No result for " <> show vr
  Just (_, Right r) ->
    assertFailure $
      "Expected failure for " <> show vr <> " but got "
        <> show r
  Just (_, Left err) ->
    unless (pred err) $
      assertFailure $ "Predicate for " <> show vr <> " failed on " <> show err

options :: Options
options =
  (defaultOptions Nothing)
    { optDlintUsage = DlintDisabled,
      optEnableOfInterestRule = False,
      optEnableScripts = EnableScripts True
    }

runScripts :: SS.Handle -> [T.Text] -> IO [(VirtualResource, Either T.Text T.Text)]
runScripts service fileContent = bracket getIdeState shutdown $ \ideState -> do
  setBufferModified ideState file $ Just $ T.unlines fileContent
  setFilesOfInterest ideState (HashSet.singleton file)
  mbResult <- runActionSync ideState $ use RunScripts file
  case mbResult of
    Nothing -> do
      diags <- getDiagnostics ideState
      fail (T.unpack $ showDiagnostics diags)
    Just xs -> do
      world <- runActionSync ideState (worldForFile file)
      let render (vr, r) = (vr,) <$> prettyResult world r
      mapM render xs
  where
    prettyResult world (Left err) = case err of
      SS.BackendError err -> assertFailure $ "Unexpected result " <> show err
      SS.ExceptionError err -> assertFailure $ "Unexpected result " <> show err
      SS.ScenarioError err -> pure $ Left $ renderPlain (prettyScenarioError world err)
    prettyResult world (Right r) = pure $ Right $ renderPlain (prettyScenarioResult world r)
    file = toNormalizedFilePath' "Test.daml"
    getIdeState = do
      vfs <- makeVFSHandle
      logger <- Logger.newStderrLogger Logger.Error "script-service"
      getDamlIdeState
        options
        (Just service)
        logger
        noopDebouncer
        def
        (pure $ LSP.IdInt 0)
        (const $ pure ())
        vfs
        (IdeReportProgress False)
