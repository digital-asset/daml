-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Test.ScriptService (main) where

{- HLINT ignore "locateRunfiles/package_app" -}

import Control.Exception
import Control.Monad
import DA.Bazel.Runfiles
import DA.Cli.Damlc.Packaging
import qualified DA.Daml.LF.Ast.Version as LF
import DA.Daml.LF.PrettyScript (prettyScriptError, prettyScriptResult)
import qualified DA.Daml.LF.ScriptServiceClient as SS
import DA.Daml.Options.Types
import DA.Daml.Package.Config
import DA.Daml.Project.Types
import DA.Pretty
import qualified DA.Service.Logger as Logger
import qualified DA.Service.Logger.Impl.IO as Logger
import DA.Test.Util (withResourceCps)
import qualified Data.HashSet as HashSet
import Data.List
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Vector as V
import Development.IDE.Core.Debouncer (noopDebouncer)
import Development.IDE.Core.FileStore (makeVFSHandle, setBufferModified)
import Development.IDE.Core.IdeState.Daml (getDamlIdeState)
import Development.IDE.Core.OfInterest (setFilesOfInterest)
import Development.IDE.Core.RuleTypes.Daml (RunScripts (..), VirtualResource (..))
import Development.IDE.Core.Rules.Daml (worldForFile)
import Development.IDE.Core.Service (getDiagnostics, runActionSync, shutdown)
import Development.IDE.Core.Shake (ShakeLspEnv(..), NotificationHandler(..), use)
import Development.IDE.Types.Diagnostics (showDiagnostics)
import Development.IDE.Types.Location (toNormalizedFilePath')
import SdkVersion (SdkVersioned, sdkVersion, withSdkVersions)
import System.Directory.Extra
import System.Environment.Blank
import System.FilePath
import System.IO.Extra
import Test.Tasty
import Test.Tasty.HUnit
import Text.Regex.TDFA

main :: IO ()
main = withSdkVersions $ do
    setEnv "TASTY_NUM_THREADS" "1" True
    defaultMain $
        testGroup
            "Script Service"
            [ testGroup
                "Without Contract keys"
                [ withResourceCps
                    (withScriptService lfVersion)
                    (testScriptService lfVersion)
                | lfVersion <-
                    map
                        LF.defaultOrLatestStable
                        [minBound @LF.MajorVersion .. maxBound]
                ]
            , testGroup
                "With Contract Keys"
                [ withResourceCps
                    (withScriptService lfVersion)
                    (testScriptServiceWithKeys lfVersion)
                | Just lfVersion <-
                    map
                        (LF.featureMinVersion LF.featureContractKeys)
                        [minBound @LF.MajorVersion .. maxBound]
                ]
            ]

damlScriptDarPath :: LF.Version -> FilePath
damlScriptDarPath = \case
    (LF.Version LF.V2 LF.PointDev) -> prefix </> "daml-script-2.dev.dar"
    (LF.Version LF.V2 _) -> prefix </> "daml-script.dar"
  where
    prefix = mainWorkspace </> "daml-script" </> "daml"

withScriptService :: SdkVersioned => LF.Version -> (SS.Handle -> IO ()) -> IO ()
withScriptService lfVersion action =
  withTempDir $ \dir -> do
    withCurrentDirectory dir $ do

      -- Package DB setup, we only need to do this once so we do it at the beginning.
      scriptDar <- locateRunfiles $ damlScriptDarPath lfVersion
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
      withPackageConfig (ProjectPath ".") $
        setupPackageDbFromPackageConfig (toNormalizedFilePath' dir) $ options lfVersion
      logger <- Logger.newStderrLogger Logger.Debug "script-service"

      -- Spinning up the script service is expensive so we do it once at the beginning.
      SS.withScriptService lfVersion logger scriptConfig $ \scriptService -> do
        action scriptService
  where
    scriptConfig = SS.defaultScriptServiceConfig {SS.cnfJvmOptions = ["-Xmx200M"]}

testScriptService :: SdkVersioned => LF.Version -> IO SS.Handle -> TestTree
testScriptService lfVersion getScriptService =
          testGroup
            ("LF " <> LF.renderVersion lfVersion)
            [ testCase "createCmd + exerciseCmd + createAndExerciseCmd" $ do
                rs <-
                  runScripts
                    getScriptService
                    lfVersion
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
                      "  p <- allocatePartyByHint (PartyIdHint \"p\")",
                      "  cid <- submit p (createCmd (T p 42))",
                      "  submit p (archiveCmd cid)"
                    ]
                expectScriptSuccess rs (vr "testCreate") $ \r ->
                  matchRegex r "Active contracts:  #0:0\n\nReturn value: #0:0\n$"
                expectScriptSuccess rs (vr "testExercise") $ \r ->
                  matchRegex r "Active contracts: \n\nReturn value: 42\n$"
                expectScriptSuccess rs (vr "testCreateAndExercise") $ \r ->
                  matchRegex r "Active contracts: \n\nReturn value: 42\n$"
                expectScriptSuccess rs (vr "testMulti") $ \r ->
                  matchRegex r $
                    T.unlines
                      [ "Active contracts: "
                      , ""
                      , "Return value:"
                      , "  DA\\.Types:Tuple2@[a-z0-9]+ with"
                      , "    _1 = 23; _2 = 42"
                      ]
                expectScriptSuccess rs (vr "testArchive") $ \r ->
                  matchRegex r "'p' exercises Archive on #0:0",
              testCase "query" $
                do
                  rs <-
                    runScripts
                      getScriptService
                      lfVersion
                      [ "module Test where",
                        "import Daml.Script",
                        "import DA.Assert",
                        "import DA.List",
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
                        "deriving instance Ord TShared",
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
                        "  sortOn snd sharedp1 === [(cidSharedp1, TShared p1 p2), (cidSharedp2, TShared p2 p1)]",
                        "  sharedp2 <- query @TShared p2",
                        "  sortOn snd sharedp2 === [(cidSharedp1, TShared p1 p2), (cidSharedp2, TShared p2 p1)]"
                      ]
                  expectScriptSuccess rs (vr "testQueryInactive") $ \r ->
                    matchRegex r "Active contracts:  #0:0, #2:0\n\n"
                  expectScriptSuccess rs (vr "testQueryVisibility") $ \r ->
                    matchRegex r "Active contracts:  #0:0, #1:0, #2:0, #3:0, #4:0\n\n"
                  pure (),
              testCase "submitMustFail" $ do
                  rs <-
                    runScripts
                      getScriptService
                      lfVersion
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
                    matchRegex r "Active contracts:  #0:0, #2:0\n\nReturn value: {}\n$"
                  pure (),
              testCase "time" $ do
                rs <-
                  runScripts
                    getScriptService
                    lfVersion
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
                        [ "Return value:"
                        , "  DA\\.Types:Tuple2@[a-z0-9]+ with"
                        , "    _1 = 1970-01-01T00:00:00Z; _2 = 2000-02-02T00:01:02Z"
                        ]
                expectScriptSuccess rs (vr "testChoiceTime") $ \r ->
                    matchRegex r $
                      T.unlines
                        [ "Return value:"
                        , "  DA\\.Types:Tuple2@[a-z0-9]+ with"
                        , "    _1 = 1970-01-01T00:00:00Z; _2 = 2000-02-02T00:01:02Z"
                        ]
                expectScriptFailure rs (vr "testPassTime") $ \r ->
                    matchRegex r "Attempt to fetch or exercise a contract not yet effective",
              testCase "partyManagement" $ do
                rs <-
                  runScripts
                    getScriptService
                    lfVersion
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
                      "partyManagement = do",
                      "  alice <- allocatePartyByHint (PartyIdHint \"alice\")",
                      "  alice1 <- allocateParty \"alice\"",
                      "  t1 <- submit alice $ createCmd T { owner = alice, observer = alice1 }",
                      "  bob <- allocateParty \"bob\"",
                      "  details <- listKnownParties",
                      "  assertEq (length details) 3",
                      "  let [aliceDetails, alice1Details, bobDetails] = details",
                      "  assertEq aliceDetails (PartyDetails alice True)",
                      "  assertEq alice1Details (PartyDetails alice1 True)",
                      "  assertEq bobDetails (PartyDetails bob True)",
                      "duplicateAllocateByHint = do",
                      "  _ <- allocatePartyByHint (PartyIdHint \"alice\")",
                      "  _ <- allocatePartyByHint (PartyIdHint \"alice\")",
                      "  pure ()",
                      "partyWithEmptyDisplayName = do",
                      "  p1 <- allocateParty \"\"",
                      "  p2 <- allocatePartyWithHint \"\" (PartyIdHint \"hint\")",
                      "  details <- listKnownParties",
                      "  let [p1Details, p2Details] = details",
                      "  assertEq p2Details.party (fromSome $ partyFromText \"hint\")",
                      "  t1 <- submit p1 $ createCmd T { owner = p1, observer = p2 }",
                      "  pure ()",
                      "mismatchingNameAndHint = do",
                      "  _ <- allocatePartyWithHint \"alice\" (PartyIdHint \"hint\")",
                      "  pure ()"
                    ]
                expectScriptSuccess rs (vr "partyManagement") $ \r ->
                  matchRegex r "Active contracts:  #0:0\n\nReturn value: {}\n$"
                expectScriptFailure rs (vr "duplicateAllocateByHint") $ \r ->
                  matchRegex r "Tried to allocate a party that already exists: alice"
                expectScriptSuccess rs (vr "partyWithEmptyDisplayName") $ \r ->
                  matchRegex r "Active contracts:  #0:0\n\nReturn value: {}\n$"
                expectScriptFailure rs (vr "mismatchingNameAndHint") $ \r ->
                  matchRegex r "Requested name 'alice' cannot be different from id hint 'hint'",
              testCase "trace" $ do
                rs <-
                  runScripts
                    getScriptService
                    lfVersion
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
              testCase "multi-party submissions" $ do
                rs <-
                  runScripts
                    getScriptService
                    lfVersion
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
                  matchRegex r "Active contracts:  #2:0, #3:0"
                expectScriptFailure rs (vr "testFail") $ \r ->
                  matchRegex r "missing authorization from 'p1-[a-z0-9]+'",
              testCase "submitTree" $ do
                rs <-
                  runScripts
                    getScriptService
                    lfVersion
                    [ "module Test where"
                    , "import DA.Assert"
                    , "import DA.Foldable"
                    , "import Daml.Script"
                    , "template T"
                    , "  with"
                    , "    p : Party"
                    , "    v : Int"
                    , "  where"
                    , "    signatory p"
                    , "    nonconsuming choice CreateN : ()"
                    , "      with n : Int"
                    , "      controller p"
                    , "      do forA_ [ 1 .. n ] $ \\i -> create (T p i)"
                    , "test = do"
                    , "  p <- allocateParty \"p\""
                    , "  TransactionTree [CreatedEvent (Created cid arg)] <- submitTree p (createCmd (T p 0))"
                    , "  fromAnyTemplate arg === Some (T p 0)"
                    , "  let Some cid' = fromAnyContractId @T cid"
                    , "  optT <- queryContractId p cid'"
                    , "  optT === Some (T p 0)"
                    , "  TransactionTree [ExercisedEvent ex] <- submitTree p (exerciseCmd cid' (CreateN 3))"
                    , "  fromAnyContractId ex.contractId === Some cid'"
                    , "  let [CreatedEvent c1, CreatedEvent c2, CreatedEvent c3] = ex.childEvents"
                    , "  fromAnyTemplate c1.argument === Some (T p 1)"
                    , "  fromAnyTemplate c2.argument === Some (T p 2)"
                    , "  fromAnyTemplate c3.argument === Some (T p 3)"
                    ]
                expectScriptSuccess rs (vr "test") $ \r ->
                  matchRegex r "Active contracts:",
              testCase "exceptions" $ do
                rs <-
                  runScripts
                    getScriptService
                    lfVersion
                    [ "module Test where"
                    , "import DA.Exception"
                    , "import DA.Assert"
                    , "import DA.Foldable"
                    , "import Daml.Script"
                    , "template T"
                    , "  with"
                    , "    p : Party"
                    , "  where"
                    , "    signatory p"
                    , "template Helper"
                    , "  with"
                    , "    p : Party"
                    , "  where"
                    , "    signatory p"
                    , "    postconsuming choice C : ()"
                    , "      with"
                    , "        cid : ContractId T"
                    , "      controller p"
                    , "      do try do"
                    , "           -- rolled back direct create"
                    , "           create (T p)"
                    , "           -- rolled back archive"
                    , "           archive cid"
                    , "           -- rolled back create under exercise"
                    , "           exercise self CreateT"
                    , "           try do"
                    , "             create (T p)"
                    , "             error \"\""
                    , "           catch"
                    , "             (GeneralError _) -> pure ()"
                    , "           -- rolled back create after nested rollback"
                    , "           create (T p)"
                    , "           error \"\""
                    , "         catch"
                    , "           (GeneralError _) -> pure ()"
                    , "    nonconsuming choice CreateT : ContractId T"
                    , "      controller p"
                    , "      do create (T p)"
                    , "    choice Fail : ()"
                    , "      controller p"
                    , "      do assert False"
                    -- Check that we display activeness correctly.
                    -- There are 3 main cases:
                    -- 1. Direct children of a rollback node are rolled back.
                    -- 2. Children of an exercise under a rollback node are rolled back.
                    -- 3. After exiting a nested rollback node, we rollback further children
                    --    if we’re still below a rollback node.
                    , "testActive = do"
                    , "  p <- allocateParty \"p\""
                    , "  cid <- submit p $ createCmd (T p)"
                    , "  submit p $ createAndExerciseCmd (Helper p) (C cid)"
                    , "  r <- query @T p"
                    , "  r === [(cid, T p)]"
                    , "  pure ()"
                    , "unhandledOffLedger = script $ assert False"
                    , "unhandledOnLedger = script $ do"
                    , "  p <- allocateParty \"p\""
                    , "  submit p $ createAndExerciseCmd (Helper p) Fail"
                    ]
                expectScriptSuccess rs (vr "testActive") $ \r ->
                  matchRegex r "Active contracts:  #0:0\n"
                expectScriptFailure rs (vr "unhandledOffLedger") $ \r -> matchRegex r "UNHANDLED_EXCEPTION"
                expectScriptFailure rs (vr "unhandledOnLedger") $ \r -> matchRegex r "UNHANDLED_EXCEPTION",
              testCase "user management" $ do
                rs <- runScripts getScriptService lfVersion
                  [ "module Test where"
                  , "import DA.Assert"
                  , "import Daml.Script"
                  , "import DA.List (sort)"
                  , "isValidUserId : Text -> Script Bool"
                  , "isValidUserId name = try do _ <- validateUserId name; pure True catch InvalidUserId _ -> pure False"
                  , "userExists : UserId -> Script Bool"
                  , "userExists u = do try do _ <- getUser u; pure True catch UserNotFound _ -> pure False"
                  , "expectUserNotFound : Script a -> Script ()"
                  , "expectUserNotFound script = try do _ <- script; undefined catch UserNotFound _ -> pure ()"
                  , "testUserManagement = do"
                  , "  True <- isValidUserId \"Good\""
                  , "  False <- isValidUserId \"BAD?\" -- contains invalid character '?'"
                  , "  u1 <- validateUserId \"user1\""
                  , "  u2 <- validateUserId \"user2\""
                  , "  let user1 = User u1 None"
                  , "  let user2 = User u2 None"
                  , "  userIdToText u1 === \"user1\""
                  , "  userIdToText u2 === \"user2\""
                  , "  users <- listAllUsers"
                  , "  users === []"
                  , "  createUser user1 []"
                  , "  True <- userExists u1"
                  , "  False <- userExists u2"
                  , "  try do _ <- createUser user1 []; undefined catch UserAlreadyExists _ -> pure ()"
                  , "  createUser user2 []"
                  , "  True <- userExists u1"
                  , "  True <- userExists u2"
                  , "  u <- getUser u1"
                  , "  u === user1"
                  , "  u <- getUser u2"
                  , "  u === user2"
                  , "  users <- listAllUsers"
                  , "  sort users === [user1, user2]"
                  , "  deleteUser u1"
                  , "  users <- listAllUsers"
                  , "  users === [user2]"
                  , "  deleteUser u2"
                  , "  users <- listAllUsers"
                  , "  users === []"
                  , "  nonexistent <- validateUserId \"nonexistent\""
                  , "  expectUserNotFound (getUser nonexistent)"
                  , "  expectUserNotFound (deleteUser nonexistent)"
                  , "  pure ()"
                  , "testUserRightManagement = do"
                  , "  p1 <- allocateParty \"p1\""
                  , "  p2 <- allocateParty \"p2\""
                  , "  u1 <- validateUserId \"user1\""
                  , "  createUser (User u1 None) []"
                  , "  rights <- listUserRights u1"
                  , "  rights === []"
                  , "  newRights <- grantUserRights u1 [ParticipantAdmin]"
                  , "  newRights === [ParticipantAdmin]"
                  , "  newRights <- grantUserRights u1 [ParticipantAdmin]"
                  , "  newRights === []"
                  , "  rights <- listUserRights u1"
                  , "  rights === [ParticipantAdmin]"
                  , "  newRights <- grantUserRights u1 [CanActAs p1, CanReadAs p2]"
                  , "  newRights === [CanActAs p1, CanReadAs p2]"
                  , "  rights <- listUserRights u1"
                  , "  rights === [ParticipantAdmin, CanActAs p1, CanReadAs p2]"
                  , "  revoked <- revokeUserRights u1 [ParticipantAdmin]"
                  , "  revoked === [ParticipantAdmin]"
                  , "  revoked <- revokeUserRights u1 [ParticipantAdmin]"
                  , "  revoked === []"
                  , "  rights <- listUserRights u1"
                  , "  rights === [CanActAs p1, CanReadAs p2]"
                  , "  revoked <- revokeUserRights u1 [CanActAs p1, CanReadAs p2]"
                  , "  revoked === [CanActAs p1, CanReadAs p2]"
                  , "  rights <- listUserRights u1"
                  , "  rights === []"
                  , "  nonexistent <- validateUserId \"nonexistent\""
                  , "  expectUserNotFound (listUserRights nonexistent)"
                  , "  expectUserNotFound (revokeUserRights nonexistent [])"
                  , "  expectUserNotFound (grantUserRights nonexistent [])"
                  , "  pure ()"
                  ]
                expectScriptSuccess rs (vr "testUserManagement") $ \r ->
                    matchRegex r "Active contracts: \n"
                expectScriptSuccess rs (vr "testUserRightManagement") $ \r ->
                    matchRegex r "Active contracts: \n",
              testCase "implicit party allocation" $ do
                rs <- runScripts getScriptService lfVersion
                  [ "module Test where"
                  , "import DA.Assert"
                  , "import DA.Optional"
                  , "import Daml.Script"
                  , "template T"
                  , "  with"
                  , "    s: Party"
                  , "    o: Party"
                  , "  where"
                  , "    signatory s"
                  , "    observer o"
                  , "submitterNotAllocated : Script ()"
                  , "submitterNotAllocated = do"
                  , "  x <- allocateParty \"x\""
                  , "  let unallocated  = fromSome (partyFromText \"y\")"
                  , "  submitMulti [x, unallocated] [] $ createCmd (T x x)"
                  , "  pure ()"
                  , "observerNotAllocated : Script ()"
                  , "observerNotAllocated = do"
                  , "  x <- allocateParty \"x\""
                  , "  let unallocated  = fromSome (partyFromText \"y\")"
                  , "  submit x $ createCmd (T x unallocated)"
                  , "  pure ()"
                  ]
                expectScriptFailure rs (vr "submitterNotAllocated") $ \r ->
                    matchRegex r "Tried to submit a command for parties that have not ben allocated:\n  'y'"
                expectScriptFailure rs (vr "observerNotAllocated") $ \r ->
                    matchRegex r "Tried to submit a command for parties that have not ben allocated:\n  'y'",
              -- Regression test for issue https://github.com/digital-asset/daml/issues/13835
              testCase "rollback archive" $ do
                rs <- runScripts getScriptService lfVersion
                  [ "module Test where"
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
                expectScriptSuccess rs (vr "test") $ \r ->
                   matchRegex r "Active contracts:  #0:0\n"
            ]
  where
    vr n = VRScript (toNormalizedFilePath' "Test.daml") n

testScriptServiceWithKeys :: SdkVersioned => LF.Version -> IO SS.Handle -> TestTree
testScriptServiceWithKeys lfVersion getScriptService =
          testGroup
            ("LF " <> LF.renderVersion lfVersion)
            [ testCase "exerciseByKeyCmd" $ do
                rs <-
                  runScripts
                    getScriptService
                    lfVersion
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
                  matchRegex r "Active contracts: \n\nReturn value: 42\n$",
              testCase "fetch and exercising by key shows key in log" $ do
                rs <-
                  runScripts
                    getScriptService
                    lfVersion
                    [ "module Test where",
                      "import Daml.Script",
                      "",
                      "template T",
                      "  with",
                      "    owner : Party",
                      "  where",
                      "    signatory owner",
                      "    key owner : Party",
                      "    maintainer key",
                      "    nonconsuming choice C : ()",
                      "      controller owner",
                      "      do",
                      "        pure ()",
                      "",
                      "template Runner",
                      "  with",
                      "    owner : Party",
                      "  where",
                      "    signatory owner",
                      "",
                      "    choice RunByKey : ()",
                      "      with",
                      "        party : Party",
                      "      controller owner",
                      "      do",
                      "        cid <- create T with owner = party",
                      "        exerciseByKey @T party C",
                      "        fetchByKey @T party",
                      "        pure ()",
                      "",
                      "    choice Run : ()",
                      "      with",
                      "        party : Party",
                      "      controller owner",
                      "      do",
                      "        cid <- create T with owner = party",
                      "        exercise cid C",
                      "        fetch cid",
                      "        pure ()",
                      "",
                      "testReportsKey = do",
                      "  p <- allocateParty \"p\"",
                      "  submit p $ createAndExerciseCmd (Runner p) (RunByKey p)",
                      "",
                      "testDoesNotReportKey = do",
                      "  p <- allocateParty \"p\"",
                      "  submit p $ createAndExerciseCmd (Runner p) (Run p)"
                    ]
                expectScriptSuccess rs (vr "testReportsKey") $ \r ->
                  matchRegex r (T.unlines
                    [ ".*exercises.*"
                    , ".*by key.*"
                    ]) &&
                  matchRegex r (T.unlines
                    [ ".*fetch.*"
                    , ".*by key.*"
                    ])
                expectScriptSuccess rs (vr "testDoesNotReportKey") $ \r ->
                  matchRegex r ".*exercises.*" &&
                  matchRegex r ".*fetch.*" &&
                  not (matchRegex r (T.unlines
                    [ ".*exercises.*"
                    , ".*by key.*"
                    ])) &&
                  not (matchRegex r (T.unlines
                    [ ".*fetch.*"
                    , ".*by key.*"
                    ])),
              testCase "failing transactions" $ do
                rs <-
                  runScripts
                    getScriptService
                    lfVersion
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
                      "  party <- allocateParty \"party\"",
                      "  party1 <- allocateParty \"party1\"",
                      "  submit party (createCmd (MultiSignatory party party1))",
                      "testDuplicateKey = do",
                      "  p <- allocateParty \"p\"",
                      "  submit p (createCmd (TKey p))",
                      "  submit p (createCmd (TKey p))",
                      "testNotVisible = do",
                      "  party <- allocateParty \"party\"",
                      "  party1 <- allocateParty \"party1\"",
                      "  cid <- submit party (createCmd (TKey party))",
                      "  helperCid <- submit party1 (createCmd (Helper party1))",
                      "  submit party1 (exerciseCmd helperCid (Fetch cid))",
                      "testError = do",
                      "  p <- allocateParty \"p\"",
                      "  cid <- submit p (createCmd (Helper p))",
                      "  submit p (exerciseCmd cid Error)",
                      "testAbort = do",
                      "  p <- allocateParty \"p\"",
                      "  cid <- submit p (createCmd (Helper p))",
                      "  submit p (exerciseCmd cid Abort)",
                      "testPartialSubmit = do",
                      "  party <- allocateParty \"party\"",
                      "  party1 <- allocateParty \"party1\"",
                      "  submit party (createCmd (Helper party))",
                      "  submit party1 (createCmd (Helper party))",
                      "testPartialSubmitMustFail = do",
                      "  party <- allocateParty \"party\"",
                      "  party1 <- allocateParty \"party1\"",
                      "  submit party (createCmd (Helper party))",
                      "  submitMustFail party1 (createCmd (Helper party1))"
                    ]
                expectScriptFailure rs (vr "testMissingAuthorization") $ \r ->
                  matchRegex r "failed due to a missing authorization from 'party1-[a-z0-9]+'"
                expectScriptFailure rs (vr "testDuplicateKey") $ \r ->
                  matchRegex r "due to unique key violation for key"
                expectScriptFailure rs (vr "testNotVisible") $ \r ->
                  matchRegex r "Attempt to fetch or exercise a contract not visible to the reading parties"
                expectScriptFailure rs (vr "testError") $ \r ->
                  matchRegex r "errorCrash"
                expectScriptFailure rs (vr "testAbort") $ \r ->
                  matchRegex r "abortCrash"
                expectScriptFailure rs (vr "testPartialSubmit") $ \r ->
                  matchRegex r  $ T.unlines
                    [ "Script execution failed on commit at Test:57:3:"
                    , ".*"
                    , ".*failed due to a missing authorization.*"
                    , ".*"
                    , ".*"
                    , ".*"
                    , "Partial transaction:"
                    , "  Sub-transactions:"
                    , "     0"
                    , ".*'party-[a-z0-9]+' creates Test:Helper.*"
                    ]
                expectScriptFailure rs (vr "testPartialSubmitMustFail") $ \r ->
                  matchRegex r $ T.unlines
                    [ "Script execution failed on commit at Test:62:3:"
                    , "  Aborted:  Expected submit to fail but it succeeded"
                    , ".*"
                    , ".*"
                    , ".*"
                    , "Partial transaction:"
                    , "  Sub-transactions:"
                    , "     0"
                    , ".*'party1-[a-z0-9]+' creates Test:Helper.*"
                    ]
                pure (),
              testCase "contract keys" $ do
                rs <-
                  runScripts
                    getScriptService
                    lfVersion
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
              testCase "queryContractId/Key" $ do
                rs <-
                  runScripts
                    getScriptService
                    lfVersion
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
                  matchRegex r "Active contracts:  #0:0, #1:0, #2:0",
              testCase "multi-party query" $ do
                rs <-
                  runScripts
                    getScriptService
                    lfVersion
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
              testCase "local key visibility" $ do
                rs <-
                  runScripts
                    getScriptService
                    lfVersion
                    [ "module Test where"
                    , "import DA.Assert"
                    , "import DA.Foldable"
                    , "import Daml.Script"
                    , "template WithKey"
                    , "  with"
                    , "    p : Party"
                    , "  where"
                    , "    signatory p"
                    , "    key p : Party"
                    , "    maintainer key"
                    , "template LocalKeyVisibility"
                    , "  with"
                    , "    p1 : Party"
                    , "    p2 : Party"
                    , "  where"
                    , "    signatory p1"
                    , "    observer p2"
                    , "    nonconsuming choice LocalLookup : ()"
                    , "      controller p2"
                    , "      do cid <- create (WithKey p1)"
                    , "         Some _ <- lookupByKey @WithKey p1"
                    , "         archive cid"
                    , "    nonconsuming choice LocalFetch : ()"
                    , "      controller p2"
                    , "      do cid <- create (WithKey p1)"
                    , "         _ <- fetchByKey @WithKey p1"
                    , "         archive cid"
                    , "localLookup = do"
                    , "  p1 <- allocateParty \"p1\""
                    , "  p2 <- allocateParty \"p2\""
                    , "  cid <- submit p1 $ createCmd (LocalKeyVisibility p1 p2)"
                    , "  submit p2 $ exerciseCmd cid LocalLookup"
                    , "localFetch = do"
                    , "  p1 <- allocateParty \"p1\""
                    , "  p2 <- allocateParty \"p2\""
                    , "  cid <- submit p1 $ createCmd (LocalKeyVisibility p1 p2)"
                    , "  submit p2 $ exerciseCmd cid LocalFetch"
                    , "localLookupFetchMulti = do"
                    , "  p1 <- allocateParty \"p1\""
                    , "  p2 <- allocateParty \"p2\""
                    , "  cid <- submit p1 $ createCmd (LocalKeyVisibility p1 p2)"
                    , "  submitMulti [p2] [p1]  $ exerciseCmd cid LocalLookup"
                    , "  submitMulti [p2] [p1] $ exerciseCmd cid LocalFetch"
                    ]
                expectScriptSuccess rs (vr "localLookup") $ \r ->
                  matchRegex r "Active contracts:"
                expectScriptSuccess rs (vr "localFetch") $ \r ->
                  matchRegex r "Active contracts:"
                expectScriptSuccess rs (vr "localLookupFetchMulti") $ \r ->
                  matchRegex r "Active contracts:"
            ]
  where
    vr n = VRScript (toNormalizedFilePath' "Test.daml") n

matchRegex :: T.Text -> T.Text -> Bool
matchRegex s regex = matchTest (makeRegex regex :: Regex) s

expectScriptSuccess :: HasCallStack =>
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

options :: LF.Version -> Options
options lfVersion = defaultOptions (Just lfVersion)

runScripts :: SdkVersioned => IO SS.Handle -> LF.Version -> [T.Text] -> IO [(VirtualResource, Either T.Text T.Text)]
runScripts getService lfVersion fileContent = bracket getIdeState shutdown $ \ideState -> do
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
      SS.ScriptError err -> pure $ Left $ renderPlain $
        prettyScriptError prettyNormal world err
          $$ text "" -- add a newline at the end
    prettyResult world (Right r) = pure $ Right $ renderPlain $
      prettyScriptResult prettyNormal world (S.fromList (V.toList (SS.scriptResultActiveContracts r))) r
        $$ text "" -- add a newline at the end
    file = toNormalizedFilePath' "Test.daml"
    getIdeState = do
      vfs <- makeVFSHandle
      logger <- Logger.newStderrLogger Logger.Error "script-service"
      service <- getService
      getDamlIdeState
        (options lfVersion)
        (StudioAutorunAllScripts True)
        (Just service)
        logger
        noopDebouncer
        (DummyLspEnv $ NotificationHandler $ \_ _ -> pure ())
        vfs
