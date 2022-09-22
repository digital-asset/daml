-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DuplicateRecordFields #-}

module DA.Ledger.Tests (main) where

import Control.Monad
import Control.Monad.IO.Class(liftIO)
import DA.Bazel.Runfiles
import DA.Daml.LF.Proto3.Archive (decodeArchivePackageId)
import DA.Daml.LF.Reader(DalfManifest(..), Dalfs(..), readDalfs, readDalfManifest)
import DA.Ledger as Ledger
import DA.Test.Sandbox
import Data.List (isInfixOf,(\\))
import Data.IORef
import GHC.Stack
import Prelude hiding(Enum)
import System.Environment.Blank (setEnv)
import System.FilePath
import System.Random (randomIO)
import System.Time.Extra (timeout)
import Test.Tasty as Tasty (TestName,TestTree,testGroup,withResource,defaultMain)
import Test.Tasty.HUnit as Tasty(assertFailure,assertBool,assertEqual,testCase)
import qualified "zip-archive" Codec.Archive.Zip as Zip
import qualified DA.Daml.LF.Ast as LF
import qualified Data.ByteString as BS (readFile)
import qualified Data.ByteString.Lazy as BSL (readFile,toStrict)
import qualified Data.ByteString.UTF8 as BS (ByteString,fromString)
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Text.Lazy as TL(Text,pack,unpack,fromStrict)
import qualified Data.UUID as UUID (toString)
import Data.Text (unpack)
import Data.Either.Extra(maybeToEither)
import qualified Data.Aeson as Aeson
import Data.Aeson.KeyMap(KeyMap)
import qualified Data.Aeson.KeyMap as KeyMap
import Data.Aeson.Key(fromString)
import Data.Aeson(decode)

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    testDar <- locateRunfiles darPath
    Tasty.defaultMain $ testGroup "Ledger bindings"
        [ sharedSandboxTests testDar
        , authenticatingSandboxTests testDar
        , tMeteringReportJson
        ]

type SandboxTest = WithSandbox -> TestTree

sharedSandboxTests :: FilePath -> TestTree
sharedSandboxTests testDar = testGroupWithSandbox testDar Nothing "shared sandbox"
    [ tGetLedgerIdentity
    , tListPackages
    , tGetPackage
    , tGetPackageBad
    , tGetPackageStatusRegistered
    , tGetPackageStatusUnknown
    , tSubmit
    , tSubmitBad
    , tSubmitComplete
    , tCreateWithKey
    , tCreateWithoutKey
    , tStakeholders
    , tPastFuture
    , tGetFlatTransactionByEventId
    , tGetFlatTransactionById
    , tGetTransactions
    , tGetTransactionTrees
    , tGetTransactionByEventId
    , tGetTransactionById
    , tGetActiveContracts
    , tGetLedgerConfiguration
    , tGetTime
    , tSetTime
    , tSubmitAndWait
    , tSubmitAndWaitForTransactionId
    , tSubmitAndWaitForTransaction
    , tSubmitAndWaitForTransactionTree
    , tGetParticipantId
    , tValueConversion
    , tUploadDarFileBad
    , tUploadDarFileGood
    , tAllocateParty
    , tMeteringReport
    ]

authenticatingSandboxTests :: FilePath -> TestTree
authenticatingSandboxTests testDar =
  testGroupWithSandbox testDar mbSecret "shared authenticating sandbox"
  [ tGetLedgerIdentity
  , tListPackages
  , tAllocateParty
  , tUploadDarFileGood
  ]
  where mbSecret = Just (Secret "Brexit-is-a-very-silly-idea")

allocateTestParty :: Party -> LedgerService PartyDetails
allocateTestParty party =
    allocateParty AllocatePartyRequest { partyIdHint = unParty party, displayName = "" }

run :: WithSandbox -> (DarMetadata -> TestId -> LedgerService ()) -> IO ()
run withSandbox f =
  withSandbox $ \sandbox maybeAuth darMetadata testId -> runWithSandbox sandbox maybeAuth testId (f darMetadata testId)

tGetLedgerIdentity :: SandboxTest
tGetLedgerIdentity withSandbox = testCase "getLedgerIdentity" $ run withSandbox $ \_darMetadata _testId -> do
    lid <- getLedgerIdentity
    liftIO $ assertEqual "ledger-id" lid (LedgerId "my-ledger-id")

tListPackages :: SandboxTest
tListPackages withSandbox = testCase "listPackages" $ run withSandbox $ \DarMetadata{mainPackageId,manifest} _testId -> do
    lid <- getLedgerIdentity
    pids <- listPackages lid
    liftIO $ do
        assertEqual "#packages" (length $ dalfPaths manifest) (length pids)
        assertBool "The pid is listed" (mainPackageId `elem` pids)

tGetPackage :: SandboxTest
tGetPackage withSandbox = testCase "getPackage" $ run withSandbox $ \DarMetadata{mainPackageId} _testId -> do
    lid <-  getLedgerIdentity
    Just (Package bs) <- getPackage lid mainPackageId
    liftIO $ assertBool "contents" ("currency" `isInfixOf` show bs)

tGetPackageBad :: SandboxTest
tGetPackageBad withSandbox = testCase "getPackage/bad" $ run withSandbox $ \_darMetadata _testId -> do
    lid <- getLedgerIdentity
    let pid = PackageId "xxxxxxxxxxxxxxxxxxxxxx"
    Nothing <- getPackage lid pid
    return ()

tGetPackageStatusRegistered :: SandboxTest
tGetPackageStatusRegistered withSandbox = testCase "getPackageStatus/Registered" $ run withSandbox $ \DarMetadata{mainPackageId} _testId -> do
    lid <- getLedgerIdentity
    status <- getPackageStatus lid mainPackageId
    liftIO $ assertBool "status" (status == PackageStatusREGISTERED)

tGetPackageStatusUnknown :: SandboxTest
tGetPackageStatusUnknown withSandbox = testCase "getPackageStatus/Unknown" $ run withSandbox $ \_darMetadata _testId -> do
    lid <- getLedgerIdentity
    let pid = PackageId "xxxxxxxxxxxxxxxxxxxxxx"
    status <- getPackageStatus lid pid
    liftIO $ assertBool "status" (status == PackageStatusUNKNOWN)

tSubmit :: SandboxTest
tSubmit withSandbox = testCase "submit" $ run withSandbox $ \DarMetadata{mainPackageId} testId -> do
    lid <- getLedgerIdentity
    let command =  createIOU mainPackageId (alice testId) "A-coin" 100
    _ <- fromRight =<< submitCommand lid (alice testId) command
    return ()

tSubmitBad :: SandboxTest
tSubmitBad withSandbox = testCase "submit/bad" $ run withSandbox $ \_darMetadata testId -> do
    lid <- getLedgerIdentity
    let pid = PackageId "xxxxxxxxxxxxxxxxxxxxxx"
    let command =  createIOU pid (alice testId) "A-coin" 100
    Left err <- submitCommand lid (alice testId) command
    liftIO $ assertTextContains err "Couldn't find package"

fromRight :: (Show a, MonadFail m) => HasCallStack => Either a b -> m b
fromRight (Left err) =
    fail $ unlines
      ["Expected a Right but got a Left: " <> show err
      , prettyCallStack callStack
      ]
fromRight (Right r) = pure r

tSubmitComplete :: SandboxTest
tSubmitComplete withSandbox = testCase "tSubmitComplete" $ run withSandbox $ \DarMetadata{mainPackageId} testId -> do
    lid <- getLedgerIdentity
    let command = createIOU mainPackageId (alice testId) "A-coin" 100
    completions <- completionStream (lid,myAid,[alice testId],Nothing)
    off0 <- completionEnd lid
    cidA1 <- fromRight =<< submitCommand lid (alice testId) command
    (Just Checkpoint{offset=cp1},[Completion{cid=cidB1}]) <- fromRight =<< liftIO (takeStream completions)
    off1 <- completionEnd lid
    cidA2 <- fromRight =<< submitCommand lid (alice testId) command
    (Just Checkpoint{offset=cp2},[Completion{cid=cidB2}]) <- fromRight =<< liftIO (takeStream completions)
    off2 <- completionEnd lid

    liftIO $ do
        assertEqual "cidB1" cidA1 cidB1
        assertEqual "cidB2" cidA2 cidB2
        assertBool "off0 /= off1" (off0 /= off1)
        assertBool "off1 /= off2" (off1 /= off2)

        assertEqual "cp1" off1 cp1
        assertEqual "cp2" off2 cp2

    completionsX <- completionStream (lid,myAid,[alice testId],Just (LedgerAbsOffset off0))
    completionsY <- completionStream (lid,myAid,[alice testId],Just (LedgerAbsOffset off1))

    (Just Checkpoint{offset=cpX},[Completion{cid=cidX}]) <- fromRight =<< liftIO (takeStream completionsX)
    (Just Checkpoint{offset=cpY},[Completion{cid=cidY}]) <- fromRight =<< liftIO (takeStream completionsY)

    liftIO $ do
        assertEqual "cidX" cidA1 cidX
        assertEqual "cidY" cidA2 cidY
        assertEqual "cpX" cp1 cpX
        assertEqual "cpY" cp2 cpY

tCreateWithKey :: SandboxTest
tCreateWithKey withSandbox = testCase "createWithKey" $ run withSandbox $ \DarMetadata{mainPackageId} testId -> do
    lid <- getLedgerIdentity
    _ <- allocateTestParty (alice testId)
    withGetAllTransactions lid (alice testId) (Verbosity False) $ \txs -> do
    let command = createWithKey mainPackageId (alice testId) 100
    _ <- fromRight =<< submitCommand lid (alice testId) command
    liftIO $ do
        Just (Right [Transaction{events=[CreatedEvent{key}]}]) <- timeout 1 (takeStream txs)
        assertEqual "contract has right key" key (Just (VRecord (Record Nothing [ RecordField "" (VParty (alice testId)), RecordField "" (VInt 100) ])))

tCreateWithoutKey :: SandboxTest
tCreateWithoutKey withSandbox = testCase "createWithoutKey" $ run withSandbox $ \DarMetadata{mainPackageId} testId -> do
    lid <- getLedgerIdentity
    _ <- allocateTestParty (alice testId)
    withGetAllTransactions lid (alice testId) (Verbosity False) $ \txs -> do
    let command = createWithoutKey mainPackageId (alice testId) 100
    _ <- fromRight =<< submitCommand lid (alice testId) command
    liftIO $ do
        Just (Right [Transaction{events=[CreatedEvent{key}]}]) <- timeout 1 (takeStream txs)
        assertEqual "contract has no key" key Nothing

tStakeholders :: WithSandbox -> Tasty.TestTree
tStakeholders withSandbox = testCase "stakeholders are exposed correctly" $ run withSandbox $ \DarMetadata{mainPackageId} testId -> do
    lid <- getLedgerIdentity
    _ <- allocateTestParty (alice testId)
    withGetTransactionsPF lid (alice testId) $ \PastAndFuture {future=txs} -> do
    let command = createIOU mainPackageId (alice testId) "(alice testId)-in-chains" 100
    _ <- submitCommand lid (alice testId) command
    liftIO $ do
        Just (Right [Transaction{events=[CreatedEvent{signatories,observers}]}]) <- timeout 1 (takeStream txs)
        assertEqual "the only signatory" signatories [alice testId]
        assertEqual "observers are empty" observers []

tPastFuture :: SandboxTest
tPastFuture withSandbox = testCase "past/future" $ run withSandbox $ \DarMetadata{mainPackageId} testId -> do
    lid <- getLedgerIdentity
    _ <- allocateTestParty (alice testId)
    let command =  createIOU mainPackageId (alice testId) "A-coin" 100
    withGetTransactionsPF lid (alice testId) $ \PastAndFuture {past=past1,future=future1} -> do
    -- We need a submitandWait here to make sure that the
    -- second subscription to the transaction stream
    -- comes after this has been applied.
    _ <- fromRight =<< submitAndWaitCommand lid (alice testId) command
    withGetTransactionsPF lid (alice testId) $ \PastAndFuture {past=past2,future=future2} -> do
    _ <- fromRight =<< submitAndWaitCommand lid (alice testId) command
    liftIO $ do
        Just (Right x1) <- timeout 1 (takeStream future1)
        Just (Right y1) <- timeout 1 (takeStream future1)
        Just (Right y2) <- timeout 1 (takeStream future2)
        assertEqual "past is initially empty" [] past1
        assertEqual "future becomes the past" [x1] past2
        assertEqual "continuing future matches" y1 y2

tGetFlatTransactionByEventId :: SandboxTest
tGetFlatTransactionByEventId withSandbox = testCase "tGetFlatTransactionByEventId" $ run withSandbox $ \DarMetadata{mainPackageId} testId -> do
    lid <- getLedgerIdentity
    _ <- allocateTestParty (alice testId)
    withGetAllTransactions lid (alice testId) (Verbosity True) $ \txs -> do
    _ <- fromRight =<< submitCommand lid (alice testId) (createIOU mainPackageId (alice testId) "A-coin" 100)
    Just (Right [txOnStream]) <- liftIO $ timeout 1 (takeStream txs)
    Transaction{events=[CreatedEvent{eid}]} <- return txOnStream
    Just txByEventId <- getFlatTransactionByEventId lid eid [alice testId]
    liftIO $ assertEqual "tx" txOnStream txByEventId
    Nothing <- getFlatTransactionByEventId lid (EventId "eeeeee") [alice testId]
    return ()

tGetFlatTransactionById :: SandboxTest
tGetFlatTransactionById withSandbox = testCase "tGetFlatTransactionById" $ run withSandbox $ \DarMetadata{mainPackageId} testId -> do
    lid <- getLedgerIdentity
    _ <- allocateTestParty (alice testId)
    withGetAllTransactions lid (alice testId) (Verbosity True) $ \txs -> do
    _ <- fromRight =<< submitCommand lid (alice testId) (createIOU mainPackageId (alice testId) "A-coin" 100)
    Just (Right [txOnStream]) <- liftIO $ timeout 1 (takeStream txs)
    Transaction{trid} <- return txOnStream
    Just txById <- getFlatTransactionById lid trid [alice testId]
    liftIO $ assertEqual "tx" txOnStream txById
    Nothing <- getFlatTransactionById lid (TransactionId "xxxxx") [alice testId]
    return ()

tGetTransactions :: SandboxTest
tGetTransactions withSandbox = testCase "tGetTransactions" $ run withSandbox $ \DarMetadata{mainPackageId} testId -> do
    lid <- getLedgerIdentity
    _ <- allocateTestParty (alice testId)
    withGetAllTransactions lid (alice testId) (Verbosity True) $ \txs -> do
    cidA <- fromRight =<< submitCommand lid (alice testId) (createIOU mainPackageId (alice testId) "A-coin" 100)
    Just (Right [Transaction{cid=Just cidB}]) <- liftIO $ timeout 1 (takeStream txs)
    liftIO $ do assertEqual "cid" cidA cidB

tGetTransactionTrees :: SandboxTest
tGetTransactionTrees withSandbox = testCase "tGetTransactionTrees" $ run withSandbox $ \DarMetadata{mainPackageId} testId -> do
    lid <- getLedgerIdentity
    _ <- allocateTestParty (alice testId)
    withGetAllTransactionTrees lid (alice testId) (Verbosity True) $ \txs -> do
    cidA <- fromRight =<< submitCommand lid (alice testId) (createIOU mainPackageId (alice testId) "A-coin" 100)
    Just (Right [TransactionTree{cid=Just cidB}]) <- liftIO $ timeout 1 (takeStream txs)
    liftIO $ do assertEqual "cid" cidA cidB

tGetTransactionByEventId :: SandboxTest
tGetTransactionByEventId withSandbox = testCase "tGetTransactionByEventId" $ run withSandbox $ \DarMetadata{mainPackageId} testId -> do
    lid <- getLedgerIdentity
    _ <- allocateTestParty (alice testId)
    withGetAllTransactionTrees lid (alice testId) (Verbosity True) $ \txs -> do
    _ <- fromRight =<< submitCommand lid (alice testId) (createIOU mainPackageId (alice testId) "A-coin" 100)
    Just (Right [txOnStream]) <- liftIO $ timeout 1 (takeStream txs)
    TransactionTree{roots=[eid]} <- return txOnStream
    Just txByEventId <- getTransactionByEventId lid eid [alice testId]
    liftIO $ assertEqual "tx" txOnStream txByEventId
    Nothing <- getTransactionByEventId lid (EventId "eeeeee") [alice testId]
    return ()

tGetTransactionById :: SandboxTest
tGetTransactionById withSandbox = testCase "tGetTransactionById" $ run withSandbox $ \DarMetadata{mainPackageId} testId -> do
    lid <- getLedgerIdentity
    _ <- allocateTestParty (alice testId)
    withGetAllTransactionTrees lid (alice testId) (Verbosity True) $ \txs -> do
    _ <- fromRight =<< submitCommand lid (alice testId) (createIOU mainPackageId (alice testId) "A-coin" 100)
    Just (Right [txOnStream]) <- liftIO $ timeout 1 (takeStream txs)
    TransactionTree{trid} <- return txOnStream
    Just txById <- getTransactionById lid trid [alice testId]
    liftIO $ assertEqual "tx" txOnStream txById
    Nothing <- getTransactionById lid (TransactionId "xxxxx") [alice testId]
    return ()

tGetActiveContracts :: SandboxTest
tGetActiveContracts withSandbox = testCase "tGetActiveContracts" $ run withSandbox $ \DarMetadata{mainPackageId} testId -> do
    lid <- getLedgerIdentity
    _ <- allocateTestParty (alice testId)
    -- no active contracts here
    [(off1,_,[])] <- getActiveContracts lid (filterEverythingForParty (alice testId)) (Verbosity True)
    -- so let's create one
    _ <- fromRight =<< submitCommand lid (alice testId) (createIOU mainPackageId (alice testId) "A-coin" 100)
    withGetAllTransactions lid (alice testId) (Verbosity True) $ \txs -> do
    Just (Right [Transaction{events=[ev]}]) <- liftIO $ timeout 1 (takeStream txs)
    -- and then we get it
    [(_,_,[active]),(off3,_,[])] <- getActiveContracts lid (filterEverythingForParty (alice testId)) (Verbosity True)
    liftIO $ do
        -- All but the last offset are meaningless
        assertBool "off3 > off1" (off3 > off1)
        assertEqual "active" ev active

tGetLedgerConfiguration :: SandboxTest
tGetLedgerConfiguration withSandbox = testCase "tGetLedgerConfiguration" $ run withSandbox $ \_darMetadata _testId -> do
    lid <- getLedgerIdentity
    xs <- getLedgerConfiguration lid
    Just (Right config) <- liftIO $ timeout 1 (takeStream xs)
    let expected = LedgerConfiguration {
            maxDeduplicationDuration = Duration {durationSeconds = 1800, durationNanos = 0}}
    liftIO $ assertEqual "config" expected config

tUploadDarFileBad :: SandboxTest
tUploadDarFileBad withSandbox = testCase "tUploadDarFileBad" $ run withSandbox $ \_darMetadata _testId -> do
    lid <- getLedgerIdentity
    let bytes = BS.fromString "not-the-bytes-for-a-darfile"
    Left err <- uploadDarFileGetPid lid bytes
    liftIO $ assertTextContains err "Dar file is corrupt"

tUploadDarFileGood :: SandboxTest
tUploadDarFileGood withSandbox = testCase "tUploadDarFileGood" $ run withSandbox $ \_darMetadata testId -> do
    lid <- getLedgerIdentity
    _ <- allocateTestParty (alice testId)
    bytes <- liftIO getBytesForUpload
    before <- listKnownPackages
    pid <- uploadDarFileGetPid lid bytes >>= either (liftIO . assertFailure) return
    after <- listKnownPackages
    let getPid PackageDetails{pid} = pid
    liftIO $ assertEqual "new pids"
        (Set.fromList (map getPid after) Set.\\ Set.fromList (map getPid before))
        (Set.singleton pid)
    cidA <- submitCommand lid (alice testId) (createExtra pid (alice testId)) >>= either (liftIO . assertFailure) return
    withGetAllTransactions lid (alice testId) (Verbosity True) $ \txs -> do
    Just (Right [Transaction{cid=Just cidB}]) <- liftIO $ timeout 10 (takeStream txs)
    liftIO $ do assertEqual "cid" cidA cidB
    where
        createExtra :: PackageId -> Party -> Command
        createExtra pid party = CreateCommand {tid,args}
            where
                tid = TemplateId (Identifier pid mod ent)
                mod = ModuleName "ExtraModule"
                ent = EntityName "ExtraTemplate"
                args = Record Nothing [
                    RecordField "owner" (VParty party),
                    RecordField "message" (VText "Hello extra module")
                    ]

getBytesForUpload :: IO BS.ByteString
getBytesForUpload = do
        let extraDarFilename = "language-support/hs/bindings/for-upload.dar"
        file <- locateRunfiles (mainWorkspace </> extraDarFilename)
        BS.readFile file

-- Would be nice if the underlying service returned the pid on successful upload.
uploadDarFileGetPid :: LedgerId -> BS.ByteString -> LedgerService (Either String PackageId)
uploadDarFileGetPid lid bytes = do
    before <- listPackages lid
    uploadDarFile bytes >>= \case -- call the actual service
        Left m -> return $ Left m
        Right () -> do
            after <- listPackages lid
            [newPid] <- return (after \\ before) -- see what new pid appears
            return $ Right newPid


tGetTime :: SandboxTest
tGetTime withSandbox = testCase "tGetTime" $ run withSandbox $ \_ _testId -> do
    lid <- getLedgerIdentity
    xs <- Ledger.getTime lid
    Just (Right time1) <- liftIO $ timeout 1 (takeStream xs)
    let expect1 = Timestamp {seconds = 0, nanos = 0}
    liftIO $  assertEqual "time1" expect1 time1


tSetTime :: SandboxTest
tSetTime withSandbox = testCase "tSetTime" $ run withSandbox $ \_ _testId -> do
    lid <- getLedgerIdentity
    xs <- Ledger.getTime lid

    let t00 = Timestamp {seconds = 0, nanos = 0}
    let t11 = Timestamp {seconds = 1, nanos = 1}
    let t22 = Timestamp {seconds = 2, nanos = 2}
    let t33 = Timestamp {seconds = 3, nanos = 3}

    Just (Right time) <- liftIO $ timeout 1 (takeStream xs)
    liftIO $ assertEqual "time1" t00 time -- initially the time is 0,0

    () <- fromRight =<< Ledger.setTime lid t00 t11
    Just (Right time) <- liftIO $ timeout 1 (takeStream xs)
    liftIO $ assertEqual "time2" t11 time -- time is 1,1 as we set it

    _bad <- Ledger.setTime lid t00 t22 -- the wrong current_time was passed, so the time was not set
    -- Left _ <- return _bad -- Bug in the sandbox cause this to fail

    () <- fromRight =<< Ledger.setTime lid t11 t33
    Just (Right time) <- liftIO $ timeout 1 (takeStream xs)
    liftIO $ assertEqual "time3" t33 time  -- time is 3,3 as we set it

requiresAuthorizerButGot :: Party -> Party -> String -> IO ()
requiresAuthorizerButGot (Party required) (Party given) err =
    assertTextContains err $ "requires authorizers " <> TL.unpack required <> ", but only " <> TL.unpack given <> " were given"

tSubmitAndWait :: SandboxTest
tSubmitAndWait withSandbox =
    testCase "tSubmitAndWait" $ run withSandbox $ \DarMetadata{mainPackageId} testId -> do
    lid <- getLedgerIdentity
    _ <- allocateTestParty (alice testId)
    withGetAllTransactions lid (alice testId) (Verbosity False) $ \txs -> do
    -- bad
    (_cid,commands) <- liftIO $ makeCommands lid (alice testId) $ createIOU mainPackageId (bob testId) "B-coin" 100
    Left err <- submitAndWait commands
    liftIO $ requiresAuthorizerButGot (bob testId) (alice testId) err
    -- good
    (_cid,commands) <- liftIO $ makeCommands lid (alice testId) $ createIOU mainPackageId (alice testId) "A-coin" 100
    () <- fromRight =<< submitAndWait commands
    Just (Right [_]) <- liftIO $ timeout 1 $ takeStream txs
    return ()

tSubmitAndWaitForTransactionId :: SandboxTest
tSubmitAndWaitForTransactionId withSandbox =
    testCase "tSubmitAndWaitForTransactionId" $ run withSandbox $ \DarMetadata{mainPackageId} testId -> do
    lid <- getLedgerIdentity
    _ <- allocateTestParty (alice testId)
    withGetAllTransactions lid (alice testId) (Verbosity False) $ \txs -> do
    -- bad
    (_cid,commands) <- liftIO $ makeCommands lid (alice testId) $ createIOU mainPackageId (bob testId) "B-coin" 100
    Left err <- submitAndWaitForTransactionId commands
    liftIO $ requiresAuthorizerButGot (bob testId) (alice testId) err
    -- good
    (_cid,commands) <- liftIO $ makeCommands lid (alice testId) $ createIOU mainPackageId (alice testId) "A-coin" 100
    trid <- fromRight =<< submitAndWaitForTransactionId commands
    Just (Right [Transaction{trid=tridExpected}]) <- liftIO $ timeout 1 $ takeStream txs
    liftIO $ assertEqual "trid" tridExpected trid

tSubmitAndWaitForTransaction :: SandboxTest
tSubmitAndWaitForTransaction withSandbox =
    testCase "tSubmitAndWaitForTransaction" $ run withSandbox $ \DarMetadata{mainPackageId} testId -> do
    lid <- getLedgerIdentity
    _ <- allocateTestParty (alice testId)
    withGetAllTransactions lid (alice testId) (Verbosity True) $ \txs -> do
    -- bad
    (_cid,commands) <- liftIO $ makeCommands lid (alice testId) $ createIOU mainPackageId (bob testId) "B-coin" 100
    Left err <- submitAndWaitForTransaction commands
    liftIO $ requiresAuthorizerButGot (bob testId) (alice testId) err
    -- good
    (_cid,commands) <- liftIO $ makeCommands lid (alice testId) $ createIOU mainPackageId (alice testId) "A-coin" 100
    trans <- fromRight =<< submitAndWaitForTransaction commands
    Just (Right [transExpected]) <- liftIO $ timeout 1 $ takeStream txs
    liftIO $ assertEqual "trans" transExpected trans

tSubmitAndWaitForTransactionTree :: SandboxTest
tSubmitAndWaitForTransactionTree withSandbox =
    testCase "tSubmitAndWaitForTransactionTree" $ run withSandbox $ \DarMetadata{mainPackageId} testId -> do
    lid <- getLedgerIdentity
    _ <- allocateTestParty (alice testId)
    withGetAllTransactionTrees lid (alice testId) (Verbosity True) $ \txs -> do
    -- bad
    (_cid,commands) <- liftIO $ makeCommands lid (alice testId) $ createIOU mainPackageId (bob testId) "B-coin" 100
    Left err <- submitAndWaitForTransactionTree commands
    liftIO $ requiresAuthorizerButGot (bob testId) (alice testId) err
    -- good
    (_cid,commands) <- liftIO $ makeCommands lid (alice testId) $ createIOU mainPackageId (alice testId) "A-coin" 100
    tree <- fromRight =<< submitAndWaitForTransactionTree commands
    Just (Right [treeExpected]) <- liftIO $ timeout 1 $ takeStream txs
    liftIO $ assertEqual "tree" treeExpected tree


tGetParticipantId :: SandboxTest
tGetParticipantId withSandbox = testCase "tGetParticipantId" $ run withSandbox $ \_darMetadata _testId -> do
    id <- getParticipantId
    liftIO $ assertEqual "participant" (ParticipantId "sandbox-participant") id

tAllocateParty :: SandboxTest
tAllocateParty withSandbox = testCase "tAllocateParty" $ run withSandbox $ \_darMetadata (TestId testId) -> do
    let party = Party (TL.pack $ "me" <> show testId)
    before <- listKnownParties
    let displayName = "Only Me"
    let request = AllocatePartyRequest { partyIdHint = unParty party, displayName }
    deats <- allocateParty request
    let expected = PartyDetails { party, displayName, isLocal = True }
    liftIO $ assertEqual "deats" expected deats
    after <- listKnownParties
    liftIO $ assertEqual "new parties"
        (Set.fromList after Set.\\ Set.fromList before)
        (Set.singleton expected)

bucket :: Value
bucket = VRecord $ Record Nothing
    [ RecordField "record" $ VRecord $ Record Nothing
        [ RecordField "foo" $ VBool False
        , RecordField "bar" $ VText "sheep"
        ]
    , RecordField "variants" $ VList
        [ VVariant $ Variant Nothing (ConstructorId "B") (VBool True)
        , VVariant $ Variant Nothing (ConstructorId "I") (VInt 99)
        ]
    , RecordField "contract"$ VContract (ContractId "00010203040506070809101112131415161718192021212121212121212121303132")
    , RecordField "list"    $ VList []
    , RecordField "int"     $ VInt 42
    , RecordField "decimal" $ VDecimal 123.456
    , RecordField "text"    $ VText "OMG lol"
    , RecordField "time"    $ VTime (MicroSecondsSinceEpoch $ 1000 * 1000 * 60 * 60 * 24 * 365 * 50)
    , RecordField "party"   $ VParty $ Party "good time"
    , RecordField "bool"    $ VBool False
    , RecordField "unit"      VUnit
    , RecordField "date"    $ VDate $ DaysSinceEpoch 123
    , RecordField "opts"    $ VList
        [ VOpt Nothing
        , VOpt $ Just $ VText "something"
        ]
    , RecordField "map"     $ VMap $ Map.fromList [("one",VInt 1),("two",VInt 2),("three",VInt 3)]
    , RecordField "enum"    $ VEnum $ Enum Nothing (ConstructorId "Green")

    ]

tValueConversion :: SandboxTest
tValueConversion withSandbox = testCase "tValueConversion" $ run withSandbox $ \DarMetadata{mainPackageId} testId -> do
    let owner = alice testId
    let mod = ModuleName "Valuepedia"
    let tid = TemplateId (Identifier mainPackageId mod $ EntityName "HasBucket")
    let args = Record Nothing [ RecordField "owner" (VParty owner), RecordField "bucket" bucket ]
    let command = CreateCommand {tid,args}
    lid <- getLedgerIdentity
    _ <- allocateTestParty (alice testId)
    _::CommandId <- submitCommand lid (alice testId) command >>= either (liftIO . assertFailure) return
    withGetAllTransactions lid (alice testId) (Verbosity True) $ \txs -> do
    Just elem <- liftIO $ timeout 1 (takeStream txs)
    trList <- either (liftIO . assertFailure . show) return elem
    [Transaction{events=[CreatedEvent{createArgs=Record{fields}}]}] <- return trList
    [RecordField{label="owner"},RecordField{label="bucket",fieldValue=bucketReturned}] <- return fields
    liftIO $ assertEqual "bucket" bucket (detag bucketReturned)

tMeteringReportJson :: TestTree
tMeteringReportJson = testCase "tMeteringReportJson" $ do
    let sample = "{ \"s\": \"abc\", \"b\": true, \"n\": null, \"d\": 2.3, \"a\": [1,2,3], \"o\": { \"x\": 1, \"y\": 2 } }"
    let expected = decode sample :: Maybe Aeson.Value
    let struct = fmap toRawStructValue expected
    let actual = fmap toRawAesonValue struct
    assertEqual "MeteringReport Serialization" expected actual

expectObject :: Aeson.Value -> Either String (KeyMap Aeson.Value)
expectObject (Aeson.Object keyMap) = Right keyMap
expectObject other = Left $ "Expected object not " <> show other

expectString :: Aeson.Value -> Either String String
expectString (Aeson.String string) = Right (unpack string)
expectString other = Left $ "Expected string not " <> show other

expectField :: String -> KeyMap Aeson.Value -> Either String Aeson.Value
expectField key keyMap = maybeToEither ("Did not find " <> key <> " in " <> show (KeyMap.keys keyMap)) (KeyMap.lookup (fromString key) keyMap)

tMeteringReport :: SandboxTest
tMeteringReport withSandbox = testCase "tMeteringReport" $ run withSandbox $ \_ _testId -> do
    let timestamp = Timestamp {seconds = 3600, nanos = 0}  -- Must be rounded to hour
    let expected = timestampToIso8601 timestamp
    reportValue <- getMeteringReport timestamp Nothing Nothing
    let actual = do
        report <- expectObject reportValue
        requestValue <- expectField "request" report
        request <- expectObject requestValue
        fromValue <- expectField "from" request
        expectString fromValue

    liftIO $ assertEqual "report from date" (Right expected) actual

-- Strip the rid,vid,eid tags recusively from record, variant and enum values
detag :: Value -> Value
detag = \case
    VRecord r -> VRecord $ detagRecord r
    VVariant v -> VVariant $ detagVariant v
    VEnum e -> VEnum $ detagEnum e
    VList xs -> VList $ fmap detag xs
    VOpt opt -> VOpt $ fmap detag opt
    VMap m -> VMap $ fmap detag m
    v -> v
    where
        detagRecord :: Record -> Record
        detagRecord r = r { rid = Nothing, fields = map detagField $ fields r }

        detagField :: RecordField -> RecordField
        detagField f = f { fieldValue = detag $ fieldValue f }

        detagVariant :: Variant -> Variant
        detagVariant v = v { vid = Nothing, value = detag $ value v }

        detagEnum :: Enum -> Enum
        detagEnum e = e { eid = Nothing }

----------------------------------------------------------------------
-- misc ledger ops/commands

newtype TestId = TestId Int
  deriving Show

newtype Secret = Secret { getSecret :: String }

nextTestId :: TestId -> TestId
nextTestId (TestId i) = TestId (i + 1)

alice,bob :: TestId -> Party
alice (TestId i) = Party $ TL.pack $ "Alice" <> show i
bob (TestId i) = Party $ TL.pack $ "Bob" <> show i

createIOU :: PackageId -> Party -> TL.Text -> Int -> Command
createIOU pid party currency quantity = CreateCommand {tid,args}
    where
        tid = TemplateId (Identifier pid mod ent)
        mod = ModuleName "Iou"
        ent = EntityName "Iou"
        args = Record Nothing [
            RecordField "issuer" (VParty party),
            RecordField "owner" (VParty party),
            RecordField "currency" (VText currency),
            RecordField "amount" (VInt quantity)
            ]

createWithKey :: PackageId -> Party -> Int -> Command
createWithKey pid owner n = CreateCommand {tid,args}
    where
        tid = TemplateId (Identifier pid mod ent)
        mod = ModuleName "ContractKeys"
        ent = EntityName "WithKey"
        args = Record Nothing [
            RecordField "owner" (VParty owner),
            RecordField "n" (VInt n)
            ]

createWithoutKey :: PackageId -> Party -> Int -> Command
createWithoutKey pid owner n = CreateCommand {tid,args}
    where
        tid = TemplateId (Identifier pid mod ent)
        mod = ModuleName "ContractKeys"
        ent = EntityName "WithoutKey"
        args = Record Nothing [
            RecordField "owner" (VParty owner),
            RecordField "n" (VInt n)
            ]

submitAndWaitCommand :: LedgerId -> Party -> Command -> LedgerService (Either String CommandId)
submitAndWaitCommand lid party com = do
    (cid,commands) <- liftIO $ makeCommands lid party com
    Ledger.submitAndWait commands >>= \case
        Left s -> return $ Left s
        Right () -> return $ Right cid

submitCommand :: LedgerId -> Party -> Command -> LedgerService (Either String CommandId)
submitCommand lid party com = do
    (cid,commands) <- liftIO $ makeCommands lid party com
    Ledger.submit commands >>= \case
        Left s -> return $ Left s
        Right () -> return $ Right cid

makeCommands :: LedgerId -> Party -> Command -> IO (CommandId,Commands)
makeCommands lid party com = do
    cid <- liftIO randomCid
    let wid = Nothing
    return $ (cid,) $ Commands {lid,wid,aid=myAid,cid,actAs=[party],readAs=[],dedupPeriod=Nothing,coms=[com],minLeTimeAbs=Nothing,minLeTimeRel=Nothing,sid=Nothing}


myAid :: ApplicationId
myAid = ApplicationId ":my-application:"

randomCid :: IO CommandId
randomCid = do fmap (CommandId . TL.pack . UUID.toString) randomIO

----------------------------------------------------------------------
-- runWithSandbox

runWithSandbox :: forall a. Port -> Maybe Secret -> TestId -> LedgerService a -> IO a
runWithSandbox port mbSecret tid ls = runLedgerService ls' timeout (configOfPort port)
    where timeout = 60 :: TimeoutSeconds
          ls' :: LedgerService a
          ls' = case mbSecret of
            Nothing -> ls
            Just secret -> do
              let tok = Ledger.Token ("Bearer " <> makeSignedJwt' secret tid)
              setToken tok ls

makeSignedJwt' :: Secret -> TestId -> String
makeSignedJwt' secret tid =
    makeSignedJwt (getSecret secret) [TL.unpack $ unParty $ p tid | p <- [alice, bob]]

----------------------------------------------------------------------
-- misc expectation combinators

assertTextContains :: String -> String -> IO ()
assertTextContains text frag =
    unless (frag `isInfixOf` text) (assertFailure msg)
    where msg = "expected frag: " ++ frag ++ "\n contained in: " ++ text

darPath :: FilePath
darPath = mainWorkspace </> "language-support/hs/bindings/for-tests.dar"

testGroupWithSandbox :: FilePath -> Maybe Secret -> TestName -> [WithSandbox -> TestTree] -> TestTree
testGroupWithSandbox testDar mbSecret name tests =
    withSandbox defaultSandboxConf { dars = [ testDar ], timeMode = Static, mbLedgerId = Just "my-ledger-id" } $ \getSandboxPort ->
    withResource (readDarMetadata testDar) (const $ pure ()) $ \getDarMetadata ->
    withResource (newIORef $ TestId 0) (const $ pure ()) $ \getTestCounter ->
    let run :: WithSandbox
        run f = do
            port <- getSandboxPort
            darMetadata <- getDarMetadata
            testCounter <- getTestCounter
            testId <- atomicModifyIORef testCounter (\x -> (nextTestId x, x))
            f (Port port) mbSecret darMetadata testId
    in testGroup name $ map (\f -> f run) tests

readDarMetadata :: FilePath -> IO DarMetadata
readDarMetadata dar = do
    archive <- Zip.toArchive <$> BSL.readFile dar
    Dalfs { mainDalf } <- either fail pure $ readDalfs archive
    manifest <- either fail pure $ readDalfManifest archive
    LF.PackageId pId <- either (fail . show) pure $ decodeArchivePackageId (BSL.toStrict mainDalf)
    pure $ DarMetadata (PackageId $ TL.fromStrict pId) manifest

data DarMetadata = DarMetadata
  { mainPackageId :: PackageId
  , manifest :: DalfManifest
  }

type WithSandbox = (Port -> Maybe Secret -> DarMetadata -> TestId -> IO ()) -> IO ()
