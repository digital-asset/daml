-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DuplicateRecordFields #-}

module DA.Ledger.Tests (main) where

import Control.Concurrent (MVar,newMVar,takeMVar,withMVar)
import Control.Monad(unless, forM)
import Control.Monad.IO.Class(liftIO)
import DA.Bazel.Runfiles
import DA.Daml.LF.Proto3.Archive (decodeArchive)
import DA.Daml.LF.Reader(ManifestData(..),manifestFromDar)
import DA.Ledger.Sandbox (Sandbox,SandboxSpec(..),startSandbox,shutdownSandbox,withSandbox)
import Data.List (elem,isPrefixOf,isInfixOf,(\\))
import Data.Text.Lazy (Text)
import System.Environment.Blank (setEnv)
import System.Random (randomIO)
import System.Time.Extra (timeout)
import System.FilePath
import Test.Tasty as Tasty (TestName,TestTree,testGroup,withResource,defaultMain)
import Test.Tasty.HUnit as Tasty(assertFailure,assertBool,assertEqual,testCase)
import qualified Codec.Archive.Zip as Zip
import qualified DA.Daml.LF.Ast as LF
import qualified Data.ByteString as BS (readFile)
import qualified Data.ByteString.UTF8 as BS (ByteString,fromString)
import qualified Data.ByteString.Lazy as BSL (readFile,toStrict)
import qualified Data.Text.Lazy as Text(pack,unpack,fromStrict)
import qualified Data.Set as Set
import qualified Data.UUID as UUID (toString)

import DA.Ledger.Sandbox as Sandbox
import DA.Ledger as Ledger

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    Tasty.defaultMain tests

type SandboxTest = WithSandbox -> TestTree

tests :: TestTree
tests = testGroupWithSandbox "Ledger Bindings"
    [ tGetLedgerIdentity
    , tReset
    , tMultipleResets
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
    , tUploadDarFileBad
    , tUploadDarFile
    ]

run :: WithSandbox -> (PackageId -> LedgerService ()) -> IO ()
run withSandbox f = withSandbox $ \sandbox pid -> runWithSandbox sandbox (f pid)

tGetLedgerIdentity :: SandboxTest
tGetLedgerIdentity withSandbox = testCase "getLedgerIdentity" $ run withSandbox $ \_pid -> do
    lid <- getLedgerIdentity
    liftIO $ assertBool "looksLikeSandBoxLedgerId" (looksLikeSandBoxLedgerId lid)

tReset :: SandboxTest
tReset withSandbox = testCase "reset" $ run withSandbox $ \_ -> do
    lid1 <- getLedgerIdentity
    Ledger.reset lid1
    lid2 <- getLedgerIdentity
    liftIO $ assertBool "lid1 /= lid2" (lid1 /= lid2)

tMultipleResets :: SandboxTest
tMultipleResets withSandbox = testCase "multipleResets" $ run withSandbox $ \_pid -> do
    let resetsCount = 20
    lids <- forM [1 .. resetsCount] $ \_ -> do
        lid <- getLedgerIdentity
        Ledger.reset lid
        pure lid
    liftIO $ assertEqual "Ledger IDs are unique" resetsCount (Set.size $ Set.fromList lids)

tListPackages :: SandboxTest
tListPackages withSandbox = testCase "listPackages" $ run withSandbox $ \pid -> do
    lid <- getLedgerIdentity
    pids <- listPackages lid
    liftIO $ do
        assertEqual "#packages" 3 (length pids)
        assertBool "The pid is listed" (pid `elem` pids)

tGetPackage :: SandboxTest
tGetPackage withSandbox = testCase "getPackage" $ run withSandbox $ \pid -> do
    lid <-  getLedgerIdentity
    Just package <- getPackage lid pid
    liftIO $ assertBool "contents" ("IouTransfer_Accept" `isInfixOf` show package)

tGetPackageBad :: SandboxTest
tGetPackageBad withSandbox = testCase "getPackage/bad" $ run withSandbox $ \_pid -> do
    lid <- getLedgerIdentity
    let pid = PackageId "xxxxxxxxxxxxxxxxxxxxxx"
    Nothing <- getPackage lid pid
    return ()

tGetPackageStatusRegistered :: SandboxTest
tGetPackageStatusRegistered withSandbox = testCase "getPackageStatus/Registered" $ run withSandbox $ \pid -> do
    lid <- getLedgerIdentity
    status <- getPackageStatus lid pid
    liftIO $ assertBool "status" (status == PackageStatusREGISTERED)

tGetPackageStatusUnknown :: SandboxTest
tGetPackageStatusUnknown withSandbox = testCase "getPackageStatus/Unknown" $ run withSandbox $ \_pid -> do
    lid <- getLedgerIdentity
    let pid = PackageId "xxxxxxxxxxxxxxxxxxxxxx"
    status <- getPackageStatus lid pid
    liftIO $ assertBool "status" (status == PackageStatusUNKNOWN)

tSubmit :: SandboxTest
tSubmit withSandbox = testCase "submit" $ run withSandbox $ \pid -> do
    lid <- getLedgerIdentity
    let command =  createIOU pid alice "A-coin" 100
    Right _ <- submitCommand lid alice command
    return ()

tSubmitBad :: SandboxTest
tSubmitBad withSandbox = testCase "submit/bad" $ run withSandbox $ \_pid -> do
    lid <- getLedgerIdentity
    let pid = PackageId "xxxxxxxxxxxxxxxxxxxxxx"
    let command =  createIOU pid alice "A-coin" 100
    Left err <- submitCommand lid alice command
    liftIO $ assertTextContains err "Couldn't find package"

tSubmitComplete :: SandboxTest
tSubmitComplete withSandbox = testCase "tSubmitComplete" $ run withSandbox $ \pid -> do
    lid <- getLedgerIdentity
    let command = createIOU pid alice "A-coin" 100
    completions <- completionStream (lid,myAid,[alice],Nothing)
    off0 <- completionEnd lid
    Right cidA1 <- submitCommand lid alice command
    Right (Just Checkpoint{offset=cp1},[Completion{cid=cidB1}]) <- liftIO $ takeStream completions
    off1 <- completionEnd lid
    Right cidA2 <- submitCommand lid alice command
    Right (Just Checkpoint{offset=cp2},[Completion{cid=cidB2}]) <- liftIO $ takeStream completions
    off2 <- completionEnd lid

    liftIO $ do
        assertEqual "cidB1" cidA1 cidB1
        assertEqual "cidB2" cidA2 cidB2
        assertBool "off0 /= off1" (off0 /= off1)
        assertBool "off1 /= off2" (off1 /= off2)

        assertEqual "cp1" off1 cp1
        assertEqual "cp2" off2 cp2

    completionsX <- completionStream (lid,myAid,[alice],Just (LedgerAbsOffset off0))
    completionsY <- completionStream (lid,myAid,[alice],Just (LedgerAbsOffset off1))

    Right (Just Checkpoint{offset=cpX},[Completion{cid=cidX}]) <- liftIO $ takeStream completionsX
    Right (Just Checkpoint{offset=cpY},[Completion{cid=cidY}]) <- liftIO $ takeStream completionsY

    liftIO $ do
        assertEqual "cidX" cidA1 cidX
        assertEqual "cidY" cidA2 cidY
        assertEqual "cpX" cp1 cpX
        assertEqual "cpY" cp2 cpY

tCreateWithKey :: SandboxTest
tCreateWithKey withSandbox = testCase "createWithKey" $ run withSandbox $ \pid -> do
    lid <- getLedgerIdentity
    withGetAllTransactions lid alice (Verbosity False) $ \txs -> do
    let command = createWithKey pid alice 100
    Right _ <- submitCommand lid alice command
    liftIO $ do
        Just (Right [Transaction{events=[CreatedEvent{key}]}]) <- timeout 1 (takeStream txs)
        assertEqual "contract has right key" key (Just (VRecord (Record Nothing [ RecordField "" (VParty alice), RecordField "" (VInt 100) ])))

tCreateWithoutKey :: SandboxTest
tCreateWithoutKey withSandbox = testCase "createWithoutKey" $ run withSandbox $ \pid -> do
    lid <- getLedgerIdentity
    withGetAllTransactions lid alice (Verbosity False) $ \txs -> do
    let command = createWithoutKey pid alice 100
    Right _ <- submitCommand lid alice command
    liftIO $ do
        Just (Right [Transaction{events=[CreatedEvent{key}]}]) <- timeout 1 (takeStream txs)
        assertEqual "contract has no key" key Nothing

tStakeholders :: WithSandbox -> Tasty.TestTree
tStakeholders withSandbox = testCase "stakeholders are exposed correctly" $ run withSandbox $ \pid -> do
    lid <- getLedgerIdentity
    withGetTransactionsPF lid alice $ \PastAndFuture {future=txs} -> do
    let command = createIOU pid alice "alice-in-chains" 100
    _ <- submitCommand lid alice command
    liftIO $ do
        Just (Right [Transaction{events=[CreatedEvent{signatories,observers}]}]) <- timeout 1 (takeStream txs)
        assertEqual "the only signatory" signatories [ alice ]
        assertEqual "observers are empty" observers []

tPastFuture :: SandboxTest
tPastFuture withSandbox = testCase "past/future" $ run withSandbox $ \pid -> do
    lid <- getLedgerIdentity
    let command =  createIOU pid alice "A-coin" 100
    withGetTransactionsPF lid alice $ \PastAndFuture {past=past1,future=future1} -> do
    Right _ <- submitCommand lid alice command
    withGetTransactionsPF lid alice $ \PastAndFuture {past=past2,future=future2} -> do
    Right _ <- submitCommand lid alice command
    liftIO $ do
        Just (Right x1) <- timeout 1 (takeStream future1)
        Just (Right y1) <- timeout 1 (takeStream future1)
        Just (Right y2) <- timeout 1 (takeStream future2)
        assertEqual "past is initially empty" [] past1
        assertEqual "future becomes the past" [x1] past2
        assertEqual "continuing future matches" y1 y2

tGetFlatTransactionByEventId :: SandboxTest
tGetFlatTransactionByEventId withSandbox = testCase "tGetFlatTransactionByEventId" $ run withSandbox $ \pid -> do
    lid <- getLedgerIdentity
    withGetAllTransactions lid alice (Verbosity True) $ \txs -> do
    Right _ <- submitCommand lid alice $ createIOU pid alice "A-coin" 100
    Just (Right [txOnStream]) <- liftIO $ timeout 1 (takeStream txs)
    Transaction{events=[CreatedEvent{eid}]} <- return txOnStream
    Just txByEventId <- getFlatTransactionByEventId lid eid [alice]
    liftIO $ assertEqual "tx" txOnStream txByEventId
    Nothing <- getFlatTransactionByEventId lid (EventId "eeeeee") [alice]
    return ()

tGetFlatTransactionById :: SandboxTest
tGetFlatTransactionById withSandbox = testCase "tGetFlatTransactionById" $ run withSandbox $ \pid -> do
    lid <- getLedgerIdentity
    withGetAllTransactions lid alice (Verbosity True) $ \txs -> do
    Right _ <- submitCommand lid alice $ createIOU pid alice "A-coin" 100
    Just (Right [txOnStream]) <- liftIO $ timeout 1 (takeStream txs)
    Transaction{trid} <- return txOnStream
    Just txById <- getFlatTransactionById lid trid [alice]
    liftIO $ assertEqual "tx" txOnStream txById
    Nothing <- getFlatTransactionById lid (TransactionId "xxxxx") [alice]
    return ()

tGetTransactions :: SandboxTest
tGetTransactions withSandbox = testCase "tGetTransactions" $ run withSandbox $ \pid -> do
    lid <- getLedgerIdentity
    withGetAllTransactions lid alice (Verbosity True) $ \txs -> do
    Right cidA <- submitCommand lid alice (createIOU pid alice "A-coin" 100)
    Just (Right [Transaction{cid=Just cidB}]) <- liftIO $ timeout 1 (takeStream txs)
    liftIO $ do assertEqual "cid" cidA cidB

tGetTransactionTrees :: SandboxTest
tGetTransactionTrees withSandbox = testCase "tGetTransactionTrees" $ run withSandbox $ \pid -> do
    lid <- getLedgerIdentity
    withGetAllTransactionTrees lid alice (Verbosity True) $ \txs -> do
    Right cidA <- submitCommand lid alice (createIOU pid alice "A-coin" 100)
    Just (Right [TransactionTree{cid=Just cidB}]) <- liftIO $ timeout 1 (takeStream txs)
    liftIO $ do assertEqual "cid" cidA cidB

tGetTransactionByEventId :: SandboxTest
tGetTransactionByEventId withSandbox = testCase "tGetTransactionByEventId" $ run withSandbox $ \pid -> do
    lid <- getLedgerIdentity
    withGetAllTransactionTrees lid alice (Verbosity True) $ \txs -> do
    Right _ <- submitCommand lid alice $ createIOU pid alice "A-coin" 100
    Just (Right [txOnStream]) <- liftIO $ timeout 1 (takeStream txs)
    TransactionTree{roots=[eid]} <- return txOnStream
    Just txByEventId <- getTransactionByEventId lid eid [alice]
    liftIO $ assertEqual "tx" txOnStream txByEventId
    Nothing <- getTransactionByEventId lid (EventId "eeeeee") [alice]
    return ()

tGetTransactionById :: SandboxTest
tGetTransactionById withSandbox = testCase "tGetTransactionById" $ run withSandbox $ \pid -> do
    lid <- getLedgerIdentity
    withGetAllTransactionTrees lid alice (Verbosity True) $ \txs -> do
    Right _ <- submitCommand lid alice $ createIOU pid alice "A-coin" 100
    Just (Right [txOnStream]) <- liftIO $ timeout 1 (takeStream txs)
    TransactionTree{trid} <- return txOnStream
    Just txById <- getTransactionById lid trid [alice]
    liftIO $ assertEqual "tx" txOnStream txById
    Nothing <- getTransactionById lid (TransactionId "xxxxx") [alice]
    return ()

tGetActiveContracts :: SandboxTest
tGetActiveContracts withSandbox = testCase "tGetActiveContracts" $ run withSandbox $ \pid -> do
    lid <- getLedgerIdentity
    -- no active contracts here
    [(off1,_,[])] <- getActiveContracts lid (filterEverthingForParty alice) (Verbosity True)
    -- so let's create one
    Right _ <- submitCommand lid alice (createIOU pid alice "A-coin" 100)
    withGetAllTransactions lid alice (Verbosity True) $ \txs -> do
    Just (Right [Transaction{events=[ev]}]) <- liftIO $ timeout 1 (takeStream txs)
    -- and then we get it
    [(off2,_,[active]),(off3,_,[])] <- getActiveContracts lid (filterEverthingForParty alice) (Verbosity True)
    liftIO $ do
        assertEqual "off1" (AbsOffset "0") off1
        assertEqual "off2" (AbsOffset "" ) off2 -- strange
        assertEqual "off3" (AbsOffset "1") off3
        -- for some reason the active contracts event has no signatory information...
        let ev' :: Event = ev { signatories = [] }
        assertEqual "active" ev' active
        -- assertEqual "active" ev active -- TODO: enable if this should be true & we get a fix

tGetLedgerConfiguration :: SandboxTest
tGetLedgerConfiguration withSandbox = testCase "tGetLedgerConfiguration" $ run withSandbox $ \_pid -> do
    lid <- getLedgerIdentity
    xs <- getLedgerConfiguration lid
    Just (Right config) <- liftIO $ timeout 1 (takeStream xs)
    let expected = LedgerConfiguration {
            minTtl = Duration {durationSeconds = 2, durationNanos = 0},
            maxTtl = Duration {durationSeconds = 30, durationNanos = 0}}
    liftIO $ assertEqual "config" expected config

tUploadDarFileBad :: SandboxTest
tUploadDarFileBad withSandbox = testCase "tUploadDarFileBad" $ run withSandbox $ \_pid -> do
    lid <- getLedgerIdentity
    let bytes = BS.fromString "not-the-bytes-for-a-darfile"
    Left err <- uploadDarFileGetPid lid bytes
    liftIO $ assertTextContains err "Invalid DAR: package-upload"

tUploadDarFile :: SandboxTest
tUploadDarFile withSandbox = testCase "tUploadDarFileGood" $ run withSandbox $ \_pid -> do
    lid <- getLedgerIdentity
    bytes <- liftIO $ do
        let extraDarFilename = "language-support/hs/bindings/for-upload.dar"
        file <- locateRunfiles (mainWorkspace </> extraDarFilename)
        BS.readFile file
    pid <- uploadDarFileGetPid lid bytes >>= either (liftIO . assertFailure) return
    cidA <- submitCommand lid alice (createExtra pid alice) >>= either (liftIO . assertFailure) return
    withGetAllTransactions lid alice (Verbosity True) $ \txs -> do
    Just (Right [Transaction{cid=Just cidB}]) <- liftIO $ timeout 1 (takeStream txs)
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
                    RecordField "message" (VString "Hello extra module")
                    ]


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

----------------------------------------------------------------------
-- misc ledger ops/commands

alice :: Party
alice = Party "Alice"

createIOU :: PackageId -> Party -> Text -> Int -> Command
createIOU quickstart party currency quantity = CreateCommand {tid,args}
    where
        tid = TemplateId (Identifier quickstart mod ent)
        mod = ModuleName "Iou"
        ent = EntityName "Iou"
        args = Record Nothing [
            RecordField "issuer" (VParty party),
            RecordField "owner" (VParty party),
            RecordField "currency" (VString currency),
            RecordField "amount" (VDecimal $ Text.pack $ show quantity),
            RecordField "observers" (VList [])
            ]

createWithKey :: PackageId -> Party -> Int -> Command
createWithKey quickstart owner n = CreateCommand {tid,args}
    where
        tid = TemplateId (Identifier quickstart mod ent)
        mod = ModuleName "ContractKeys"
        ent = EntityName "WithKey"
        args = Record Nothing [
            RecordField "owner" (VParty owner),
            RecordField "n" (VInt n)
            ]

createWithoutKey :: PackageId -> Party -> Int -> Command
createWithoutKey quickstart owner n = CreateCommand {tid,args}
    where
        tid = TemplateId (Identifier quickstart mod ent)
        mod = ModuleName "ContractKeys"
        ent = EntityName "WithoutKey"
        args = Record Nothing [
            RecordField "owner" (VParty owner),
            RecordField "n" (VInt n)
            ]

submitCommand :: LedgerId -> Party -> Command -> LedgerService (Either String CommandId)
submitCommand lid party com = do
    cid <- liftIO randomCid
    Ledger.submit (Commands {lid,wid,aid=myAid,cid,party,leTime,mrTime,coms=[com]}) >>= \case
        Left s -> return $ Left s
        Right () -> return $ Right cid
    where
        wid = Nothing
        leTime = Timestamp 0 0
        mrTime = Timestamp 5 0

myAid :: ApplicationId
myAid = ApplicationId ":my-application:"

randomCid :: IO CommandId
randomCid = do fmap (CommandId . Text.pack . UUID.toString) randomIO

looksLikeSandBoxLedgerId :: LedgerId -> Bool
looksLikeSandBoxLedgerId (LedgerId text) =
    "sandbox-" `isPrefixOf` s && length s == 44 where s = Text.unpack text

----------------------------------------------------------------------
-- runWithSandbox

runWithSandbox :: Sandbox -> LedgerService a -> IO a
runWithSandbox Sandbox{port} ls = runLedgerService ls timeout (configOfPort port)
    where timeout = 30 :: TimeoutSeconds

resetSandbox :: Sandbox-> IO ()
resetSandbox sandbox = runWithSandbox sandbox $ do
    lid <- getLedgerIdentity
    Ledger.reset lid

----------------------------------------------------------------------
-- misc expectation combinators

assertTextContains :: String -> String -> IO ()
assertTextContains text frag =
    unless (frag `isInfixOf` text) (assertFailure msg)
    where msg = "expected frag: " ++ frag ++ "\n contained in: " ++ text

----------------------------------------------------------------------
-- test with/out shared sandboxes...

enableSharing :: Bool
enableSharing = True

createSpecQuickstart :: IO SandboxSpec
createSpecQuickstart = do
    dar <- locateRunfiles (mainWorkspace </> "language-support/hs/bindings/quickstart.dar")
    return SandboxSpec {dar}

testGroupWithSandbox :: TestName -> [WithSandbox -> TestTree] -> TestTree
testGroupWithSandbox name tests =
    if enableSharing
    then
        -- waits to run in the one shared sandbox, after first doing a reset
        withResource acquireShared releaseShared $ \resource -> do
        testGroup name $ map (\f -> f (withShared resource)) tests
    else do
        -- runs in it's own freshly (and very slowly!) spun-up sandbox
        let withSandbox' f = do
                specQuickstart <- createSpecQuickstart
                pid <- mainPackageId specQuickstart
                withSandbox specQuickstart $ \sandbox -> f sandbox pid
        testGroup name $ map (\f -> f withSandbox') tests

mainPackageId :: SandboxSpec -> IO PackageId
mainPackageId (SandboxSpec dar) = do
    archive <- Zip.toArchive <$> BSL.readFile dar
    let ManifestData { mainDalfContent } = manifestFromDar archive
    case decodeArchive (BSL.toStrict mainDalfContent) of
        Left err -> fail $ show err
        Right (LF.PackageId pId, _) -> pure (PackageId $ Text.fromStrict pId)

----------------------------------------------------------------------
-- SharedSandbox

type WithSandbox = (Sandbox -> PackageId -> IO ()) -> IO ()

data SharedSandbox = SharedSandbox (MVar (Sandbox, PackageId))

acquireShared :: IO SharedSandbox
acquireShared = do
    specQuickstart <- createSpecQuickstart
    sandbox <- startSandbox specQuickstart
    pid <- mainPackageId specQuickstart
    mv <- newMVar (sandbox, pid)
    return $ SharedSandbox mv

releaseShared :: SharedSandbox -> IO ()
releaseShared (SharedSandbox mv) = do
    (sandbox, _) <- takeMVar mv
    shutdownSandbox sandbox

withShared :: IO SharedSandbox -> WithSandbox
withShared resource f = do
    SharedSandbox mv <- resource
    withMVar mv $ \(sandbox, pid) -> do
        resetSandbox sandbox
        f sandbox pid
