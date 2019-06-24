-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Ledger.Tests (main)
where

import qualified Codec.Archive.Zip as Zip
import Control.Concurrent (MVar,newMVar,takeMVar,withMVar)
import Control.Exception (SomeException, try)
import Control.Monad (unless)
import DA.Ledger.Sandbox (Sandbox,SandboxSpec(..),startSandbox,shutdownSandbox,resetSandbox,withSandbox)
import qualified  DA.Daml.LF.Ast as LF
import DA.Daml.LF.Proto3.Archive (decodeArchive)
import DA.Daml.LF.Reader
import qualified Data.ByteString.Lazy as BSL
import Data.List (isPrefixOf,isInfixOf)
import Data.Text.Lazy (Text)
import qualified Data.Text.Lazy as TL
import System.Environment.Blank (setEnv)
import System.Random (randomIO)
import System.Time.Extra (timeout)
import Test.Tasty as Tasty (TestName,TestTree,defaultMain,testGroup,withResource)
import Test.Tasty.HUnit as Tasty (assertBool,assertEqual,assertFailure,testCase)
import qualified DA.Ledger.Sandbox as Sandbox(port)
import qualified Data.Text.Lazy as Text (pack,unpack)
import qualified Data.UUID as UUID (toString)

import DA.Ledger as Ledger -- module under test; import everything

----------------------------------------------------------------------
--main...

main :: IO ()
main = do
    setEnv "TASTY_NUM_THREADS" "1" True
    Tasty.defaultMain tests

tests :: TestTree
tests = testGroupWithSandbox "Haskell Ledger Bindings" [
    tConnect,
    tPastFuture, -- After this test, there is a big delay when resetting the service.
    tSubmitBad,
    tSubmitGood,
    tCreateWithKey,
    tCreateWithoutKey,
    tListPackages,
    tGetPackage
    ]

----------------------------------------------------------------------
-- tests...

tConnect :: WithSandbox -> TestTree
tConnect withSandbox =
    testCase "connect, ledgerid" $
    withSandbox $ \sandbox _ -> do
        h <- connect sandbox
        let lid = Ledger.identity h
        let got = Text.unpack $ unLedgerId lid
        assertBool "bad ledgerId" (looksLikeSandBoxLedgerId got)
            where looksLikeSandBoxLedgerId s = "sandbox-" `isPrefixOf` s && length s == 44

tPastFuture :: WithSandbox -> Tasty.TestTree
tPastFuture withSandbox =
    testCase "past/future" $ do
    withSandbox $ \sandbox pid -> do
        h <- connect sandbox
        let command =  createIOU pid alice "A-coin" 100
        PastAndFuture{past=past1,future=future1} <- Ledger.getTransactionsPF h alice
        _ <- submitCommand h alice command
        PastAndFuture{past=past2,future=future2} <- Ledger.getTransactionsPF h alice
        _ <- submitCommand h alice command
        Just (Right x1) <- timeout 1 (takeStream future1)
        Just (Right y1) <- timeout 1 (takeStream future1)
        Just (Right y2) <- timeout 1 (takeStream future2)
        assertEqual "past is initially empty" [] past1
        assertEqual "future becomes the past" [x1] past2
        assertEqual "continuing future matches" y1 y2
        closeStream future1 gone
        closeStream future2 gone
        where gone = Abnormal "client gone"

tSubmitBad :: WithSandbox -> Tasty.TestTree
tSubmitBad withSandbox =
    testCase "submit bad package id" $ do
    withSandbox $ \sandbox _ -> do
        h <- connect sandbox
        e <- expectException (submitCommand h alice command)
        assertExceptionTextContains e "Couldn't find package"
            where command =  createIOU pid alice "A-coin" 100
                  pid = PackageId "xxxxxxxxxxxxxxxxxxxxxx"

tSubmitGood :: WithSandbox -> Tasty.TestTree
tSubmitGood withSandbox =
    testCase "submit good package id" $ do
    withSandbox $ \sandbox pid -> do
        h <- connect sandbox
        let command =  createIOU pid alice "A-coin" 100
        completions <- Ledger.completions h myAid [alice]
        cid1 <- submitCommand h alice command
        Right comp1 <- takeStream completions
        let Completion{completionCommandId} = comp1
        let cid1' = CommandId completionCommandId
        assertEqual "submit1" cid1' cid1

tCreateWithKey :: WithSandbox -> Tasty.TestTree
tCreateWithKey withSandbox =
    testCase "create contract with key" $ do
    withSandbox $ \sandbox pid -> do
        h <- connect sandbox
        PastAndFuture{future=txs} <- Ledger.getTransactionsPF h alice
        let command = createWithKey pid alice 100
        _ <- submitCommand h alice command
        Just (Right Transaction{events=[CreatedEvent{key}]}) <- timeout 1 (takeStream txs)
        assertEqual "contract has right key" key (Just (VRecord (Record Nothing [ RecordField "" (VParty alice), RecordField "" (VInt 100) ])))
        closeStream txs gone
        where gone = Abnormal "client gone"

tCreateWithoutKey :: WithSandbox -> Tasty.TestTree
tCreateWithoutKey withSandbox =
    testCase "create contract without key" $ do
    withSandbox $ \sandbox pid -> do
        h <- connect sandbox
        PastAndFuture{future=txs} <- Ledger.getTransactionsPF h alice
        let command = createWithoutKey pid alice 100
        _ <- submitCommand h alice command
        Just (Right Transaction{events=[CreatedEvent{key}]}) <- timeout 1 (takeStream txs)
        assertEqual "contract has no key" key Nothing
        closeStream txs gone
        where gone = Abnormal "client gone"

tListPackages :: WithSandbox -> Tasty.TestTree
tListPackages withSandbox =
    testCase "package service, listPackages" $ do
    withSandbox $ \sandbox _ -> do
        h <- connect sandbox
        ids <- Ledger.listPackages h
        assertEqual "#packages" 3 (length ids)

tGetPackage :: WithSandbox -> Tasty.TestTree
tGetPackage withSandbox =
    testCase "package service, get Package" $ do
    withSandbox $ \sandbox _ -> do
        h <- connect sandbox
        ids <- Ledger.listPackages h
        ps <- mapM (Ledger.getPackage h) ids
        assertEqual "#packages" 3 (length ps)

----------------------------------------------------------------------
-- misc expectation combinators

expectException :: IO a -> IO SomeException
expectException io =
    try io >>= \case
        Right _ -> assertFailure "exception was expected"
        Left (e::SomeException) -> return e

assertExceptionTextContains :: SomeException -> String -> IO ()
assertExceptionTextContains e frag =
    unless (frag `isInfixOf` show e) (assertFailure msg)
    where msg = "expected frag: " ++ frag ++ "\n contained in: " ++ show e

----------------------------------------------------------------------
-- misc ledger ops/commands

connect :: Sandbox -> IO LedgerHandle
connect sandbox = Ledger.connectLogging putStrLn (Sandbox.port sandbox)

alice :: Ledger.Party
alice = Ledger.Party "Alice"

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

submitCommand :: LedgerHandle -> Party -> Command -> IO CommandId
submitCommand h party com = do
    let lid = Ledger.identity h
    cid <- randomCid
    Ledger.submitCommands h
        (Commands {lid,wid,aid=myAid,cid,party,leTime,mrTime,coms=[com]})
    return cid
    where
        wid = Nothing
        leTime = Timestamp 0 0
        mrTime = Timestamp 5 0

myAid :: ApplicationId
myAid = ApplicationId ":my-application:"

randomCid :: IO CommandId
randomCid = do fmap (CommandId . Text.pack . UUID.toString) randomIO

----------------------------------------------------------------------
-- test with/out shared sandboxes...

enableSharing :: Bool
enableSharing = True

specQuickstart :: SandboxSpec
specQuickstart = SandboxSpec {dar}
    where dar = "language-support/hs/bindings/quickstart.dar"

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
                pid <- mainPackageId specQuickstart
                withSandbox specQuickstart $ \sandbox -> f sandbox pid
        testGroup name $ map (\f -> f withSandbox') tests

mainPackageId :: SandboxSpec -> IO PackageId
mainPackageId (SandboxSpec dar) = do
    archive <- Zip.toArchive <$> BSL.readFile dar
    let ManifestData { mainDalfContent } = manifestFromDar archive
    case decodeArchive (BSL.toStrict mainDalfContent) of
        Left err -> fail $ show err
        Right (LF.PackageId pId, _) -> pure (PackageId $ TL.fromStrict pId)

----------------------------------------------------------------------
-- SharedSandbox

type WithSandbox = (Sandbox -> PackageId -> IO ()) -> IO ()

data SharedSandbox = SharedSandbox (MVar (Sandbox, PackageId))

acquireShared :: IO SharedSandbox
acquireShared = do
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
