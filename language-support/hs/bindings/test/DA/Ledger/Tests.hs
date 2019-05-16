-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Ledger.Tests (main) where

import Control.Monad(unless)
import Control.Exception (SomeException, try)
import DA.Ledger as Ledger
import Data.List (isPrefixOf,isInfixOf)
import qualified Data.Text.Lazy as Text (pack,unpack)
import DA.Ledger.Sandbox as Sandbox(SandboxSpec (..), port, shutdownSandbox, withSandbox)
import Test.Tasty as Tasty (TestTree, defaultMain, testGroup)
import Test.Tasty.HUnit as Tasty (assertEqual, assertBool, assertFailure, testCase)
import Data.Text.Lazy(Text)
import qualified Data.UUID as UUID
import System.Random(randomIO)

import qualified DA.Ledger.LowLevel as LL(Completion(..))

expectException :: IO a -> IO SomeException
expectException io =
    try io >>= \case
        Right _ -> assertFailure "exception was expected"
        Left (e::SomeException) -> return e

assertExceptionTextContains :: SomeException -> String -> IO ()
assertExceptionTextContains e frag =
    unless (frag `isInfixOf` show e) (assertFailure msg)
    where msg = "expected frag: " ++ frag ++ "\n contained in: " ++ show e

main :: IO ()
main = Tasty.defaultMain tests

spec1 :: SandboxSpec
spec1 = SandboxSpec {dar}
    where dar = "language-support/hs/bindings/quickstart.dar"

tests :: TestTree
tests = testGroup "Haskell Ledger Bindings" [
    t1, t2, t3,
    t4, t4_1,
    t5, t6
    -- we really need sandboxes shared between tests..
    --,t1,t1,t1,t1,t1,t1
    ]

t1 :: Tasty.TestTree
t1 = testCase "connect, ledgerid" $ do
    withSandbox spec1 $ \sandbox -> do
        h <- Ledger.connect (Sandbox.port sandbox)
        let lid = Ledger.identity h
        let got = Text.unpack $ Ledger.unLedgerId lid
        assertBool "bad ledgerId" (looksLikeSandBoxLedgerId got)
            where looksLikeSandBoxLedgerId s =
                      "sandbox-" `isPrefixOf` s && length s == 44

t2 :: Tasty.TestTree
t2 = testCase "connect, sandbox dead -> exception" $ do
    withSandbox spec1 $ \sandbox -> do
        shutdownSandbox sandbox -- kill it here
        e <- expectException (Ledger.connect (Sandbox.port sandbox))
        assertExceptionTextContains e "ClientIOError"

t3 :: Tasty.TestTree
t3 = testCase "no transcations to start with" $ do
    withSandbox spec1 $ \sandbox -> do
        h <- Ledger.connect (Sandbox.port sandbox)
        stream <- Ledger.transactions h alice
        (ts,_) <- Ledger.getStreamContents stream
        assertEqual "#transactions" 0 (length ts)

t4 :: Tasty.TestTree
t4 = testCase "submit bad package id" $ do
    withSandbox spec1 $ \sandbox -> do
        h <- Ledger.connect (Sandbox.port sandbox)
        e <- expectException (submitCommand h alice command)
        assertExceptionTextContains e "Couldn't find package"
            where command =  createIOU pid alice "A-coin" 100
                  pid = PackageId "xxxxxxxxxxxxxxxxxxxxxx"

t4_1 :: Tasty.TestTree
t4_1 = testCase "submit good package id" $ do
    withSandbox spec1 $ \sandbox -> do
        h <- Ledger.connect (Sandbox.port sandbox)
        -- TODO: Use Ledger.getPackage to find the correct package with the "Iou" contract.
        [pid,_,_] <- Ledger.listPackages h -- for now assume it's in the 1st of the 3 listed packages.
        let command =  createIOU pid alice "A-coin" 100
        completions <- Ledger.completions h myAid [alice]
        (cs1,_) <- Ledger.getStreamContents completions
        assertEqual "before submit 1" [] cs1
        cid1 <- submitCommand h alice command
        comp1 <- takeStream completions
        let LL.Completion{completionCommandId} = comp1
        let cid1' = CommandId completionCommandId
        assertEqual "submit1" cid1' cid1

t5 :: Tasty.TestTree
t5 = testCase "package service, listPackages" $ do
    withSandbox spec1 $ \sandbox -> do
        h <- Ledger.connect (Sandbox.port sandbox)
        ids <- Ledger.listPackages h
        assertEqual "#packages" 3 (length ids)

t6 :: Tasty.TestTree -- WIP (Ledger.getPackage not working yet)
t6 = testCase "package service, get Package" $ do
    withSandbox spec1 $ \sandbox -> do
        h <- Ledger.connect (Sandbox.port sandbox)
        ids <- Ledger.listPackages h
        ps <- mapM (Ledger.getPackage h) ids
        assertEqual "#packages" 3 (length ps)
        return ()

alice :: Ledger.Party
alice = Ledger.Party "Alice"

createIOU :: PackageId -> Party -> Text -> Int -> Command
createIOU quickstart party currency quantity = CreateCommand {tid,args}
    where
        tid = TemplateId (Identifier quickstart mod ent)
        -- TODO: use package-service to find package-id
        -- da run damlc inspect-dar target/quickstart.dar
        mod = ModuleName "Iou"
        ent = EntityName "Iou"
        args = Record Nothing [
            RecordField "issuer" (VParty party),
            RecordField "owner" (VParty party),
            RecordField "currency" (VString currency),
            RecordField "amount" (VDecimal $ Text.pack $ show quantity),
            RecordField "observers" (VList [])
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
myAid = ApplicationId "<my-application>"

randomCid :: IO CommandId
randomCid = do fmap (CommandId . Text.pack . UUID.toString) randomIO
