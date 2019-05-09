-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module Main(main) where

import Control.Concurrent
import Control.Monad (void)
import Data.Text.Lazy (Text)
import qualified Data.Text.Lazy as Text

import Data.UUID as UUID
import System.Random
import System.Time.Extra (sleep)

import DA.Ledger as Ledger

main :: IO ()
main = do
    let port = Port 7600
    putStrLn $ "Connecting to ledger on port: " ++ show port

    h <- Ledger.connect port
    let lid = Ledger.identity h
    putStrLn $ "LedgerIdentity = " <> show lid

    aliceTs <- Ledger.getTransactionStream h alice
    bobTs <- Ledger.getTransactionStream h bob
    watch (show alice) aliceTs
    watch (show bob) bobTs

    cs <- Ledger.getCompletionStream h myAid [alice,bob]
    watch "completions" cs

    sleep 1
    putStrLn "Create some new contracts..."
    submitCommand h alice (createIOU alice "A-coin" 100)
    submitCommand h bob (createIOU bob "B-coin" 200)

    sleep 1
    putStrLn "Press enter to quit"
    _ <- getLine
    return ()

submitCommand :: LedgerHandle -> Party -> Command -> IO ()
submitCommand h party com = do
    let lid = Ledger.identity h
    cid <- randomCid
    Ledger.submitCommands h
        (Commands {lid,wid,aid=myAid,cid,party,leTime,mrTime,coms=[com]})
    where
        wid = Nothing
        leTime = Timestamp 0 0
        mrTime = Timestamp 5 0


createIOU :: Party -> Text -> Int -> Command
createIOU party currency quantity = CreateCommand {tid,args}
    where
        tid = TemplateId (Identifier quickstart mod ent)
        -- TODO: use package-service to find package-id
        -- da run damlc inspect-dar target/quickstart.dar
        quickstart = PackageId "d2738d858a282364bc66e9c0843ab77e4970769905df03f00934c6716771c972"
        mod = ModuleName "Iou"
        ent = EntityName "Iou"
        args = Record Nothing [
            RecordField "issuer" (VParty party),
            RecordField "owner" (VParty party),
            RecordField "currency" (VString currency),
            RecordField "amount" (VDecimal $ Text.pack $ show quantity),
            RecordField "observers" (VList [])
            ]

alice,bob :: Party
alice = Party "Alice"
bob = Party "Bob"

myAid :: ApplicationId
myAid = ApplicationId "<my-application>"

randomCid :: IO CommandId
randomCid = do fmap (CommandId . Text.pack . UUID.toString) randomIO

watch :: Show a => String -> ResponseStream a -> IO ()
watch tag rs = void $ forkIO $ loop (1::Int)
    where loop n = do
              x <- nextResponse rs
              putStrLn $ tag <> "(" <> show n <> ") = " <> show x
              loop (n+1)
