-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

-- Abstraction for a Ledger which is hosting the Nim domain model.
module NimLedger(Handle, connect, sendCommand, getTrans) where

import Control.Concurrent(forkIO)
import Control.Exception(SomeException,try)
import Control.Monad(join)
import System.Random(randomIO)
import qualified Data.Text.Lazy as Text (pack)
import qualified Data.UUID as UUID

import DA.Ledger as Ledger
import Domain
import Logging
import NimCommand
import NimTrans

mapListPF :: (a -> IO [b]) -> PastAndFuture a -> IO (PastAndFuture b)
mapListPF f PastAndFuture{past=past0,future=future0} = do
    past <- fmap join $ mapM f past0
    future <- mapListStream f future0
    return $ PastAndFuture {past, future}

-- Here a problem with Stream is revealed: To map one stream into another requires concurrency.
-- TODO: restructure processing to avoid the need for a sep Stream/PF
mapListStream :: (a -> IO [b]) -> Stream a -> IO (Stream b)
mapListStream f source = do
    target <- newStream
    let loop = do
            takeStream source >>= \case
                Left closed -> writeStream target (Left closed)
                Right x -> do
                    ys <- f x
                    mapM_ (\y -> writeStream target (Right y)) ys
                    loop
    _ <- forkIO loop
    return target

data Handle = Handle {
    log :: Logger,
    lh :: LedgerHandle,
    pid :: PackageId
    }

type Rejection = String

port :: Port
port = 6865 -- port on which we expect to find a ledger. should be a command line option

connect :: Logger -> IO Handle
connect log = do
    lh <- Ledger.connectLogging log port
    ids <- Ledger.listPackages lh
    --log $ show ids
    [pid,_,_] <- return ids -- assume Nim stuff is in the 1st of 3 packages
    return Handle{log,lh,pid}

sendCommand :: Party -> Handle -> NimCommand -> IO (Maybe Rejection)
sendCommand asParty Handle{pid,lh} xcom = do
    let com = makeLedgerCommands pid xcom
    submitCommand lh asParty com >>= \case
        Left e -> return $ Just (show e)
        Right _cid -> return Nothing

getTrans :: Player -> Handle -> IO (PastAndFuture NimTrans)
getTrans player Handle{log,lh} = do
    let party = partyOfPlayer player
    pf <- Ledger.getTransactionsPF lh party
    mapListPF (extractTransaction log) pf

submitCommand :: LedgerHandle -> Party -> Command -> IO (Either SomeException CommandId)
submitCommand h party com = try $ do
    let lid = Ledger.identity h
    cid <- randomCid
    Ledger.submitCommands h (Commands {lid,wid,aid=myAid,cid,party,leTime,mrTime,coms=[com]})
    return cid
    where
        wid = Nothing
        leTime = Timestamp 0 0
        mrTime = Timestamp 5 0
        myAid = ApplicationId "nim-console"

randomCid :: IO CommandId
randomCid = do fmap (CommandId . Text.pack . UUID.toString) randomIO
