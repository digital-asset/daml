-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

-- Abstraction for a Ledger which is hosting the Chat domain model.
-- This is basically unchanged from the Nim example
module ChatLedger(Handle, connect, sendCommand, getTrans) where

import Contracts(ChatContract,extractTransaction,makeLedgerCommand)
import Control.Concurrent (forkIO)
import Control.Exception (SomeException,try)
import Control.Monad (join)
import DA.Ledger as Ledger
import Data.Maybe (maybeToList)
import Logging (Logger)
import System.Random (randomIO)
import qualified Data.Text.Lazy as Text (pack)
import qualified Data.UUID as UUID

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
    onClose target (closeStream source)
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
    [pid,_,_] <- return ids -- assume Chat stuff is in the 1st of 3 packages
    return Handle{log,lh,pid}

sendCommand :: Party -> Handle -> ChatContract -> IO (Maybe Rejection)
sendCommand asParty Handle{pid,lh} cc = do
    let com = makeLedgerCommand pid cc
    submitCommand lh asParty com >>= \case
        Left e -> return $ Just (show e)
        Right _cid -> return Nothing

getTrans :: Party -> Handle -> IO (PastAndFuture ChatContract)
getTrans party Handle{log,lh} = do
    pf <- Ledger.getTransactionsPF lh party
    mapListPF (fmap maybeToList . extractTransaction log) pf

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
        myAid = ApplicationId "chat-console"

randomCid :: IO CommandId
randomCid = do fmap (CommandId . Text.pack . UUID.toString) randomIO
