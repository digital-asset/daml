-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

-- Abstraction for a Ledger which is hosting the Chat domain model.
-- This is basically unchanged from the Nim example
module DA.Chat.ChatLedger (Handle, connect, sendCommand, getTrans) where

import DA.Chat.Contracts (ChatContract,extractTransaction,makeLedgerCommand)
import DA.Chat.Logging (Logger)
import DA.Ledger as Ledger
import Data.Maybe (maybeToList)
import System.Random (randomIO)
import qualified Data.Text.Lazy as Text (pack)
import qualified Data.UUID as UUID

data Handle = Handle {
    log :: Logger,
    lid :: LedgerId,
    pid :: PackageId
    }

type Rejection = String

port :: Port
port = 6865 -- port on which we expect to find a ledger. should be a command line option

run :: TimeoutSeconds -> LedgerService a -> IO a
run timeout ls  = runLedgerService ls timeout (configOfPort port)

connect :: Logger -> IO Handle
connect log = do
    lid <- run 5 getLedgerIdentity
    ids <- run 5 $ listPackages lid
    [_,_,pid] <- return ids -- guess which is the Chat package -- TODO: fix this properly!
    return Handle{log,lid,pid}

sendCommand :: Party -> Handle -> ChatContract -> IO (Maybe Rejection)
sendCommand asParty h@Handle{pid} cc = do
    let com = makeLedgerCommand pid cc
    submitCommand h asParty com >>= \case
        Left rejection -> return $ Just rejection
        Right () -> return Nothing

getTrans :: Party -> Handle -> IO (PastAndFuture ChatContract)
getTrans party Handle{log,lid} = do
    pf <- run 6000 $ getTransactionsPF lid party
    mapListPF (fmap concat . mapM (fmap maybeToList . extractTransaction log)) pf

submitCommand :: Handle -> Party -> Command -> IO (Either String ())
submitCommand Handle{lid} party com = do
    cid <- randomCid
    run 5 $ Ledger.submit (Commands {lid,wid,aid=myAid,cid,party,leTime,mrTime,coms=[com]})
    where
        wid = Nothing
        leTime = Timestamp 0 0
        mrTime = Timestamp 5 0
        myAid = ApplicationId "chat-console"

randomCid :: IO CommandId
randomCid = do fmap (CommandId . Text.pack . UUID.toString) randomIO
