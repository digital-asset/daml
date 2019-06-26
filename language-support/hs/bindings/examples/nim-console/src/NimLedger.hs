-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

-- Abstraction for a Ledger which is hosting the Nim domain model.
module NimLedger(Handle, connect, sendCommand, getTrans) where

import System.Random(randomIO)
import qualified Data.Text.Lazy as Text (pack)
import qualified Data.UUID as UUID

import DA.Ledger as Ledger
import Domain
import Logging
import NimCommand
import NimTrans

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
    [pid,_,_] <- return ids -- assume Nim stuff is in the 1st of 3 packages
    return Handle{log,lid,pid}

sendCommand :: Party -> Handle -> NimCommand -> IO (Maybe Rejection)
sendCommand asParty h@Handle{pid} xcom = do
    let com = makeLedgerCommands pid xcom
    submitCommand h asParty com >>= \case
        Left rejection -> return $ Just rejection
        Right () -> return Nothing

getTrans :: Player -> Handle -> IO (PastAndFuture NimTrans)
getTrans player Handle{log,lid} = do
    let party = partyOfPlayer player
    pf <- run 6000 $ getTransactionsPF lid party
    mapListPF (extractTransaction log) pf

submitCommand :: Handle -> Party -> Command -> IO (Either String ())
submitCommand Handle{lid} party com = do
    cid <- randomCid
    run 5 (Ledger.submit (Commands {lid,wid,aid=myAid,cid,party,leTime,mrTime,coms=[com]}))
    where
        wid = Nothing
        leTime = Timestamp 0 0
        mrTime = Timestamp 5 0
        myAid = ApplicationId "nim-console"

randomCid :: IO CommandId
randomCid = do fmap (CommandId . Text.pack . UUID.toString) randomIO
