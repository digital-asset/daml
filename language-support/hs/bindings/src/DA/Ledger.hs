-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Ledger ( -- High level interface to the Ledger API

    Port(..), Host(..), ClientConfig(..),

    module DA.Ledger.LedgerService,
    module DA.Ledger.PastAndFuture,
    module DA.Ledger.Services,
    module DA.Ledger.Stream,
    module DA.Ledger.Types,

    configOfPort, getAllTransactions, getTransactionsPF,
    getAllTransactionTrees,

    ) where

import Network.GRPC.HighLevel.Generated(Port(..),Host(..),ClientConfig(..))
import DA.Ledger.LedgerService
import DA.Ledger.PastAndFuture
import DA.Ledger.Services
import DA.Ledger.Stream
import DA.Ledger.Types

import Control.Monad.IO.Class(liftIO)

configOfPort :: Port -> ClientConfig
configOfPort port =
    ClientConfig { clientServerHost = Host "localhost"
                 , clientServerPort = port
                 , clientArgs = []
                 , clientSSLConfig = Nothing
                 }

-- Non-primitive, but useful way to get Transactions
-- TODO: move to separate Utils module?

getAllTransactions :: LedgerId -> Party -> Verbosity -> LedgerService (Stream [Transaction])
getAllTransactions lid party verbose = do
    let filter = filterEverthingForParty party
    let req = GetTransactionsRequest lid LedgerBegin Nothing filter verbose
    getTransactions req

getAllTransactionTrees :: LedgerId -> Party -> Verbosity -> LedgerService (Stream [TransactionTree])
getAllTransactionTrees lid party verbose = do
    let filter = filterEverthingForParty party
    let req = GetTransactionsRequest lid LedgerBegin Nothing filter verbose
    getTransactionTrees req

getTransactionsPF :: LedgerId -> Party -> LedgerService (PastAndFuture [Transaction])
getTransactionsPF lid party = do
    now <- fmap LedgerAbsOffset (ledgerEnd lid)
    let filter = filterEverthingForParty party
    let verbose = Verbosity False
    let req1 = GetTransactionsRequest lid LedgerBegin (Just now) filter verbose
    let req2 = GetTransactionsRequest lid now         Nothing    filter verbose
    stream <- getTransactions req1
    future <- getTransactions req2
    past <- liftIO $ streamToList stream
    return PastAndFuture { past, future }
