-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Ledger.Services.TransactionService (
    ledgerEnd,
    GetTransactionsRequest(..), getTransactions,
    ) where

import Com.Digitalasset.Ledger.Api.V1.LedgerOffset
import Com.Digitalasset.Ledger.Api.V1.TransactionFilter
import Control.Concurrent(forkIO)
import DA.Ledger.Convert(raiseTransaction,RaiseFailureReason)
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Stream
import DA.Ledger.Types
import Network.GRPC.HighLevel.Generated
import qualified Data.Vector as Vector
import qualified Com.Digitalasset.Ledger.Api.V1.TransactionService as LL

-- TODO:: all the other RPCs

ledgerEnd :: LedgerId -> LedgerService LedgerOffset
ledgerEnd lid =
    makeLedgerService $ \timeout config -> do
    withGRPCClient config $ \client -> do
        service <- LL.transactionServiceClient client
        let LL.TransactionService{transactionServiceGetLedgerEnd=rpc} = service
        let request = LL.GetLedgerEndRequest (unLedgerId lid) noTrace
        response <- rpc (ClientNormalRequest request timeout emptyMdm)
        LL.GetLedgerEndResponse (Just offset) <- unwrap response --fail if not Just
        return offset

data GetTransactionsRequest = GetTransactionsRequest {
    lid :: LedgerId,
    begin :: LedgerOffset,
    end :: Maybe LedgerOffset,
    filter :: TransactionFilter,
    verbose :: Bool
    }

lowerRequest :: GetTransactionsRequest -> LL.GetTransactionsRequest
lowerRequest = \case
    GetTransactionsRequest{lid, begin, end, filter, verbose} ->
        LL.GetTransactionsRequest {
        getTransactionsRequestLedgerId = unLedgerId lid,
        getTransactionsRequestBegin = Just begin,
        getTransactionsRequestEnd = end,
        getTransactionsRequestFilter = Just filter,
        getTransactionsRequestVerbose = verbose,
        getTransactionsRequestTraceContext = noTrace
        }

getTransactions :: GetTransactionsRequest -> LedgerService (Stream Transaction)
getTransactions tup =
    makeLedgerService $ \timeout config -> do
    stream <- newStream
    let request = lowerRequest tup
    _ <- forkIO $
        withGRPCClient config $ \client -> do
            service <- LL.transactionServiceClient client
            let LL.TransactionService {transactionServiceGetTransactions=rpc} = service
            sendToStream timeout request f stream rpc
    return stream
    where
        f = map (raise . raiseTransaction) . Vector.toList . LL.getTransactionsResponseTransactions

raise :: Either RaiseFailureReason Transaction -> Either Closed Transaction
raise x = case x of
    Left reason ->
        Left (Abnormal $ "failed to parse transaction because: " <> show reason <> ":\n" <> show x)
    Right h -> Right h
