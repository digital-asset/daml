-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Ledger.Services.TransactionService (
    ledgerEnd,
    GetTransactionsRequest(..), filterEverthingForParty, getTransactions,
    getFlatTransactionByEventId,
    getFlatTransactionById,
    ) where

import Com.Digitalasset.Ledger.Api.V1.TransactionFilter
import Control.Concurrent(forkIO)
import DA.Ledger.Convert
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Stream
import DA.Ledger.Types
import Network.GRPC.HighLevel.Generated
import qualified Com.Digitalasset.Ledger.Api.V1.TransactionService as LL
import qualified Data.Map as Map

-- TODO:: all the other RPCs

ledgerEnd :: LedgerId -> LedgerService AbsOffset
ledgerEnd lid =
    makeLedgerService $ \timeout config -> do
    withGRPCClient config $ \client -> do
        service <- LL.transactionServiceClient client
        let LL.TransactionService{transactionServiceGetLedgerEnd=rpc} = service
        let request = LL.GetLedgerEndRequest (unLedgerId lid) noTrace
        response <- rpc (ClientNormalRequest request timeout emptyMdm)
        unwrap response >>= \case
            LL.GetLedgerEndResponse (Just offset) ->
                case raiseAbsLedgerOffset offset of
                    Left reason -> fail (show reason)
                    Right abs -> return abs
            LL.GetLedgerEndResponse Nothing ->
                fail "GetLedgerEndResponse, offset field is missing"

data GetTransactionsRequest = GetTransactionsRequest {
    lid :: LedgerId,
    begin :: LedgerOffset,
    end :: Maybe LedgerOffset,
    filter :: TransactionFilter,
    verbose :: Bool
    }

filterEverthingForParty :: Party -> TransactionFilter
filterEverthingForParty party = TransactionFilter (Map.singleton (unParty party) (Just noFilters))
    where
        noFilters :: Filters
        noFilters = Filters Nothing

lowerRequest :: GetTransactionsRequest -> LL.GetTransactionsRequest
lowerRequest = \case
    GetTransactionsRequest{lid, begin, end, filter, verbose} ->
        LL.GetTransactionsRequest {
        getTransactionsRequestLedgerId = unLedgerId lid,
        getTransactionsRequestBegin = Just (lowerLedgerOffset begin),
        getTransactionsRequestEnd = fmap lowerLedgerOffset end,
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
            sendToStreamFlat timeout request f stream rpc
    return stream
    where
        f = raiseList raiseTransaction . LL.getTransactionsResponseTransactions

getFlatTransactionByEventId :: LedgerId -> EventId -> [Party] -> LedgerService (Maybe Transaction)
getFlatTransactionByEventId lid eid parties =
    makeLedgerService $ \timeout config -> do
    withGRPCClient config $ \client -> do
        service <- LL.transactionServiceClient client
        let LL.TransactionService{transactionServiceGetFlatTransactionByEventId=rpc} = service
        rpc (ClientNormalRequest (mkRequest lid eid parties) timeout emptyMdm)
        >>= \case
            ClientNormalResponse (LL.GetFlatTransactionResponse Nothing) _m1 _m2 _status _details ->
                fail "GetFlatTransactionResponse, transaction field is missing"
            ClientNormalResponse (LL.GetFlatTransactionResponse (Just tx)) _m1 _m2 _status _details ->
                case raiseTransaction tx of
                    Left reason -> fail (show reason)
                    Right x -> return $ Just x
            ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode StatusNotFound _details)) ->
                return Nothing
            ClientErrorResponse e ->
                fail (show e)
    where
    mkRequest :: LedgerId -> EventId -> [Party] -> LL.GetTransactionByEventIdRequest
    mkRequest lid eid parties =
        LL.GetTransactionByEventIdRequest
        (unLedgerId lid)
        (unEventId eid)
        (lowerList unParty parties)
        noTrace

getFlatTransactionById :: LedgerId -> TransactionId -> [Party] -> LedgerService (Maybe Transaction)
getFlatTransactionById lid trid parties =
    makeLedgerService $ \timeout config -> do
    withGRPCClient config $ \client -> do
        service <- LL.transactionServiceClient client
        let LL.TransactionService{transactionServiceGetFlatTransactionById=rpc} = service
        rpc (ClientNormalRequest (mkRequest lid trid parties) timeout emptyMdm)
        >>= \case
            ClientNormalResponse (LL.GetFlatTransactionResponse Nothing) _m1 _m2 _status _details ->
                fail "GetFlatTransactionResponse, transaction field is missing"
            ClientNormalResponse (LL.GetFlatTransactionResponse (Just tx)) _m1 _m2 _status _details ->
                case raiseTransaction tx of
                    Left reason -> fail (show reason)
                    Right x -> return $ Just x
            ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode StatusNotFound _details)) ->
                return Nothing
            ClientErrorResponse e ->
                fail (show e)
    where
    mkRequest :: LedgerId -> TransactionId -> [Party] -> LL.GetTransactionByIdRequest
    mkRequest lid trid parties =
        LL.GetTransactionByIdRequest
        (unLedgerId lid)
        (unTransactionId trid)
        (lowerList unParty parties)
        noTrace
