-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}

module DA.Ledger.Services.TransactionService (
    getTransactions,
    getTransactionTrees,
    getTransactionByEventId,
    getTransactionById,
    getFlatTransactionByEventId,
    getFlatTransactionById,
    ledgerEnd,
    GetTransactionsRequest(..), filterEverthingForParty,
    ) where

import Com.Digitalasset.Ledger.Api.V1.TransactionFilter --TODO: HL mirror
import Control.Concurrent(forkIO)
import DA.Ledger.Convert
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Stream
import DA.Ledger.Types
import Network.GRPC.HighLevel.Generated
import qualified Com.Digitalasset.Ledger.Api.V1.TransactionService as LL
import qualified Data.Map as Map
import qualified Data.Vector as Vector

getTransactions :: GetTransactionsRequest -> LedgerService (Stream [Transaction])
getTransactions req =
    makeLedgerService $ \timeout config -> do
    stream <- newStream
    _ <- forkIO $
        withGRPCClient config $ \client -> do
            service <- LL.transactionServiceClient client
            let LL.TransactionService {transactionServiceGetTransactions=rpc} = service
            sendToStream timeout (lowerRequest req) f stream rpc
    return stream
    where f = raiseList raiseTransaction . LL.getTransactionsResponseTransactions

getTransactionTrees :: GetTransactionsRequest -> LedgerService (Stream [TransactionTree])
getTransactionTrees req =
    makeLedgerService $ \timeout config -> do
    stream <- newStream
    _ <- forkIO $
        withGRPCClient config $ \client -> do
            service <- LL.transactionServiceClient client
            let LL.TransactionService {transactionServiceGetTransactionTrees=rpc} = service
            sendToStream timeout (lowerRequest req) f stream rpc
    return stream
    where f = raiseList raiseTransactionTree . LL.getTransactionTreesResponseTransactions

getTransactionByEventId :: LedgerId -> EventId -> [Party] -> LedgerService (Maybe TransactionTree)
getTransactionByEventId lid eid parties =
    makeLedgerService $ \timeout config -> do
    withGRPCClient config $ \client -> do
        service <- LL.transactionServiceClient client
        let LL.TransactionService{transactionServiceGetTransactionByEventId=rpc} = service
        rpc (ClientNormalRequest (mkByEventIdRequest lid eid parties) timeout emptyMdm)
        >>= \case
            ClientNormalResponse (LL.GetTransactionResponse Nothing) _m1 _m2 _status _details ->
                fail "GetTransactionResponse, transaction field is missing"
            ClientNormalResponse (LL.GetTransactionResponse (Just tx)) _m1 _m2 _status _details ->
                case raiseTransactionTree tx of
                    Left reason -> fail (show reason)
                    Right x -> return $ Just x
            ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode StatusNotFound _details)) ->
                return Nothing
            ClientErrorResponse e ->
                fail (show e)

getTransactionById :: LedgerId -> TransactionId -> [Party] -> LedgerService (Maybe TransactionTree)
getTransactionById lid trid parties =
    makeLedgerService $ \timeout config -> do
    withGRPCClient config $ \client -> do
        service <- LL.transactionServiceClient client
        let LL.TransactionService{transactionServiceGetTransactionById=rpc} = service
        rpc (ClientNormalRequest (mkByIdRequest lid trid parties) timeout emptyMdm)
        >>= \case
            ClientNormalResponse (LL.GetTransactionResponse Nothing) _m1 _m2 _status _details ->
                fail "GetTransactionResponse, transaction field is missing"
            ClientNormalResponse (LL.GetTransactionResponse (Just tx)) _m1 _m2 _status _details ->
                case raiseTransactionTree tx of
                    Left reason -> fail (show reason)
                    Right x -> return $ Just x
            ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode StatusNotFound _details)) ->
                return Nothing
            ClientErrorResponse e ->
                fail (show e)

getFlatTransactionByEventId :: LedgerId -> EventId -> [Party] -> LedgerService (Maybe Transaction)
getFlatTransactionByEventId lid eid parties =
    makeLedgerService $ \timeout config -> do
    withGRPCClient config $ \client -> do
        service <- LL.transactionServiceClient client
        let LL.TransactionService{transactionServiceGetFlatTransactionByEventId=rpc} = service
        rpc (ClientNormalRequest (mkByEventIdRequest lid eid parties) timeout emptyMdm)
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

getFlatTransactionById :: LedgerId -> TransactionId -> [Party] -> LedgerService (Maybe Transaction)
getFlatTransactionById lid trid parties =
    makeLedgerService $ \timeout config -> do
    withGRPCClient config $ \client -> do
        service <- LL.transactionServiceClient client
        let LL.TransactionService{transactionServiceGetFlatTransactionById=rpc} = service
        rpc (ClientNormalRequest (mkByIdRequest lid trid parties) timeout emptyMdm)
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
    verbose :: Verbosity
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
        getTransactionsRequestVerbose = unVerbosity verbose,
        getTransactionsRequestTraceContext = noTrace
        }

mkByEventIdRequest :: LedgerId -> EventId -> [Party] -> LL.GetTransactionByEventIdRequest
mkByEventIdRequest lid eid parties =
    LL.GetTransactionByEventIdRequest
    (unLedgerId lid)
    (unEventId eid)
    (Vector.fromList $ map unParty parties)
    noTrace

mkByIdRequest :: LedgerId -> TransactionId -> [Party] -> LL.GetTransactionByIdRequest
mkByIdRequest lid trid parties =
    LL.GetTransactionByIdRequest
    (unLedgerId lid)
    (unTransactionId trid)
    (Vector.fromList $ map unParty parties)
    noTrace
