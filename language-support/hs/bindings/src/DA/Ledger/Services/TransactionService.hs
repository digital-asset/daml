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
import qualified DA.Ledger.Convert as Convert
import qualified DA.Ledger.GrpcWrapUtils as GRPC
import DA.Ledger.LedgerService (LedgerService, makeLedgerService)
import DA.Ledger.Stream (Stream, newStream)
import DA.Ledger.Types
import qualified Network.GRPC.HighLevel.Generated as HL
import qualified Com.Digitalasset.Ledger.Api.V1.TransactionService as LL
import qualified Data.Map as Map

getTransactions :: GetTransactionsRequest -> LedgerService (Stream Transaction)
getTransactions req =
    makeLedgerService $ \timeout config -> do
    stream <- newStream
    _ <- forkIO $
        HL.withGRPCClient config $ \client -> do
            service <- LL.transactionServiceClient client
            let LL.TransactionService {transactionServiceGetTransactions=rpc} = service
            GRPC.sendToStreamFlat timeout (lowerRequest req) f stream rpc
    return stream
    where f = Convert.raiseList Convert.raiseTransaction . LL.getTransactionsResponseTransactions

getTransactionTrees :: GetTransactionsRequest -> LedgerService (Stream TransactionTree)
getTransactionTrees req =
    makeLedgerService $ \timeout config -> do
    stream <- newStream
    _ <- forkIO $
        HL.withGRPCClient config $ \client -> do
            service <- LL.transactionServiceClient client
            let LL.TransactionService {transactionServiceGetTransactionTrees=rpc} = service
            GRPC.sendToStreamFlat timeout (lowerRequest req) f stream rpc
    return stream
    where f = Convert.raiseList Convert.raiseTransactionTree . LL.getTransactionTreesResponseTransactions

getTransactionByEventId :: LedgerId -> EventId -> [Party] -> LedgerService (Maybe TransactionTree)
getTransactionByEventId lid eid parties =
    makeLedgerService $ \timeout config -> do
    HL.withGRPCClient config $ \client -> do
        service <- LL.transactionServiceClient client
        let LL.TransactionService{transactionServiceGetTransactionByEventId=rpc} = service
        rpc (HL.ClientNormalRequest (mkByEventIdRequest lid eid parties) timeout GRPC.emptyMdm)
        >>= \case
            HL.ClientNormalResponse (LL.GetTransactionResponse Nothing) _m1 _m2 _status _details ->
                fail "GetTransactionResponse, transaction field is missing"
            HL.ClientNormalResponse (LL.GetTransactionResponse (Just tx)) _m1 _m2 _status _details ->
                case Convert.raiseTransactionTree tx of
                    Left reason -> fail (show reason)
                    Right x -> return $ Just x
            HL.ClientErrorResponse (HL.ClientIOError (HL.GRPCIOBadStatusCode HL.StatusNotFound _details)) ->
                return Nothing
            HL.ClientErrorResponse e ->
                fail (show e)

getTransactionById :: LedgerId -> TransactionId -> [Party] -> LedgerService (Maybe TransactionTree)
getTransactionById lid trid parties =
    makeLedgerService $ \timeout config -> do
    HL.withGRPCClient config $ \client -> do
        service <- LL.transactionServiceClient client
        let LL.TransactionService{transactionServiceGetTransactionById=rpc} = service
        rpc (HL.ClientNormalRequest (mkByIdRequest lid trid parties) timeout GRPC.emptyMdm)
        >>= \case
            HL.ClientNormalResponse (LL.GetTransactionResponse Nothing) _m1 _m2 _status _details ->
                fail "GetTransactionResponse, transaction field is missing"
            HL.ClientNormalResponse (LL.GetTransactionResponse (Just tx)) _m1 _m2 _status _details ->
                case Convert.raiseTransactionTree tx of
                    Left reason -> fail (show reason)
                    Right x -> return $ Just x
            HL.ClientErrorResponse (HL.ClientIOError (HL.GRPCIOBadStatusCode HL.StatusNotFound _details)) ->
                return Nothing
            HL.ClientErrorResponse e ->
                fail (show e)

getFlatTransactionByEventId :: LedgerId -> EventId -> [Party] -> LedgerService (Maybe Transaction)
getFlatTransactionByEventId lid eid parties =
    makeLedgerService $ \timeout config -> do
    HL.withGRPCClient config $ \client -> do
        service <- LL.transactionServiceClient client
        let LL.TransactionService{transactionServiceGetFlatTransactionByEventId=rpc} = service
        rpc (HL.ClientNormalRequest (mkByEventIdRequest lid eid parties) timeout GRPC.emptyMdm)
        >>= \case
            HL.ClientNormalResponse (LL.GetFlatTransactionResponse Nothing) _m1 _m2 _status _details ->
                fail "GetFlatTransactionResponse, transaction field is missing"
            HL.ClientNormalResponse (LL.GetFlatTransactionResponse (Just tx)) _m1 _m2 _status _details ->
                case Convert.raiseTransaction tx of
                    Left reason -> fail (show reason)
                    Right x -> return $ Just x
            HL.ClientErrorResponse (HL.ClientIOError (HL.GRPCIOBadStatusCode HL.StatusNotFound _details)) ->
                return Nothing
            HL.ClientErrorResponse e ->
                fail (show e)

getFlatTransactionById :: LedgerId -> TransactionId -> [Party] -> LedgerService (Maybe Transaction)
getFlatTransactionById lid trid parties =
    makeLedgerService $ \timeout config -> do
    HL.withGRPCClient config $ \client -> do
        service <- LL.transactionServiceClient client
        let LL.TransactionService{transactionServiceGetFlatTransactionById=rpc} = service
        rpc (HL.ClientNormalRequest (mkByIdRequest lid trid parties) timeout GRPC.emptyMdm)
        >>= \case
            HL.ClientNormalResponse (LL.GetFlatTransactionResponse Nothing) _m1 _m2 _status _details ->
                fail "GetFlatTransactionResponse, transaction field is missing"
            HL.ClientNormalResponse (LL.GetFlatTransactionResponse (Just tx)) _m1 _m2 _status _details ->
                case Convert.raiseTransaction tx of
                    Left reason -> fail (show reason)
                    Right x -> return $ Just x
            HL.ClientErrorResponse (HL.ClientIOError (HL.GRPCIOBadStatusCode HL.StatusNotFound _details)) ->
                return Nothing
            HL.ClientErrorResponse e ->
                fail (show e)

ledgerEnd :: LedgerId -> LedgerService AbsOffset
ledgerEnd lid =
    makeLedgerService $ \timeout config -> do
    HL.withGRPCClient config $ \client -> do
        service <- LL.transactionServiceClient client
        let LL.TransactionService{transactionServiceGetLedgerEnd=rpc} = service
        let request = LL.GetLedgerEndRequest (unLedgerId lid) GRPC.noTrace
        response <- rpc (HL.ClientNormalRequest request timeout GRPC.emptyMdm)
        GRPC.unwrap response >>= \case
            LL.GetLedgerEndResponse (Just offset) ->
                case Convert.raiseAbsLedgerOffset offset of
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
        getTransactionsRequestBegin = Just (Convert.lowerLedgerOffset begin),
        getTransactionsRequestEnd = fmap Convert.lowerLedgerOffset end,
        getTransactionsRequestFilter = Just filter,
        getTransactionsRequestVerbose = verbose,
        getTransactionsRequestTraceContext = GRPC.noTrace
        }

mkByEventIdRequest :: LedgerId -> EventId -> [Party] -> LL.GetTransactionByEventIdRequest
mkByEventIdRequest lid eid parties =
    LL.GetTransactionByEventIdRequest
    (unLedgerId lid)
    (unEventId eid)
    (Convert.lowerList unParty parties)
    GRPC.noTrace

mkByIdRequest :: LedgerId -> TransactionId -> [Party] -> LL.GetTransactionByIdRequest
mkByIdRequest lid trid parties =
    LL.GetTransactionByIdRequest
    (unLedgerId lid)
    (unTransactionId trid)
    (Convert.lowerList unParty parties)
    GRPC.noTrace
