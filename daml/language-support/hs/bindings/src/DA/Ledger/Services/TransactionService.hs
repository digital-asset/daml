-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    GetTransactionsRequest(..), filterEverythingForParty,
    ) where

import Com.Daml.Ledger.Api.V1.TransactionFilter --TODO: HL mirror
import DA.Ledger.Convert
import DA.Ledger.GrpcWrapUtils
import DA.Ledger.LedgerService
import DA.Ledger.Stream
import DA.Ledger.Types
import Network.GRPC.HighLevel.Generated
import qualified Com.Daml.Ledger.Api.V1.TransactionService as LL
import qualified Data.Map as Map
import qualified Data.Vector as Vector

getTransactions :: GetTransactionsRequest -> LedgerService (Stream [Transaction])
getTransactions req =
    makeLedgerService $ \timeout config mdm -> do
    asyncStreamGen $ \stream ->
        withGRPCClient config $ \client -> do
            service <- LL.transactionServiceClient client
            let LL.TransactionService {transactionServiceGetTransactions=rpc} = service
            sendToStream timeout mdm (lowerRequest req) f stream rpc
    where f = raiseList raiseTransaction . LL.getTransactionsResponseTransactions

getTransactionTrees :: GetTransactionsRequest -> LedgerService (Stream [TransactionTree])
getTransactionTrees req =
    makeLedgerService $ \timeout config mdm -> do
    asyncStreamGen $ \stream ->
        withGRPCClient config $ \client -> do
            service <- LL.transactionServiceClient client
            let LL.TransactionService {transactionServiceGetTransactionTrees=rpc} = service
            sendToStream timeout mdm (lowerRequest req) f stream rpc
    where f = raiseList raiseTransactionTree . LL.getTransactionTreesResponseTransactions

getTransactionByEventId :: LedgerId -> EventId -> [Party] -> LedgerService (Maybe TransactionTree)
getTransactionByEventId lid eid parties =
    makeLedgerService $ \timeout config mdm -> do
    withGRPCClient config $ \client -> do
        service <- LL.transactionServiceClient client
        let LL.TransactionService{transactionServiceGetTransactionByEventId=rpc} = service
        rpc (ClientNormalRequest (mkByEventIdRequest lid eid parties) timeout mdm)
            >>= unwrapWithTransactionFailures
            >>= \case
            Right (LL.GetTransactionResponse Nothing) ->
                fail "GetTransactionResponse, transaction field is missing"
            Right (LL.GetTransactionResponse (Just tx)) ->
                case raiseTransactionTree tx of
                    Left reason -> fail (show reason)
                    Right x -> return $ Just x
            Left _err ->
                return Nothing

getTransactionById :: LedgerId -> TransactionId -> [Party] -> LedgerService (Maybe TransactionTree)
getTransactionById lid trid parties =
    makeLedgerService $ \timeout config mdm -> do
    withGRPCClient config $ \client -> do
        service <- LL.transactionServiceClient client
        let LL.TransactionService{transactionServiceGetTransactionById=rpc} = service
        rpc (ClientNormalRequest (mkByIdRequest lid trid parties) timeout mdm)
            >>= unwrapWithTransactionFailures
            >>= \case
            Right (LL.GetTransactionResponse Nothing) ->
                fail "GetTransactionResponse, transaction field is missing"
            Right (LL.GetTransactionResponse (Just tx)) ->
                case raiseTransactionTree tx of
                    Left reason -> fail (show reason)
                    Right x -> return $ Just x
            Left _err ->
                return Nothing

getFlatTransactionByEventId :: LedgerId -> EventId -> [Party] -> LedgerService (Maybe Transaction)
getFlatTransactionByEventId lid eid parties =
    makeLedgerService $ \timeout config mdm -> do
    withGRPCClient config $ \client -> do
        service <- LL.transactionServiceClient client
        let LL.TransactionService{transactionServiceGetFlatTransactionByEventId=rpc} = service
        rpc (ClientNormalRequest (mkByEventIdRequest lid eid parties) timeout mdm)
            >>= unwrapWithTransactionFailures
            >>= \case
            Right (LL.GetFlatTransactionResponse Nothing) ->
                fail "GetFlatTransactionResponse, transaction field is missing"
            Right (LL.GetFlatTransactionResponse (Just tx)) ->
                case raiseTransaction tx of
                    Left reason -> fail (show reason)
                    Right x -> return $ Just x
            Left _err ->
                return Nothing

getFlatTransactionById :: LedgerId -> TransactionId -> [Party] -> LedgerService (Maybe Transaction)
getFlatTransactionById lid trid parties =
    makeLedgerService $ \timeout config mdm -> do
    withGRPCClient config $ \client -> do
        service <- LL.transactionServiceClient client
        let LL.TransactionService{transactionServiceGetFlatTransactionById=rpc} = service
        rpc (ClientNormalRequest (mkByIdRequest lid trid parties) timeout mdm)
            >>= unwrapWithTransactionFailures
            >>= \case
            Right (LL.GetFlatTransactionResponse Nothing) ->
                fail "GetFlatTransactionResponse, transaction field is missing"
            Right (LL.GetFlatTransactionResponse (Just tx)) ->
                case raiseTransaction tx of
                    Left reason -> fail (show reason)
                    Right x -> return $ Just x
            Left _err ->
                return Nothing

ledgerEnd :: LedgerId -> LedgerService AbsOffset
ledgerEnd lid =
    makeLedgerService $ \timeout config mdm -> do
    withGRPCClient config $ \client -> do
        service <- LL.transactionServiceClient client
        let LL.TransactionService{transactionServiceGetLedgerEnd=rpc} = service
        let request = LL.GetLedgerEndRequest (unLedgerId lid)
        rpc (ClientNormalRequest request timeout mdm)
            >>= unwrap
            >>= \case
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

filterEverythingForParty :: Party -> TransactionFilter
filterEverythingForParty party = TransactionFilter (Map.singleton (unParty party) (Just noFilters))
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
        getTransactionsRequestVerbose = unVerbosity verbose
        }

mkByEventIdRequest :: LedgerId -> EventId -> [Party] -> LL.GetTransactionByEventIdRequest
mkByEventIdRequest lid eid parties =
    LL.GetTransactionByEventIdRequest
    (unLedgerId lid)
    (unEventId eid)
    (Vector.fromList $ map unParty parties)

mkByIdRequest :: LedgerId -> TransactionId -> [Party] -> LL.GetTransactionByIdRequest
mkByIdRequest lid trid parties =
    LL.GetTransactionByIdRequest
    (unLedgerId lid)
    (unTransactionId trid)
    (Vector.fromList $ map unParty parties)
