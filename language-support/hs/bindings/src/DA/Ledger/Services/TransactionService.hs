-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE GADTs #-}
{-# LANGUAGE DataKinds #-}

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

getTransactionBy :: (LL.TransactionService HL.ClientRequest HL.ClientResult
                      -> HL.ClientRequest 'HL.Normal request0 response0
                      -> IO (HL.ClientResult streamType0 response1))
                    -> (LedgerId -> tx_ref -> [Party] -> request0)
                    -> (response1 -> Convert.Perhaps tx)
                    -> LedgerId
                    -> tx_ref
                    -> [Party]
                    -> LedgerService (Maybe tx)
getTransactionBy get_rpc mk_request resp_handler ledger_id transaction_ref parties
    = makeLedgerService $ \timeout config -> do
      HL.withGRPCClient config $ \client -> do
          service <- LL.transactionServiceClient client
          let rpc = get_rpc service
          rpc (HL.ClientNormalRequest (mk_request ledger_id transaction_ref parties) timeout GRPC.emptyMdm)
          >>= \case
              HL.ClientNormalResponse resp _m1 _m2 _status _details -> case resp_handler resp of
                                                                         Left reason -> fail (show reason)
                                                                         Right x -> return $ Just x
              HL.ClientErrorResponse (HL.ClientIOError (HL.GRPCIOBadStatusCode HL.StatusNotFound _details)) ->
                  return Nothing
              HL.ClientErrorResponse e ->
                  fail (show e)
              _ -> fail "I don't know how to express that request0 is either LL.GetTransactionByIdRequest or LL.GetTransactionByEventIdRequest"

extractTxResponse :: LL.GetTransactionResponse -> Convert.Perhaps TransactionTree
extractTxResponse (LL.GetTransactionResponse Nothing) = fail "GetTransactionResponse, transaction field is missing"
extractTxResponse (LL.GetTransactionResponse (Just tx)) = Convert.raiseTransactionTree tx

extractTxFlatResponse :: LL.GetFlatTransactionResponse -> Convert.Perhaps Transaction
extractTxFlatResponse (LL.GetFlatTransactionResponse Nothing) = fail "GetFlatTransactionResponse, transaction field is missing"
extractTxFlatResponse (LL.GetFlatTransactionResponse (Just txTree)) = Convert.raiseTransaction txTree

getTransactionByEventId :: LedgerId -> EventId -> [Party] -> LedgerService (Maybe TransactionTree)
getTransactionByEventId = getTransactionBy LL.transactionServiceGetTransactionByEventId
                                           mkByEventIdRequest
                                           extractTxResponse

getTransactionById :: LedgerId -> TransactionId -> [Party] -> LedgerService (Maybe TransactionTree)
getTransactionById = getTransactionBy LL.transactionServiceGetTransactionById
                                      mkByIdRequest
                                      extractTxResponse

getFlatTransactionByEventId :: LedgerId -> EventId -> [Party] -> LedgerService (Maybe Transaction)
getFlatTransactionByEventId = getTransactionBy LL.transactionServiceGetFlatTransactionByEventId
                                               mkByEventIdRequest
                                               extractTxFlatResponse

getFlatTransactionById :: LedgerId -> TransactionId -> [Party] -> LedgerService (Maybe Transaction)
getFlatTransactionById = getTransactionBy LL.transactionServiceGetFlatTransactionById
                                          mkByIdRequest
                                          extractTxFlatResponse

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
